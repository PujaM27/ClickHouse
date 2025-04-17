from fastapi import FastAPI, HTTPException, Depends, UploadFile, File, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from typing import Optional, List, Dict, Any
import pandas as pd
from clickhouse_driver import Client
import os
from pydantic import BaseModel
import jwt
from datetime import datetime, timedelta
import json
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import tempfile
import csv
import asyncio
from typing import Generator

app = FastAPI()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# JWT Settings
SECRET_KEY = "your-secret-key"  # In production, use a secure secret key
ALGORITHM = "HS256"
security = HTTPBearer()

# ClickHouse connection settings
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

# Models
class TokenRequest(BaseModel):
    username: str
    password: str

class ClickHouseConfig(BaseModel):
    host: str
    port: int
    database: str
    user: str
    password: str
    jwtToken: Optional[str] = None

class FlatFileConfig(BaseModel):
    file_path: str
    delimiter: str = ','
    encoding: str = 'utf-8'

class ColumnSelection(BaseModel):
    columns: list[str]

class JoinCondition(BaseModel):
    leftTable: str
    rightTable: str
    leftKey: str
    rightKey: str

class DataTransferRequest(BaseModel):
    source: str
    target: str
    tables: List[str]
    columns: List[str]
    joinConditions: Optional[List[JoinCondition]] = None

class LoginRequest(BaseModel):
    username: str
    password: str

# Type mapping between ClickHouse and Python
CLICKHOUSE_TO_PYTHON_TYPES = {
    'UInt8': int,
    'UInt16': int,
    'UInt32': int,
    'UInt64': int,
    'Int8': int,
    'Int16': int,
    'Int32': int,
    'Int64': int,
    'Float32': float,
    'Float64': float,
    'String': str,
    'Date': str,
    'DateTime': str,
    'Array': list,
    'Nullable': lambda x: x
}

def get_clickhouse_client(config: Optional[ClickHouseConfig] = None):
    try:
        if config:
            client = Client(
                host=config.host,
                port=config.port,
                database=config.database,
                user=config.user,
                password=config.jwtToken or config.password
            )
        else:
            client = Client(
                host=CLICKHOUSE_HOST,
                port=CLICKHOUSE_PORT,
                user=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD,
                database='default'
            )
        # Test connection
        client.execute('SELECT 1')
        return client
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to connect to ClickHouse: {str(e)}")

def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        token = credentials.credentials
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired")
    except jwt.JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

def check_type_compatibility(value, ch_type):
    """Check if a value is compatible with ClickHouse type"""
    try:
        if value is None:
            return True, None
        
        # Handle Nullable types
        if ch_type.startswith('Nullable('):
            base_type = ch_type[9:-1]
            return check_type_compatibility(value, base_type)
        
        # Handle Array types
        if ch_type.startswith('Array('):
            base_type = ch_type[6:-1]
            if not isinstance(value, list):
                return False, f"Expected array, got {type(value)}"
            for item in value:
                compatible, warning = check_type_compatibility(item, base_type)
                if not compatible:
                    return False, warning
            return True, None
        
        # Get the Python type converter
        python_type = CLICKHOUSE_TO_PYTHON_TYPES.get(ch_type, str)
        try:
            python_type(value)
            return True, None
        except (ValueError, TypeError) as e:
            return False, f"Cannot convert {value} to {ch_type}: {str(e)}"
    except Exception as e:
        return False, f"Type checking error: {str(e)}"

async def stream_data(client: Client, query: str, batch_size: int = 1000) -> Generator[List[Dict], None, None]:
    """Stream data from ClickHouse in batches"""
    offset = 0
    while True:
        batch_query = f"{query} LIMIT {batch_size} OFFSET {offset}"
        batch = client.execute(batch_query)
        if not batch:
            break
        yield batch
        offset += batch_size

# Routes
@app.get("/")
async def root():
    return {"message": "Data Ingestion API"}

@app.post("/connect/clickhouse")
async def connect_clickhouse(config: ClickHouseConfig, credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        client = get_clickhouse_client(config)
        return {"status": "success", "message": "Connected successfully"}
    except Exception as e:
        return JSONResponse(
            status_code=400,
            content={"status": "error", "message": str(e)}
        )

@app.get("/clickhouse/tables")
async def get_tables(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        client = get_clickhouse_client()
        result = client.execute("SHOW TABLES")
        return {"tables": [row[0] for row in result]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/clickhouse/columns")
async def get_columns(table: str, credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        client = get_clickhouse_client()
        result = client.execute(f"DESCRIBE TABLE {table}")
        return {"columns": [{"name": row[0], "type": row[1]} for row in result]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/clickhouse/preview")
async def preview_clickhouse(
    table: str,
    columns: List[str],
    limit: int = 100,
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    try:
        client = get_clickhouse_client()
        columns_str = ', '.join(columns)
        result = client.execute(f"SELECT {columns_str} FROM {table} LIMIT {limit}")
        
        # Get column types
        column_types = client.execute(f"DESCRIBE TABLE {table}")
        type_map = {col[0]: col[1] for col in column_types}
        
        # Check type compatibility
        type_warnings = []
        for row in result:
            for i, value in enumerate(row):
                col_name = columns[i]
                col_type = type_map.get(col_name)
                if col_type:
                    compatible, warning = check_type_compatibility(value, col_type)
                    if not compatible and warning:
                        type_warnings.append(warning)
        
        return {
            "data": result,
            "columns": columns,
            "typeWarnings": type_warnings
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/flatfile/preview")
async def preview_flatfile(
    file: UploadFile = File(...),
    delimiter: str = ','
):
    try:
        # Read the first 100 rows
        df = pd.read_csv(file.file, delimiter=delimiter, nrows=100)
        return {
            "columns": df.columns.tolist(),
            "data": df.to_dict('records')
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/ingest/ch-to-file")
async def clickhouse_to_file(
    table: str,
    columns: List[str],
    config: ClickHouseConfig,
    joinConfig: Optional[Dict] = None,
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    try:
        client = get_clickhouse_client(config)
        columns_str = ', '.join(columns)
        
        # Build query based on join config
        if joinConfig and len(joinConfig.get('tables', [])) > 1:
            query = build_join_query(joinConfig, columns)
        else:
            query = f"SELECT {columns_str} FROM {table}"
        
        # Create a temporary file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as tmp_file:
            writer = csv.writer(tmp_file)
            writer.writerow(columns)
            
            total_rows = 0
            async for batch in stream_data(client, query):
                writer.writerows(batch)
                total_rows += len(batch)
        
        return FileResponse(
            tmp_file.name,
            media_type='text/csv',
            filename=f"{table}_export.csv",
            headers={"X-Record-Count": str(total_rows)}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/ingest/file-to-ch")
async def file_to_clickhouse(
    table: str,
    file: UploadFile = File(...),
    delimiter: str = ',',
    config: ClickHouseConfig = None,
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    try:
        client = get_clickhouse_client(config)
        
        # Read the file in chunks
        df = pd.read_csv(file.file, delimiter=delimiter, chunksize=1000)
        
        # Get column types from ClickHouse
        column_types = client.execute(f'DESCRIBE TABLE {table}')
        type_map = {col[0]: col[1] for col in column_types}
        
        total_rows = 0
        type_warnings = []
        
        for chunk in df:
            # Check type compatibility
            for col in chunk.columns:
                if col in type_map:
                    for value in chunk[col]:
                        compatible, warning = check_type_compatibility(value, type_map[col])
                        if not compatible and warning:
                            type_warnings.append(warning)
            
            # Prepare data for insertion
            data = [tuple(row) for row in chunk.itertuples(index=False)]
            client.execute(
                f'INSERT INTO {table} ({", ".join(chunk.columns)}) VALUES',
                data
            )
            total_rows += len(data)
        
        return {
            "status": "success",
            "message": f"Successfully imported {total_rows} rows",
            "records_processed": total_rows,
            "typeWarnings": type_warnings
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def build_join_query(joinConfig: Dict, columns: List[str]) -> str:
    """Build SQL query for joining multiple tables"""
    join_type = joinConfig.get('joinType', 'INNER')
    tables = joinConfig.get('tables', [])
    
    if len(tables) < 2:
        raise HTTPException(status_code=400, detail="At least two tables required for join")
    
    # Start with the first table
    query = f"SELECT {', '.join(columns)} FROM {tables[0].get('table')}"
    
    # Add joins
    for i in range(1, len(tables)):
        prev_table = tables[i-1]
        curr_table = tables[i]
        query += f" {join_type} JOIN {curr_table.get('table')} ON "
        query += f"{prev_table.get('table')}.{prev_table.get('key')} = "
        query += f"{curr_table.get('table')}.{curr_table.get('key')}"
    
    return query

@app.post("/api/login")
async def login(login_request: LoginRequest):
    if login_request.username == "admin" and login_request.password == "admin123":
        # Create token
        access_token_expires = timedelta(minutes=30)
        access_token = jwt.encode(
            {
                "sub": login_request.username,
                "exp": datetime.utcnow() + access_token_expires
            },
            SECRET_KEY,
            algorithm=ALGORITHM
        )
        return {"token": access_token}
    else:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password"
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001) 