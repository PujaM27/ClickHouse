from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import clickhouse_driver # type: ignore
import pandas as pd # type: ignore
import os
import time
import jwt # type: ignore
from datetime import datetime, timedelta
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# JWT Configuration
SECRET_KEY = "your-secret-key"  # In production, use environment variable
ALGORITHM = "HS256"
security = HTTPBearer()

# ClickHouse to Python type mapping
CLICKHOUSE_TO_PYTHON = {
    'UInt8': 'int',
    'UInt16': 'int',
    'UInt32': 'int',
    'UInt64': 'int',
    'Int8': 'int',
    'Int16': 'int',
    'Int32': 'int',
    'Int64': 'int',
    'Float32': 'float',
    'Float64': 'float',
    'String': 'str',
    'Date': 'date',
    'DateTime': 'datetime',
    'Bool': 'bool'
}

# Python to ClickHouse type mapping
PYTHON_TO_CLICKHOUSE = {
    'int': 'Int64',
    'float': 'Float64',
    'str': 'String',
    'bool': 'Bool',
    'date': 'Date',
    'datetime': 'DateTime'
}

# Store progress for each transfer
transfer_progress: Dict[str, float] = {}

CLICKHOUSE_CONFIG = {
    'host': 'localhost',
    'port': 9000,
    'database': 'test_db',
    'user': 'default',
    'password': 'default'  # Default password for ClickHouse Docker
}

class ClickhouseConfig(BaseModel):
    host: str
    port: str
    database: str
    username: Optional[str] = None
    password: Optional[str] = None
    jwt_token: Optional[str] = None

class FlatFileConfig(BaseModel):
    filePath: str
    delimiter: str

class TransferRequest(BaseModel):
    source: str
    target: str
    table: str
    columns: List[str]
    config: ClickhouseConfig | FlatFileConfig
    transfer_id: str
    type_mappings: Optional[Dict[str, str]] = None

def verify_token(credentials: HTTPAuthorizationCredentials = Security(security)):
    try:
        token = credentials.credentials
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired")
    except jwt.JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

def get_clickhouse_schema(client: clickhouse_driver.Client, database: str, table: str) -> Dict[str, str]:
    query = f"""
    SELECT name, type
    FROM system.columns
    WHERE database = '{database}' AND table = '{table}'
    """
    result = client.execute(query)
    return {row[0]: row[1] for row in result}

def infer_python_type(value: Any) -> str:
    if isinstance(value, int):
        return 'int'
    elif isinstance(value, float):
        return 'float'
    elif isinstance(value, bool):
        return 'bool'
    elif isinstance(value, datetime):
        return 'datetime'
    elif isinstance(value, str):
        try:
            datetime.strptime(value, '%Y-%m-%d')
            return 'date'
        except ValueError:
            try:
                datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                return 'datetime'
            except ValueError:
                return 'str'
    return 'str'

def convert_value(value: Any, target_type: str) -> Any:
    if value is None:
        return None
    try:
        if target_type == 'int':
            return int(value)
        elif target_type == 'float':
            return float(value)
        elif target_type == 'bool':
            return bool(value)
        elif target_type == 'date':
            return datetime.strptime(value, '%Y-%m-%d').date()
        elif target_type == 'datetime':
            return datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        return str(value)
    except (ValueError, TypeError):
        return str(value)

@app.get("/preview")
async def preview_data(
    source: str,
    table: str,
    columns: str,
    token: str = Depends(verify_token)
):
    try:
        if source == "clickhouse":
            client = clickhouse_driver.Client(
                host='localhost',
                port=9000,
                database='test_data',
                jwt_token=token
            )
            
            # Get schema information
            schema = get_clickhouse_schema(client, 'test_data', table)
            
            # Get total count for progress calculation
            count_query = f"SELECT count() FROM {table}"
            total_count = client.execute(count_query)[0][0]
            
            # Get sample data
            query = f"SELECT {columns} FROM {table} LIMIT 100"
            result = client.execute(query)
            columns_list = columns.split(',')
            
            # Convert data types
            converted_data = []
            for row in result:
                converted_row = {}
                for col, val in zip(columns_list, row):
                    clickhouse_type = schema.get(col, 'String')
                    python_type = CLICKHOUSE_TO_PYTHON.get(clickhouse_type, 'str')
                    converted_row[col] = convert_value(val, python_type)
                converted_data.append(converted_row)
            
            return {
                "data": converted_data,
                "total_count": total_count,
                "columns": columns_list,
                "schema": {col: schema.get(col, 'String') for col in columns_list}
            }
        else:
            raise HTTPException(status_code=400, detail="Preview only supported for ClickHouse source")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/progress/{transfer_id}")
async def get_progress(transfer_id: str):
    return {"progress": transfer_progress.get(transfer_id, 0)}

async def update_progress(transfer_id: str, progress: float):
    transfer_progress[transfer_id] = progress

@app.post("/transfer")
async def transfer_data(
    source: str,
    target: str,
    table: str,
    columns: List[str],
    config: Dict[str, Any],
    transfer_id: str,
    join_conditions: Optional[List[Dict[str, str]]] = None,
    token: str = Depends(verify_token)
):
    try:
        if source == "clickhouse":
            # Handle JOIN query if join_conditions are provided
            if join_conditions and len(join_conditions) > 0:
                # Construct JOIN query
                join_clause = " ".join([
                    f"JOIN {cond['right_table']} ON {cond['left_table']}.{cond['left_key']} = {cond['right_table']}.{cond['right_key']}"
                    for cond in join_conditions
                ])
                query = f"SELECT {', '.join(columns)} FROM {table} {join_clause}"
            else:
                query = f"SELECT {', '.join(columns)} FROM {table}"
            
            client = clickhouse_driver.Client(
                host='localhost',
                port=9000,
                database='test_data',
                jwt_token=token
            )
            
            # Get schema information
            schema = get_clickhouse_schema(client, 'test_data', table)
            
            # Get total count for progress calculation
            count_query = f"SELECT count() FROM {table}"
            total_count = client.execute(count_query)[0][0]
            
            # Get sample data
            result = client.execute(query)
            columns_list = columns
            
            # Convert data types
            converted_data = []
            for row in result:
                converted_row = {}
                for col, val in zip(columns_list, row):
                    clickhouse_type = schema.get(col, 'String')
                    python_type = CLICKHOUSE_TO_PYTHON.get(clickhouse_type, 'str')
                    converted_row[col] = convert_value(val, python_type)
                converted_data.append(converted_row)
            
            return {
                "data": converted_data,
                "total_count": total_count,
                "columns": columns_list,
                "schema": {col: schema.get(col, 'String') for col in columns_list}
            }
            
        elif source == "flatfile" and target == "clickhouse":
            if not os.path.exists(config.filePath):
                raise HTTPException(status_code=400, detail="File not found")
                
            df = pd.read_csv(config.filePath, sep=config.delimiter)
            total_rows = len(df)
            
            client = clickhouse_driver.Client(
                host=config.host,
                port=config.port,
                database=config.database,
                user=config.username,
                password=config.password,
                jwt_token=token
            )
            
            # Infer types from data
            type_mappings = {}
            for col in columns:
                sample = df[col].dropna().iloc[0] if not df[col].empty else None
                if sample is not None:
                    python_type = infer_python_type(sample)
                    clickhouse_type = PYTHON_TO_CLICKHOUSE.get(python_type, 'String')
                    type_mappings[col] = clickhouse_type
                else:
                    type_mappings[col] = 'String'
            
            # Create table with inferred types
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table} (
                {', '.join([f'{col} {type_mappings[col]}' for col in columns])}
            ) ENGINE = MergeTree()
            ORDER BY tuple()
            """
            client.execute(create_table_query)
            
            # Insert data in chunks
            chunk_size = 1000
            for i in range(0, total_rows, chunk_size):
                chunk = df.iloc[i:i+chunk_size]
                # Convert data types
                converted_data = []
                for _, row in chunk.iterrows():
                    converted_row = []
                    for col in columns:
                        clickhouse_type = type_mappings[col]
                        python_type = CLICKHOUSE_TO_PYTHON.get(clickhouse_type, 'str')
                        converted_row.append(convert_value(row[col], python_type))
                    converted_data.append(tuple(converted_row))
                
                client.execute(f"INSERT INTO {table} ({', '.join(columns)}) VALUES", converted_data)
                
                progress = ((i + len(chunk)) / total_rows) * 100
                await update_progress(transfer_id, progress)
            
            return {"status": "success", "records_processed": total_rows}
            
        else:
            raise HTTPException(status_code=400, detail="Unsupported source/target combination")
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if transfer_id in transfer_progress:
            del transfer_progress[transfer_id] 

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

@app.post("/api/connect/clickhouse")
async def connect_clickhouse(config: ClickhouseConfig):
    try:
        client = clickhouse_driver.Client(
            host=config.host,
            port=int(config.port),
            database=config.database,
            user=config.username or 'default',
            password=config.password or 'default'
        )
        
        # Test connection
        client.execute('SELECT 1')
        
        return {"status": "connected", "message": "Successfully connected to ClickHouse"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/clickhouse/tables")
async def get_tables():
    try:
        client = clickhouse_driver.Client(**CLICKHOUSE_CONFIG)
        tables = client.execute('SHOW TABLES')
        return [table[0] for table in tables]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 