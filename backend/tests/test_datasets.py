import pytest
import os
import requests
from clickhouse_driver import Client
import pandas as pd
import time
from main import app, get_clickhouse_client
from fastapi.testclient import TestClient
import jwt
from datetime import datetime, timedelta
import tempfile
import csv

# Test client setup
client = TestClient(app)

# ClickHouse test client
clickhouse_client = Client(
    host='clickhouse',
    port=9000,
    user='default',
    password='',
    database='default'
)

# JWT token for testing
SECRET_KEY = "your-secret-key"
ALGORITHM = "HS256"
test_token = jwt.encode(
    {"sub": "test", "exp": datetime.utcnow() + timedelta(minutes=30)},
    SECRET_KEY,
    algorithm=ALGORITHM
)

# Test datasets
TEST_DATASETS = {
    'uk_price_paid': {
        'columns': ['price', 'date', 'postcode', 'property_type', 'new_build', 'duration', 'paon', 'saon', 'street', 'locality', 'town', 'district', 'county', 'ppd_category', 'record_status'],
        'sample_data': [
            (100000, '2023-01-01', 'SW1A 1AA', 'D', 'N', 'F', '1', '', 'Downing Street', '', 'London', 'Westminster', 'Greater London', 'A', 'A'),
            (200000, '2023-01-02', 'SW1A 2AA', 'S', 'Y', 'L', '2', '', 'Whitehall', '', 'London', 'Westminster', 'Greater London', 'A', 'A')
        ]
    },
    'ontime': {
        'columns': ['Year', 'Quarter', 'Month', 'DayofMonth', 'DayOfWeek', 'FlightDate', 'UniqueCarrier', 'FlightNum', 'Origin', 'Dest', 'CRSDepTime', 'DepTime', 'DepDelay', 'ArrTime', 'ArrDelay', 'Cancelled', 'Diverted'],
        'sample_data': [
            (2023, 1, 1, 1, 1, '2023-01-01', 'AA', '100', 'JFK', 'LAX', 800, 800, 0, 1100, 0, 0, 0),
            (2023, 1, 1, 1, 1, '2023-01-01', 'UA', '200', 'LAX', 'JFK', 900, 900, 0, 1200, 0, 0, 0)
        ]
    }
}

@pytest.fixture
def auth_headers():
    return {"Authorization": f"Bearer {test_token}"}

@pytest.fixture
def backend_client():
    return requests.Session()

@pytest.fixture
def clickhouse_client():
    return Client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.getenv("CLICKHOUSE_PORT", "9000")),
        user=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "")
    )

@pytest.fixture
def setup_datasets(clickhouse_client):
    # Create test tables
    for table_name, dataset in TEST_DATASETS.items():
        columns_str = ', '.join([f"{col} String" for col in dataset['columns']])
        clickhouse_client.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_str}) ENGINE = Memory")
        
        # Insert sample data
        clickhouse_client.execute(
            f"INSERT INTO {table_name} VALUES",
            dataset['sample_data']
        )
    
    yield
    
    # Cleanup
    for table_name in TEST_DATASETS.keys():
        clickhouse_client.execute(f"DROP TABLE IF EXISTS {table_name}")

def test_uk_price_paid_dataset():
    """Test UK Price Paid dataset setup and basic queries"""
    # Verify table exists
    result = clickhouse_client.execute("SHOW TABLES LIKE 'uk_price_paid'")
    assert len(result) > 0, "uk_price_paid table not found"
    
    # Verify data exists
    count = clickhouse_client.execute("SELECT count() FROM uk_price_paid")[0][0]
    assert count > 0, "No data in uk_price_paid table"
    
    # Test sample query
    result = clickhouse_client.execute("""
        SELECT toYear(date) AS year, round(avg(price)) AS price
        FROM uk_price_paid
        GROUP BY year
        ORDER BY year
    """)
    assert len(result) > 0, "Sample query returned no results"

def test_ontime_dataset():
    """Test OnTime dataset setup and basic queries"""
    # Verify table exists
    result = clickhouse_client.execute("SHOW TABLES LIKE 'ontime'")
    assert len(result) > 0, "ontime table not found"
    
    # Verify data exists
    count = clickhouse_client.execute("SELECT count() FROM ontime")[0][0]
    assert count > 0, "No data in ontime table"
    
    # Test sample query
    result = clickhouse_client.execute("""
        SELECT Carrier, count() AS c
        FROM ontime
        GROUP BY Carrier
        ORDER BY c DESC
        LIMIT 10
    """)
    assert len(result) > 0, "Sample query returned no results"

def test_clickhouse_to_flatfile(auth_headers, setup_datasets):
    """Test Case 1: Single ClickHouse table -> Flat File"""
    # Test with uk_price_paid dataset
    response = client.post(
        "/ingest/ch-to-file",
        headers=auth_headers,
        json={
            "table": "uk_price_paid",
            "columns": ["price", "date", "postcode"],
            "config": {
                "host": "clickhouse",
                "port": 9000,
                "user": "default",
                "password": "",
                "database": "default"
            }
        }
    )
    assert response.status_code == 200
    assert "X-Record-Count" in response.headers
    assert int(response.headers["X-Record-Count"]) == len(TEST_DATASETS['uk_price_paid']['sample_data'])

def test_flatfile_to_clickhouse(auth_headers, setup_datasets):
    """Test Case 2: Flat File -> ClickHouse"""
    # Create a temporary CSV file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv') as tmp_file:
        writer = csv.writer(tmp_file)
        writer.writerow(TEST_DATASETS['ontime']['columns'])
        writer.writerows(TEST_DATASETS['ontime']['sample_data'])
        tmp_file.flush()
        
        with open(tmp_file.name, 'rb') as f:
            response = client.post(
                "/ingest/file-to-ch",
                headers=auth_headers,
                files={"file": ("test.csv", f, "text/csv")},
                data={
                    "table": "ontime",
                    "delimiter": ",",
                    "config": {
                        "host": "clickhouse",
                        "port": 9000,
                        "user": "default",
                        "password": "",
                        "database": "default"
                    }
                }
            )
    
    assert response.status_code == 200
    assert response.json()["status"] == "success"
    assert response.json()["records_processed"] == len(TEST_DATASETS['ontime']['sample_data'])

def test_joined_tables_to_flatfile(auth_headers, setup_datasets):
    """Test Case 3: Joined ClickHouse tables -> Flat File"""
    # Create test tables with join
    clickhouse_client.execute("""
        CREATE TABLE IF NOT EXISTS test_orders (
            order_id UInt32,
            customer_id UInt32,
            order_date Date
        ) ENGINE = MergeTree()
        ORDER BY order_id
    """)
    
    clickhouse_client.execute("""
        CREATE TABLE IF NOT EXISTS test_customers (
            customer_id UInt32,
            customer_name String
        ) ENGINE = MergeTree()
        ORDER BY customer_id
    """)
    
    # Insert test data
    clickhouse_client.execute(
        "INSERT INTO test_orders VALUES",
        [(1, 1, '2023-01-01'), (2, 2, '2023-01-02')]
    )
    
    clickhouse_client.execute(
        "INSERT INTO test_customers VALUES",
        [(1, 'Customer 1'), (2, 'Customer 2')]
    )
    
    # Test joined export
    response = client.post(
        "/ingest/ch-to-file",
        headers=auth_headers,
        json={
            "table": "test_orders",
            "columns": ["order_id", "order_date", "customer_name"],
            "joinConfig": {
                "joinType": "INNER",
                "tables": [
                    {"table": "test_orders", "key": "customer_id"},
                    {"table": "test_customers", "key": "customer_id"}
                ]
            },
            "config": {
                "host": "clickhouse",
                "port": 9000,
                "user": "default",
                "password": "",
                "database": "default"
            }
        }
    )
    assert response.status_code == 200
    assert "X-Record-Count" in response.headers
    assert int(response.headers["X-Record-Count"]) == 2
    
    # Cleanup
    clickhouse_client.execute("DROP TABLE IF EXISTS test_orders")
    clickhouse_client.execute("DROP TABLE IF EXISTS test_customers")

def test_authentication_failure():
    """Test Case 4: Authentication failures"""
    # Test invalid token
    response = client.get(
        "/clickhouse/tables",
        headers={"Authorization": "Bearer invalid_token"}
    )
    assert response.status_code == 401
    
    # Test expired token
    expired_token = jwt.encode(
        {"sub": "test", "exp": datetime.utcnow() - timedelta(minutes=1)},
        SECRET_KEY,
        algorithm=ALGORITHM
    )
    response = client.get(
        "/clickhouse/tables",
        headers={"Authorization": f"Bearer {expired_token}"}
    )
    assert response.status_code == 401
    
    # Test missing token
    response = client.get("/clickhouse/tables")
    assert response.status_code == 401

def test_data_preview(auth_headers, setup_datasets):
    """Test Case 5: Data preview"""
    # Test ClickHouse preview
    response = client.post(
        "/clickhouse/preview",
        headers=auth_headers,
        json={
            "table": "uk_price_paid",
            "columns": ["price", "date", "postcode"],
            "limit": 100
        }
    )
    assert response.status_code == 200
    assert "data" in response.json()
    assert "columns" in response.json()
    assert "typeWarnings" in response.json()
    assert len(response.json()["data"]) <= 100
    
    # Test Flat File preview
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv') as tmp_file:
        writer = csv.writer(tmp_file)
        writer.writerow(TEST_DATASETS['ontime']['columns'])
        writer.writerows(TEST_DATASETS['ontime']['sample_data'])
        tmp_file.flush()
        
        with open(tmp_file.name, 'rb') as f:
            response = client.post(
                "/flatfile/preview",
                files={"file": ("test.csv", f, "text/csv")},
                data={"delimiter": ","}
            )
    
    assert response.status_code == 200
    assert "data" in response.json()
    assert "columns" in response.json()
    assert len(response.json()["data"]) <= 100

def test_connection_failures(backend_client):
    """Test connection and authentication failures"""
    # Test invalid host
    with pytest.raises(Exception):
        Client('invalid-host', port=9000)
    
    # Test invalid credentials
    with pytest.raises(Exception):
        Client('clickhouse', port=9000, user='invalid', password='invalid')
    
    # Test invalid JWT token
    headers = {'Authorization': 'Bearer invalid-token'}
    response = backend_client.get('http://backend:8000/api/tables', headers=headers)
    assert response.status_code == 401

def test_uk_price_paid_export(setup_datasets):
    """Test exporting uk_price_paid data to CSV"""
    response = client.post(
        "/ingest/ch-to-file",
        json={
            "table": "uk_price_paid",
            "columns": ["price", "date", "postcode"],
            "config": {
                "host": os.getenv("CLICKHOUSE_HOST", "clickhouse"),
                "port": int(os.getenv("CLICKHOUSE_PORT", "9000")),
                "user": os.getenv("CLICKHOUSE_USER", "default"),
                "password": os.getenv("CLICKHOUSE_PASSWORD", "")
            }
        }
    )
    assert response.status_code == 200
    assert "X-Record-Count" in response.headers
    assert int(response.headers["X-Record-Count"]) == len(TEST_DATASETS['uk_price_paid']['sample_data'])

def test_ontime_import(setup_datasets):
    """Test importing data into ontime table"""
    # Create a temporary CSV file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv') as tmp_file:
        writer = csv.writer(tmp_file)
        writer.writerow(TEST_DATASETS['ontime']['columns'])
        writer.writerows(TEST_DATASETS['ontime']['sample_data'])
        tmp_file.flush()
        
        with open(tmp_file.name, 'rb') as f:
            response = client.post(
                "/ingest/file-to-ch",
                files={"file": ("test.csv", f, "text/csv")},
                data={
                    "table": "ontime",
                    "delimiter": ",",
                    "config": {
                        "host": os.getenv("CLICKHOUSE_HOST", "clickhouse"),
                        "port": int(os.getenv("CLICKHOUSE_PORT", "9000")),
                        "user": os.getenv("CLICKHOUSE_USER", "default"),
                        "password": os.getenv("CLICKHOUSE_PASSWORD", "")
                    }
                }
            )
    
    assert response.status_code == 200
    assert response.json()["status"] == "success"
    assert response.json()["records_processed"] == len(TEST_DATASETS['ontime']['sample_data'])

def test_multi_table_join(setup_datasets):
    """Test joining uk_price_paid and ontime tables"""
    response = client.post(
        "/ingest/ch-to-file",
        json={
            "table": "uk_price_paid",
            "columns": ["price", "date", "postcode", "Year", "FlightDate"],
            "joinConfig": {
                "joinType": "INNER",
                "tables": [
                    {"table": "uk_price_paid", "key": "date"},
                    {"table": "ontime", "key": "FlightDate"}
                ]
            },
            "config": {
                "host": os.getenv("CLICKHOUSE_HOST", "clickhouse"),
                "port": int(os.getenv("CLICKHOUSE_PORT", "9000")),
                "user": os.getenv("CLICKHOUSE_USER", "default"),
                "password": os.getenv("CLICKHOUSE_PASSWORD", "")
            }
        }
    )
    assert response.status_code == 200
    assert "X-Record-Count" in response.headers

def test_invalid_jwt():
    """Test invalid JWT token"""
    response = client.get(
        "/clickhouse/tables",
        headers={"Authorization": "Bearer invalid_token"}
    )
    assert response.status_code == 401

def test_wrong_port():
    """Test connection with wrong port"""
    response = client.post(
        "/connect/clickhouse",
        json={
            "host": "localhost",
            "port": 9999,  # Invalid port
            "user": "default",
            "password": "",
            "database": "default"
        }
    )
    assert response.status_code == 400

def test_preview_data(setup_datasets):
    """Test previewing first 100 records"""
    response = client.post(
        "/clickhouse/preview",
        json={
            "table": "uk_price_paid",
            "columns": ["price", "date", "postcode"],
            "limit": 100
        }
    )
    assert response.status_code == 200
    assert "data" in response.json()
    assert "columns" in response.json()
    assert "typeWarnings" in response.json()
    assert len(response.json()["data"]) <= 100

def test_jwt_token_validation():
    """Test JWT token validation"""
    # Test expired token
    expired_token = jwt.encode(
        {"sub": "test", "exp": datetime.utcnow() - timedelta(minutes=1)},
        SECRET_KEY,
        algorithm=ALGORITHM
    )
    response = client.get(
        "/clickhouse/tables",
        headers={"Authorization": f"Bearer {expired_token}"}
    )
    assert response.status_code == 401
    
    # Test invalid token format
    response = client.get(
        "/clickhouse/tables",
        headers={"Authorization": "InvalidTokenFormat"}
    )
    assert response.status_code == 401
    
    # Test token with invalid signature
    invalid_token = jwt.encode(
        {"sub": "test", "exp": datetime.utcnow() + timedelta(minutes=30)},
        "wrong-secret-key",
        algorithm=ALGORITHM
    )
    response = client.get(
        "/clickhouse/tables",
        headers={"Authorization": f"Bearer {invalid_token}"}
    )
    assert response.status_code == 401 