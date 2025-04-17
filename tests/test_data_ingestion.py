import pytest # type: ignore
import requests # type: ignore
import pandas as pd # type: ignore
import os
import time
from clickhouse_driver import Client # type: ignore
import jwt # type: ignore
from datetime import datetime, timedelta

# Test configuration
TEST_CONFIG = {
    "clickhouse": {
        "host": "localhost",
        "port": "9000",
        "database": "test_data",
        "username": "default",
        "password": ""
    },
    "api": {
        "base_url": "http://localhost:8000"
    }
}

# JWT Configuration
SECRET_KEY = "your-secret-key"
ALGORITHM = "HS256"

def generate_token():
    payload = {
        "exp": datetime.utcnow() + timedelta(days=1),
        "iat": datetime.utcnow()
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

@pytest.fixture
def clickhouse_client():
    client = Client(
        host=TEST_CONFIG["clickhouse"]["host"],
        port=TEST_CONFIG["clickhouse"]["port"],
        database=TEST_CONFIG["clickhouse"]["database"],
        user=TEST_CONFIG["clickhouse"]["username"],
        password=TEST_CONFIG["clickhouse"]["password"]
    )
    return client

@pytest.fixture
def auth_token():
    return generate_token()

@pytest.fixture
def headers(auth_token):
    return {"Authorization": f"Bearer {auth_token}"}

def setup_test_data(clickhouse_client):
    """Setup test data in ClickHouse"""
    # Create test database if not exists
    clickhouse_client.execute("CREATE DATABASE IF NOT EXISTS test_data")
    
    # Create and populate test tables
    clickhouse_client.execute("""
    CREATE TABLE IF NOT EXISTS test_data.test_table1 (
        id UInt32,
        name String,
        value Float64,
        created_at DateTime
    ) ENGINE = MergeTree()
    ORDER BY id
    """)
    
    clickhouse_client.execute("""
    CREATE TABLE IF NOT EXISTS test_data.test_table2 (
        id UInt32,
        category String,
        amount Float64,
        created_at DateTime
    ) ENGINE = MergeTree()
    ORDER BY id
    """)
    
    # Insert test data
    test_data1 = [
        (1, "Item 1", 10.5, datetime.now()),
        (2, "Item 2", 20.7, datetime.now()),
        (3, "Item 3", 30.9, datetime.now())
    ]
    
    test_data2 = [
        (1, "Category A", 100.0, datetime.now()),
        (2, "Category B", 200.0, datetime.now()),
        (3, "Category C", 300.0, datetime.now())
    ]
    
    clickhouse_client.execute("INSERT INTO test_data.test_table1 VALUES", test_data1)
    clickhouse_client.execute("INSERT INTO test_data.test_table2 VALUES", test_data2)

def test_1_clickhouse_to_flatfile(clickhouse_client, headers):
    """Test Case 1: Single ClickHouse table -> Flat File"""
    # Setup
    setup_test_data(clickhouse_client)
    output_file = "test_output.csv"
    
    # Test parameters
    params = {
        "source": "clickhouse",
        "target": "flatfile",
        "table": "test_table1",
        "columns": ["id", "name", "value"],
        "config": {
            "filePath": output_file,
            "delimiter": ","
        },
        "transfer_id": f"test_{int(time.time())}"
    }
    
    # Execute transfer
    response = requests.post(
        f"{TEST_CONFIG['api']['base_url']}/transfer",
        json=params,
        headers=headers
    )
    assert response.status_code == 200
    
    # Verify output file
    assert os.path.exists(output_file)
    df = pd.read_csv(output_file)
    assert len(df) == 3
    assert list(df.columns) == ["id", "name", "value"]
    
    # Cleanup
    os.remove(output_file)

def test_2_flatfile_to_clickhouse(clickhouse_client, headers):
    """Test Case 2: Flat File -> ClickHouse"""
    # Setup
    input_file = "test_input.csv"
    test_data = pd.DataFrame({
        "id": [4, 5, 6],
        "name": ["Item 4", "Item 5", "Item 6"],
        "value": [40.1, 50.2, 60.3]
    })
    test_data.to_csv(input_file, index=False)
    
    # Test parameters
    params = {
        "source": "flatfile",
        "target": "clickhouse",
        "table": "test_table3",
        "columns": ["id", "name", "value"],
        "config": {
            "filePath": input_file,
            "delimiter": ","
        },
        "transfer_id": f"test_{int(time.time())}"
    }
    
    # Execute transfer
    response = requests.post(
        f"{TEST_CONFIG['api']['base_url']}/transfer",
        json=params,
        headers=headers
    )
    assert response.status_code == 200
    
    # Verify data in ClickHouse
    result = clickhouse_client.execute("SELECT * FROM test_data.test_table3")
    assert len(result) == 3
    
    # Cleanup
    os.remove(input_file)
    clickhouse_client.execute("DROP TABLE IF EXISTS test_data.test_table3")

def test_3_joined_tables_to_flatfile(clickhouse_client, headers):
    """Test Case 3: Joined ClickHouse tables -> Flat File"""
    # Setup
    setup_test_data(clickhouse_client)
    output_file = "test_join_output.csv"
    
    # Test parameters
    params = {
        "source": "clickhouse",
        "target": "flatfile",
        "table": "test_table1 JOIN test_table2 USING id",
        "columns": ["id", "name", "category", "value", "amount"],
        "config": {
            "filePath": output_file,
            "delimiter": ","
        },
        "transfer_id": f"test_{int(time.time())}"
    }
    
    # Execute transfer
    response = requests.post(
        f"{TEST_CONFIG['api']['base_url']}/transfer",
        json=params,
        headers=headers
    )
    assert response.status_code == 200
    
    # Verify output file
    assert os.path.exists(output_file)
    df = pd.read_csv(output_file)
    assert len(df) == 3
    assert list(df.columns) == ["id", "name", "category", "value", "amount"]
    
    # Cleanup
    os.remove(output_file)

def test_4_authentication_failures(headers):
    """Test Case 4: Authentication failures"""
    # Test with invalid token
    invalid_headers = {"Authorization": "Bearer invalid_token"}
    response = requests.get(
        f"{TEST_CONFIG['api']['base_url']}/tables",
        headers=invalid_headers
    )
    assert response.status_code == 401
    
    # Test with expired token
    expired_payload = {
        "exp": datetime.utcnow() - timedelta(days=1),
        "iat": datetime.utcnow() - timedelta(days=2)
    }
    expired_token = jwt.encode(expired_payload, SECRET_KEY, algorithm=ALGORITHM)
    expired_headers = {"Authorization": f"Bearer {expired_token}"}
    response = requests.get(
        f"{TEST_CONFIG['api']['base_url']}/tables",
        headers=expired_headers
    )
    assert response.status_code == 401

def test_5_data_preview(clickhouse_client, headers):
    """Test Case 5: Data preview"""
    # Setup
    setup_test_data(clickhouse_client)
    
    # Test preview
    response = requests.get(
        f"{TEST_CONFIG['api']['base_url']}/preview",
        params={
            "source": "clickhouse",
            "table": "test_table1",
            "columns": "id,name,value"
        },
        headers=headers
    )
    assert response.status_code == 200
    data = response.json()
    assert "data" in data
    assert "total_count" in data
    assert "columns" in data
    assert "schema" in data
    assert len(data["data"]) <= 100  # Preview should return at most 100 records

def cleanup_test_data(clickhouse_client):
    """Cleanup test data"""
    clickhouse_client.execute("DROP TABLE IF EXISTS test_data.test_table1")
    clickhouse_client.execute("DROP TABLE IF EXISTS test_data.test_table2")
    clickhouse_client.execute("DROP TABLE IF EXISTS test_data.test_table3")
    clickhouse_client.execute("DROP DATABASE IF EXISTS test_data")

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