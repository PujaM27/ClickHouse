import pytest
import os
from clickhouse_driver import Client
import jwt
from datetime import datetime, timedelta

# Test configuration
TEST_CONFIG = {
    'CLICKHOUSE_HOST': os.getenv('CLICKHOUSE_HOST', 'clickhouse'),
    'CLICKHOUSE_PORT': int(os.getenv('CLICKHOUSE_PORT', '9000')),
    'CLICKHOUSE_USER': os.getenv('CLICKHOUSE_USER', 'default'),
    'CLICKHOUSE_PASSWORD': os.getenv('CLICKHOUSE_PASSWORD', ''),
    'CLICKHOUSE_DATABASE': os.getenv('CLICKHOUSE_DATABASE', 'default'),
    'JWT_SECRET_KEY': 'test-secret-key',
    'JWT_ALGORITHM': 'HS256'
}

@pytest.fixture(scope='session')
def clickhouse_client():
    """Create a ClickHouse client for testing"""
    client = Client(
        host=TEST_CONFIG['CLICKHOUSE_HOST'],
        port=TEST_CONFIG['CLICKHOUSE_PORT'],
        user=TEST_CONFIG['CLICKHOUSE_USER'],
        password=TEST_CONFIG['CLICKHOUSE_PASSWORD'],
        database=TEST_CONFIG['CLICKHOUSE_DATABASE']
    )
    
    # Test connection
    client.execute('SELECT 1')
    return client

@pytest.fixture(scope='session')
def test_token():
    """Create a test JWT token"""
    return jwt.encode(
        {
            'sub': 'test_user',
            'exp': datetime.utcnow() + timedelta(minutes=30)
        },
        TEST_CONFIG['JWT_SECRET_KEY'],
        algorithm=TEST_CONFIG['JWT_ALGORITHM']
    )

@pytest.fixture(scope='session')
def auth_headers(test_token):
    """Create authorization headers with test token"""
    return {'Authorization': f'Bearer {test_token}'}

@pytest.fixture(autouse=True)
def setup_test_environment(clickhouse_client):
    """Setup test environment before each test"""
    # Create test database if it doesn't exist
    clickhouse_client.execute(f"CREATE DATABASE IF NOT EXISTS {TEST_CONFIG['CLICKHOUSE_DATABASE']}")
    
    yield
    
    # Cleanup after tests
    clickhouse_client.execute(f"DROP DATABASE IF EXISTS {TEST_CONFIG['CLICKHOUSE_DATABASE']}") 