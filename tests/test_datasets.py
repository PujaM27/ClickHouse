import pytest
from clickhouse_driver import Client
import os

@pytest.fixture
def clickhouse_client():
    return Client('localhost')

def test_uk_price_paid_dataset(clickhouse_client):
    # Test if table exists
    result = clickhouse_client.execute("SHOW TABLES LIKE 'uk_price_paid'")
    assert len(result) > 0, "uk_price_paid table does not exist"
    
    # Test if data is loaded
    count = clickhouse_client.execute("SELECT count() FROM uk_price_paid")[0][0]
    assert count > 0, "uk_price_paid table is empty"
    
    # Test sample query
    result = clickhouse_client.execute("""
        SELECT 
            toYear(date) AS year,
            round(avg(price)) AS price,
            bar(price, 0, 1000000, 80)
        FROM uk_price_paid
        GROUP BY year
        ORDER BY year
    """)
    assert len(result) > 0, "Sample query on uk_price_paid failed"

def test_ontime_dataset(clickhouse_client):
    # Test if table exists
    result = clickhouse_client.execute("SHOW TABLES LIKE 'ontime'")
    assert len(result) > 0, "ontime table does not exist"
    
    # Test if data is loaded
    count = clickhouse_client.execute("SELECT count() FROM ontime")[0][0]
    assert count > 0, "ontime table is empty"
    
    # Test sample query
    result = clickhouse_client.execute("""
        SELECT 
            Carrier,
            count() AS flights,
            round(avg(DepDelay)) AS avg_delay
        FROM ontime
        GROUP BY Carrier
        ORDER BY flights DESC
        LIMIT 10
    """)
    assert len(result) > 0, "Sample query on ontime failed"

def test_data_ingestion_with_datasets(clickhouse_client):
    # Test data ingestion from ClickHouse to flat file
    from main import transfer_data
    import tempfile
    
    # Create a temporary file
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
        temp_file = f.name
    
    try:
        # Test UK Price Paid dataset
        result = transfer_data(
            source_type="clickhouse",
            target_type="flat_file",
            clickhouse_config={
                "host": "localhost",
                "port": 9000,
                "database": "default",
                "table": "uk_price_paid",
                "columns": ["price", "date", "postcode1", "postcode2"]
            },
            flat_file_config={
                "file_path": temp_file,
                "delimiter": ","
            }
        )
        assert result["status"] == "success"
        assert os.path.getsize(temp_file) > 0
        
        # Test OnTime dataset
        result = transfer_data(
            source_type="clickhouse",
            target_type="flat_file",
            clickhouse_config={
                "host": "localhost",
                "port": 9000,
                "database": "default",
                "table": "ontime",
                "columns": ["Year", "Month", "DayofMonth", "Carrier", "FlightNum"]
            },
            flat_file_config={
                "file_path": temp_file,
                "delimiter": ","
            }
        )
        assert result["status"] == "success"
        assert os.path.getsize(temp_file) > 0
        
    finally:
        # Clean up
        if os.path.exists(temp_file):
            os.unlink(temp_file) 