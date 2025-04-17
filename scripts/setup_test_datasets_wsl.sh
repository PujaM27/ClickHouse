#!/bin/bash

# Function to check if ClickHouse is running
check_clickhouse() {
    if ! ./clickhouse client --query "SELECT 1" >/dev/null 2>&1; then
        echo "Error: ClickHouse server is not running or not accessible"
        echo "Please start the ClickHouse server first using './clickhouse server'"
        exit 1
    fi
}

# Function to download and import UK Price Paid dataset
setup_uk_price_paid() {
    echo "Setting up UK Price Paid dataset..."
    
    # Create database if not exists
    ./clickhouse client --query "CREATE DATABASE IF NOT EXISTS uk_price_paid"
    
    # Create table
    ./clickhouse client --database uk_price_paid --query "
    CREATE TABLE IF NOT EXISTS price_paid (
        price UInt32,
        date Date,
        postcode String,
        property_type String,
        new_build String,
        duration String,
        locality String,
        town String,
        district String,
        county String
    ) ENGINE = MergeTree()
    ORDER BY (date, postcode)
    "
    
    # Download and import sample data
    wget -qO- https://clickhouse.com/docs/getting-started/example-datasets/uk-price-paid/uk_price_paid.csv | \
    ./clickhouse client --database uk_price_paid --query "INSERT INTO price_paid FORMAT CSV"
}

# Function to download and import OnTime dataset
setup_ontime() {
    echo "Setting up OnTime dataset..."
    
    # Create database if not exists
    ./clickhouse client --query "CREATE DATABASE IF NOT EXISTS ontime"
    
    # Create table
    ./clickhouse client --database ontime --query "
    CREATE TABLE IF NOT EXISTS flights (
        Year UInt16,
        Quarter UInt8,
        Month UInt8,
        DayofMonth UInt8,
        DayOfWeek UInt8,
        FlightDate Date,
        UniqueCarrier String,
        FlightNum String,
        Origin String,
        Dest String
    ) ENGINE = MergeTree()
    ORDER BY (FlightDate, UniqueCarrier, FlightNum)
    "
    
    # Download and import sample data
    wget -qO- https://clickhouse.com/docs/getting-started/example-datasets/ontime/ontime.csv | \
    ./clickhouse client --database ontime --query "INSERT INTO flights FORMAT CSV"
}

# Main script
echo "Starting ClickHouse example datasets setup..."

# Check if ClickHouse is running
check_clickhouse

# Setup datasets
setup_uk_price_paid
setup_ontime

echo "Setup completed successfully!" 