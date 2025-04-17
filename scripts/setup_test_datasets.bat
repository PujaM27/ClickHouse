@echo off
setlocal enabledelayedexpansion

REM Function to check if ClickHouse is running
:check_clickhouse
clickhouse-client --query "SELECT 1" >nul 2>&1
if errorlevel 1 (
    echo Error: ClickHouse server is not running or not accessible
    exit /b 1
)
goto :eof

REM Function to download and import UK Price Paid dataset
:setup_uk_price_paid
echo Setting up UK Price Paid dataset...

REM Create database if not exists
clickhouse-client --query "CREATE DATABASE IF NOT EXISTS uk_price_paid"

REM Create table
clickhouse-client --database uk_price_paid --query ^
"CREATE TABLE IF NOT EXISTS price_paid (
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
ORDER BY (date, postcode)"

REM Download and import sample data
curl -s https://clickhouse.com/docs/getting-started/example-datasets/uk-price-paid/uk_price_paid.csv | ^
clickhouse-client --database uk_price_paid --query "INSERT INTO price_paid FORMAT CSV"
goto :eof

REM Function to download and import OnTime dataset
:setup_ontime
echo Setting up OnTime dataset...

REM Create database if not exists
clickhouse-client --query "CREATE DATABASE IF NOT EXISTS ontime"

REM Create table
clickhouse-client --database ontime --query ^
"CREATE TABLE IF NOT EXISTS flights (
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
ORDER BY (FlightDate, UniqueCarrier, FlightNum)"

REM Download and import sample data
curl -s https://clickhouse.com/docs/getting-started/example-datasets/ontime/ontime.csv | ^
clickhouse-client --database ontime --query "INSERT INTO flights FORMAT CSV"
goto :eof

REM Main script
echo Starting ClickHouse example datasets setup...

REM Check if ClickHouse is running
call :check_clickhouse
if errorlevel 1 exit /b 1

REM Setup datasets
call :setup_uk_price_paid
call :setup_ontime

echo Setup completed successfully! 