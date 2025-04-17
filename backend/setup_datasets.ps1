# Create datasets directory if it doesn't exist
New-Item -ItemType Directory -Force -Path "datasets"

# Download UK Price Paid dataset
Write-Host "Downloading UK Price Paid dataset..."
Invoke-WebRequest -Uri "https://datasets.clickhouse.com/uk_price_paid/data.csv.gz" -OutFile "datasets/uk_price_paid.csv.gz"

# Download OnTime dataset
Write-Host "Downloading OnTime dataset..."
Invoke-WebRequest -Uri "https://datasets.clickhouse.com/ontime/data.csv.gz" -OutFile "datasets/ontime.csv.gz"

# Extract the files using 7-Zip
Write-Host "Extracting files..."
if (-not (Test-Path "C:\Program Files\7-Zip\7z.exe")) {
    Write-Host "7-Zip not found. Please install 7-Zip and try again."
    exit 1
}

& "C:\Program Files\7-Zip\7z.exe" x "datasets/uk_price_paid.csv.gz" -o"datasets" -y
& "C:\Program Files\7-Zip\7z.exe" x "datasets/ontime.csv.gz" -o"datasets" -y

# Create table
Write-Host "Creating table..."
docker exec clickhouse-server clickhouse-client --query "
CREATE TABLE IF NOT EXISTS uk_price_paid (
    price UInt32,
    date Date,
    postcode1 String,
    postcode2 String,
    type Enum8('terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4, 'other' = 0),
    is_new UInt8,
    duration Enum8('freehold' = 1, 'leasehold' = 2, 'unknown' = 0),
    addr1 String,
    addr2 String,
    street String,
    locality String,
    town String,
    district String,
    county String
) ENGINE = MergeTree()
ORDER BY (postcode1, postcode2, addr1, addr2);"

# Insert sample data
Write-Host "Inserting sample data..."
docker exec clickhouse-server clickhouse-client --query "
INSERT INTO uk_price_paid VALUES
(250000, '2023-01-15', 'SW1', '1AA', 'terraced', 0, 'freehold', '10', '', 'High Street', 'Westminster', 'London', 'City of Westminster', 'Greater London'),
(350000, '2023-02-20', 'M1', '1AD', 'semi-detached', 0, 'freehold', '25', '', 'Oak Road', 'Central', 'Manchester', 'Manchester City', 'Greater Manchester'),
(450000, '2023-03-10', 'B1', '1BE', 'detached', 1, 'freehold', '8', '', 'Pine Lane', 'Edgbaston', 'Birmingham', 'Birmingham City', 'West Midlands'),
(180000, '2023-04-05', 'L1', '1CF', 'flat', 0, 'leasehold', '15', 'Apt 3', 'River Street', 'Central', 'Liverpool', 'Liverpool City', 'Merseyside'),
(550000, '2023-05-01', 'BS1', '1DG', 'detached', 0, 'freehold', '42', '', 'Maple Avenue', 'Clifton', 'Bristol', 'Bristol City', 'Bristol');"

# Verify the data was imported
Write-Host "Verifying data import..."
docker exec clickhouse-server clickhouse-client --query "SELECT count() FROM uk_price_paid"
docker exec clickhouse-server clickhouse-client --query "SELECT * FROM uk_price_paid LIMIT 5 FORMAT Pretty"

Write-Host "Dataset setup completed successfully!" 