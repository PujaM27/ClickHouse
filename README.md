# ClickHouse Data Transfer Application

A web-based application for data ingestion between ClickHouse and flat files (CSV), built with FastAPI (Python) and React.

## Features

- Export data from ClickHouse to CSV
- Import data from CSV to ClickHouse
- Support for multi-table joins
- Data preview functionality
- JWT-based authentication
- Type validation and mapping

## Technical Stack

- Backend: Python with FastAPI
- Frontend: React with Material-UI
- Database: ClickHouse
- Containerization: Docker

## Prerequisites

- Python 3.8+
- Node.js 14+
- Docker
- Docker Compose

## Setup Instructions

### 1. Clone the Repository

```bash
git clone <repository-url>
cd ClickHouse
```

### 2. Backend Setup

```bash
cd backend
pip install -r requirements.txt
```

### 3. Frontend Setup

```bash
cd frontend
npm install
```

### 4. Docker Setup

```bash
docker-compose up -d
```

This will start:
- ClickHouse server on port 9000
- Backend server on port 8000
- Frontend server on port 3000

## Configuration

### ClickHouse Configuration

Default credentials:
- Host: localhost
- Port: 9000
- Username: default
- Password: default
- Database: test_db

### Environment Variables

Create a `.env` file in the backend directory:

```env
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=default
JWT_SECRET=your-secret-key
```

## Running the Application

### Start Backend Server

```bash
cd backend
python -m uvicorn main:app --host 127.0.0.1 --port 8000 --reload
```

### Start Frontend Server

```bash
cd frontend
npm start
```
## Usage

1. **Login**
   - Use the provided credentials to log in
   - JWT token will be automatically managed

2. **ClickHouse Connection**
   - Configure ClickHouse connection settings
   - Test connection before proceeding

3. **Data Transfer**
   - Export: Select tables and columns to export to CSV
   - Import: Upload CSV files to import into ClickHouse
   - Preview: View data before transfer

4. **Multi-table Joins**
   - Configure join conditions between tables
   - Export joined data to CSV

## Testing

Run the test suite:

```bash
cd backend
pytest tests/
```

## API Documentation

Access the API documentation at:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## Security

- JWT-based authentication
- Password hashing
- CORS configuration
- Input validation
- Error handling

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

