# ClickHouse Data Transfer Application

A web-based application for data ingestion between ClickHouse and flat files (CSV), built with FastAPI (Python) and React.

## Features

- Export data from ClickHouse to CSV
- Import data from CSV to ClickHouse
- Support for multi-table joins
- Data preview functionality
- JWT-based authentication
- Type validation and mapping
- Progress tracking for large transfers

## Technical Stack

- Backend: Python with FastAPI
- Frontend: React with Material-UI
- Database: ClickHouse
- Containerization: Docker

## Prerequisites

- Python 3.8+
- Node.js 14+
- Docker and Docker Compose
- Git

## Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/ClickHouse.git
cd ClickHouse
```

### 2. Backend Setup

```bash
cd backend
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
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

### Access the Application

Open your browser and navigate to:
- Frontend: http://localhost:3000
- Backend API: http://localhost:8000

## Testing

### Backend Tests

```bash
cd backend
pytest
```

### Frontend Tests

```bash
cd frontend
npm test
```

## Sample Data

The application includes two sample datasets:

1. UK Price Paid Dataset
   - Property price data
   - Columns: price, date, postcode, property_type, etc.

2. OnTime Dataset
   - Flight data
   - Columns: Year, Quarter, Month, FlightDate, etc.

## API Documentation

Once the backend server is running, access the API documentation at:
http://localhost:8000/docs

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

## License

MIT License

## Support

For support, please open an issue in the GitHub repository. 