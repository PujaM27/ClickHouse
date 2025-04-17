import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import axios from 'axios';
import DataIngestion from '../components/DataIngestion';

// Mock axios
jest.mock('axios');

// Mock the example datasets
const mockDatasets = {
  uk_price_paid: {
    tables: ['price_paid'],
    columns: ['price', 'date', 'postcode', 'property_type', 'new_build', 'duration', 'locality', 'town', 'district', 'county'],
    sampleData: [
      { price: 100000, date: '2023-01-01', postcode: 'SW1A 1AA', property_type: 'D', new_build: 'N', duration: 'F', locality: 'LONDON', town: 'LONDON', district: 'CITY OF WESTMINSTER', county: 'GREATER LONDON' }
    ],
    totalRecords: 1000
  },
  ontime: {
    tables: ['flights'],
    columns: ['Year', 'Quarter', 'Month', 'DayofMonth', 'DayOfWeek', 'FlightDate', 'UniqueCarrier', 'FlightNum', 'Origin', 'Dest'],
    sampleData: [
      { Year: 2023, Quarter: 1, Month: 1, DayofMonth: 1, DayOfWeek: 1, FlightDate: '2023-01-01', UniqueCarrier: 'AA', FlightNum: '123', Origin: 'JFK', Dest: 'LAX' }
    ],
    totalRecords: 1000
  }
};

describe('DataIngestion Component Tests', () => {
  beforeEach(() => {
    // Reset all mocks before each test
    jest.clearAllMocks();
    
    // Mock successful ClickHouse connection
    axios.post.mockImplementation((url) => {
      if (url.includes('/connect/clickhouse')) {
        return Promise.resolve({ data: { message: 'Connected successfully' } });
      }
      return Promise.reject(new Error('Not mocked'));
    });
  });

  test('1. Single ClickHouse table -> Flat File (selected columns)', async () => {
    // Mock table fetch
    axios.post.mockImplementationOnce(() => 
      Promise.resolve({ data: { tables: ['price_paid'] } })
    );

    // Mock column fetch
    axios.get.mockImplementationOnce(() => 
      Promise.resolve({ data: { columns: mockDatasets.uk_price_paid.columns } })
    );

    // Mock preview
    axios.post.mockImplementationOnce(() => 
      Promise.resolve({ 
        data: { 
          sample: mockDatasets.uk_price_paid.sampleData,
          columns: mockDatasets.uk_price_paid.columns,
          totalRecords: mockDatasets.uk_price_paid.totalRecords,
          dataTypes: { price: 'UInt32', date: 'Date', postcode: 'String' }
        } 
      })
    );

    // Mock transfer
    axios.post.mockImplementationOnce(() => 
      Promise.resolve({ data: { message: 'Transfer completed', records_processed: 1000 } })
    );

    render(<DataIngestion />);

    // Select ClickHouse as source
    fireEvent.change(screen.getByLabelText(/source type/i), { target: { value: 'clickhouse' } });

    // Fill ClickHouse config
    fireEvent.change(screen.getByLabelText(/host/i), { target: { value: 'localhost' } });
    fireEvent.change(screen.getByLabelText(/port/i), { target: { value: '9000' } });
    fireEvent.change(screen.getByLabelText(/database/i), { target: { value: 'default' } });
    fireEvent.change(screen.getByLabelText(/user/i), { target: { value: 'default' } });
    fireEvent.change(screen.getByLabelText(/jwt token/i), { target: { value: 'test-token' } });

    // Connect
    fireEvent.click(screen.getByText(/connect/i));

    // Wait for connection
    await waitFor(() => {
      expect(screen.getByText(/connected successfully/i)).toBeInTheDocument();
    });

    // Load tables
    fireEvent.click(screen.getByText(/load tables/i));

    // Select table
    await waitFor(() => {
      expect(screen.getByText(/price_paid/i)).toBeInTheDocument();
    });
    fireEvent.click(screen.getByLabelText(/price_paid/i));

    // Load columns
    fireEvent.click(screen.getByText(/load columns/i));

    // Select columns
    await waitFor(() => {
      expect(screen.getByText(/price/i)).toBeInTheDocument();
    });
    fireEvent.click(screen.getByLabelText(/price/i));
    fireEvent.click(screen.getByLabelText(/date/i));

    // Preview
    fireEvent.click(screen.getByText(/generate preview/i));

    // Verify preview
    await waitFor(() => {
      expect(screen.getByText(/sample data/i)).toBeInTheDocument();
    });

    // Transfer
    fireEvent.click(screen.getByText(/clickhouse to file/i));

    // Verify transfer completion
    await waitFor(() => {
      expect(screen.getByText(/transfer completed/i)).toBeInTheDocument();
      expect(screen.getByText(/records processed: 1000/i)).toBeInTheDocument();
    });
  });

  test('2. Flat File -> ClickHouse table', async () => {
    // Mock file upload
    const mockFile = new File(['price,date\n100000,2023-01-01'], 'test.csv', { type: 'text/csv' });

    render(<DataIngestion />);

    // Select Flat File as source
    fireEvent.change(screen.getByLabelText(/source type/i), { target: { value: 'flatfile' } });

    // Fill Flat File config
    fireEvent.change(screen.getByLabelText(/file path/i), { target: { value: 'test.csv' } });
    fireEvent.change(screen.getByLabelText(/delimiter/i), { target: { value: ',' } });

    // Mock preview
    axios.post.mockImplementationOnce(() => 
      Promise.resolve({ 
        data: { 
          sample: [{ price: 100000, date: '2023-01-01' }],
          columns: ['price', 'date'],
          totalRecords: 1,
          dataTypes: { price: 'UInt32', date: 'Date' }
        } 
      })
    );

    // Mock transfer
    axios.post.mockImplementationOnce(() => 
      Promise.resolve({ data: { message: 'Transfer completed', records_processed: 1 } })
    );

    // Connect
    fireEvent.click(screen.getByText(/connect/i));

    // Preview
    fireEvent.click(screen.getByText(/generate preview/i));

    // Select columns
    await waitFor(() => {
      expect(screen.getByText(/price/i)).toBeInTheDocument();
    });
    fireEvent.click(screen.getByLabelText(/price/i));
    fireEvent.click(screen.getByLabelText(/date/i));

    // Transfer
    fireEvent.click(screen.getByText(/file to clickhouse/i));

    // Verify transfer completion
    await waitFor(() => {
      expect(screen.getByText(/transfer completed/i)).toBeInTheDocument();
      expect(screen.getByText(/records processed: 1/i)).toBeInTheDocument();
    });
  });

  test('3. Joined ClickHouse tables -> Flat File', async () => {
    // Mock table fetch
    axios.post.mockImplementationOnce(() => 
      Promise.resolve({ data: { tables: ['price_paid', 'flights'] } })
    );

    // Mock column fetch
    axios.get.mockImplementationOnce(() => 
      Promise.resolve({ data: { columns: [...mockDatasets.uk_price_paid.columns, ...mockDatasets.ontime.columns] } })
    );

    render(<DataIngestion />);

    // Select ClickHouse as source
    fireEvent.change(screen.getByLabelText(/source type/i), { target: { value: 'clickhouse' } });

    // Fill ClickHouse config
    fireEvent.change(screen.getByLabelText(/host/i), { target: { value: 'localhost' } });
    fireEvent.change(screen.getByLabelText(/port/i), { target: { value: '9000' } });
    fireEvent.change(screen.getByLabelText(/database/i), { target: { value: 'default' } });
    fireEvent.change(screen.getByLabelText(/user/i), { target: { value: 'default' } });
    fireEvent.change(screen.getByLabelText(/jwt token/i), { target: { value: 'test-token' } });

    // Connect
    fireEvent.click(screen.getByText(/connect/i));

    // Load tables
    fireEvent.click(screen.getByText(/load tables/i));

    // Select multiple tables
    await waitFor(() => {
      expect(screen.getByText(/price_paid/i)).toBeInTheDocument();
      expect(screen.getByText(/flights/i)).toBeInTheDocument();
    });
    fireEvent.click(screen.getByLabelText(/price_paid/i));
    fireEvent.click(screen.getByLabelText(/flights/i));

    // Add join condition
    fireEvent.click(screen.getByText(/add join condition/i));

    // Verify join UI elements
    expect(screen.getByText(/join conditions/i)).toBeInTheDocument();
  });

  test('4. Test connection/authentication failures', async () => {
    // Mock failed connection
    axios.post.mockImplementationOnce(() => 
      Promise.reject({ 
        response: { 
          data: { message: 'Authentication failed: Invalid JWT token' } 
        } 
      })
    );

    render(<DataIngestion />);

    // Select ClickHouse as source
    fireEvent.change(screen.getByLabelText(/source type/i), { target: { value: 'clickhouse' } });

    // Fill ClickHouse config with invalid token
    fireEvent.change(screen.getByLabelText(/host/i), { target: { value: 'localhost' } });
    fireEvent.change(screen.getByLabelText(/port/i), { target: { value: '9000' } });
    fireEvent.change(screen.getByLabelText(/database/i), { target: { value: 'default' } });
    fireEvent.change(screen.getByLabelText(/user/i), { target: { value: 'default' } });
    fireEvent.change(screen.getByLabelText(/jwt token/i), { target: { value: 'invalid-token' } });

    // Connect
    fireEvent.click(screen.getByText(/connect/i));

    // Verify error message
    await waitFor(() => {
      expect(screen.getByText(/authentication failed/i)).toBeInTheDocument();
    });
  });

  test('5. Test data preview', async () => {
    // Mock preview
    axios.post.mockImplementationOnce(() => 
      Promise.resolve({ 
        data: { 
          sample: mockDatasets.uk_price_paid.sampleData,
          columns: mockDatasets.uk_price_paid.columns,
          totalRecords: mockDatasets.uk_price_paid.totalRecords,
          dataTypes: { price: 'UInt32', date: 'Date', postcode: 'String' }
        } 
      })
    );

    render(<DataIngestion />);

    // Select ClickHouse as source
    fireEvent.change(screen.getByLabelText(/source type/i), { target: { value: 'clickhouse' } });

    // Fill ClickHouse config
    fireEvent.change(screen.getByLabelText(/host/i), { target: { value: 'localhost' } });
    fireEvent.change(screen.getByLabelText(/port/i), { target: { value: '9000' } });
    fireEvent.change(screen.getByLabelText(/database/i), { target: { value: 'default' } });
    fireEvent.change(screen.getByLabelText(/user/i), { target: { value: 'default' } });
    fireEvent.change(screen.getByLabelText(/jwt token/i), { target: { value: 'test-token' } });

    // Connect
    fireEvent.click(screen.getByText(/connect/i));

    // Load tables
    fireEvent.click(screen.getByText(/load tables/i));

    // Select table
    await waitFor(() => {
      expect(screen.getByText(/price_paid/i)).toBeInTheDocument();
    });
    fireEvent.click(screen.getByLabelText(/price_paid/i));

    // Load columns
    fireEvent.click(screen.getByText(/load columns/i));

    // Select columns
    await waitFor(() => {
      expect(screen.getByText(/price/i)).toBeInTheDocument();
    });
    fireEvent.click(screen.getByLabelText(/price/i));

    // Preview
    fireEvent.click(screen.getByText(/generate preview/i));

    // Verify preview content
    await waitFor(() => {
      expect(screen.getByText(/sample data/i)).toBeInTheDocument();
      expect(screen.getByText(/100000/i)).toBeInTheDocument();
      expect(screen.getByText(/total records: 1000/i)).toBeInTheDocument();
    });
  });
}); 