import React, { useState, useEffect } from 'react';
import {
  Box,
  TextField,
  Button,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  CircularProgress,
  Alert,
  Paper,
  Typography,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Checkbox,
  LinearProgress,
  Tooltip,
} from '@mui/material';
import axios from 'axios';

const ClickHouseConfig = ({ mode }) => {
  // Load saved config from localStorage
  const loadSavedConfig = () => {
    const savedConfig = localStorage.getItem('clickhouseConfig');
    return savedConfig ? JSON.parse(savedConfig) : {
      host: 'localhost',
      port: '8124',
      user: 'default',
      password: '',
      database: 'default',
      jwtToken: '',
    };
  };

  const [config, setConfig] = useState(loadSavedConfig());
  const [tables, setTables] = useState([]);
  const [selectedTable, setSelectedTable] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [previewData, setPreviewData] = useState(null);
  const [columns, setColumns] = useState([]);
  const [selectedColumns, setSelectedColumns] = useState([]);
  const [recordCount, setRecordCount] = useState(null);
  const [progress, setProgress] = useState({
    percentage: 0,
    status: '',
    currentOperation: '',
    recordsProcessed: 0,
    timeElapsed: 0,
    estimatedTimeRemaining: 0
  });
  const [typeWarnings, setTypeWarnings] = useState([]);
  const [joinConfig, setJoinConfig] = useState({
    tables: [],
    joinType: 'INNER',
    conditions: [],
  });

  // Save config to localStorage whenever it changes
  useEffect(() => {
    localStorage.setItem('clickhouseConfig', JSON.stringify(config));
  }, [config]);

  const handleConfigChange = (field) => (event) => {
    setConfig({ ...config, [field]: event.target.value });
  };

  const handleConnect = async () => {
    setLoading(true);
    setError('');
    try {
      const response = await axios.post('/api/connect/clickhouse', config);
      const tables = await axios.get('/api/clickhouse/tables');
      setTables(tables.data);
    } catch (err) {
      handleError(err);
    } finally {
      setLoading(false);
    }
  };

  const handlePreview = async () => {
    if (!selectedTable) return;
    setLoading(true);
    setError('');
    setTypeWarnings([]);
    try {
      const response = await axios.get(`/api/clickhouse/preview/${selectedTable}`);
      setPreviewData(response.data.data);
      setColumns(response.data.columns);
      setSelectedColumns(response.data.columns);
      
      // Check for type warnings
      const warnings = response.data.typeWarnings || [];
      setTypeWarnings(warnings);
    } catch (err) {
      handleError(err);
    } finally {
      setLoading(false);
    }
  };

  const handleTransfer = async () => {
    if (!selectedTable) return;
    setLoading(true);
    setError('');
    setProgress({
      percentage: 0,
      status: 'Starting transfer...',
      currentOperation: 'Initializing',
      recordsProcessed: 0,
      timeElapsed: 0,
      estimatedTimeRemaining: 0
    });
    setTypeWarnings([]);

    try {
      const response = await axios.post('/api/transfer/clickhouse-to-csv', {
        table: selectedTable,
        columns: selectedColumns,
        config,
        joinConfig: joinConfig.tables.length > 1 ? joinConfig : null,
      }, {
        onUploadProgress: (progressEvent) => {
          const percentCompleted = Math.round((progressEvent.loaded * 100) / progressEvent.total);
          setProgress(prev => ({
            ...prev,
            percentage: percentCompleted,
            status: 'Transferring data...',
            currentOperation: 'Processing records'
          }));
        }
      });

      setRecordCount(response.data.recordCount);
      
      // Check for type warnings
      const warnings = response.data.typeWarnings || [];
      setTypeWarnings(warnings);

      setProgress(prev => ({
        ...prev,
        percentage: 100,
        status: 'Transfer complete',
        currentOperation: 'Finalizing',
        recordsProcessed: response.data.records_processed
      }));
    } catch (err) {
      handleError(err);
    } finally {
      setLoading(false);
    }
  };

  const handleJoinTableAdd = () => {
    setJoinConfig(prev => ({
      ...prev,
      tables: [...prev.tables, { table: '', key: '' }]
    }));
  };

  const handleJoinTableChange = (index, field, value) => {
    setJoinConfig(prev => ({
      ...prev,
      tables: prev.tables.map((table, i) => 
        i === index ? { ...table, [field]: value } : table
      )
    }));
  };

  const handleJoinTableRemove = (index) => {
    setJoinConfig(prev => ({
      ...prev,
      tables: prev.tables.filter((_, i) => i !== index)
    }));
  };

  const handleJoinTypeChange = (event) => {
    setJoinConfig(prev => ({
      ...prev,
      joinType: event.target.value
    }));
  };

  const handleError = (error) => {
    let errorMessage = 'An error occurred';
    let recoverySteps = [];
    
    if (error.response) {
      switch (error.response.status) {
        case 401:
          errorMessage = 'Authentication failed';
          recoverySteps = [
            'Check your JWT token',
            'Verify your credentials',
            'Ensure the token is not expired'
          ];
          break;
        case 403:
          errorMessage = 'Access denied';
          recoverySteps = [
            'Check your permissions',
            'Verify database access',
            'Contact your administrator'
          ];
          break;
        case 404:
          errorMessage = 'Resource not found';
          recoverySteps = [
            'Verify table exists',
            'Check database name',
            'Ensure table is accessible'
          ];
          break;
        case 500:
          errorMessage = 'Server error';
          recoverySteps = [
            'Try again later',
            'Contact administrator',
            'Check server logs'
          ];
          break;
        default:
          errorMessage = error.response.data?.detail || error.message;
          recoverySteps = ['Try again', 'Contact support'];
      }
    } else if (error.request) {
      errorMessage = 'Cannot connect to server';
      recoverySteps = [
        'Check server is running',
        'Verify network connection',
        'Check firewall settings'
      ];
    }
    
    setError({
      message: errorMessage,
      steps: recoverySteps,
      timestamp: new Date().toISOString()
    });
  };

  return (
    <Box sx={{ p: 3 }}>
      <Paper elevation={3} sx={{ p: 3 }}>
        <Typography variant="h6" gutterBottom>
          ClickHouse Configuration
        </Typography>
        {error && (
          <Alert severity="error" sx={{ mb: 2 }}>
            <Typography variant="subtitle2" gutterBottom>
              {error.message}
            </Typography>
            {error.steps && error.steps.length > 0 && (
              <Box sx={{ mt: 1 }}>
                <Typography variant="body2" color="text.secondary" gutterBottom>
                  Suggested steps:
                </Typography>
                <ul style={{ margin: 0, paddingLeft: '20px' }}>
                  {error.steps.map((step, index) => (
                    <li key={index}>{step}</li>
                  ))}
                </ul>
              </Box>
            )}
            <Typography variant="caption" color="text.secondary" sx={{ mt: 1, display: 'block' }}>
              Error occurred at: {new Date(error.timestamp).toLocaleString()}
            </Typography>
          </Alert>
        )}
        <form onSubmit={(e) => { e.preventDefault(); handleConnect(); }}>
          <TextField
            fullWidth
            id="host"
            name="host"
            label="Host"
            value={config.host}
            onChange={handleConfigChange('host')}
            margin="normal"
            required
            autoComplete="off"
            aria-label="ClickHouse Host"
          />
          <TextField
            fullWidth
            id="port"
            name="port"
            label="Port"
            value={config.port}
            onChange={handleConfigChange('port')}
            margin="normal"
            required
            autoComplete="off"
            aria-label="ClickHouse Port"
          />
          <TextField
            fullWidth
            id="database"
            name="database"
            label="Database"
            value={config.database}
            onChange={handleConfigChange('database')}
            margin="normal"
            required
            autoComplete="off"
            aria-label="ClickHouse Database"
          />
          <TextField
            fullWidth
            id="username"
            name="username"
            label="Username"
            value={config.user}
            onChange={handleConfigChange('user')}
            margin="normal"
            required
            autoComplete="username"
            aria-label="ClickHouse Username"
          />
          <TextField
            fullWidth
            id="password"
            name="password"
            label="Password"
            type="password"
            value={config.password}
            onChange={handleConfigChange('password')}
            margin="normal"
            required
            autoComplete="current-password"
            aria-label="ClickHouse Password"
          />
          <Button
            type="submit"
            fullWidth
            variant="contained"
            color="primary"
            disabled={loading}
            sx={{ mt: 3 }}
          >
            {loading ? <CircularProgress size={24} /> : 'Connect'}
          </Button>
        </form>
      </Paper>

      {typeWarnings.length > 0 && (
        <Alert severity="warning" sx={{ mb: 3 }}>
          <Typography variant="subtitle2" gutterBottom>
            Data Type Warnings
          </Typography>
          <Box sx={{ mt: 1 }}>
            {typeWarnings.map((warning, index) => (
              <Box key={index} sx={{ mb: 1 }}>
                <Typography variant="body2" color="text.secondary">
                  Column: {warning.column}
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Expected Type: {warning.expectedType}
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Found Type: {warning.foundType}
                </Typography>
                {warning.suggestion && (
                  <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
                    Suggestion: {warning.suggestion}
                  </Typography>
                )}
              </Box>
            ))}
          </Box>
        </Alert>
      )}

      {tables.length > 0 && (
        <>
          <FormControl fullWidth sx={{ mb: 3 }}>
            <InputLabel>Select Table</InputLabel>
            <Select
              value={selectedTable}
              onChange={(e) => setSelectedTable(e.target.value)}
              label="Select Table"
            >
              {tables.map((table) => (
                <MenuItem key={table} value={table}>{table}</MenuItem>
              ))}
            </Select>
          </FormControl>

          {mode === 'export' && (
            <>
              <Typography variant="h6" gutterBottom>
                Join Configuration
              </Typography>
              <Box sx={{ mb: 3 }}>
                <FormControl fullWidth sx={{ mb: 2 }}>
                  <InputLabel>Join Type</InputLabel>
                  <Select
                    value={joinConfig.joinType}
                    onChange={handleJoinTypeChange}
                    label="Join Type"
                  >
                    <MenuItem value="INNER">INNER JOIN</MenuItem>
                    <MenuItem value="LEFT">LEFT JOIN</MenuItem>
                    <MenuItem value="RIGHT">RIGHT JOIN</MenuItem>
                  </Select>
                </FormControl>

                {joinConfig.tables.map((table, index) => (
                  <Box key={index} sx={{ display: 'flex', gap: 2, mb: 2 }}>
                    <FormControl fullWidth>
                      <InputLabel>Table</InputLabel>
                      <Select
                        value={table.table}
                        onChange={(e) => handleJoinTableChange(index, 'table', e.target.value)}
                        label="Table"
                      >
                        {tables.map((t) => (
                          <MenuItem key={t} value={t}>{t}</MenuItem>
                        ))}
                      </Select>
                    </FormControl>
                    <FormControl fullWidth>
                      <InputLabel>Join Key</InputLabel>
                      <Select
                        value={table.key}
                        onChange={(e) => handleJoinTableChange(index, 'key', e.target.value)}
                        label="Join Key"
                        disabled={!table.table}
                      >
                        {table.table && columns.map((col) => (
                          <MenuItem key={col} value={col}>{col}</MenuItem>
                        ))}
                      </Select>
                    </FormControl>
                    {index > 0 && (
                      <Button
                        variant="outlined"
                        color="error"
                        onClick={() => handleJoinTableRemove(index)}
                      >
                        Remove
                      </Button>
                    )}
                  </Box>
                ))}
                <Button
                  variant="outlined"
                  onClick={handleJoinTableAdd}
                  sx={{ mt: 1 }}
                  disabled={joinConfig.tables.length >= tables.length}
                >
                  Add Join Table
                </Button>
              </Box>
            </>
          )}

          <Box sx={{ display: 'flex', gap: 2 }}>
            <Button
              variant="contained"
              onClick={handlePreview}
              disabled={loading || !selectedTable}
            >
              Preview Data
            </Button>
            {mode === 'export' && (
              <Button
                variant="contained"
                onClick={handleTransfer}
                disabled={loading || !selectedTable}
              >
                Export to CSV
              </Button>
            )}
          </Box>
        </>
      )}

      {loading && (
        <Box sx={{ width: '100%', mt: 2 }}>
          <LinearProgress variant="determinate" value={progress.percentage} />
          <Box sx={{ display: 'flex', justifyContent: 'space-between', mt: 1 }}>
            <Typography variant="body2" color="text.secondary">
              {progress.status}
            </Typography>
            <Typography variant="body2" color="text.secondary">
              {progress.recordsProcessed} records processed
            </Typography>
          </Box>
          {progress.timeElapsed > 0 && (
            <Box sx={{ display: 'flex', justifyContent: 'space-between', mt: 1 }}>
              <Typography variant="body2" color="text.secondary">
                Time elapsed: {Math.round(progress.timeElapsed)}s
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Estimated time remaining: {Math.round(progress.estimatedTimeRemaining)}s
              </Typography>
            </Box>
          )}
        </Box>
      )}

      {previewData && (
        <Box sx={{ mt: 3 }}>
          <Typography variant="h6" gutterBottom>
            Data Preview (First 100 Records)
          </Typography>
          <Box sx={{ mb: 2 }}>
            <Typography variant="body2" color="text.secondary">
              Total Records: {previewData.length}
            </Typography>
            <Typography variant="body2" color="text.secondary">
              Selected Columns: {selectedColumns.length} of {columns.length}
            </Typography>
          </Box>
          <TableContainer sx={{ maxHeight: 440 }}>
            <Table stickyHeader>
              <TableHead>
                <TableRow>
                  {selectedColumns.map((column) => (
                    <TableCell key={column}>
                      <Tooltip 
                        title={
                          <Box>
                            <Typography variant="body2">
                              Type: {columns.find(c => c.name === column)?.type || 'Unknown'}
                            </Typography>
                            <Typography variant="body2">
                              Nullable: {columns.find(c => c.name === column)?.nullable ? 'Yes' : 'No'}
                            </Typography>
                            {columns.find(c => c.name === column)?.sample && (
                              <Typography variant="body2">
                                Sample: {columns.find(c => c.name === column)?.sample}
                              </Typography>
                            )}
                          </Box>
                        }
                      >
                        <span>{column}</span>
                      </Tooltip>
                    </TableCell>
                  ))}
                </TableRow>
              </TableHead>
              <TableBody>
                {previewData.slice(0, 100).map((row, index) => (
                  <TableRow key={index}>
                    {selectedColumns.map((column) => (
                      <TableCell key={column}>
                        {row[column] === null ? (
                          <Typography variant="body2" color="text.secondary">
                            NULL
                          </Typography>
                        ) : (
                          row[column]
                        )}
                      </TableCell>
                    ))}
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        </Box>
      )}

      {recordCount !== null && (
        <Alert severity="success" sx={{ mt: 3 }}>
          Successfully transferred {recordCount} records
        </Alert>
      )}

      <Table>
        <TableHead>
          <TableRow>
            <TableCell>Column</TableCell>
            <TableCell>Select</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {columns.map((column) => (
            <TableRow key={column}>
              <TableCell>{column}</TableCell>
              <TableCell>
                <Checkbox checked={selectedColumns.includes(column)} />
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </Box>
  );
};

export default ClickHouseConfig; 