import React, { useState } from 'react';
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
} from '@mui/material';
import axios from 'axios';

const FlatFileConfig = () => {
  const [file, setFile] = useState(null);
  const [delimiter, setDelimiter] = useState(',');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [previewData, setPreviewData] = useState(null);
  const [columns, setColumns] = useState([]);
  const [selectedColumns, setSelectedColumns] = useState([]);
  const [clickhouseConfig, setClickhouseConfig] = useState({
    host: 'localhost',
    port: '8124',
    user: 'default',
    password: '',
    database: 'default',
  });
  const [fileName, setFileName] = useState('');
  const [progress, setProgress] = useState({
    percentage: 0,
    status: '',
    currentOperation: '',
    recordsProcessed: 0,
    timeElapsed: 0,
    estimatedTimeRemaining: 0
  });

  const handleFileChange = async (event) => {
    const file = event.target.files[0];
    if (file) {
      setFile(file);
      setFileName(file.name);
      
      // Preview file
      const formData = new FormData();
      formData.append('file', file);
      formData.append('delimiter', delimiter);
      
      try {
        const previewResponse = await axios.post('/api/flatfile/preview', formData);
        setPreviewData(previewResponse.data.data);
        setColumns(previewResponse.data.columns);
        setSelectedColumns(previewResponse.data.columns);
      } catch (err) {
        setError(err.response?.data?.detail || 'Failed to preview file');
      }
    }
  };

  const handleDelimiterChange = (event) => {
    setDelimiter(event.target.value);
  };

  const handlePreview = async () => {
    if (!file) return;
    setLoading(true);
    setError('');

    const formData = new FormData();
    formData.append('file', file);
    formData.append('delimiter', delimiter);

    try {
      const response = await axios.post('/api/flatfile/preview', formData, {
        headers: {
          'Content-Type': 'multipart/form-data',
        },
      });
      setPreviewData(response.data.data);
      setColumns(response.data.columns);
      setSelectedColumns(response.data.columns);
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to preview file');
    } finally {
      setLoading(false);
    }
  };

  const handleColumnToggle = (column) => {
    setSelectedColumns(prev =>
      prev.includes(column)
        ? prev.filter(c => c !== column)
        : [...prev, column]
    );
  };

  const handleClickhouseConfigChange = (field) => (event) => {
    setClickhouseConfig({ ...clickhouseConfig, [field]: event.target.value });
  };

  const handleTransfer = async () => {
    const startTime = Date.now();
    try {
      setProgress({
        percentage: 0,
        status: 'Starting transfer...',
        currentOperation: 'Initializing',
        recordsProcessed: 0,
        timeElapsed: 0,
        estimatedTimeRemaining: 0
      });
      
      if (!file || selectedColumns.length === 0) return;
      setLoading(true);
      setError('');

      const formData = new FormData();
      formData.append('file', file);
      formData.append('delimiter', delimiter);
      formData.append('columns', JSON.stringify(selectedColumns));
      formData.append('config', JSON.stringify(clickhouseConfig));

      const updateProgress = (loaded, total) => {
        const currentTime = Date.now();
        const timeElapsed = (currentTime - startTime) / 1000; // in seconds
        const percentCompleted = Math.round((loaded * 100) / total);
        const estimatedTotalTime = timeElapsed / (percentCompleted / 100);
        const estimatedTimeRemaining = estimatedTotalTime - timeElapsed;
        
        setProgress(prev => ({
          ...prev,
          percentage: percentCompleted,
          status: 'Transferring data...',
          currentOperation: 'Processing records',
          timeElapsed: Math.round(timeElapsed),
          estimatedTimeRemaining: Math.round(estimatedTimeRemaining)
        }));
      };

      const response = await axios.post('/api/ingest/file-to-ch', formData, {
        headers: {
          'Content-Type': 'multipart/form-data',
        },
        onUploadProgress: (event) => {
          if (event.total) {
            updateProgress(event.loaded, event.total);
          }
        }
      });
      // Handle successful transfer
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to transfer data');
    } finally {
      setLoading(false);
    }
  };

  return (
    <Paper elevation={2} sx={{ p: 3 }}>
      <Typography variant="h6" gutterBottom>
        Flat File Configuration
      </Typography>

      <Box sx={{ mb: 3 }}>
        <Button
          variant="contained"
          component="label"
          sx={{ mb: 2 }}
        >
          Upload File
          <input
            type="file"
            hidden
            accept=".csv,.tsv,.txt"
            onChange={handleFileChange}
          />
        </Button>
        {fileName && <Typography>{fileName}</Typography>}
      </Box>

      <FormControl fullWidth sx={{ mb: 3 }}>
        <InputLabel>Delimiter</InputLabel>
        <Select
          value={delimiter}
          onChange={handleDelimiterChange}
          label="Delimiter"
        >
          <MenuItem value=",">Comma</MenuItem>
          <MenuItem value="\t">Tab</MenuItem>
        </Select>
      </FormControl>

      <Button
        variant="contained"
        onClick={handlePreview}
        disabled={loading || !file}
        sx={{ mb: 3 }}
      >
        {loading ? <CircularProgress size={24} /> : 'Preview Data'}
      </Button>

      {error && <Alert severity="error" sx={{ mb: 3 }}>{error}</Alert>}

      {previewData && (
        <>
          <Typography variant="h6" gutterBottom>
            Select Columns
          </Typography>
          <TableContainer sx={{ mb: 3 }}>
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
                      <Checkbox
                        checked={selectedColumns.includes(column)}
                        onChange={() => handleColumnToggle(column)}
                      />
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>

          <Typography variant="h6" gutterBottom>
            Preview Data
          </Typography>
          <TableContainer sx={{ mb: 3 }}>
            <Table>
              <TableHead>
                <TableRow>
                  {selectedColumns.map((column) => (
                    <TableCell key={column}>{column}</TableCell>
                  ))}
                </TableRow>
              </TableHead>
              <TableBody>
                {previewData.slice(0, 5).map((row, index) => (
                  <TableRow key={index}>
                    {selectedColumns.map((column) => (
                      <TableCell key={column}>{row[column]}</TableCell>
                    ))}
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>

          <Typography variant="h6" gutterBottom>
            ClickHouse Destination
          </Typography>
          <Box sx={{ display: 'flex', gap: 2, mb: 3 }}>
            <TextField
              label="Host"
              value={clickhouseConfig.host}
              onChange={handleClickhouseConfigChange('host')}
              fullWidth
            />
            <TextField
              label="Port"
              value={clickhouseConfig.port}
              onChange={handleClickhouseConfigChange('port')}
              fullWidth
            />
          </Box>

          <Box sx={{ display: 'flex', gap: 2, mb: 3 }}>
            <TextField
              label="User"
              value={clickhouseConfig.user}
              onChange={handleClickhouseConfigChange('user')}
              fullWidth
            />
            <TextField
              label="Password"
              type="password"
              value={clickhouseConfig.password}
              onChange={handleClickhouseConfigChange('password')}
              fullWidth
            />
          </Box>

          <TextField
            label="Database"
            value={clickhouseConfig.database}
            onChange={handleClickhouseConfigChange('database')}
            fullWidth
            sx={{ mb: 3 }}
          />

          <Button
            variant="contained"
            onClick={handleTransfer}
            disabled={loading || selectedColumns.length === 0}
          >
            {loading ? <CircularProgress size={24} /> : 'Transfer to ClickHouse'}
          </Button>
        </>
      )}
    </Paper>
  );
};

export default FlatFileConfig; 