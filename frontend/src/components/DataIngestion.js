import React, { useState, useEffect } from 'react';
import {
  Box,
  Paper,
  Typography,
  TextField,
  Button,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  FormGroup,
  FormControlLabel,
  Checkbox,
  Alert,
  CircularProgress,
  Stepper,
  Step,
  StepLabel,
  Grid,
  IconButton,
  Tooltip,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  LinearProgress,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Chip,
  List,
  ListItem,
  ListItemText,
  Divider
} from '@mui/material';
import {
  Preview as PreviewIcon,
  Add as AddIcon,
  Remove as RemoveIcon,
  Refresh as RefreshIcon,
  Info as InfoIcon,
  Lock as LockIcon,
} from '@mui/icons-material';
import axios from 'axios';
import { styled } from '@mui/material/styles';

const API_BASE_URL = 'http://localhost:8000';

const steps = ['Configure Source', 'Select Data', 'Preview', 'Transfer'];

const Root = styled(Box)(({ theme }) => ({
  padding: theme.spacing(3),
}));

const Form = styled('form')(({ theme }) => ({
  display: 'flex',
  flexDirection: 'column',
  gap: theme.spacing(2),
}));

const ButtonGroup = styled(Box)(({ theme }) => ({
  display: 'flex',
  gap: theme.spacing(2),
  marginTop: theme.spacing(2),
}));

const PreviewTable = styled(TableContainer)(({ theme }) => ({
  marginTop: theme.spacing(2),
}));

const TypeChip = styled(Chip)(({ theme }) => ({
  margin: theme.spacing(0.5),
}));

const ProgressContainer = styled(Box)(({ theme }) => ({
  marginTop: theme.spacing(2),
}));

const SchemaInfo = styled(Paper)(({ theme }) => ({
  marginTop: theme.spacing(2),
  padding: theme.spacing(2),
}));

function DataIngestion() {
  const [activeStep, setActiveStep] = useState(0);
  const [sourceType, setSourceType] = useState('clickhouse');
  const [clickhouseConfig, setClickhouseConfig] = useState({
    host: 'localhost',
    port: '9000',
    database: 'test_data',
    username: 'default',
    password: '',
  });
  const [flatFileConfig, setFlatFileConfig] = useState({
    filePath: '',
    delimiter: ',',
  });
  const [tables, setTables] = useState([]);
  const [selectedTables, setSelectedTables] = useState([]);
  const [joinConditions, setJoinConditions] = useState([]);
  const [columns, setColumns] = useState([]);
  const [selectedColumns, setSelectedColumns] = useState([]);
  const [status, setStatus] = useState({ type: '', message: '' });
  const [loading, setLoading] = useState(false);
  const [previewData, setPreviewData] = useState(null);
  const [previewOpen, setPreviewOpen] = useState(false);
  const [progress, setProgress] = useState(0);
  const [transferId, setTransferId] = useState('');
  const [previewInfo, setPreviewInfo] = useState(null);
  const [jwtToken, setJwtToken] = useState('');
  const [showTokenDialog, setShowTokenDialog] = useState(false);
  const [showJoinDialog, setShowJoinDialog] = useState(false);
  const [token, setToken] = useState('');
  const [selectedTable, setSelectedTable] = useState('');
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  const [transferStatus, setTransferStatus] = useState('');

  const handleClickhouseConfigChange = (field) => (event) => {
    setClickhouseConfig({
      ...clickhouseConfig,
      [field]: event.target.value,
    });
  };

  const handleFlatFileConfigChange = (field) => (event) => {
    setFlatFileConfig({
      ...flatFileConfig,
      [field]: event.target.value,
    });
  };

  const handleColumnToggle = (column) => () => {
    setSelectedColumns((prev) =>
      prev.includes(column)
        ? prev.filter((c) => c !== column)
        : [...prev, column]
    );
  };

  const handleTableToggle = (table) => () => {
    setSelectedTables((prev) =>
      prev.includes(table)
        ? prev.filter((t) => t !== table)
        : [...prev, table]
    );
  };

  const addJoinCondition = () => {
    setJoinConditions([...joinConditions, { leftTable: '', rightTable: '', leftKey: '', rightKey: '' }]);
  };

  const removeJoinCondition = (index) => {
    setJoinConditions(joinConditions.filter((_, i) => i !== index));
  };

  const updateJoinCondition = (index, field, value) => {
    const newConditions = [...joinConditions];
    newConditions[index] = { ...newConditions[index], [field]: value };
    setJoinConditions(newConditions);
  };

  const connectToSource = async () => {
    try {
      setLoading(true);
      setStatus({ type: 'info', message: 'Connecting...' });
      if (sourceType === 'clickhouse') {
        const config = {
          host: clickhouseConfig.host,
          port: parseInt(clickhouseConfig.port),
          database: clickhouseConfig.database,
          user: clickhouseConfig.username,
          jwt_token: clickhouseConfig.password
        };
        const response = await axios.post(`${API_BASE_URL}/connect/clickhouse`, config);
        setStatus({ type: 'success', message: response.data.message });
        setActiveStep(1);
      } else {
        // For flat file, we just need to verify the file exists
        setStatus({ type: 'success', message: 'File source ready' });
        setActiveStep(1);
      }
    } catch (error) {
      setStatus({ type: 'error', message: error.response?.data?.message || 'Connection failed' });
    } finally {
      setLoading(false);
    }
  };

  const fetchTables = async () => {
    try {
      setLoading(true);
      setStatus({ type: 'info', message: 'Fetching tables...' });
      const config = {
        host: clickhouseConfig.host,
        port: parseInt(clickhouseConfig.port),
        database: clickhouseConfig.database,
        user: clickhouseConfig.username,
        jwt_token: clickhouseConfig.password
      };
      const response = await axios.post(`${API_BASE_URL}/tables`, config);
      setTables(response.data.tables);
      setStatus({ type: 'success', message: 'Tables fetched successfully' });
    } catch (error) {
      setStatus({ type: 'error', message: error.response?.data?.message || 'Failed to fetch tables' });
    } finally {
      setLoading(false);
    }
  };

  const fetchColumns = async () => {
    if (!selectedTables.length) return;
    try {
      setLoading(true);
      setStatus({ type: 'info', message: 'Fetching columns...' });
      const config = {
        host: clickhouseConfig.host,
        port: parseInt(clickhouseConfig.port),
        database: clickhouseConfig.database,
        user: clickhouseConfig.username,
        jwt_token: clickhouseConfig.password
      };
      const response = await axios.get(`${API_BASE_URL}/columns/${selectedTables[0]}`, {
        params: config,
      });
      setColumns(response.data.columns);
      setSelectedColumns([]);
      setStatus({ type: 'success', message: 'Columns fetched successfully' });
    } catch (error) {
      setStatus({ type: 'error', message: error.response?.data?.message || 'Failed to fetch columns' });
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    let interval;
    if (loading && transferId) {
      interval = setInterval(async () => {
        try {
          const response = await axios.get(`${API_BASE_URL}/progress/${transferId}`);
          setProgress(response.data.progress);
        } catch (error) {
          console.error('Error fetching progress:', error);
        }
      }, 1000);
    }
    return () => {
      if (interval) clearInterval(interval);
    };
  }, [loading, transferId]);

  const handlePreview = async () => {
    try {
      setLoading(true);
      const response = await axios.get(`${API_BASE_URL}/preview`, {
        params: {
          source: sourceType,
          table: selectedTables[0],
          columns: selectedColumns.join(',')
        }
      });
      setPreviewData(response.data.data);
      setPreviewInfo({
        totalCount: response.data.total_count,
        columns: response.data.columns
      });
    } catch (error) {
      setStatus({ type: 'error', message: 'Failed to fetch preview data' });
    } finally {
      setLoading(false);
    }
  };

  const handleTransfer = async (source, target) => {
    if (!token || selectedTables.length === 0 || selectedColumns.length === 0) {
      setError('Please login, select tables, and choose columns first');
      return;
    }

    try {
      setLoading(true);
      setProgress(0);
      setTransferStatus('Starting transfer...');

      const response = await fetch('http://localhost:8001/api/transfer', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          source,
          target,
          tables: selectedTables,
          columns: selectedColumns,
          joinConditions: joinConditions.length > 0 ? joinConditions : undefined
        })
      });

      // Simulate progress updates
      const interval = setInterval(() => {
        setProgress((prev) => {
          if (prev >= 90) {
            clearInterval(interval);
            return prev;
          }
          return prev + 10;
        });
      }, 1000);

      const data = await response.json();
      clearInterval(interval);
      setProgress(100);
      setTransferStatus('Transfer completed!');
      setSuccess(data.message);
      setError('');
    } catch (err) {
      setError('Failed to transfer data');
      setTransferStatus('Transfer failed');
    } finally {
      setLoading(false);
    }
  };

  const handleNext = () => {
    setActiveStep((prevStep) => prevStep + 1);
  };

  const handleBack = () => {
    setActiveStep((prevStep) => prevStep - 1);
  };

  // Generate a JWT token
  const generateToken = async () => {
    try {
      setLoading(true);
      setStatus({ type: 'info', message: 'Generating token...' });
      const response = await axios.post(`${API_BASE_URL}/generate-token`, {
        username: clickhouseConfig.username,
        password: clickhouseConfig.password
      });
      const token = response.data.token;
      setJwtToken(token);
      localStorage.setItem('jwtToken', token);
      setClickhouseConfig(prev => ({
        ...prev,
        password: token
      }));
      setStatus({ type: 'success', message: 'Token generated successfully' });
    } catch (error) {
      setStatus({ type: 'error', message: error.response?.data?.message || 'Failed to generate token' });
    } finally {
      setLoading(false);
    }
  };

  // Load token from localStorage on component mount
  useEffect(() => {
    const savedToken = localStorage.getItem('jwtToken');
    if (savedToken) {
      setJwtToken(savedToken);
    }
  }, []);

  // Login and get token
  const handleLogin = async () => {
    try {
      const response = await fetch('http://localhost:8001/api/auth', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          username: 'test',
          password: 'test'
        })
      });
      const data = await response.json();
      setToken(data.access_token);
      setError('');
    } catch (err) {
      setError('Failed to login');
    }
  };

  // Load tables
  const loadTables = async () => {
    if (!token) {
      setError('Please login first');
      return;
    }

    try {
      setLoading(true);
      const response = await fetch('http://localhost:8001/api/tables', {
        headers: {
          'Authorization': `Bearer ${token}`
        }
      });
      const data = await response.json();
      setTables(data.tables);
      setError('');
    } catch (err) {
      setError('Failed to load tables');
    } finally {
      setLoading(false);
    }
  };

  // Load columns
  const loadColumns = async (table) => {
    if (!token) {
      setError('Please login first');
      return;
    }

    try {
      setLoading(true);
      const response = await fetch(`http://localhost:8001/api/columns/${table}`, {
        headers: {
          'Authorization': `Bearer ${token}`
        }
      });
      const data = await response.json();
      setColumns(data.columns);
      setSelectedColumns([]);
      setError('');
    } catch (err) {
      setError('Failed to load columns');
    } finally {
      setLoading(false);
    }
  };

  const handleTableSelect = (table) => {
    if (selectedTables.includes(table)) {
      setSelectedTables(selectedTables.filter(t => t !== table));
    } else {
      setSelectedTables([...selectedTables, table]);
    }
  };

  return (
    <Root>
      <Paper elevation={3} style={{ padding: '20px' }}>
        <Typography variant="h5" gutterBottom>
          Data Ingestion Tool
          <Tooltip title="JWT Token Management">
            <IconButton onClick={() => setShowTokenDialog(true)}>
              <LockIcon />
            </IconButton>
          </Tooltip>
        </Typography>

        {!token ? (
          <Button variant="contained" onClick={handleLogin}>
            Login
          </Button>
        ) : (
          <>
            <Button variant="contained" onClick={loadTables} sx={{ mb: 2 }}>
              Load Tables
            </Button>

            {tables.length > 0 && (
              <>
                <Typography variant="h6" gutterBottom>
                  Select Tables
                </Typography>
                <List>
                  {tables.map((table) => (
                    <ListItem key={table}>
                      <Checkbox
                        checked={selectedTables.includes(table)}
                        onChange={() => handleTableSelect(table)}
                      />
                      <ListItemText primary={table} />
                    </ListItem>
                  ))}
                </List>

                {selectedTables.length > 1 && (
                  <Button
                    variant="outlined"
                    onClick={() => setShowJoinDialog(true)}
                    sx={{ mb: 2 }}
                  >
                    Configure Table Joins
                  </Button>
                )}
              </>
            )}

            {columns.length > 0 && (
              <>
                <Typography variant="h6" gutterBottom>
                  Select Columns
                </Typography>
                <List>
                  {columns.map((column) => (
                    <ListItem key={column.name}>
                      <Checkbox
                        checked={selectedColumns.includes(column.name)}
                        onChange={(e) => {
                          if (e.target.checked) {
                            setSelectedColumns([...selectedColumns, column.name]);
                          } else {
                            setSelectedColumns(selectedColumns.filter(col => col !== column.name));
                          }
                        }}
                      />
                      <ListItemText primary={column.name} secondary={column.type} />
                    </ListItem>
                  ))}
                </List>

                <Box sx={{ mt: 2 }}>
                  <Button
                    variant="contained"
                    onClick={handlePreview}
                    sx={{ mr: 2 }}
                    disabled={loading}
                  >
                    Preview Data
                  </Button>
                  <Button
                    variant="contained"
                    onClick={() => handleTransfer('clickhouse', 'flatfile')}
                    sx={{ mr: 2 }}
                    disabled={loading}
                  >
                    Export to Flat File
                  </Button>
                  <Button
                    variant="contained"
                    onClick={() => handleTransfer('flatfile', 'clickhouse')}
                    disabled={loading}
                  >
                    Import from Flat File
                  </Button>
                </Box>
              </>
            )}
          </>
        )}

        {loading && (
          <Box sx={{ mt: 2 }}>
            <Typography variant="body2" color="textSecondary" gutterBottom>
              {transferStatus}
            </Typography>
            <LinearProgress variant="determinate" value={progress} sx={{ mb: 1 }} />
            <Typography variant="body2" color="textSecondary" align="right">
              {progress}%
            </Typography>
          </Box>
        )}

        {error && <Alert severity="error" sx={{ mt: 2 }}>{error}</Alert>}
        {success && <Alert severity="success" sx={{ mt: 2 }}>{success}</Alert>}

        {previewData && (
          <PreviewTable component={PreviewTable}>
            <Table>
              <TableHead>
                <TableRow>
                  {selectedColumns.map((column) => (
                    <TableCell key={column}>
                      {column}
                      <Tooltip title={previewInfo?.columns.find(c => c.name === column)?.type || 'Unknown type'}>
                        <InfoIcon fontSize="small" style={{ marginLeft: 5 }} />
                      </Tooltip>
                    </TableCell>
                  ))}
                </TableRow>
              </TableHead>
              <TableBody>
                {previewData.map((row, index) => (
                  <TableRow key={index}>
                    {selectedColumns.map((column) => (
                      <TableCell key={column}>{row[column]?.toString() || ''}</TableCell>
                    ))}
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </PreviewTable>
        )}
      </Paper>

      <Dialog open={showTokenDialog} onClose={() => setShowTokenDialog(false)}>
        <DialogTitle>JWT Token Management</DialogTitle>
        <DialogContent>
          <TextField
            fullWidth
            label="Current Token"
            value={jwtToken}
            disabled
            margin="normal"
          />
          <Typography variant="body2" color="textSecondary">
            Token will be automatically included in all requests.
          </Typography>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setShowTokenDialog(false)}>Close</Button>
          <Button onClick={generateToken} color="primary">
            Generate New Token
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog open={showJoinDialog} onClose={() => setShowJoinDialog(false)}>
        <DialogTitle>Configure Table Joins</DialogTitle>
        <DialogContent>
          {joinConditions.map((condition, index) => (
            <Box key={index} sx={{ display: 'flex', gap: 2, mb: 2 }}>
              <FormControl>
                <InputLabel>Left Table</InputLabel>
                <Select
                  value={condition.leftTable}
                  onChange={(e) => updateJoinCondition(index, 'leftTable', e.target.value)}
                >
                  {selectedTables.map((table) => (
                    <MenuItem key={table} value={table}>{table}</MenuItem>
                  ))}
                </Select>
              </FormControl>
              <TextField
                label="Left Key"
                value={condition.leftKey}
                onChange={(e) => updateJoinCondition(index, 'leftKey', e.target.value)}
              />
              <FormControl>
                <InputLabel>Right Table</InputLabel>
                <Select
                  value={condition.rightTable}
                  onChange={(e) => updateJoinCondition(index, 'rightTable', e.target.value)}
                >
                  {selectedTables.map((table) => (
                    <MenuItem key={table} value={table}>{table}</MenuItem>
                  ))}
                </Select>
              </FormControl>
              <TextField
                label="Right Key"
                value={condition.rightKey}
                onChange={(e) => updateJoinCondition(index, 'rightKey', e.target.value)}
              />
              <IconButton onClick={() => removeJoinCondition(index)}>
                <RemoveIcon />
              </IconButton>
            </Box>
          ))}
          <Button
            variant="outlined"
            startIcon={<AddIcon />}
            onClick={addJoinCondition}
            sx={{ mt: 2 }}
          >
            Add Join Condition
          </Button>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setShowJoinDialog(false)}>Cancel</Button>
          <Button onClick={() => setShowJoinDialog(false)} color="primary">
            Save
          </Button>
        </DialogActions>
      </Dialog>
    </Root>
  );
}

export default DataIngestion; 