import React, { useState } from 'react';
import {
  Box,
  Typography,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  Paper,
} from '@mui/material';
import ClickHouseConfig from './ClickHouseConfig';
import FlatFileConfig from './FlatFileConfig';

const HomePage = () => {
  const [selectedMode, setSelectedMode] = useState('');

  const handleModeChange = (event) => {
    setSelectedMode(event.target.value);
  };

  return (
    <Box sx={{ p: 3 }}>
      <Paper elevation={3} sx={{ p: 3, mb: 3 }}>
        <Typography variant="h5" gutterBottom>
          Data Ingestion Tool
        </Typography>
        <FormControl fullWidth sx={{ mb: 3 }}>
          <InputLabel>Select Mode</InputLabel>
          <Select
            value={selectedMode}
            onChange={handleModeChange}
            label="Select Mode"
          >
            <MenuItem value="ch-to-file">ClickHouse → Flat File</MenuItem>
            <MenuItem value="file-to-ch">Flat File → ClickHouse</MenuItem>
          </Select>
        </FormControl>

        {selectedMode === 'ch-to-file' && <ClickHouseConfig mode="export" />}
        {selectedMode === 'file-to-ch' && <FlatFileConfig />}
      </Paper>
    </Box>
  );
};

export default HomePage; 