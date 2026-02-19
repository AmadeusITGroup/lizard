// ui-react/src/components/CloudModeToggle.tsx
import React from 'react'
import {
  Box,
  Switch,
  Typography,
  Tooltip,
  CircularProgress,
  Chip,
  alpha,
} from '@mui/material'
import CloudIcon from '@mui/icons-material/Cloud'
import ComputerIcon from '@mui/icons-material/Computer'
import { useCloud } from '../context/CloudContext'

export default function CloudModeToggle() {
  const { mode, loading, error, scheduler, switchMode } = useCloud()
  const isCloud = mode === 'cloud'

  const handleToggle = () => {
    switchMode(isCloud ? 'local' : 'cloud')
  }

  return (
    <Tooltip
      title={
        error
          ? `Error: ${error}`
          : isCloud
            ? `Cloud Mode — Scheduler: ${scheduler?.running ? 'Running' : 'Stopped'} (${scheduler?.job_count ?? 0} jobs)`
            : 'Local Mode — Click to enable Cloud'
      }
    >
      <Box
        sx={{
          display: 'flex',
          alignItems: 'center',
          gap: 0.5,
          px: 1,
          py: 0.25,
          borderRadius: 2,
          bgcolor: isCloud ? alpha('#4CAF50', 0.15) : 'transparent',
          transition: 'background-color 0.3s',
        }}
      >
        <ComputerIcon sx={{ fontSize: 16, opacity: isCloud ? 0.4 : 1 }} />

        {loading ? (
          <CircularProgress size={18} color="inherit" />
        ) : (
          <Switch
            checked={isCloud}
            onChange={handleToggle}
            size="small"
            sx={{
              '& .MuiSwitch-switchBase.Mui-checked': {
                color: '#4CAF50',
              },
              '& .MuiSwitch-switchBase.Mui-checked + .MuiSwitch-track': {
                backgroundColor: '#4CAF50',
              },
            }}
          />
        )}

        <CloudIcon sx={{ fontSize: 16, opacity: isCloud ? 1 : 0.4 }} />

        <Typography
          variant="caption"
          sx={{
            fontWeight: 600,
            fontSize: 10,
            textTransform: 'uppercase',
            letterSpacing: 0.5,
            minWidth: 36,
          }}
        >
          {isCloud ? 'Cloud' : 'Local'}
        </Typography>

        {isCloud && scheduler?.running && (
          <Chip
            label={`${scheduler.jobs.filter(j => j.last_status === 'ok').length}/${scheduler.job_count}`}
            size="small"
            sx={{
              height: 18,
              fontSize: 9,
              bgcolor: alpha('#4CAF50', 0.2),
              color: '#4CAF50',
            }}
          />
        )}
      </Box>
    </Tooltip>
  )
}