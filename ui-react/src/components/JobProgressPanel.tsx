// ui-react/src/components/JobProgressPanel.tsx
// Dashboard panel that shows live job/scheduler progress when in cloud mode
import React, { useState } from 'react'
import {
  Box,
  Typography,
  Chip,
  LinearProgress,
  IconButton,
  Tooltip,
  Stack,
  Switch,
  alpha,
  Divider,
  Alert,
} from '@mui/material'
import PlayArrowIcon from '@mui/icons-material/PlayArrow'
import PauseIcon from '@mui/icons-material/Pause'
import RefreshIcon from '@mui/icons-material/Refresh'
import CheckCircleIcon from '@mui/icons-material/CheckCircle'
import ErrorIcon from '@mui/icons-material/Error'
import ScheduleIcon from '@mui/icons-material/Schedule'
import CloudOffIcon from '@mui/icons-material/CloudOff'
import { useCloud } from '../context/CloudContext'
import { runJobNow, enableJob, disableJob, type SchedulerJob } from '../api'

function formatDuration(ms: number | null): string {
  if (ms === null) return '—'
  if (ms < 1000) return `${ms.toFixed(0)}ms`
  return `${(ms / 1000).toFixed(1)}s`
}

function formatInterval(seconds: number): string {
  if (seconds < 60) return `${seconds}s`
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`
  return `${(seconds / 3600).toFixed(1)}h`
}

function formatTimeAgo(iso: string | null): string {
  if (!iso) return 'never'
  const diff = Date.now() - new Date(iso).getTime()
  const secs = Math.floor(diff / 1000)
  if (secs < 60) return `${secs}s ago`
  if (secs < 3600) return `${Math.floor(secs / 60)}m ago`
  return `${Math.floor(secs / 3600)}h ago`
}

function StatusIcon({ status }: { status: string | null }) {
  if (status === 'ok') return <CheckCircleIcon sx={{ fontSize: 16, color: 'success.main' }} />
  if (status === 'error') return <ErrorIcon sx={{ fontSize: 16, color: 'error.main' }} />
  return <ScheduleIcon sx={{ fontSize: 16, color: 'text.disabled' }} />
}

function JobRow({ job, onRefresh }: { job: SchedulerJob; onRefresh: () => void }) {
  const [running, setRunning] = useState(false)

  const handleRunNow = async () => {
    setRunning(true)
    try {
      await runJobNow(job.name)
      onRefresh()
    } finally {
      setRunning(false)
    }
  }

  const handleToggleEnabled = async () => {
    if (job.enabled) {
      await disableJob(job.name)
    } else {
      await enableJob(job.name)
    }
    onRefresh()
  }

  return (
    <Box
      sx={{
        display: 'flex',
        alignItems: 'center',
        gap: 1,
        py: 0.75,
        px: 1,
        borderRadius: 1,
        '&:hover': { bgcolor: 'action.hover' },
        opacity: job.enabled ? 1 : 0.5,
      }}
    >
      <StatusIcon status={job.last_status} />

      <Box sx={{ flex: 1, minWidth: 0 }}>
        <Typography variant="body2" sx={{ fontWeight: 600, fontSize: 12 }}>
          {job.name.replace(/_/g, ' ')}
        </Typography>
        <Typography variant="caption" color="text.secondary" sx={{ fontSize: 10 }}>
          every {formatInterval(job.interval_seconds)} · {formatTimeAgo(job.last_run)}
          {job.last_duration_ms !== null && ` · ${formatDuration(job.last_duration_ms)}`}
        </Typography>
      </Box>

      <Chip
        label={`${job.run_count}`}
        size="small"
        sx={{ height: 18, fontSize: 9, minWidth: 30 }}
      />

      {job.error_count > 0 && (
        <Tooltip title={job.last_error || 'Errors occurred'}>
          <Chip
            label={`${job.error_count} err`}
            size="small"
            color="error"
            variant="outlined"
            sx={{ height: 18, fontSize: 9 }}
          />
        </Tooltip>
      )}

      <Tooltip title={job.enabled ? 'Disable' : 'Enable'}>
        <Switch
          checked={job.enabled}
          onChange={handleToggleEnabled}
          size="small"
          sx={{ transform: 'scale(0.7)' }}
        />
      </Tooltip>

      <Tooltip title="Run now">
        <IconButton size="small" onClick={handleRunNow} disabled={running}>
          {running ? (
            <RefreshIcon sx={{ fontSize: 14, animation: 'spin 1s linear infinite', '@keyframes spin': { '100%': { transform: 'rotate(360deg)' } } }} />
          ) : (
            <PlayArrowIcon sx={{ fontSize: 14 }} />
          )}
        </IconButton>
      </Tooltip>
    </Box>
  )
}

export default function JobProgressPanel() {
  const { mode, scheduler, refreshScheduler, toggleScheduler } = useCloud()

  if (mode !== 'cloud') {
    return (
      <Box
        sx={{
          height: '100%',
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          justifyContent: 'center',
          color: 'text.secondary',
          gap: 1,
          p: 2,
        }}
      >
        <CloudOffIcon sx={{ fontSize: 40, opacity: 0.3 }} />
        <Typography variant="body2">Cloud mode is not active</Typography>
        <Typography variant="caption" color="text.disabled">
          Switch to Cloud mode using the toggle in the header
        </Typography>
      </Box>
    )
  }

  if (!scheduler) {
    return (
      <Box sx={{ p: 2 }}>
        <LinearProgress />
        <Typography variant="caption" sx={{ mt: 1, display: 'block' }}>
          Loading scheduler status...
        </Typography>
      </Box>
    )
  }

  const okCount = scheduler.jobs.filter(j => j.last_status === 'ok').length
  const errCount = scheduler.jobs.filter(j => j.last_status === 'error').length
  const totalRuns = scheduler.jobs.reduce((s, j) => s + j.run_count, 0)

  return (
    <Box sx={{ height: '100%', display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
      {/* Header stats */}
      <Box sx={{ px: 1.5, py: 1, borderBottom: '1px solid', borderColor: 'divider' }}>
        <Stack direction="row" alignItems="center" justifyContent="space-between">
          <Stack direction="row" spacing={0.5} alignItems="center">
            <Chip
              label={scheduler.running ? 'Running' : 'Stopped'}
              size="small"
              color={scheduler.running ? 'success' : 'default'}
              sx={{ height: 20, fontSize: 10 }}
            />
            <Typography variant="caption" color="text.secondary">
              {scheduler.job_count} jobs · {totalRuns} runs
            </Typography>
          </Stack>

          <Stack direction="row" spacing={0.5}>
            <Tooltip title={scheduler.running ? 'Stop scheduler' : 'Start scheduler'}>
              <IconButton size="small" onClick={toggleScheduler}>
                {scheduler.running ? (
                  <PauseIcon sx={{ fontSize: 16 }} />
                ) : (
                  <PlayArrowIcon sx={{ fontSize: 16 }} />
                )}
              </IconButton>
            </Tooltip>
            <Tooltip title="Refresh">
              <IconButton size="small" onClick={refreshScheduler}>
                <RefreshIcon sx={{ fontSize: 16 }} />
              </IconButton>
            </Tooltip>
          </Stack>
        </Stack>

        {/* Summary bar */}
        {scheduler.jobs.length > 0 && (
          <Box sx={{ mt: 0.5, display: 'flex', gap: 0.5 }}>
            {okCount > 0 && (
              <Chip
                icon={<CheckCircleIcon sx={{ fontSize: '12px !important' }} />}
                label={okCount}
                size="small"
                color="success"
                variant="outlined"
                sx={{ height: 18, fontSize: 9 }}
              />
            )}
            {errCount > 0 && (
              <Chip
                icon={<ErrorIcon sx={{ fontSize: '12px !important' }} />}
                label={errCount}
                size="small"
                color="error"
                variant="outlined"
                sx={{ height: 18, fontSize: 9 }}
              />
            )}
          </Box>
        )}
      </Box>

      {/* Job list */}
      <Box sx={{ flex: 1, overflow: 'auto', py: 0.5 }}>
        {scheduler.jobs.length === 0 ? (
          <Alert severity="info" sx={{ m: 1 }}>
            No scheduled jobs configured
          </Alert>
        ) : (
          scheduler.jobs.map((job) => (
            <JobRow key={job.name} job={job} onRefresh={refreshScheduler} />
          ))
        )}
      </Box>
    </Box>
  )
}