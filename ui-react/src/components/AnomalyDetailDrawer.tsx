// ui-react/src/components/AnomalyDetailDrawer.tsx
import React from 'react'
import {
  Drawer,
  Box,
  Typography,
  IconButton,
  Chip,
  Stack,
  Divider,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Alert,
  LinearProgress,
  Tooltip,
  Card,
  CardContent,
} from '@mui/material'
import CloseIcon from '@mui/icons-material/Close'
import WarningAmberIcon from '@mui/icons-material/WarningAmber'
import ErrorIcon from '@mui/icons-material/Error'
import InfoIcon from '@mui/icons-material/Info'
import PersonIcon from '@mui/icons-material/Person'
import DevicesIcon from '@mui/icons-material/Devices'
import LocationOnIcon from '@mui/icons-material/LocationOn'
import AccessTimeIcon from '@mui/icons-material/AccessTime'
import type { TimelineAnomalyEvent } from '../api'

// Severity color mapping
const SEVERITY_CONFIG = {
  critical: { color: '#9C27B0', bg: '#F3E5F5', label: 'Critical', icon: ErrorIcon },
  high: { color: '#F44336', bg: '#FFEBEE', label: 'High', icon: ErrorIcon },
  medium: { color: '#FF9800', bg:  '#FFF3E0', label: 'Medium', icon: WarningAmberIcon },
  low:  { color: '#FFC107', bg: '#FFFDE7', label: 'Low', icon: WarningAmberIcon },
  normal: { color: '#4CAF50', bg: '#E8F5E9', label: 'Normal', icon: InfoIcon },
}

function getSeverity(score: number): keyof typeof SEVERITY_CONFIG {
  if (score >= 0.9) return 'critical'
  if (score >= 0.75) return 'high'
  if (score >= 0.5) return 'medium'
  if (score >= 0.25) return 'low'
  return 'normal'
}

interface AnomalyDetailDrawerProps {
  open: boolean
  onClose: () => void
  bucketTime:  string | null
  score: number
  reasons: string[]
  explain: string
  events: TimelineAnomalyEvent[]
  thresholds?:  {
    mode?:  'simple' | 'advanced'
    z_thr?: number
    score_thr?: number
    contamination?: number
    score_quantile?: number
  }
}

export default function AnomalyDetailDrawer({
  open,
  onClose,
  bucketTime,
  score,
  reasons,
  explain,
  events,
  thresholds,
}: AnomalyDetailDrawerProps) {
  const severity = getSeverity(score)
  const config = SEVERITY_CONFIG[severity]
  const SeverityIcon = config.icon

  const formatTimestamp = (ts: string | null) => {
    if (!ts) return '—'
    try {
      return new Date(ts).toLocaleString()
    } catch {
      return ts
    }
  }

  return (
    <Drawer
      anchor="right"
      open={open}
      onClose={onClose}
      PaperProps={{
        sx: { width: { xs: '100%', sm:  520, md: 620 }, p: 0 },
      }}
    >
      {/* Header */}
      <Box
        sx={{
          p: 2,
          background: `linear-gradient(135deg, ${config.color}15 0%, ${config.bg} 100%)`,
          borderBottom: `3px solid ${config.color}`,
        }}
      >
        <Stack direction="row" justifyContent="space-between" alignItems="flex-start">
          <Stack spacing={1}>
            <Stack direction="row" spacing={1} alignItems="center">
              <SeverityIcon sx={{ color: config.color, fontSize: 28 }} />
              <Typography variant="h6" fontWeight={700}>
                Anomaly Details
              </Typography>
            </Stack>
            <Typography variant="body2" color="text.secondary">
              <AccessTimeIcon sx={{ fontSize: 14, mr: 0.5, verticalAlign: 'middle' }} />
              {formatTimestamp(bucketTime)}
            </Typography>
          </Stack>
          <IconButton onClick={onClose} size="small">
            <CloseIcon />
          </IconButton>
        </Stack>
      </Box>

      <Box sx={{ p: 2, overflowY: 'auto', height: 'calc(100% - 100px)' }}>
        {/* Score Card */}
        <Card variant="outlined" sx={{ mb: 2, borderColor: config.color }}>
          <CardContent>
            <Stack direction="row" justifyContent="space-between" alignItems="center" mb={1}>
              <Typography variant="subtitle2" color="text.secondary">
                Anomaly Score
              </Typography>
              <Chip
                label={config.label}
                size="small"
                sx={{
                  bgcolor: config.color,
                  color: '#fff',
                  fontWeight: 600,
                }}
              />
            </Stack>
            <Stack direction="row" alignItems="flex-end" spacing={1}>
              <Typography variant="h3" fontWeight={700} color={config.color}>
                {(score * 100).toFixed(1)}%
              </Typography>
              <Typography variant="body2" color="text.secondary" pb={0.5}>
                ({score.toFixed(4)})
              </Typography>
            </Stack>
            <LinearProgress
              variant="determinate"
              value={score * 100}
              sx={{
                mt: 1.5,
                height: 8,
                borderRadius: 4,
                bgcolor: '#e0e0e0',
                '& .MuiLinearProgress-bar': {
                  bgcolor: config.color,
                  borderRadius: 4,
                },
              }}
            />
            {thresholds && (
              <Typography variant="caption" color="text.secondary" sx={{ mt: 1, display: 'block' }}>
                {thresholds.mode === 'simple'
                  ? `Threshold: z=${thresholds.z_thr} (score ≥ ${((thresholds.score_thr || 0) * 100).toFixed(1)}%)`
                  : `Contamination: ${((thresholds.contamination || 0) * 100).toFixed(1)}% (score quantile ≥ ${((thresholds.score_quantile || 0) * 100).toFixed(1)}%)`}
              </Typography>
            )}
          </CardContent>
        </Card>

        {/* Explanation */}
        {explain && (
          <Alert severity="warning" sx={{ mb: 2 }} icon={<WarningAmberIcon />}>
            <Typography variant="body2">{explain}</Typography>
          </Alert>
        )}

        {/* Reasons */}
        {reasons.length > 0 && (
          <Box mb={2}>
            <Typography variant="subtitle2" gutterBottom fontWeight={600}>
              Anomaly Reasons
            </Typography>
            <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
              {reasons.map((reason, i) => (
                <Chip
                  key={i}
                  label={reason}
                  size="small"
                  variant="outlined"
                  color="warning"
                  sx={{ mb: 0.5 }}
                />
              ))}
            </Stack>
          </Box>
        )}

        <Divider sx={{ my: 2 }} />

        {/* Events Table */}
        <Typography variant="subtitle2" gutterBottom fontWeight={600}>
          Events in this time bucket ({events.length})
        </Typography>

        {events.length === 0 ? (
          <Alert severity="info">No detailed events available for this bucket.</Alert>
        ) : (
          <TableContainer component={Paper} variant="outlined" sx={{ maxHeight: 400 }}>
            <Table size="small" stickyHeader>
              <TableHead>
                <TableRow>
                  <TableCell sx={{ fontWeight: 600, bgcolor: '#f5f5f5' }}>Time</TableCell>
                  <TableCell sx={{ fontWeight: 600, bgcolor: '#f5f5f5' }}>User</TableCell>
                  <TableCell sx={{ fontWeight: 600, bgcolor: '#f5f5f5' }}>Event</TableCell>
                  <TableCell sx={{ fontWeight: 600, bgcolor: '#f5f5f5' }}>Score</TableCell>
                  <TableCell sx={{ fontWeight: 600, bgcolor: '#f5f5f5' }}>Details</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {events.map((event, idx) => {
                  const evtSeverity = getSeverity(event.anom_score)
                  const evtConfig = SEVERITY_CONFIG[evtSeverity]
                  return (
                    <TableRow
                      key={idx}
                      sx={{
                        bgcolor: event.anom_score >= 0.5 ? `${evtConfig.color}08` : 'inherit',
                        '&:hover': { bgcolor: `${evtConfig.color}15` },
                      }}
                    >
                      <TableCell sx={{ fontSize: 12 }}>
                        {event.ts ? new Date(event.ts).toLocaleTimeString() : '—'}
                      </TableCell>
                      <TableCell>
                        <Tooltip title={event.user_id || '—'}>
                          <Stack direction="row" spacing={0.5} alignItems="center">
                            <PersonIcon sx={{ fontSize: 14, color: 'text.secondary' }} />
                            <Typography variant="body2" noWrap sx={{ maxWidth: 80 }}>
                              {event.user_id || '—'}
                            </Typography>
                          </Stack>
                        </Tooltip>
                      </TableCell>
                      <TableCell>
                        <Chip
                          label={event.event_type || 'unknown'}
                          size="small"
                          variant="outlined"
                          sx={{ fontSize: 11 }}
                        />
                      </TableCell>
                      <TableCell>
                        <Chip
                          label={`${(event.anom_score * 100).toFixed(0)}%`}
                          size="small"
                          sx={{
                            bgcolor: evtConfig.color,
                            color: '#fff',
                            fontWeight: 600,
                            fontSize: 11,
                          }}
                        />
                      </TableCell>
                      <TableCell sx={{ fontSize: 11 }}>
                        <Stack direction="row" spacing={0.5} flexWrap="wrap">
                          {event.ip && (
                            <Tooltip title={`IP: ${event.ip}`}>
                              <Chip label={event.ip} size="small" sx={{ fontSize: 10 }} />
                            </Tooltip>
                          )}
                          {event.country && (
                            <Tooltip title={`${event.city || ''}, ${event.country}`}>
                              <Chip
                                icon={<LocationOnIcon sx={{ fontSize: 12 }} />}
                                label={event.country}
                                size="small"
                                sx={{ fontSize: 10 }}
                              />
                            </Tooltip>
                          )}
                          {event.device_id && (
                            <Tooltip title={`Device: ${event.device_id}`}>
                              <Chip
                                icon={<DevicesIcon sx={{ fontSize: 12 }} />}
                                label="Device"
                                size="small"
                                sx={{ fontSize:  10 }}
                              />
                            </Tooltip>
                          )}
                        </Stack>
                      </TableCell>
                    </TableRow>
                  )
                })}
              </TableBody>
            </Table>
          </TableContainer>
        )}

        {/* Event Reasons Details */}
        {events.some((e) => e.reasons && e.reasons.length > 0) && (
          <Box mt={2}>
            <Typography variant="subtitle2" gutterBottom fontWeight={600}>
              Detailed Reasons by Event
            </Typography>
            <Stack spacing={1}>
              {events
                .filter((e) => e.reasons && e.reasons.length > 0)
                .slice(0, 5)
                .map((event, idx) => (
                  <Paper key={idx} variant="outlined" sx={{ p: 1.5 }}>
                    <Stack direction="row" justifyContent="space-between" alignItems="center" mb={0.5}>
                      <Typography variant="body2" fontWeight={600}>
                        {event.user_id || 'Unknown'} — {event.event_type || 'event'}
                      </Typography>
                      <Chip
                        label={`${(event.anom_score * 100).toFixed(0)}%`}
                        size="small"
                        color="error"
                      />
                    </Stack>
                    <Stack direction="row" spacing={0.5} flexWrap="wrap">
                      {event.reasons?.map((r, ri) => (
                        <Tooltip key={ri} title={r.desc || r.code}>
                          <Chip
                            label={r.code}
                            size="small"
                            variant="outlined"
                            sx={{ fontSize: 10, mb: 0.5 }}
                          />
                        </Tooltip>
                      ))}
                    </Stack>
                    {event.explain && (
                      <Typography variant="caption" color="text.secondary">
                        {event.explain}
                      </Typography>
                    )}
                  </Paper>
                ))}
            </Stack>
          </Box>
        )}
      </Box>
    </Drawer>
  )
}