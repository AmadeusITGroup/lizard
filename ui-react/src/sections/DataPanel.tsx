// ui-react/src/sections/DataPanel.tsx
// Enhanced Data Panel with full UX parity with TimelinePanel and MapPanel
// - Multi-field sorting
// - Multiple aggregated metrics with different functions
// - Data source selection
// - Analytics with sensitivity presets
// - Severity filtering
// - CSV export
// - JSON metadata viewer

import React from 'react'
import {
  Box,
  Stack,
  Button,
  Chip,
  TextField,
  MenuItem,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  IconButton,
  Tooltip,
  Autocomplete,
  Checkbox,
  FormControlLabel,
  Alert,
  Typography,
  Paper,
  Badge,
  Collapse,
  Divider,
  ToggleButton,
  ToggleButtonGroup,
  LinearProgress,
  FormControl,
  InputLabel,
  Select,
  Slider,
  CircularProgress,
  alpha,
} from '@mui/material'
import { DataGrid, GridToolbar, GridColDef, GridRenderCellParams } from '@mui/x-data-grid'
import { useQuery } from '@tanstack/react-query'
import {
  vizGrid,
  fetchSchemaFields,
  fetchDataSources,
  type AnalyticsMode,
  type Bucket,
  type Metric,
} from '../api'
import { useFilters } from '../context/FiltersContext'
import CloseIcon from '@mui/icons-material/Close'
import OpenInFullIcon from '@mui/icons-material/OpenInFull'
import WarningAmberIcon from '@mui/icons-material/WarningAmber'
import RefreshIcon from '@mui/icons-material/Refresh'
import TuneIcon from '@mui/icons-material/Tune'
import StorageIcon from '@mui/icons-material/Storage'
import GridOnIcon from '@mui/icons-material/GridOn'
import InfoOutlinedIcon from '@mui/icons-material/InfoOutlined'
import DownloadIcon from '@mui/icons-material/Download'
import CodeIcon from '@mui/icons-material/Code'
import AddIcon from '@mui/icons-material/Add'
import DeleteIcon from '@mui/icons-material/Delete'

// ============================================================
// Constants & Configuration
// ============================================================

const BUCKET_OPTIONS:  { value:  Bucket; label: string }[] = [
  { value: '30s', label: '30 sec' },
  { value: '1m', label: '1 min' },
  { value:  '2m', label: '2 min' },
  { value: '5m', label: '5 min' },
  { value:  '10m', label: '10 min' },
  { value: '15m', label: '15 min' },
  { value: '30m', label: '30 min' },
  { value:  '1h', label: '1 hour' },
  { value: '3h', label: '3 hours' },
  { value: '6h', label: '6 hours' },
  { value:  '12h', label: '12 hours' },
  { value: '1d', label: '1 day' },
]

const THRESHOLD_PRESETS = [
  { label: 'High Sensitivity', zThreshold: 2.0, contamination: 0.10, color: '#D32F2F', icon: '🔴' },
  { label:  'Balanced', zThreshold: 3.0, contamination: 0.05, color: '#F57C00', icon: '🟡' },
  { label:  'Low Sensitivity', zThreshold: 5.0, contamination: 0.02, color: '#388E3C', icon: '🟢' },
]

const AGG_FUNCTIONS = [
  { value: 'count', label: 'Count' },
  { value: 'sum', label: 'Sum' },
  { value: 'avg', label: 'Average' },
  { value: 'min', label: 'Minimum' },
  { value:  'max', label: 'Maximum' },
]

// Anomaly severity configuration - MATCHING Timeline & Map
const SEVERITY_LEVELS = {
  critical: { min: 0.9, color: '#7B1FA2', bgColor: '#F3E5F5', label: 'Critical' },
  high: { min: 0.75, color: '#D32F2F', bgColor:  '#FFEBEE', label: 'High' },
  medium: { min: 0.5, color: '#F57C00', bgColor: '#FFF3E0', label: 'Medium' },
  low: { min: 0.25, color: '#FBC02D', bgColor: '#FFFDE7', label: 'Low' },
  normal: { min:  0, color: '#388E3C', bgColor: '#E8F5E9', label: 'Normal' },
}

function getAnomalySeverity(score: number): keyof typeof SEVERITY_LEVELS {
  if (score >= SEVERITY_LEVELS.critical.min) return 'critical'
  if (score >= SEVERITY_LEVELS.high.min) return 'high'
  if (score >= SEVERITY_LEVELS.medium.min) return 'medium'
  if (score >= SEVERITY_LEVELS.low.min) return 'low'
  return 'normal'
}

function getSeverityColor(severity: keyof typeof SEVERITY_LEVELS): string {
  return SEVERITY_LEVELS[severity].color
}

function getRowBackgroundColor(score: number): string {
  const severity = getAnomalySeverity(score)
  if (severity === 'critical') return 'rgba(123, 31, 162, 0.12)'
  if (severity === 'high') return 'rgba(211, 47, 47, 0.12)'
  if (severity === 'medium') return 'rgba(245, 124, 0, 0.1)'
  if (severity === 'low') return 'rgba(251, 192, 45, 0.08)'
  return 'transparent'
}

// ============================================================
// Types
// ============================================================

interface AggregateMetric {
  id: string
  function:  Metric
  field: string
  alias:  string
}

interface SortField {
  id: string
  field: string
  direction: 'asc' | 'desc'
}

// ============================================================
// Main Component
// ============================================================

export default function DataPanel() {
  const { startISO, endISO, filters } = useFilters()

  // State Management
  const [mode, setMode] = React.useState<AnalyticsMode>('none')
  const [open, setOpen] = React.useState(false)

  const [bucket, setBucket] = React.useState<Bucket>('5m')
  const [aggregate, setAggregate] = React.useState<boolean>(false)
  const [groupBy, setGroupBy] = React.useState<string[]>(['bucket', 'user_id', 'event_type'])

  // Multi-metric aggregation
  const [aggregateMetrics, setAggregateMetrics] = React.useState<AggregateMetric[]>([
    { id: '1', function: 'count', field: '', alias: 'count_events' }
  ])

  // Multi-field sorting
  const [sortFields, setSortFields] = React.useState<SortField[]>([
    { id: '1', field: 'ts', direction: 'desc' }
  ])

  const [numericFields, setNumericFields] = React.useState<string[]>([])

  // Enhanced features
  const [dataSource, setDataSource] = React.useState<string>('')
  const [zThreshold, setZThreshold] = React.useState<number>(3.0)
  const [contamination, setContamination] = React.useState<number>(0.05)
  const [thresholdPreset, setThresholdPreset] = React.useState<string | null>('Balanced')
  const [selectedSeverities, setSelectedSeverities] = React.useState<string[]>([
    'critical', 'high', 'medium', 'low', 'normal'
  ])
  const [showAdvancedControls, setShowAdvancedControls] = React.useState(false)
  const [metadataDialogOpen, setMetadataDialogOpen] = React.useState(false)
  const [selectedMetadata, setSelectedMetadata] = React.useState<any>(null)
  const [isTransitioning, setIsTransitioning] = React.useState(false)

  // Fetch schema fields
  React.useEffect(() => {
    (async () => {
      const schema = await fetchSchemaFields()
      const nums = Object.entries(schema.types)
        .filter(([_, t]) => t === 'number')
        .map(([f]) => f)
      setNumericFields(Array.from(new Set([...nums, 'anom_score', 'zscore', 'amount'])))
    })()
  }, [])

  // Fetch data sources
  const dataSourcesQuery = useQuery({
    queryKey: ['dataSources'],
    queryFn: fetchDataSources,
    staleTime: 60000,
  })

  const dataSources = React.useMemo(() => {
    const sources = dataSourcesQuery.data || []
    return [
      { name: '', label: 'All Sources', type: 'all' as const, icon: null },
      ...sources.map((s:  any) => ({
        name:  s.name,
        label: s.name,
        sublabel: `${s.type}${s.row_count ? ` • ${s.row_count.toLocaleString()} rows` : ''}`,
        type: s.type,
        icon: s.type === 'view' ? '📊' : '📁',
      })),
    ]
  }, [dataSourcesQuery.data])

  // Query data - NOTE: Backend may not support all features yet
  const q = useQuery({
    queryKey: [
      'grid2', startISO, endISO, mode, sortFields, bucket, aggregate, 
      groupBy, aggregateMetrics, filters, dataSource, zThreshold, 
      contamination, selectedSeverities,
    ],
    queryFn: () => {
      // For now, use first sort field and first metric for backend compatibility
      const primarySort = sortFields[0]
      const primaryMetric = aggregateMetrics[0]
      
      return vizGrid({
        start: startISO,
        end: endISO,
        analytics: mode,
        z_thr: mode === 'simple' ? zThreshold : undefined,
        contamination: mode === 'advanced' ? contamination : undefined,
        sort_by: primarySort?.field,
        sort_dir: primarySort?.direction,
        limit: 20000,
        offset: 0,
        bucket,
        aggregate,
        group_by: groupBy,
        metric: primaryMetric?.function,
        value_field: primaryMetric?.field || undefined,
        where: filters. filter(f => f.field !== 'source'),
        source: dataSource || undefined,
      })
    },
    enabled: false,
    //onSettled: () => setIsTransitioning(false),
  })

  // Client-side multi-field sorting
  const sortedRows = React.useMemo(() => {
    const rawRows = (q.data ?? []).map((r: any, i: number) => ({ id: i, ...r }))
    
    // Filter by selected severities
    let filtered = rawRows
    if (mode !== 'none' && selectedSeverities.length < 5) {
      filtered = rawRows.filter((r: any) => {
        const score = r.anom_score ?? 0
        const severity = getAnomalySeverity(score)
        return selectedSeverities.includes(severity)
      })
    }
    
    // Multi-field sorting
    if (sortFields.length > 0) {
      return [...filtered].sort((a, b) => {
        for (const sortField of sortFields) {
          let aVal = a[sortField.field]
          let bVal = b[sortField.field]
          
          if (aVal === null || aVal === undefined) return 1
          if (bVal === null || bVal === undefined) return -1
          
          if (typeof aVal === 'string') aVal = aVal.toLowerCase()
          if (typeof bVal === 'string') bVal = bVal.toLowerCase()
          
          let comparison = 0
          if (aVal < bVal) comparison = -1
          if (aVal > bVal) comparison = 1
          
          if (comparison !== 0) {
            return sortField.direction === 'asc' ? comparison : -comparison
          }
        }
        return 0
      })
    }
    
    return filtered
  }, [q.data, mode, selectedSeverities, sortFields])

  // Count anomalies
  const anomalyStats = React.useMemo(() => {
    const stats = { total: 0, critical: 0, high: 0, medium:  0, low: 0 }
    sortedRows.forEach((r: any) => {
      const score = r.anom_score ?? 0
      const sev = getAnomalySeverity(score)
      if (sev === 'critical') stats.critical++
      else if (sev === 'high') stats.high++
      else if (sev === 'medium') stats.medium++
      else if (sev === 'low') stats.low++
    })
    stats.total = stats.critical + stats.high + stats.medium + stats.low
    return stats
  }, [sortedRows])

  // Export to CSV
  const handleExport = React.useCallback(() => {
    if (! sortedRows.length) return
    
    const headers = Object.keys(sortedRows[0]).filter(k => k !== 'id')
    const csvContent = [
      headers.join(','),
      ...sortedRows.map(row => headers.map(h => {
        const val = row[h]
        if (val === null || val === undefined) return ''
        if (typeof val === 'object') return `"${JSON.stringify(val).replace(/"/g, '""')}"`
        return `"${String(val).replace(/"/g, '""')}"`
      }).join(','))
    ].join('\n')
    
    const blob = new Blob([csvContent], { type: 'text/csv' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `lizard-data-export-${new Date().toISOString().slice(0, 19).replace(/:/g, '-')}.csv`
    a.click()
    URL.revokeObjectURL(url)
  }, [sortedRows])

  // Handle threshold preset change
  const handleThresholdPresetChange = React.useCallback((presetLabel: string) => {
    const preset = THRESHOLD_PRESETS.find(p => p.label === presetLabel)
    if (!preset) return
    setZThreshold(preset.zThreshold)
    setContamination(preset.contamination)
    setThresholdPreset(presetLabel)
    setIsTransitioning(true)
  }, [])

  // Add/remove sort fields
  const addSortField = () => {
    setSortFields([...sortFields, { id: Date.now().toString(), field: 'ts', direction: 'desc' }])
  }

  const removeSortField = (id: string) => {
    if (sortFields.length > 1) {
      setSortFields(sortFields.filter(f => f.id !== id))
    }
  }

  const updateSortField = (id: string, updates: Partial<SortField>) => {
    setSortFields(sortFields.map(f => f.id === id ? { ...f, ...updates } : f))
  }

  // Add/remove aggregate metrics
  const addAggregateMetric = () => {
    setAggregateMetrics([
      ...aggregateMetrics, 
      { id: Date.now().toString(), function: 'count', field: '', alias: `metric_${aggregateMetrics.length + 1}` }
    ])
  }

  const removeAggregateMetric = (id: string) => {
    if (aggregateMetrics.length > 1) {
      setAggregateMetrics(aggregateMetrics.filter(m => m.id !== id))
    }
  }

  const updateAggregateMetric = (id: string, updates:  Partial<AggregateMetric>) => {
    setAggregateMetrics(aggregateMetrics.map(m => m.id === id ? { ...m, ...updates } : m))
  }

  // Build columns
  const cols:  GridColDef[] = React.useMemo(() => {
    const keys = Object.keys(sortedRows[0] ?? {}).filter((k) => k !== 'id')
    return keys.map((k) => {
      // Special rendering for anom_score
      if (k === 'anom_score') {
        return {
          field: k,
          headerName:  '⚠️ Anomaly Score',
          flex: 1,
          minWidth: 150,
          renderCell: (params:  GridRenderCellParams) => {
            const score = params.value as number
            if (score == null || score < 0.25) return <span style={{ opacity: 0.5 }}>—</span>
            const severity = getAnomalySeverity(score)
            const color = getSeverityColor(severity)
            return (
              <Stack direction="row" spacing={1} alignItems="center">
                <Box sx={{ width: 8, height: 8, borderRadius: '50%', bgcolor: color }} />
                <Typography variant="body2" fontWeight={score >= 0.5 ? 600 : 400}>
                  {score.toFixed(3)}
                </Typography>
                <Chip
                  label={SEVERITY_LEVELS[severity].label}
                  size="small"
                  sx={{ bgcolor: color, color: '#fff', fontWeight: 600, fontSize: 10, height: 18 }}
                />
              </Stack>
            )
          },
        }
      }

      // Special rendering for anomaly boolean
      if (k === 'anomaly') {
        return {
          field: k,
          headerName:  'Anomaly? ',
          flex: 0.5,
          minWidth: 100,
          renderCell:  (params: GridRenderCellParams) => {
            const isAnom = params.value
            return isAnom ? (
              <Chip label="YES" size="small" color="error" icon={<WarningAmberIcon />} />
            ) : (
              <span style={{ opacity: 0.4 }}>No</span>
            )
          },
        }
      }

      // Special rendering for reasons
      if (k === 'reasons' || k === 'top_reason') {
        return {
          field: k,
          headerName: k === 'reasons' ? 'Reasons' :  'Top Reason',
          flex: 1.5,
          minWidth: 200,
          renderCell: (params: GridRenderCellParams) => {
            const val = params.value
            if (! val) return <span style={{ opacity: 0.4 }}>—</span>
            if (Array.isArray(val)) {
              const codes = val.slice(0, 3).map((r: any) => r.code || r).join(', ')
              return (
                <Tooltip title={val.map((r: any) => r.desc || r.code || r).join('; ')}>
                  <Chip label={codes} size="small" variant="outlined" />
                </Tooltip>
              )
            }
            return <Chip label={String(val)} size="small" variant="outlined" />
          },
        }
      }

      // Special rendering for metadata
      if (k === 'meta' || k === 'metadata') {
        return {
          field:  k,
          headerName: 'Metadata',
          flex:  0.5,
          minWidth: 100,
          renderCell: (params: GridRenderCellParams) => {
            const val = params.value
            if (!val || typeof val !== 'object') return <span style={{ opacity: 0.4 }}>—</span>
            return (
              <IconButton
                size="small"
                onClick={() => {
                  setSelectedMetadata(val)
                  setMetadataDialogOpen(true)
                }}
              >
                <CodeIcon fontSize="small" />
              </IconButton>
            )
          },
        }
      }

      return { field: k, headerName: k, flex: 1, minWidth: 120 }
    })
  }, [sortedRows])

  // Grid component
  const GridBody = (
    <Box sx={{ flex: 1, minHeight: 0 }}>
      <DataGrid
        density="compact"
        rows={sortedRows}
        columns={cols}
        disableRowSelectionOnClick
        slots={{ toolbar: GridToolbar }}
        getRowClassName={(params) => {
          const score = params.row.anom_score ?? 0
          if (score >= 0.75) return 'row-anomaly-high'
          if (score >= 0.5) return 'row-anomaly-medium'
          if (score >= 0.25) return 'row-anomaly-low'
          return ''
        }}
        sx={{
          '& .row-anomaly-high': {
            bgcolor: getRowBackgroundColor(0.9),
            '&:hover': { bgcolor: 'rgba(211, 47, 47, 0.2)' },
          },
          '& .row-anomaly-medium': {
            bgcolor: getRowBackgroundColor(0.5),
            '&:hover':  { bgcolor: 'rgba(245, 124, 0, 0.18)' },
          },
          '& .row-anomaly-low': {
            bgcolor: getRowBackgroundColor(0.25),
            '&:hover':  { bgcolor: 'rgba(251, 192, 45, 0.15)' },
          },
        }}
      />
    </Box>
  )

  return (
    <Stack sx={{ height: '100%', width: '100%' }} spacing={1}>
      {/* Loading Progress */}
      {(q.isFetching || isTransitioning) && (
        <LinearProgress sx={{ position: 'absolute', top: 0, left: 0, right: 0, zIndex: 20, height: 3 }} />
      )}

      {/* Controls Panel */}
      <Paper variant="outlined" sx={{ p: 1.5 }}>
        <Stack spacing={1.5}>
          {/* Primary Controls Row */}
          <Stack direction="row" spacing={1.5} alignItems="center" flexWrap="wrap" useFlexGap>
            <Badge 
              badgeContent={anomalyStats.critical + anomalyStats.high} 
              color="error" 
              invisible={anomalyStats.critical + anomalyStats.high === 0}
            >
              <Chip icon={<GridOnIcon />} label="Data" color="primary" sx={{ fontWeight: 600 }} />
            </Badge>

            {/* Data Source */}
            <FormControl size="small" sx={{ minWidth: 180 }}>
              <InputLabel>Data Source</InputLabel>
              <Select
                value={dataSource}
                onChange={e => setDataSource(e.target.value)}
                label="Data Source"
                startAdornment={<StorageIcon sx={{ mr: 0.5, fontSize: 18, color: 'action.active' }} />}
              >
                {dataSources.map(src => (
                  <MenuItem key={src.name} value={src.name}>
                    <Stack direction="row" spacing={1} alignItems="center">
                      {src.icon && <span>{src.icon}</span>}
                      <Box>
                        <Typography variant="body2">{src.label}</Typography>
                        {src.sublabel && (
                          <Typography variant="caption" color="text.secondary">{src.sublabel}</Typography>
                        )}
                      </Box>
                    </Stack>
                  </MenuItem>
                ))}
              </Select>
            </FormControl>

            {/* Analytics Mode */}
            <TextField
              select
              size="small"
              label="Analytics"
              value={mode}
              onChange={e => setMode(e.target.value as AnalyticsMode)}
              sx={{ minWidth: 160 }}
            >
              <MenuItem value="none">None</MenuItem>
              <MenuItem value="simple">
                <Stack>
                  <span>Simple (Z-Score)</span>
                  <Typography variant="caption" color="text.secondary">Fast statistical detection</Typography>
                </Stack>
              </MenuItem>
              <MenuItem value="advanced">
                <Stack>
                  <span>Advanced (IForest)</span>
                  <Typography variant="caption" color="text.secondary">ML-based detection</Typography>
                </Stack>
              </MenuItem>
            </TextField>

            {/* Threshold Presets */}
            {mode !== 'none' && (
              <Stack direction="row" spacing={1} alignItems="center">
                <Typography variant="caption" color="text.secondary" sx={{ mr: 0.5 }}>Sensitivity: </Typography>
                <ToggleButtonGroup
                  value={thresholdPreset}
                  exclusive
                  onChange={(_, v) => v && handleThresholdPresetChange(v)}
                  size="small"
                >
                  {THRESHOLD_PRESETS.map(preset => (
                    <ToggleButton
                      key={preset.label}
                      value={preset.label}
                      sx={{
                        fontSize: '0.75rem',
                        px: 1.5,
                        borderColor: preset.color,
                        '&.Mui-selected': {
                          bgcolor: alpha(preset.color, 0.15),
                          borderColor: preset.color,
                          color: preset.color,
                          fontWeight: 600,
                          '&:hover': { bgcolor: alpha(preset.color, 0.25) }
                        }
                      }}
                    >
                      {preset.icon} {preset.label.split(' ')[0]}
                    </ToggleButton>
                  ))}
                </ToggleButtonGroup>

                {anomalyStats.total > 0 && (
                  <Chip
                    size="small"
                    label={`${anomalyStats.total} anomalies`}
                    color={anomalyStats.critical + anomalyStats.high > 0 ? 'error' :  'warning'}
                    sx={{ ml: 1, fontWeight: 600 }}
                  />
                )}
              </Stack>
            )}

            {/* Aggregate Toggle */}
            <FormControlLabel
              control={<Checkbox checked={aggregate} onChange={(_, v) => setAggregate(v)} />}
              label="Aggregate"
            />

            {/* Bucket */}
            <TextField
              select
              size="small"
              label="Bucket"
              value={bucket}
              onChange={e => setBucket(e.target.value as Bucket)}
              sx={{ minWidth: 120 }}
            >
              {BUCKET_OPTIONS.map(opt => (
                <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
              ))}
            </TextField>

            <Box sx={{ flex: 1 }} />

            {/* Export */}
            <Tooltip title="Export to CSV">
              <IconButton size="small" onClick={handleExport} disabled={!sortedRows.length}>
                <DownloadIcon />
              </IconButton>
            </Tooltip>

            {/* Advanced Toggle */}
            <Tooltip title="Advanced Settings">
              <IconButton
                size="small"
                onClick={() => setShowAdvancedControls(!showAdvancedControls)}
                color={showAdvancedControls ? 'primary' : 'default'}
              >
                <TuneIcon />
              </IconButton>
            </Tooltip>

            {/* Run Button */}
            <Button
              variant="contained"
              onClick={() => {
                setIsTransitioning(true)
                q.refetch().finally(() => {
                  setIsTransitioning(false)
                })
              }}
              disabled={q.isFetching}
              startIcon={q.isFetching ? <CircularProgress size={16} color="inherit" /> : <RefreshIcon />}
              sx={{ minWidth: 100 }}
            >
              {q.isFetching ? 'Loading...' : 'Run'}
            </Button>

            <Tooltip title="Full screen">
              <IconButton onClick={() => setOpen(true)}>
                <OpenInFullIcon />
              </IconButton>
            </Tooltip>
          </Stack>

          {/* Advanced Controls */}
          <Collapse in={showAdvancedControls}>
            <Divider sx={{ my: 1 }} />
            <Stack spacing={2}>
              {/* Multi-Field Sorting */}
              <Paper variant="outlined" sx={{ p: 1.5, bgcolor: '#fafafa' }}>
                <Stack spacing={1}>
                  <Stack direction="row" justifyContent="space-between" alignItems="center">
                    <Typography variant="subtitle2" fontWeight={600}>Multi-Field Sorting</Typography>
                    <Button size="small" startIcon={<AddIcon />} onClick={addSortField}>
                      Add Sort Field
                    </Button>
                  </Stack>
                  {sortFields.map((sortField, idx) => (
                    <Stack key={sortField.id} direction="row" spacing={1} alignItems="center">
                      <Typography variant="caption" sx={{ minWidth: 20 }}>{idx + 1}.</Typography>
                      <Autocomplete
                        size="small"
                        options={['ts', 'anom_score', 'user_id', 'event_type', 'value', ...numericFields]}
                        value={sortField.field}
                        onChange={(_, v) => updateSortField(sortField.id, { field: v || 'ts' })}
                        renderInput={p => <TextField {...p} label="Field" />}
                        sx={{ minWidth: 200 }}
                      />
                      <TextField
                        select
                        size="small"
                        value={sortField.direction}
                        onChange={e => updateSortField(sortField.id, { direction: e.target.value as 'asc' | 'desc' })}
                        sx={{ minWidth:  120 }}
                      >
                        <MenuItem value="asc">Ascending</MenuItem>
                        <MenuItem value="desc">Descending</MenuItem>
                      </TextField>
                      <IconButton
                        size="small"
                        onClick={() => removeSortField(sortField.id)}
                        disabled={sortFields.length === 1}
                      >
                        <DeleteIcon fontSize="small" />
                      </IconButton>
                    </Stack>
                  ))}
                </Stack>
              </Paper>

              {/* Multi-Metric Aggregation */}
              {aggregate && (
                <Paper variant="outlined" sx={{ p: 1.5, bgcolor: '#fafafa' }}>
                  <Stack spacing={1}>
                    <Stack direction="row" justifyContent="space-between" alignItems="center">
                      <Typography variant="subtitle2" fontWeight={600}>Aggregate Metrics</Typography>
                      <Button size="small" startIcon={<AddIcon />} onClick={addAggregateMetric}>
                        Add Metric
                      </Button>
                    </Stack>
                    {aggregateMetrics.map((metric, idx) => (
                      <Stack key={metric.id} direction="row" spacing={1} alignItems="center">
                        <Typography variant="caption" sx={{ minWidth: 20 }}>{idx + 1}.</Typography>
                        <TextField
                          select
                          size="small"
                          label="Function"
                          value={metric.function}
                          onChange={e => updateAggregateMetric(metric.id, { function: e.target.value as Metric })}
                          sx={{ minWidth: 120 }}
                        >
                          {AGG_FUNCTIONS.map(fn => (
                            <MenuItem key={fn.value} value={fn.value}>{fn.label}</MenuItem>
                          ))}
                        </TextField>
                        <TextField
                          select
                          size="small"
                          label="Field"
                          value={metric.field}
                          onChange={e => updateAggregateMetric(metric.id, { field: e.target.value })}
                          sx={{ minWidth: 160 }}
                          disabled={metric.function === 'count'}
                        >
                          {numericFields.map(f => (
                            <MenuItem key={f} value={f}>{f}</MenuItem>
                          ))}
                        </TextField>
                        <TextField
                          size="small"
                          label="Alias"
                          value={metric.alias}
                          onChange={e => updateAggregateMetric(metric.id, { alias: e.target.value })}
                          sx={{ minWidth: 140 }}
                        />
                        <IconButton
                          size="small"
                          onClick={() => removeAggregateMetric(metric.id)}
                          disabled={aggregateMetrics.length === 1}
                        >
                          <DeleteIcon fontSize="small" />
                        </IconButton>
                      </Stack>
                    ))}
                  </Stack>
                </Paper>
              )}

              {/* Group By */}
              <Stack direction="row" spacing={2} alignItems="center" flexWrap="wrap" useFlexGap>
                <Autocomplete
                  multiple
                  options={['bucket', 'user_id', 'event_type', 'source', 'country', 'city', 'account_id', 'device_id', 'carrier', 'origin', 'dest']}
                  value={groupBy}
                  onChange={(_, v) => setGroupBy(v)}
                  renderInput={p => <TextField {...p} size="small" label="Group by" />}
                  sx={{ minWidth: 280 }}
                  disabled={!aggregate}
                  limitTags={2}
                  ChipProps={{ size: 'small' }}
                />

                {/* Severity Filtering */}
                {mode !== 'none' && (
                  <>
                    <Typography variant="caption" color="text.secondary" sx={{ mr: -1 }}>Show Severities: </Typography>
                    <ToggleButtonGroup
                      value={selectedSeverities}
                      onChange={(_, v) => { if (v.length > 0) setSelectedSeverities(v) }}
                      size="small"
                    >
                      {['critical', 'high', 'medium', 'low', 'normal'].map(sev => (
                        <ToggleButton 
                          key={sev} 
                          value={sev}
                          sx={{
                            fontSize: '0.75rem',
                            px:  1,
                            '&.Mui-selected': {
                              bgcolor: `${SEVERITY_LEVELS[sev as keyof typeof SEVERITY_LEVELS]?.color}30`,
                              borderColor:  SEVERITY_LEVELS[sev as keyof typeof SEVERITY_LEVELS]?.color,
                              color:  SEVERITY_LEVELS[sev as keyof typeof SEVERITY_LEVELS]?.color,
                              fontWeight: 600
                            }
                          }}
                        >
                          {sev.charAt(0).toUpperCase() + sev.slice(1)}
                        </ToggleButton>
                      ))}
                    </ToggleButtonGroup>
                  </>
                )}
              </Stack>

              {/* Thresholds */}
              <Stack direction="row" spacing={2} alignItems="center" flexWrap="wrap" useFlexGap>
                {mode === 'simple' && (
                  <Box sx={{ width: 220 }}>
                    <Stack direction="row" spacing={1} alignItems="center">
                      <Typography variant="caption" color="text.secondary">
                        Z-Threshold:  <strong>{zThreshold.toFixed(1)}</strong>
                      </Typography>
                      <Tooltip title="Lower values = more sensitive">
                        <InfoOutlinedIcon sx={{ fontSize: 14, color: 'text.secondary' }} />
                      </Tooltip>
                    </Stack>
                    <Slider
                      size="small"
                      value={zThreshold}
                      onChange={(_, v) => { setZThreshold(v as number); setThresholdPreset(null) }}
                      min={0.5}
                      max={10}
                      step={0.1}
                      marks={[{ value: 2, label: '2' }, { value: 3, label: '3' }, { value: 5, label: '5' }, { value: 10, label: '10' }]}
                      sx={{ '& .MuiSlider-track': { background: 'linear-gradient(90deg, #D32F2F 0%, #F57C00 50%, #388E3C 100%)' } }}
                    />
                  </Box>
                )}

                {mode === 'advanced' && (
                  <Box sx={{ width: 220 }}>
                    <Stack direction="row" spacing={1} alignItems="center">
                      <Typography variant="caption" color="text.secondary">
                        Contamination: <strong>{(contamination * 100).toFixed(0)}%</strong>
                      </Typography>
                      <Tooltip title="Expected % of anomalies">
                        <InfoOutlinedIcon sx={{ fontSize:  14, color: 'text.secondary' }} />
                      </Tooltip>
                    </Stack>
                    <Slider
                      size="small"
                      value={contamination}
                      onChange={(_, v) => { setContamination(v as number); setThresholdPreset(null) }}
                      min={0.01}
                      max={0.3}
                      step={0.01}
                      marks={[{ value: 0.02, label: '2%' }, { value: 0.05, label: '5%' }, { value: 0.10, label: '10%' }, { value: 0.20, label: '20%' }]}
                      sx={{ '& .MuiSlider-track':  { background: 'linear-gradient(90deg, #388E3C 0%, #F57C00 50%, #D32F2F 100%)' } }}
                    />
                  </Box>
                )}
              </Stack>
            </Stack>
          </Collapse>
        </Stack>
      </Paper>

      {/* Anomaly Summary */}
      {mode !== 'none' && anomalyStats.total > 0 && (
        <Alert severity={anomalyStats.critical > 0 || anomalyStats.high > 0 ? 'error' : 'warning'} icon={<WarningAmberIcon />} sx={{ py: 0.5 }}>
          <Stack direction="row" spacing={3} alignItems="center" flexWrap="wrap">
            <Typography variant="body2"><strong>{anomalyStats.total}</strong> anomalies detected</Typography>
            {anomalyStats.critical > 0 && (
              <Chip size="small" label={`${anomalyStats.critical} Critical`} sx={{ bgcolor:  SEVERITY_LEVELS.critical.color, color: '#fff' }} />
            )}
            {anomalyStats.high > 0 && (
              <Chip size="small" label={`${anomalyStats.high} High`} sx={{ bgcolor: SEVERITY_LEVELS.high.color, color: '#fff' }} />
            )}
            {anomalyStats.medium > 0 && (
              <Chip size="small" label={`${anomalyStats.medium} Medium`} sx={{ bgcolor: SEVERITY_LEVELS.medium.color, color: '#fff' }} />
            )}
            {anomalyStats.low > 0 && (
              <Chip size="small" label={`${anomalyStats.low} Low`} sx={{ bgcolor: SEVERITY_LEVELS.low.color, color: '#000' }} />
            )}
          </Stack>
        </Alert>
      )}

      {/* Color Legend */}
      {mode !== 'none' && (
        <Paper variant="outlined" sx={{ px: 2, py: 0.75, bgcolor: '#fafafa' }}>
          <Stack direction="row" spacing={2.5} alignItems="center" flexWrap="wrap">
            <Typography variant="caption" fontWeight={600} color="text.secondary">Row Colors: </Typography>
            {Object.entries(SEVERITY_LEVELS).filter(([key]) => key !== 'normal').map(([key, level]) => (
              <Stack key={key} direction="row" spacing={0.5} alignItems="center">
                <Box sx={{ width:  16, height: 12, bgcolor: getRowBackgroundColor(level.min + 0.01), border: '1px solid #ccc' }} />
                <Typography variant="caption">{level.label}</Typography>
              </Stack>
            ))}
          </Stack>
        </Paper>
      )}

      {/* Grid */}
      <Box sx={{ flex: 1, minHeight: 0, px: 1.5, pb: 1.5 }}>{GridBody}</Box>

      {/* Full Screen Dialog */}
      <Dialog fullScreen open={open} onClose={() => setOpen(false)}>
        <DialogTitle>
          Data (Full Screen)
          <IconButton onClick={() => setOpen(false)} sx={{ position: 'absolute', right: 8, top: 8 }}>
            <CloseIcon />
          </IconButton>
        </DialogTitle>
        <DialogContent sx={{ p:  0 }}>
          <Box sx={{ height: 'calc(100vh - 64px)' }}>{GridBody}</Box>
        </DialogContent>
      </Dialog>

      {/* Metadata Dialog */}
      <Dialog open={metadataDialogOpen} onClose={() => setMetadataDialogOpen(false)} maxWidth="md" fullWidth>
        <DialogTitle>
          Metadata
          <IconButton onClick={() => setMetadataDialogOpen(false)} sx={{ position: 'absolute', right:  8, top: 8 }}>
            <CloseIcon />
          </IconButton>
        </DialogTitle>
        <DialogContent>
          <Box component="pre" sx={{ bgcolor: '#1e1e1e', color: '#d4d4d4', p: 2, borderRadius: 1, overflow: 'auto', fontSize: '0.85rem', fontFamily: 'monospace' }}>
            {JSON.stringify(selectedMetadata, null, 2)}
          </Box>
        </DialogContent>
      </Dialog>
    </Stack>
  )
}