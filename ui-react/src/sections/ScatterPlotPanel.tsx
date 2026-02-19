// ui-react/src/sections/ScatterPlotPanel.tsx
// Production-grade Scatter Plot Panel for correlation and outlier analysis
//
// FEATURES:
// 1.X/Y axis field selection with any numeric or categorical field
// 2.Color by field (categorical) or anomaly score
// 3.Size by field (numeric) or fixed
// 4.Data source filtering via WHERE clause (using 'eq' operator)
// 5.Drill-down on click with breadcrumb navigation
// 6.Advanced anomaly visualization with configurable threshold
// 7.Detail drawer on click with anomaly events table
// 8.Regression line toggle
// 9.Log scale toggle for axes

import React, { useState, useMemo, useCallback, useEffect, useRef } from 'react'
import Plot from 'react-plotly.js'
import {
  Box,
  Stack,
  TextField,
  MenuItem,
  Slider,
  Typography,
  Chip,
  Button,
  Tooltip,
  Alert,
  Paper,
  Badge,
  LinearProgress,
  FormControl,
  InputLabel,
  Select,
  IconButton,
  Collapse,
  Divider,
  ToggleButton,
  ToggleButtonGroup,
  CircularProgress,
  alpha,
  FormControlLabel,
  Switch,
  Drawer,
  Card,
  CardContent,
  Table,
  TableBody,
  TableCell,
  TableRow,
  TableHead,
  TableContainer,
  Breadcrumbs,
  Link,
} from '@mui/material'
import ScatterPlotIcon from '@mui/icons-material/ScatterPlot'
import WarningAmberIcon from '@mui/icons-material/WarningAmber'
import ErrorIcon from '@mui/icons-material/Error'
import InfoIcon from '@mui/icons-material/Info'
import RefreshIcon from '@mui/icons-material/Refresh'
import TuneIcon from '@mui/icons-material/Tune'
import StorageIcon from '@mui/icons-material/Storage'
import DownloadIcon from '@mui/icons-material/Download'
import CloseIcon from '@mui/icons-material/Close'
import HomeIcon from '@mui/icons-material/Home'
import NavigateNextIcon from '@mui/icons-material/NavigateNext'
import ZoomInIcon from '@mui/icons-material/ZoomIn'
import PersonIcon from '@mui/icons-material/Person'
import DevicesIcon from '@mui/icons-material/Devices'
import LocationOnIcon from '@mui/icons-material/LocationOn'
import ShowChartIcon from '@mui/icons-material/ShowChart'
import { useQuery } from '@tanstack/react-query'
import {
  vizGrid,
  fetchSchemaFields,
  fetchDataSources,
  type AnalyticsMode,
  type FilterCond,
} from '../api'
import { useFilters } from '../context/FiltersContext'

// ============================================================
// Constants & Configuration
// ============================================================

const CHART_HEIGHT = 500

const SEVERITY_LEVELS = {
  critical: { min: 0.9, color: '#7B1FA2', bgColor: '#F3E5F5', label: 'Critical', icon: ErrorIcon },
  high: { min: 0.75, color: '#D32F2F', bgColor:  '#FFEBEE', label: 'High', icon: ErrorIcon },
  medium: { min: 0.5, color: '#F57C00', bgColor: '#FFF3E0', label: 'Medium', icon: WarningAmberIcon },
  low:  { min: 0.25, color: '#FBC02D', bgColor: '#FFFDE7', label: 'Low', icon: WarningAmberIcon },
  normal: { min: 0, color: '#388E3C', bgColor: '#E8F5E9', label: 'Normal', icon: InfoIcon },
}

const THRESHOLD_PRESETS = [
  { label: 'High Sensitivity', zThreshold: 2.0, contamination: 0.10, color: '#D32F2F', icon: '🔴' },
  { label: 'Balanced', zThreshold: 3.0, contamination: 0.05, color: '#F57C00', icon: '🟡' },
  { label: 'Low Sensitivity', zThreshold: 5.0, contamination: 0.02, color: '#388E3C', icon: '🟢' },
]

const SCATTER_COLORS = [
  '#1976D2', '#388E3C', '#7B1FA2', '#F57C00', '#C2185B',
  '#0097A7', '#AFB42B', '#5D4037', '#455A64', '#E64A19',
]

// ============================================================
// Helper Functions
// ============================================================

function getAnomalySeverity(score: number): keyof typeof SEVERITY_LEVELS {
  if (score >= SEVERITY_LEVELS.critical.min) return 'critical'
  if (score >= SEVERITY_LEVELS.high.min) return 'high'
  if (score >= SEVERITY_LEVELS.medium.min) return 'medium'
  if (score >= SEVERITY_LEVELS.low.min) return 'low'
  return 'normal'
}

function getAnomalyColor(score: number): string {
  return SEVERITY_LEVELS[getAnomalySeverity(score)].color
}

function formatFieldLabel(field: string): string {
  return field.split('_').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ')
}

function formatValue(value: any): string {
  if (value === null || value === undefined) return '—'
  if (typeof value === 'number') {
    return value.toLocaleString(undefined, { maximumFractionDigits: 2 })
  }
  return String(value)
}

// ============================================================
// Types
// ============================================================

export interface ScatterPlotPanelProps {
  instanceId?:  string
  initialConfig?:  Partial<ScatterPlotConfig>
  onConfigChange?: (config: ScatterPlotConfig) => void
  compact?: boolean
}

export interface ScatterPlotConfig {
  dataSource: string
  xField: string
  yField:  string
  colorField:  string
  sizeField: string
  analyticsMode: AnalyticsMode
  zThreshold: number
  contamination: number
  anomalyThreshold: number
  colorByAnomaly: boolean
  showRegression: boolean
  xLogScale: boolean
  yLogScale: boolean
  pointSize: number
  pointOpacity: number
  maxPoints: number
  showGrid: boolean
}

interface PointData {
  x: any
  y: any
  label: string
  anomScore: number
  severity: keyof typeof SEVERITY_LEVELS
  color: string
  size: number
  eventData:  any
}

interface DrillDownLevel {
  filters: FilterCond[]
  label: string
  fieldValues: Record<string, string>
}

const DEFAULT_CONFIG: ScatterPlotConfig = {
  dataSource: '',
  xField: 'ts',
  yField: 'anom_score',
  colorField: 'event_type',
  sizeField: '',
  analyticsMode: 'none',
  zThreshold: 3.0,
  contamination: 0.05,
  anomalyThreshold: 0.25,
  colorByAnomaly: true,
  showRegression: false,
  xLogScale: false,
  yLogScale: false,
  pointSize:  8,
  pointOpacity: 0.7,
  maxPoints: 5000,
  showGrid: true,
}

// ============================================================
// Main Component
// ============================================================

export default function ScatterPlotPanel({
  instanceId = 'default',
  initialConfig,
  onConfigChange,
  compact = false,
}: ScatterPlotPanelProps) {
  const { startISO, endISO, filters:  globalFilters } = useFilters()

  const [config, setConfig] = useState<ScatterPlotConfig>({
    ...DEFAULT_CONFIG,
    ...initialConfig,
  })

  // UI State
  const [showAdvancedControls, setShowAdvancedControls] = useState(false)
  const [isTransitioning, setIsTransitioning] = useState(false)
  const [thresholdPreset, setThresholdPreset] = useState<string | null>('Balanced')
  const [selectedPoint, setSelectedPoint] = useState<number | null>(null)

  // Drill-down state
  const [drillDownStack, setDrillDownStack] = useState<DrillDownLevel[]>([])

  // Detail drawer state
  const [drawerOpen, setDrawerOpen] = useState(false)
  const [selectedPointData, setSelectedPointData] = useState<PointData | null>(null)
  const [drawerEvents, setDrawerEvents] = useState<any[]>([])
  const [drawerEventsLoading, setDrawerEventsLoading] = useState(false)
  const [drawerReasons, setDrawerReasons] = useState<string[]>([])

  // Store points for click handling
  const pointsRef = useRef<PointData[]>([])

  // Schema state
  const [availableFields, setAvailableFields] = useState<string[]>([])
  const [numericFields, setNumericFields] = useState<string[]>([])
  const [categoricalFields, setCategoricalFields] = useState<string[]>([])

  useEffect(() => {
    onConfigChange?.(config)
  }, [config, onConfigChange])

  // Fetch schema fields
  useEffect(() => {
    ;(async () => {
      try {
        const schema = await fetchSchemaFields()
        const types = schema.types || {}

        const allFields = Object.keys(types).filter(
          f => ! f.startsWith('_') && f !== 'id' && f !== 'meta'
        )
        setAvailableFields(allFields.length > 0 ? allFields :  [
          'ts', 'event_type', 'user_id', 'account_id', 'device_id', 'ip', 'country', 'city', 'source', 'anom_score', 'amount'
        ])

        const nums = Object.entries(types)
          .filter(([_, t]) => t === 'number' || t === 'datetime')
          .map(([f]) => f)
        setNumericFields(Array.from(new Set([...nums, 'ts', 'anom_score', 'zscore', 'amount', 'geo_lat', 'geo_lon'])))

        const cats = Object.entries(types)
          .filter(([_, t]) => t === 'string')
          .map(([f]) => f)
        setCategoricalFields(Array.from(new Set([...cats, 'event_type', 'user_id', 'country', 'source'])))
      } catch (err) {
        console.error('Failed to fetch schema:', err)
        setAvailableFields(['ts', 'event_type', 'user_id', 'anom_score', 'amount', 'country', 'source'])
        setNumericFields(['ts', 'anom_score', 'zscore', 'amount', 'geo_lat', 'geo_lon'])
        setCategoricalFields(['event_type', 'user_id', 'country', 'source', 'device_id'])
      }
    })()
  }, [])

  // Fetch data sources
  const dataSourcesQuery = useQuery({
    queryKey: ['dataSources'],
    queryFn: fetchDataSources,
    staleTime: 60000,
  })

  const dataSources = useMemo(() => {
    const sources = dataSourcesQuery.data || []
    return [
      { name: '', label: 'All Sources', type: 'all' as const, icon: null, sublabel: '' },
      ...sources.map((s:  any) => ({
        name:  s.name,
        label: s.name,
        sublabel: `${s.type}${s.row_count ? ` • ${s.row_count.toLocaleString()} rows` : ''}`,
        type: s.type,
        icon: s.type === 'view' ? '📊' : '📁',
      })),
    ]
  }, [dataSourcesQuery.data])

  // Build combined filters
  const combinedFilters = useMemo((): FilterCond[] => {
    const filters: FilterCond[] = [...(globalFilters || [])]

    drillDownStack.forEach(level => {
      filters.push(...level.filters)
    })

    return filters
  }, [globalFilters, drillDownStack])

  // Main data query
  const query = useQuery({
    queryKey: [
      'scatterPlot',
      instanceId,
      startISO,
      endISO,
      config.xField,
      config.yField,
      config.colorField,
      config.sizeField,
      config.analyticsMode,
      config.zThreshold,
      config.contamination,
      config.maxPoints,
      config.colorByAnomaly,
      JSON.stringify(combinedFilters),
    ],
    queryFn: async () => {
      // Fetch raw events (not aggregated)
      const result = await vizGrid({
        start: startISO,
        end: endISO,
        analytics: config.analyticsMode,
        z_thr: config.analyticsMode === 'simple' ? config.zThreshold :  undefined,
        contamination: config.analyticsMode === 'advanced' ? config.contamination : undefined,
        aggregate: false,
        limit: config.maxPoints,
        sort_by: 'anom_score',
        sort_dir: 'desc',
        where: combinedFilters.length > 0 ? combinedFilters : undefined,
        source: config.dataSource || undefined,
      })

      const rows = Array.isArray(result) ? result : (result?.rows || [])

      if (! rows || rows.length === 0) {
        pointsRef.current = []
        return {
          points: [],
          total: 0,
          anomalyStats: { total: 0, critical: 0, high: 0, medium: 0, low:  0 },
          colorGroups: [],
        }
      }

      // Build color mapping for categorical field
      const colorGroups = new Set<string>()
      rows.forEach((r: any) => {
        if (config.colorField && r[config.colorField]) {
          colorGroups.add(String(r[config.colorField]))
        }
      })
      const colorGroupArray = Array.from(colorGroups)
      const colorMap = new Map<string, string>()
      colorGroupArray.forEach((group, idx) => {
        colorMap.set(group, SCATTER_COLORS[idx % SCATTER_COLORS.length])
      })

      // Build points
      const points: PointData[] = rows.map((r: any) => {
        const anomScore = r.anom_score || 0
        const severity = getAnomalySeverity(anomScore)

        let color:  string
        if (config.colorByAnomaly && anomScore >= config.anomalyThreshold) {
          color = getAnomalyColor(anomScore)
        } else if (config.colorField && r[config.colorField]) {
          color = colorMap.get(String(r[config.colorField])) || SCATTER_COLORS[0]
        } else {
          color = SCATTER_COLORS[0]
        }

        let size = config.pointSize
        if (config.sizeField && r[config.sizeField] !== undefined) {
          const sizeVal = Number(r[config.sizeField]) || 0
          size = Math.max(4, Math.min(30, 4 + sizeVal * 2))
        }

        const labelParts = []
        if (r.user_id) labelParts.push(r.user_id)
        if (r.event_type) labelParts.push(r.event_type)
        const label = labelParts.join(' - ') || 'Event'

        return {
          x: r[config.xField],
          y: r[config.yField],
          label,
          anomScore,
          severity,
          color,
          size,
          eventData: r,
        }
      })

      pointsRef.current = points

      // Calculate anomaly statistics
      const anomalyStats = { total: 0, critical: 0, high: 0, medium: 0, low: 0 }
      points.forEach(pt => {
        if (pt.anomScore >= config.anomalyThreshold) {
          anomalyStats.total++
          if (pt.severity !== 'normal') {
            anomalyStats[pt.severity]++
          }
        }
      })

      return {
        points,
        total: points.length,
        anomalyStats,
        colorGroups:  colorGroupArray,
      }
    },
    enabled: false,
  })

  // Run query
  const handleRunQuery = useCallback(() => {
    setIsTransitioning(true)
    setSelectedPoint(null)
    query.refetch().finally(() => setIsTransitioning(false))
  }, [query])

  // Threshold preset change
  const handleThresholdPresetChange = useCallback((presetLabel: string) => {
    const preset = THRESHOLD_PRESETS.find(p => p.label === presetLabel)
    if (! preset) return
    setConfig(prev => ({
      ...prev,
      zThreshold: preset.zThreshold,
      contamination: preset.contamination,
    }))
    setThresholdPreset(presetLabel)
  }, [])

  // Execute drill-down
  const executeDrillDown = useCallback((point: PointData) => {
    const eventData = point.eventData
    if (!eventData) return

    // Create filter based on key identifying fields
    const newFilters:  FilterCond[] = []
    if (eventData.user_id) {
      newFilters.push({ field: 'user_id', op: 'eq', value:  eventData.user_id })
    }
    if (eventData.event_type) {
      newFilters.push({ field: 'event_type', op:  'eq', value: eventData.event_type })
    }

    if (newFilters.length === 0) return

    const newLevel: DrillDownLevel = {
      filters: newFilters,
      label: point.label,
      fieldValues: {
        user_id: eventData.user_id || '',
        event_type: eventData.event_type || '',
      },
    }

    setDrillDownStack(prev => {
      const newStack = [...prev, newLevel]
      setTimeout(() => {
        query.refetch()
      }, 50)
      return newStack
    })

    setSelectedPoint(null)
    setDrawerOpen(false)
  }, [query])

  // Go back in drill-down
  const handleDrillUp = useCallback((toIndex: number) => {
    setDrillDownStack(prev => {
      const newStack = prev.slice(0, toIndex)
      setTimeout(() => {
        query.refetch()
      }, 50)
      return newStack
    })
    setSelectedPoint(null)
  }, [query])

  // Reset drill-down
  const handleResetDrillDown = useCallback(() => {
    setDrillDownStack([])
    setSelectedPoint(null)
    setTimeout(() => {
      query.refetch()
    }, 50)
  }, [query])

  // Fetch similar anomaly events for drawer
  const fetchDrawerEvents = useCallback(async (point: PointData) => {
    const eventData = point.eventData
    if (!eventData) return

    setDrawerEventsLoading(true)
    setDrawerEvents([])
    setDrawerReasons([])

    try {
      const segmentFilters:  FilterCond[] = [...combinedFilters]

      // Filter by user if available
      if (eventData.user_id) {
        segmentFilters.push({ field: 'user_id', op: 'eq', value: eventData.user_id })
      }

      const result = await vizGrid({
        start: startISO,
        end: endISO,
        analytics: config.analyticsMode,
        z_thr: config.analyticsMode === 'simple' ? config.zThreshold : undefined,
        contamination: config.analyticsMode === 'advanced' ? config.contamination : undefined,
        aggregate: false,
        limit: 50,
        sort_by: 'anom_score',
        sort_dir: 'desc',
        where: segmentFilters.length > 0 ? segmentFilters : undefined,
        source: config.dataSource || undefined,
      })

      const rows = Array.isArray(result) ? result : (result?.rows || [])

      const anomalousEvents = rows
        .filter((r: any) => (r.anom_score || 0) >= config.anomalyThreshold)
        .sort((a: any, b: any) => (b.anom_score || 0) - (a.anom_score || 0))
        .slice(0, 30)

      setDrawerEvents(anomalousEvents)

      const allReasons = new Set<string>()
      anomalousEvents.forEach((evt: any) => {
        if (evt.reasons && Array.isArray(evt.reasons)) {
          evt.reasons.forEach((r: any) => {
            if (typeof r === 'string') {
              allReasons.add(r)
            } else if (r.code) {
              allReasons.add(r.code)
            }
          })
        }
      })
      setDrawerReasons(Array.from(allReasons))

    } catch (err) {
      console.error('Failed to fetch drawer events:', err)
      setDrawerEvents([])
    } finally {
      setDrawerEventsLoading(false)
    }
  }, [combinedFilters, startISO, endISO, config.analyticsMode, config.zThreshold, config.contamination, config.anomalyThreshold])

  // Export data
  const handleExport = useCallback(() => {
    if (!query.data) return
    const exportData = {
      points: query.data.points,
      total: query.data.total,
      config,
      drillDownPath: drillDownStack.map(l => l.label),
      filters: combinedFilters,
      timeRange: { start: startISO, end:  endISO },
      exportedAt: new Date().toISOString(),
    }
    const blob = new Blob([JSON.stringify(exportData, null, 2)], { type: 'application/json' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `scatter-plot-${config.xField}-${config.yField}-${new Date().toISOString().slice(0, 19).replace(/:/g, '-')}.json`
    a.click()
    URL.revokeObjectURL(url)
  }, [query.data, config, drillDownStack, combinedFilters, startISO, endISO])

  // Handle Plotly click event
  const handlePlotClick = useCallback((eventData: any) => {
    if (!eventData || !eventData.points || eventData.points.length === 0) {
      return
    }

    const point = eventData.points[0]
    const pointIndex = point.pointNumber ?? point.pointIndex ?? point.i

    const points = pointsRef.current
    if (! points || pointIndex >= points.length || pointIndex < 0) {
      return
    }

    const pt = points[pointIndex]
    if (!pt) return

    setSelectedPoint(prev => prev === pointIndex ? null : pointIndex)
    setSelectedPointData(pt)
    setDrawerOpen(true)
  }, [])

  // Plotly data
  const plotData = useMemo(() => {
    const points = query.data?.points
    if (!points?.length) return []

    const traces: any[] = []

    // Main scatter trace
    traces.push({
      type: 'scatter',
      mode: 'markers',
      x: points.map(p => p.x),
      y: points.map(p => p.y),
      marker: {
        color: points.map(p => p.color),
        size: points.map(p => p.size),
        opacity: config.pointOpacity,
        line: { color: '#ffffff', width: 1 },
      },
      hovertemplate: points.map(p => {
        let hoverText = `<b>${p.label}</b><br>`
        hoverText += `${formatFieldLabel(config.xField)}: ${formatValue(p.x)}<br>`
        hoverText += `${formatFieldLabel(config.yField)}: ${formatValue(p.y)}<br>`

        if (config.analyticsMode !== 'none' && p.anomScore >= config.anomalyThreshold) {
          const severityConfig = SEVERITY_LEVELS[p.severity]
          hoverText += `<br><b style="color: ${severityConfig.color}">⚠ ${severityConfig.label} Anomaly</b>`
          hoverText += `<br>Score: ${(p.anomScore * 100).toFixed(1)}%`
        }

        hoverText += `<br><br><i>Click for details</i>`
        return hoverText + '<extra></extra>'
      }),
    })

    // Add regression line if enabled
    if (config.showRegression && points.length > 2) {
      const xVals = points.map(p => Number(p.x) || 0).filter(v => ! isNaN(v))
      const yVals = points.map(p => Number(p.y) || 0).filter(v => !isNaN(v))

      if (xVals.length > 2) {
        const n = xVals.length
        const sumX = xVals.reduce((a, b) => a + b, 0)
        const sumY = yVals.reduce((a, b) => a + b, 0)
        const sumXY = xVals.reduce((acc, x, i) => acc + x * yVals[i], 0)
        const sumX2 = xVals.reduce((acc, x) => acc + x * x, 0)

        const slope = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX)
        const intercept = (sumY - slope * sumX) / n

        const minX = Math.min(...xVals)
        const maxX = Math.max(...xVals)

        traces.push({
          type: 'scatter',
          mode: 'lines',
          x: [minX, maxX],
          y: [slope * minX + intercept, slope * maxX + intercept],
          line: { color: '#FF5722', width: 2, dash: 'dash' },
          name: 'Regression',
          hoverinfo: 'skip',
        })
      }
    }

    return traces
  }, [query.data, config])

  // Plotly layout
  const layout:  Partial<Plotly.Layout> = useMemo(() => ({
    margin: { l: 60, r: 30, t: 40, b: 60 },
    xaxis: {
      title: formatFieldLabel(config.xField),
      showgrid: config.showGrid,
      gridcolor: '#e0e0e0',
      type: config.xLogScale ? 'log' : undefined,
    },
    yaxis: {
      title: formatFieldLabel(config.yField),
      showgrid: config.showGrid,
      gridcolor: '#e0e0e0',
      type:  config.yLogScale ? 'log' : undefined,
    },
    paper_bgcolor: 'rgba(0,0,0,0)',
    plot_bgcolor: '#fafafa',
    hovermode: 'closest',
    showlegend: false,
  }), [config])

  const plotConfig:  Partial<Plotly.Config> = {
    displaylogo: false,
    responsive: true,
    modeBarButtonsToRemove: ['select2d', 'lasso2d'],
  }

  const data = query.data
  const anomalyStats = data?.anomalyStats ?? { total: 0, critical:  0, high: 0, medium: 0, low: 0 }

  return (
    <Box sx={{ height: '100%', display: 'flex', flexDirection:  'column', overflow: 'hidden' }}>
      <Stack spacing={1} sx={{ flex: 1, overflowY: 'auto', overflowX: 'hidden', p: 1 }}>
        {/* Loading Progress */}
        {(query.isFetching || isTransitioning) && (
          <LinearProgress sx={{ position: 'fixed', top: 0, left:  0, right: 0, zIndex: 20, height: 3 }} />
        )}

        {/* Drill-down Breadcrumb Navigation */}
        {drillDownStack.length > 0 && (
          <Paper variant="outlined" sx={{ p: 1, bgcolor: alpha('#009688', 0.05) }}>
            <Stack direction="row" spacing={1} alignItems="center">
              <Tooltip title="Reset to top level">
                <IconButton size="small" onClick={handleResetDrillDown} color="primary">
                  <HomeIcon fontSize="small" />
                </IconButton>
              </Tooltip>
              <Breadcrumbs separator={<NavigateNextIcon fontSize="small" />} maxItems={5}>
                <Link
                  component="button"
                  variant="body2"
                  onClick={handleResetDrillDown}
                  underline="hover"
                  sx={{ cursor: 'pointer', color: 'primary.main' }}
                >
                  All Data
                </Link>
                {drillDownStack.map((level, idx) => (
                  <Link
                    key={idx}
                    component="button"
                    variant="body2"
                    onClick={() => idx < drillDownStack.length - 1 ? handleDrillUp(idx + 1) : undefined}
                    underline={idx === drillDownStack.length - 1 ? 'none' : 'hover'}
                    sx={{
                      cursor: idx === drillDownStack.length - 1 ? 'default' :  'pointer',
                      fontWeight: idx === drillDownStack.length - 1 ? 600 : 400,
                      color: idx === drillDownStack.length - 1 ? 'text.primary' : 'primary.main',
                    }}
                  >
                    {level.label}
                  </Link>
                ))}
              </Breadcrumbs>
              <Chip
                size="small"
                label={`${drillDownStack.length} level${drillDownStack.length > 1 ? 's' :  ''} deep`}
                color="secondary"
                variant="outlined"
                sx={{ ml: 'auto' }}
              />
            </Stack>
          </Paper>
        )}

        {/* Controls Panel */}
        <Paper variant="outlined" sx={{ p: 1.5, flexShrink: 0 }}>
          <Stack spacing={1.5}>
            <Stack direction="row" spacing={1.5} alignItems="center" flexWrap="wrap" useFlexGap>
              <Badge
                badgeContent={anomalyStats.critical + anomalyStats.high}
                color="error"
                invisible={anomalyStats.critical + anomalyStats.high === 0}
              >
                <Chip
                  icon={<ScatterPlotIcon />}
                  label="Correlation"
                  sx={{ bgcolor: '#009688', color: '#fff', fontWeight: 600 }}
                />
              </Badge>

              {/* Data Source Selector */}
              <FormControl size="small" sx={{ minWidth:  180 }}>
                <InputLabel>Data Source</InputLabel>
                <Select
                  value={config.dataSource}
                  onChange={e => setConfig(prev => ({ ...prev, dataSource: e.target.value }))}
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

              {/* X Axis Field */}
              <TextField
                select
                size="small"
                label="X Axis"
                value={config.xField}
                onChange={e => setConfig(prev => ({ ...prev, xField: e.target.value }))}
                sx={{ minWidth: 140 }}
              >
                {availableFields.map(f => (
                  <MenuItem key={f} value={f}>{formatFieldLabel(f)}</MenuItem>
                ))}
              </TextField>

              {/* Y Axis Field */}
              <TextField
                select
                size="small"
                label="Y Axis"
                value={config.yField}
                onChange={e => setConfig(prev => ({ ...prev, yField: e.target.value }))}
                sx={{ minWidth: 140 }}
              >
                {availableFields.map(f => (
                  <MenuItem key={f} value={f}>{formatFieldLabel(f)}</MenuItem>
                ))}
              </TextField>

              {/* Analytics Mode */}
              <TextField
                select
                size="small"
                label="Analytics"
                value={config.analyticsMode}
                onChange={e => setConfig(prev => ({ ...prev, analyticsMode: e.target.value as AnalyticsMode }))}
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
              {config.analyticsMode !== 'none' && (
                <Stack direction="row" spacing={1} alignItems="center">
                  <Typography variant="caption" color="text.secondary" sx={{ mr: 0.5 }}>Sensitivity:</Typography>
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
                      sx={{ ml:  1, fontWeight: 600 }}
                    />
                  )}
                </Stack>
              )}

              <Box sx={{ flex: 1 }} />

              {/* Regression Toggle */}
              <Tooltip title="Show Regression Line">
                <IconButton
                  size="small"
                  onClick={() => setConfig(prev => ({ ...prev, showRegression: !prev.showRegression }))}
                  color={config.showRegression ? 'primary' : 'default'}
                >
                  <ShowChartIcon />
                </IconButton>
              </Tooltip>

              {/* Export */}
              <Tooltip title="Export Data">
                <IconButton size="small" onClick={handleExport} disabled={!data}>
                  <DownloadIcon />
                </IconButton>
              </Tooltip>

              {/* Advanced Settings Toggle */}
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
                onClick={handleRunQuery}
                disabled={query.isFetching}
                startIcon={query.isFetching ? <CircularProgress size={16} color="inherit" /> : <RefreshIcon />}
                sx={{ minWidth: 100, bgcolor: '#009688', '&:hover': { bgcolor: '#00796B' } }}
              >
                {query.isFetching ? 'Loading...' : 'Run'}
              </Button>
            </Stack>

            {/* Advanced Controls */}
            <Collapse in={showAdvancedControls}>
              <Divider sx={{ my: 1 }} />
              <Stack spacing={2}>
                {/* Row 1: Color and Size options */}
                <Stack direction="row" spacing={2} alignItems="center" flexWrap="wrap" useFlexGap>
                  <TextField
                    select
                    size="small"
                    label="Color By"
                    value={config.colorField}
                    onChange={e => setConfig(prev => ({ ...prev, colorField: e.target.value }))}
                    sx={{ minWidth: 140 }}
                  >
                    <MenuItem value="">None</MenuItem>
                    {categoricalFields.map(f => (
                      <MenuItem key={f} value={f}>{formatFieldLabel(f)}</MenuItem>
                    ))}
                  </TextField>

                  <TextField
                    select
                    size="small"
                    label="Size By"
                    value={config.sizeField}
                    onChange={e => setConfig(prev => ({ ...prev, sizeField: e.target.value }))}
                    sx={{ minWidth:  140 }}
                  >
                    <MenuItem value="">Fixed</MenuItem>
                    {numericFields.map(f => (
                      <MenuItem key={f} value={f}>{formatFieldLabel(f)}</MenuItem>
                    ))}
                  </TextField>

                  <Box sx={{ width: 150 }}>
                    <Typography variant="caption" color="text.secondary">
                      Point Size:  {config.pointSize}
                    </Typography>
                    <Slider
                      size="small"
                      value={config.pointSize}
                      min={3}
                      max={20}
                      onChange={(_, v) => setConfig(prev => ({ ...prev, pointSize: v as number }))}
                    />
                  </Box>

                  <Box sx={{ width:  150 }}>
                    <Typography variant="caption" color="text.secondary">
                      Opacity: {config.pointOpacity.toFixed(1)}
                    </Typography>
                    <Slider
                      size="small"
                      value={config.pointOpacity}
                      min={0.1}
                      max={1}
                      step={0.1}
                      onChange={(_, v) => setConfig(prev => ({ ...prev, pointOpacity: v as number }))}
                    />
                  </Box>

                  <Box sx={{ width: 180 }}>
                    <Typography variant="caption" color="text.secondary">
                      Max Points: {config.maxPoints.toLocaleString()}
                    </Typography>
                    <Slider
                      size="small"
                      value={config.maxPoints}
                      min={500}
                      max={10000}
                      step={500}
                      onChange={(_, v) => setConfig(prev => ({ ...prev, maxPoints: v as number }))}
                    />
                  </Box>
                </Stack>

                {/* Row 2: Scale and display options */}
                <Stack direction="row" spacing={2} alignItems="center" flexWrap="wrap" useFlexGap>
                  <FormControlLabel
                    control={
                      <Switch
                        checked={config.xLogScale}
                        onChange={e => setConfig(prev => ({ ...prev, xLogScale: e.target.checked }))}
                        size="small"
                      />
                    }
                    label={<Typography variant="caption">X Log Scale</Typography>}
                  />

                  <FormControlLabel
                    control={
                      <Switch
                        checked={config.yLogScale}
                        onChange={e => setConfig(prev => ({ ...prev, yLogScale: e.target.checked }))}
                        size="small"
                      />
                    }
                    label={<Typography variant="caption">Y Log Scale</Typography>}
                  />

                  <FormControlLabel
                    control={
                      <Switch
                        checked={config.showGrid}
                        onChange={e => setConfig(prev => ({ ...prev, showGrid: e.target.checked }))}
                        size="small"
                      />
                    }
                    label={<Typography variant="caption">Show Grid</Typography>}
                  />

                  {config.analyticsMode !== 'none' && (
                    <>
                      <Box sx={{ width: 200 }}>
                        <Typography variant="caption" color="text.secondary">
                          Anomaly Threshold: {(config.anomalyThreshold * 100).toFixed(0)}%
                        </Typography>
                        <Slider
                          size="small"
                          value={config.anomalyThreshold}
                          min={0.1}
                          max={0.9}
                          step={0.05}
                          onChange={(_, v) => setConfig(prev => ({ ...prev, anomalyThreshold: v as number }))}
                          sx={{
                            '& .MuiSlider-track': {
                              background: 'linear-gradient(90deg, #388E3C 0%, #F57C00 50%, #D32F2F 100%)',
                            },
                          }}
                        />
                      </Box>

                      <FormControlLabel
                        control={
                          <Switch
                            checked={config.colorByAnomaly}
                            onChange={e => setConfig(prev => ({ ...prev, colorByAnomaly: e.target.checked }))}
                            size="small"
                            color="error"
                          />
                        }
                        label={
                          <Stack direction="row" spacing={0.5} alignItems="center">
                            <WarningAmberIcon fontSize="small" color="error" />
                            <Typography variant="caption">Color by Anomaly</Typography>
                          </Stack>
                        }
                      />
                    </>
                  )}
                </Stack>
              </Stack>
            </Collapse>
          </Stack>
        </Paper>

        {/* Anomaly Summary Alert */}
        {config.analyticsMode !== 'none' && anomalyStats.total > 0 && (
          <Alert
            severity={anomalyStats.critical > 0 || anomalyStats.high > 0 ? 'error' :  'warning'}
            icon={<WarningAmberIcon />}
            sx={{ py: 0.5, flexShrink: 0 }}
          >
            <Stack direction="row" spacing={2} alignItems="center" flexWrap="wrap">
              <Typography variant="body2">
                <strong>{anomalyStats.total}</strong> anomalous points (score ≥ {(config.anomalyThreshold * 100).toFixed(0)}%)
              </Typography>
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

        {/* Severity Legend */}
        {config.analyticsMode !== 'none' && config.colorByAnomaly && (
          <Paper variant="outlined" sx={{ px: 2, py: 0.75, bgcolor: '#fafafa' }}>
            <Stack direction="row" spacing={2.5} alignItems="center" flexWrap="wrap">
              <Typography variant="caption" fontWeight={600} color="text.secondary">
                Anomaly Severity:
              </Typography>
              {Object.entries(SEVERITY_LEVELS)
                .filter(([key]) => key !== 'normal')
                .map(([key, level]) => (
                  <Stack key={key} direction="row" spacing={0.5} alignItems="center">
                    <Box
                      sx={{
                        width: 12,
                        height: 12,
                        borderRadius: '50%',
                        bgcolor: level.color,
                      }}
                    />
                    <Typography variant="caption">{level.label} (≥{level.min * 100}%)</Typography>
                  </Stack>
                ))}
            </Stack>
          </Paper>
        )}

        {/* Chart */}
        <Paper variant="outlined" sx={{ minHeight:  CHART_HEIGHT, height:  CHART_HEIGHT, position: 'relative', flexShrink: 0 }}>
          {! data?.points?.length && ! query.isFetching ? (
            <Box
              sx={{
                height: '100%',
                display: 'flex',
                alignItems:  'center',
                justifyContent: 'center',
                flexDirection: 'column',
                gap: 2,
                color: 'text.secondary',
              }}
            >
              <ScatterPlotIcon sx={{ fontSize: 64, opacity: 0.3 }} />
              <Typography variant="h6" color="text.secondary">
                No scatter data
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Click Run to generate scatter plot
              </Typography>
              <Button variant="outlined" startIcon={<RefreshIcon />} onClick={handleRunQuery}>
                Run Query
              </Button>
            </Box>
          ) : (
            <Plot
              data={plotData as any}
              layout={layout as any}
              config={plotConfig}
              style={{ width: '100%', height: '100%' }}
              useResizeHandler
              onClick={handlePlotClick}
            />
          )}
        </Paper>

        {/* Summary Stats */}
        {data?.points?.length > 0 && (
          <Paper variant="outlined" sx={{ p: 1.5, bgcolor: '#fafafa', flexShrink: 0 }}>
            <Stack direction="row" spacing={3} alignItems="center" flexWrap="wrap">
              <Box>
                <Typography variant="caption" color="text.secondary">Total Points</Typography>
                <Typography variant="h6" fontWeight={700}>{data.total.toLocaleString()}</Typography>
              </Box>
              <Divider orientation="vertical" flexItem />
              <Box>
                <Typography variant="caption" color="text.secondary">X Axis</Typography>
                <Typography variant="body1" fontWeight={600}>{formatFieldLabel(config.xField)}</Typography>
              </Box>
              <Divider orientation="vertical" flexItem />
              <Box>
                <Typography variant="caption" color="text.secondary">Y Axis</Typography>
                <Typography variant="body1" fontWeight={600}>{formatFieldLabel(config.yField)}</Typography>
              </Box>
              {data.colorGroups && data.colorGroups.length > 0 && (
                <>
                  <Divider orientation="vertical" flexItem />
                  <Box>
                    <Typography variant="caption" color="text.secondary">Color Groups</Typography>
                    <Typography variant="body1" fontWeight={600}>{data.colorGroups.length}</Typography>
                  </Box>
                </>
              )}
            </Stack>
          </Paper>
        )}
      </Stack>
            {/* Detail Drawer */}
      <Drawer
        anchor="right"
        open={drawerOpen}
        onClose={() => {
          setDrawerOpen(false)
          setDrawerEvents([])
          setDrawerEventsLoading(false)
        }}
        PaperProps={{
          sx: { width: { xs: '100%', sm: 520, md: 620 }, p: 0 },
        }}
      >
        {selectedPointData && (
          <>
            {/* Header */}
            <Box
              sx={{
                p: 2,
                background: `linear-gradient(135deg, ${SEVERITY_LEVELS[selectedPointData.severity].color}15 0%, ${SEVERITY_LEVELS[selectedPointData.severity].bgColor} 100%)`,
                borderBottom: `3px solid ${SEVERITY_LEVELS[selectedPointData.severity].color}`,
              }}
            >
              <Stack direction="row" justifyContent="space-between" alignItems="flex-start">
                <Stack spacing={1}>
                  <Stack direction="row" spacing={1} alignItems="center">
                    {React.createElement(SEVERITY_LEVELS[selectedPointData.severity].icon, {
                      sx: { color: SEVERITY_LEVELS[selectedPointData.severity].color, fontSize: 28 }
                    })}
                    <Typography variant="h6" fontWeight={700}>
                      Point Details
                    </Typography>
                  </Stack>
                  <Typography variant="body2" fontWeight={600} sx={{ maxWidth: 400, wordBreak: 'break-word' }}>
                    {selectedPointData.label}
                  </Typography>
                </Stack>
                <IconButton onClick={() => {
                  setDrawerOpen(false)
                  setDrawerEvents([])
                }} size="small">
                  <CloseIcon />
                </IconButton>
              </Stack>
            </Box>

            <Box sx={{ p: 2, overflowY: 'auto', height: 'calc(100% - 140px)' }}>
              {/* Anomaly Score Card */}
              {config.analyticsMode !== 'none' && selectedPointData.anomScore >= config.anomalyThreshold && (
                <Card variant="outlined" sx={{ mb: 2, borderColor:  SEVERITY_LEVELS[selectedPointData.severity].color }}>
                  <CardContent>
                    <Stack direction="row" justifyContent="space-between" alignItems="center" mb={1}>
                      <Typography variant="subtitle2" color="text.secondary">
                        Anomaly Score
                      </Typography>
                      <Chip
                        label={SEVERITY_LEVELS[selectedPointData.severity].label}
                        size="small"
                        sx={{
                          bgcolor: SEVERITY_LEVELS[selectedPointData.severity].color,
                          color: '#fff',
                          fontWeight: 600,
                        }}
                      />
                    </Stack>
                    <Stack direction="row" alignItems="flex-end" spacing={1}>
                      <Typography variant="h3" fontWeight={700} color={SEVERITY_LEVELS[selectedPointData.severity].color}>
                        {(selectedPointData.anomScore * 100).toFixed(1)}%
                      </Typography>
                      <Typography variant="body2" color="text.secondary" pb={0.5}>
                        ({selectedPointData.anomScore.toFixed(4)})
                      </Typography>
                    </Stack>
                    <LinearProgress
                      variant="determinate"
                      value={Math.min(selectedPointData.anomScore * 100, 100)}
                      sx={{
                        mt: 1.5,
                        height: 8,
                        borderRadius: 4,
                        bgcolor: '#e0e0e0',
                        '& .MuiLinearProgress-bar': {
                          bgcolor: SEVERITY_LEVELS[selectedPointData.severity].color,
                          borderRadius: 4,
                        },
                      }}
                    />
                    <Typography variant="caption" color="text.secondary" sx={{ mt: 1, display: 'block' }}>
                      {config.analyticsMode === 'simple'
                        ? `Mode: Simple (Z-Score), Threshold: z=${config.zThreshold}`
                        : `Mode: Advanced (IForest), Contamination: ${(config.contamination * 100).toFixed(1)}%`}
                    </Typography>
                  </CardContent>
                </Card>
              )}

              {/* Point Coordinates */}
              <Card variant="outlined" sx={{ mb:  2 }}>
                <CardContent>
                  <Typography variant="subtitle2" fontWeight={600} gutterBottom>
                    Point Coordinates
                  </Typography>
                  <Table size="small">
                    <TableBody>
                      <TableRow>
                        <TableCell component="th" sx={{ fontWeight: 600, border: 'none', pl: 0 }}>
                          {formatFieldLabel(config.xField)} (X)
                        </TableCell>
                        <TableCell align="right" sx={{ border: 'none', fontWeight: 700 }}>
                          {formatValue(selectedPointData.x)}
                        </TableCell>
                      </TableRow>
                      <TableRow>
                        <TableCell component="th" sx={{ fontWeight: 600, border:  'none', pl: 0 }}>
                          {formatFieldLabel(config.yField)} (Y)
                        </TableCell>
                        <TableCell align="right" sx={{ border:  'none', fontWeight: 700 }}>
                          {formatValue(selectedPointData.y)}
                        </TableCell>
                      </TableRow>
                      {config.analyticsMode !== 'none' && (
                        <TableRow>
                          <TableCell component="th" sx={{ fontWeight: 600, border: 'none', pl: 0 }}>Anomaly Score</TableCell>
                          <TableCell align="right" sx={{ border: 'none', fontWeight: 700, color: selectedPointData.anomScore >= config.anomalyThreshold ? SEVERITY_LEVELS[selectedPointData.severity].color : 'inherit' }}>
                            {(selectedPointData.anomScore * 100).toFixed(1)}%
                          </TableCell>
                        </TableRow>
                      )}
                    </TableBody>
                  </Table>
                </CardContent>
              </Card>

              {/* Event Details */}
              {selectedPointData.eventData && (
                <Card variant="outlined" sx={{ mb: 2 }}>
                  <CardContent>
                    <Typography variant="subtitle2" fontWeight={600} gutterBottom>
                      Event Details
                    </Typography>
                    <Stack spacing={1}>
                      {selectedPointData.eventData.ts && (
                        <Stack direction="row" justifyContent="space-between" alignItems="center">
                          <Typography variant="body2" color="text.secondary">Timestamp</Typography>
                          <Typography variant="body2" fontWeight={600}>
                            {new Date(selectedPointData.eventData.ts).toLocaleString()}
                          </Typography>
                        </Stack>
                      )}
                      {selectedPointData.eventData.user_id && (
                        <Stack direction="row" justifyContent="space-between" alignItems="center">
                          <Typography variant="body2" color="text.secondary">User ID</Typography>
                          <Chip label={selectedPointData.eventData.user_id} size="small" variant="outlined" />
                        </Stack>
                      )}
                      {selectedPointData.eventData.event_type && (
                        <Stack direction="row" justifyContent="space-between" alignItems="center">
                          <Typography variant="body2" color="text.secondary">Event Type</Typography>
                          <Chip label={selectedPointData.eventData.event_type} size="small" variant="outlined" />
                        </Stack>
                      )}
                      {selectedPointData.eventData.ip && (
                        <Stack direction="row" justifyContent="space-between" alignItems="center">
                          <Typography variant="body2" color="text.secondary">IP Address</Typography>
                          <Chip label={selectedPointData.eventData.ip} size="small" variant="outlined" />
                        </Stack>
                      )}
                      {selectedPointData.eventData.country && (
                        <Stack direction="row" justifyContent="space-between" alignItems="center">
                          <Typography variant="body2" color="text.secondary">Location</Typography>
                          <Chip 
                            icon={<LocationOnIcon sx={{ fontSize: '14px ! important' }} />}
                            label={`${selectedPointData.eventData.city || ''} ${selectedPointData.eventData.country}`.trim()} 
                            size="small" 
                            variant="outlined" 
                          />
                        </Stack>
                      )}
                      {selectedPointData.eventData.device_id && (
                        <Stack direction="row" justifyContent="space-between" alignItems="center">
                          <Typography variant="body2" color="text.secondary">Device</Typography>
                          <Chip 
                            icon={<DevicesIcon sx={{ fontSize: '14px !important' }} />}
                            label={selectedPointData.eventData.device_id} 
                            size="small" 
                            variant="outlined" 
                          />
                        </Stack>
                      )}
                      {selectedPointData.eventData.source && (
                        <Stack direction="row" justifyContent="space-between" alignItems="center">
                          <Typography variant="body2" color="text.secondary">Source</Typography>
                          <Chip label={selectedPointData.eventData.source} size="small" variant="outlined" />
                        </Stack>
                      )}
                    </Stack>
                  </CardContent>
                </Card>
              )}

              {/* Anomaly Events Section */}
              {config.analyticsMode !== 'none' && (
                <>
                  <Divider sx={{ my: 2 }} />

                  <Stack direction="row" justifyContent="space-between" alignItems="center" mb={1}>
                    <Typography variant="subtitle2" fontWeight={600}>
                      Related Anomaly Events
                    </Typography>
                    {! drawerEventsLoading && drawerEvents.length === 0 && (
                      <Button
                        size="small"
                        variant="outlined"
                        onClick={() => fetchDrawerEvents(selectedPointData)}
                        startIcon={<RefreshIcon />}
                      >
                        Load Events
                      </Button>
                    )}
                  </Stack>

                  {drawerEventsLoading ? (
                    <Box sx={{ py: 3, textAlign: 'center' }}>
                      <CircularProgress size={32} />
                      <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
                        Loading related events...
                      </Typography>
                    </Box>
                  ) : drawerEvents.length === 0 ? (
                    <Alert severity="info" sx={{ mb: 2 }}>
                      Click "Load Events" to fetch related anomaly events.
                    </Alert>
                  ) : (
                    <>
                      {/* Anomaly Reasons Summary */}
                      {drawerReasons.length > 0 && (
                        <Box mb={2}>
                          <Typography variant="body2" fontWeight={600} gutterBottom>
                            Anomaly Reasons
                          </Typography>
                          <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
                            {drawerReasons.map((reason, i) => (
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

                      {/* Events Table */}
                      <Typography variant="body2" color="text.secondary" gutterBottom>
                        Showing {drawerEvents.length} related anomalous events
                      </Typography>

                      <TableContainer component={Paper} variant="outlined" sx={{ maxHeight: 300, mb: 2 }}>
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
                            {drawerEvents.map((event, idx) => {
                              const evtSeverity = getAnomalySeverity(event.anom_score || 0)
                              const evtConfig = SEVERITY_LEVELS[evtSeverity]
                              return (
                                <TableRow
                                  key={idx}
                                  sx={{
                                    bgcolor: (event.anom_score || 0) >= 0.5 ? `${evtConfig.color}08` : 'inherit',
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
                                      label={`${((event.anom_score || 0) * 100).toFixed(0)}%`}
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
                                            icon={<LocationOnIcon sx={{ fontSize: '12px !important' }} />}
                                            label={event.country}
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

                      {/* Detailed Reasons by Event */}
                      {drawerEvents.some((e) => e.reasons && e.reasons.length > 0) && (
                        <Box mt={2}>
                          <Typography variant="body2" fontWeight={600} gutterBottom>
                            Detailed Reasons by Event
                          </Typography>
                          <Stack spacing={1}>
                            {drawerEvents
                              .filter((e) => e.reasons && e.reasons.length > 0)
                              .slice(0, 5)
                              .map((event, idx) => (
                                <Paper key={idx} variant="outlined" sx={{ p: 1.5 }}>
                                  <Stack direction="row" justifyContent="space-between" alignItems="center" mb={0.5}>
                                    <Typography variant="body2" fontWeight={600}>
                                      {event.user_id || 'Unknown'} — {event.event_type || 'event'}
                                    </Typography>
                                    <Chip
                                      label={`${((event.anom_score || 0) * 100).toFixed(0)}%`}
                                      size="small"
                                      sx={{ bgcolor:  SEVERITY_LEVELS[getAnomalySeverity(event.anom_score || 0)].color, color: '#fff' }}
                                    />
                                  </Stack>
                                  <Stack direction="row" spacing={0.5} flexWrap="wrap">
                                    {event.reasons?.map((r:  any, ri: number) => (
                                      <Tooltip key={ri} title={r.desc || r.code || r}>
                                        <Chip
                                          label={typeof r === 'string' ? r : r.code}
                                          size="small"
                                          variant="outlined"
                                          sx={{ fontSize:  10, mb: 0.5 }}
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
                    </>
                  )}
                </>
              )}

              <Divider sx={{ my: 2 }} />

              {/* Drill Down Button */}
              {selectedPointData.eventData && (selectedPointData.eventData.user_id || selectedPointData.eventData.event_type) && (
                <Button
                  variant="contained"
                  fullWidth
                  size="large"
                  startIcon={<ZoomInIcon />}
                  onClick={() => executeDrillDown(selectedPointData)}
                  sx={{
                    bgcolor: '#009688',
                    '&: hover': { bgcolor: '#00796B' },
                    py: 1.5,
                    fontSize: '1rem',
                    fontWeight: 600,
                  }}
                >
                  Drill Down into "{selectedPointData.label}"
                </Button>
              )}
            </Box>
          </>
        )}
      </Drawer>
    </Box>
  )
}