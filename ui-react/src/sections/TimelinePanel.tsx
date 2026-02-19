// ui-react/src/sections/TimelinePanel.tsx
// Complete Enhanced Timeline Panel with: 
// - Data source selection (tables, virtual views, materialized views)
// - Multiple group-by fields
// - Inline anomaly markers on the curve
// - Click-to-detail for anomalies
// - Persistent data across navigation
// - Smooth transitions on parameter changes
// - All visualization controls

import React, { useState, useEffect, useMemo, useCallback, useRef } from 'react'
import Plot from 'react-plotly.js'
import {
  Box,
  Stack,
  Button,
  TextField,
  MenuItem,
  Slider,
  Chip,
  Typography,
  Autocomplete,
  Paper,
  FormControlLabel,
  Switch,
  Alert,
  Tooltip,
  Badge,
  IconButton,
  Collapse,
  Divider,
  CircularProgress,
  Select,
  InputLabel,
  FormControl,
  ToggleButton,
  ToggleButtonGroup,
  Skeleton,
  OutlinedInput,
  Checkbox,
  ListItemText,
  LinearProgress,
  Card,
  CardContent,
  alpha,
} from '@mui/material'
import WarningAmberIcon from '@mui/icons-material/WarningAmber'
import RefreshIcon from '@mui/icons-material/Refresh'
import TuneIcon from '@mui/icons-material/Tune'
import StorageIcon from '@mui/icons-material/Storage'
import ViewModuleIcon from '@mui/icons-material/ViewModule'
import TimelineIcon from '@mui/icons-material/Timeline'
import DownloadIcon from '@mui/icons-material/Download'
import ZoomInIcon from '@mui/icons-material/ZoomIn'
import ZoomOutIcon from '@mui/icons-material/ZoomOut'
import RestartAltIcon from '@mui/icons-material/RestartAlt'
import InfoOutlinedIcon from '@mui/icons-material/InfoOutlined'
import TrendingUpIcon from '@mui/icons-material/TrendingUp'
import TrendingDownIcon from '@mui/icons-material/TrendingDown'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import {
  vizTimeline,
  fetchTopUsers,
  fetchSchemaFields,
  fetchDataSources,
  type AnalyticsMode,
  type Bucket,
  type Metric,
  type FilterCond,
  type TimelineResponse,
  type DataSource,
  type TimelineAnomalyEvent,
} from '../api'
import { useFilters } from '../context/FiltersContext'
import AnomalyDetailDrawer from '../components/AnomalyDetailDrawer'

// ============================================================
// Constants & Configuration
// ============================================================

const CHART_HEIGHT = 420
const CHART_HEIGHT_EXPANDED = 520

// Local storage keys for persistence
const LS_PREFIX = 'lizard.timeline'

const BUCKET_OPTIONS:  { value: Bucket; label: string }[] = [
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
  { label: 'Balanced', zThreshold: 3.0, contamination: 0.05, color: '#F57C00', icon: '🟡' },
  { label: 'Low Sensitivity', zThreshold:  5.0, contamination: 0.02, color: '#388E3C', icon: '🟢' },
]

const METRIC_OPTIONS: { value: Metric; label: string; description: string }[] = [
  { value: 'count', label: 'Count', description: 'Number of events' },
  { value: 'sum', label: 'Sum', description: 'Sum of values' },
  { value: 'avg', label: 'Average', description: 'Average value' },
  { value:  'max', label: 'Maximum', description: 'Maximum value' },
  { value: 'min', label: 'Minimum', description: 'Minimum value' },
]

const ALL_GROUP_FIELDS = [
  { value: 'user_id', label: 'User ID', icon: '👤' },
  { value:  'event_type', label:  'Event Type', icon: '📋' },
  { value: 'device_id', label: 'Device ID', icon: '📱' },
  { value: 'ip', label: 'IP Address', icon: '🌐' },
  { value: 'country', label: 'Country', icon: '🏳️' },
  { value:  'city', label: 'City', icon: '🏙️' },
  { value: 'account_id', label: 'Account ID', icon: '🏦' },
  { value: 'card_hash', label: 'Card Hash', icon: '💳' },
  { value:  'source', label: 'Data Source', icon: '💾' },
  { value: 'session_id', label: 'Session ID', icon: '🔑' },
]

const TIMEZONE_OPTIONS = [
  { value: 'UTC', label: 'UTC' },
  { value: 'Europe/London', label: 'London (GMT)' },
  { value: 'Europe/Paris', label: 'Paris (CET)' },
  { value: 'Europe/Berlin', label: 'Berlin (CET)' },
  { value: 'America/New_York', label: 'New York (EST)' },
  { value: 'America/Chicago', label: 'Chicago (CST)' },
  { value: 'America/Los_Angeles', label: 'Los Angeles (PST)' },
  { value: 'Asia/Tokyo', label: 'Tokyo (JST)' },
  { value: 'Asia/Shanghai', label: 'Shanghai (CST)' },
  { value: 'Asia/Dubai', label: 'Dubai (GST)' },
  { value: 'Australia/Sydney', label: 'Sydney (AEST)' },
]

// Anomaly severity configuration with visual properties
const SEVERITY_LEVELS = {
  critical: { 
    min: 0.9, 
    color: '#7B1FA2', 
    bgColor: '#F3E5F5',
    label: 'Critical', 
    glow: true,
    pulseAnimation: true,
  },
  high: { 
    min: 0.75, 
    color: '#D32F2F', 
    bgColor: '#FFEBEE',
    label:  'High', 
    glow: true,
    pulseAnimation: false,
  },
  medium:  { 
    min: 0.5, 
    color: '#F57C00', 
    bgColor: '#FFF3E0',
    label: 'Medium', 
    glow: false,
    pulseAnimation: false,
  },
  low:  { 
    min: 0.25, 
    color: '#FBC02D', 
    bgColor: '#FFFDE7',
    label: 'Low', 
    glow:  false,
    pulseAnimation:  false,
  },
  normal: { 
    min: 0, 
    color: '#388E3C', 
    bgColor: '#E8F5E9',
    label: 'Normal', 
    glow:  false,
    pulseAnimation:  false,
  },
}

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

function getSeverityConfig(score: number) {
  return SEVERITY_LEVELS[getAnomalySeverity(score)]
}

// Series color palette - visually distinct colors
const SERIES_PALETTE = [
  '#1976D2', // Blue
  '#388E3C', // Green
  '#7B1FA2', // Purple
  '#F57C00', // Orange
  '#C2185B', // Pink
  '#0097A7', // Cyan
  '#AFB42B', // Lime
  '#5D4037', // Brown
  '#455A64', // Blue Grey
  '#E64A19', // Deep Orange
  '#512DA8', // Deep Purple
  '#00796B', // Teal
  '#FFA000', // Amber
  '#303F9F', // Indigo
  '#689F38', // Light Green
]

function getSeriesColor(index: number, isOthers: boolean): string {
  if (isOthers) return '#9E9E9E'
  return SERIES_PALETTE[index % SERIES_PALETTE.length]
}

// ============================================================
// Types
// ============================================================

export interface TimelinePanelProps {
  instanceId?:  string
  initialConfig?:  Partial<TimelineConfig>
  onConfigChange?: (config: TimelineConfig) => void
  compact?: boolean
}

export interface TimelineConfig {
  dataSource: string
  analyticsMode: AnalyticsMode
  bucket: Bucket
  metric:  Metric
  valueField: string
  groupFields: string[]
  topN: number
  zThreshold: number
  contamination: number
  speedThreshold: number
  distanceThreshold: number
  normalize: boolean
  smooth: number
  stacked: boolean
  showAnomalyOverlay: boolean
  showOthers: boolean
  timezone: string
  autoRefresh: boolean
  refreshInterval: number
  thresholdPreset:  string | null
  selectedSeverities: string[]
  minAnomalyScore: number
}

const DEFAULT_CONFIG: TimelineConfig = {
  dataSource: '',
  analyticsMode: 'none',
  bucket: '5m',
  metric: 'count',
  valueField: 'anom_score',
  groupFields:  ['user_id'],
  topN: 8,
  zThreshold: 3.0,
  contamination: 0.05,
  speedThreshold: 900,
  distanceThreshold:  2000,
  normalize: false,
  smooth: 0,
  stacked: true,
  showAnomalyOverlay: true,
  showOthers: true,
  timezone: 'UTC',
  autoRefresh: false,
  refreshInterval: 60,
  thresholdPreset:  'balanced',
  selectedSeverities: ['critical', 'high', 'medium', 'low', 'normal'],
  minAnomalyScore: 0.0,
}

// ============================================================
// Persistence Helpers
// ============================================================

function loadPersistedConfig(instanceId: string): Partial<TimelineConfig> | null {
  try {
    const stored = localStorage.getItem(`${LS_PREFIX}.${instanceId}.config`)
    return stored ? JSON.parse(stored) : null
  } catch {
    return null
  }
}

function savePersistedConfig(instanceId: string, config:  TimelineConfig) {
  try {
    localStorage.setItem(`${LS_PREFIX}.${instanceId}.config`, JSON.stringify(config))
  } catch {
    // Ignore storage errors
  }
}

function loadPersistedData(instanceId:  string): TimelineResponse | null {
  try {
    const stored = sessionStorage.getItem(`${LS_PREFIX}.${instanceId}.data`)
    return stored ? JSON.parse(stored) : null
  } catch {
    return null
  }
}

function savePersistedData(instanceId:  string, data: TimelineResponse) {
  try {
    sessionStorage.setItem(`${LS_PREFIX}.${instanceId}.data`, JSON.stringify(data))
  } catch {
    // Ignore storage errors
  }
}

// ============================================================
// Main Component
// ============================================================

export default function TimelinePanel({
  instanceId = 'default',
  initialConfig,
  onConfigChange,
  compact = false,
}: TimelinePanelProps) {
  const { startISO, endISO, filters } = useFilters()
  const queryClient = useQueryClient()
  const plotRef = useRef<any>(null)

  // ---- State Management ----
  
  // Load persisted config on mount
  const [config, setConfig] = useState<TimelineConfig>(() => {
    const persisted = loadPersistedConfig(instanceId)
    return {
      ...DEFAULT_CONFIG,
      ...persisted,
      ...initialConfig,
    }
  })

  // UI State
  const [showAdvancedControls, setShowAdvancedControls] = useState(false)
  const [selectedGroups, setSelectedGroups] = useState<string[]>([])
  const [hiddenSeries, setHiddenSeries] = useState<Set<string>>(new Set())
  const [chartHeight, setChartHeight] = useState(CHART_HEIGHT)
  const [isTransitioning, setIsTransitioning] = useState(false)

  // Persisted data state for smooth navigation
  const [persistedData, setPersistedData] = useState<TimelineResponse | null>(() => 
    loadPersistedData(instanceId)
  )

  // Anomaly detail drawer state
  const [drawerOpen, setDrawerOpen] = useState(false)
  const [selectedAnomaly, setSelectedAnomaly] = useState<{
    bucketTime: string
    score: number
    reasons: string[]
    explain: string
    events: TimelineAnomalyEvent[]
  } | null>(null)

  // Zoom state
  const [zoomRange, setZoomRange] = useState<{ x?:  [string, string]; y?: [number, number] } | null>(null)

  // ---- Persist config changes ----
  useEffect(() => {
    savePersistedConfig(instanceId, config)
    onConfigChange?.(config)
  }, [config, instanceId, onConfigChange])

  // ---- Data Fetching ----

  // Fetch available data sources
  const dataSourcesQuery = useQuery({
    queryKey: ['dataSources'],
    queryFn: fetchDataSources,
    staleTime: 60000,
    cacheTime: 300000,
  })

  // Fetch schema fields for numeric field options
  const schemaQuery = useQuery({
    queryKey: ['schemaFields'],
    queryFn: fetchSchemaFields,
    staleTime: 60000,
    cacheTime: 300000,
  })

  const numericFields = useMemo(() => {
    if (!schemaQuery.data) return ['anom_score', 'zscore', 'amount']
    const nums = Object.entries(schemaQuery.data.types)
      .filter(([_, t]) => t === 'number')
      .map(([f]) => f)
    return Array.from(new Set([...nums, 'anom_score', 'zscore', 'amount']))
  }, [schemaQuery.data])

  // Fetch top users/groups
  const topUsersQuery = useQuery({
    queryKey: ['topUsers', startISO, endISO, config.groupFields, config.topN, filters],
    queryFn: () =>
      fetchTopUsers({
        start: startISO,
        end: endISO,
        n: Math.max(config.topN * 2, 50),
        group_field: config.groupFields.join(','),
        where: filters,
      }),
    enabled: Boolean(startISO && endISO),
    staleTime: 30000,
    cacheTime:  120000,
  })

  // Build query key for timeline data
  const timelineQueryKey = useMemo(() => [
    'timeline',
    instanceId,
    startISO,
    endISO,
    config.dataSource,
    config.analyticsMode,
    config.bucket,
    config.metric,
    config.valueField,
    config.groupFields.join(','),
    config.topN,
    config.zThreshold,
    config.contamination,
    config.normalize,
    config.smooth,
    config.timezone,
    JSON.stringify(filters),
  ], [instanceId, startISO, endISO, config, filters])

  // Main timeline data query
  const timelineQuery = useQuery({
    queryKey:  timelineQueryKey,
    queryFn: async () => {
      const result = await vizTimeline({
        start: startISO,
        end: endISO,
        analytics:  config.analyticsMode,
        bucket: config.bucket,
        metric: config.metric,
        value_field: config.metric !== 'count' ? config.valueField : undefined,
        group_field: config.groupFields.join(','),
        top_n: config.topN,
        z_thr: config.zThreshold,
        contamination: config.contamination,
        speed_thr: config.speedThreshold,
        dist_thr: config.distanceThreshold,
        normalize: config.normalize,
        smooth: config.smooth,
        tz: config.timezone,
        where: filters,
        source: config.dataSource || undefined,
      })
      // Persist data for navigation
      savePersistedData(instanceId, result)
      setPersistedData(result)
      return result
    },
    enabled: false, // Manual trigger
    staleTime: 30000,
    cacheTime:  300000,
    keepPreviousData: true, // Keep previous data during refetch for smooth transition
    onSettled: () => {
      setIsTransitioning(false)
    },
  })

  // Add this after the timelineQuery useQuery definition
    useEffect(() => {
      if (!timelineQuery.isFetching && isTransitioning) {
        setIsTransitioning(false)
      }
    }, [timelineQuery.isFetching, isTransitioning])

  // Use persisted data or query data
  const activeData = timelineQuery.data || persistedData

  // ---- Auto-refresh ----
  useEffect(() => {
    if (! config.autoRefresh || config.refreshInterval < 10) return

    const interval = setInterval(() => {
      timelineQuery.refetch()
    }, config.refreshInterval * 1000)

    return () => clearInterval(interval)
  }, [config.autoRefresh, config.refreshInterval, timelineQuery])

  // ---- Config Update Handler with Smooth Transition ----
  const updateConfig = useCallback(
    <K extends keyof TimelineConfig>(key: K, value: TimelineConfig[K]) => {
      setConfig((prev) => {
        const newConfig = { ...prev, [key]: value }
        return newConfig
      })
      // Don't clear data - keep showing previous data
      setIsTransitioning(true)
    },
    []
  )

  const handleThresholdPresetChange = useCallback((presetLabel: string) => {
  const preset = THRESHOLD_PRESETS.find(p => p.label === presetLabel)
  if (! preset) return

  setConfig((prev) => ({
    ...prev,
    zThreshold: preset.zThreshold,
    contamination: preset.contamination,
    thresholdPreset: presetLabel,
  }))
  setIsTransitioning(true)
  }, [])


  // Handle group fields change
  const handleGroupFieldsChange = useCallback((newFields: string[]) => {
    if (newFields.length === 0) return
    updateConfig('groupFields', newFields)
  }, [updateConfig])

  // ---- Run Query ----
  const handleRunQuery = useCallback(() => {
    setIsTransitioning(true)
    timelineQuery.refetch()
  }, [timelineQuery])

  // ---- Chart Data Processing ----
  const chartData = useMemo(() => {
    const data = activeData
    if (!data || ! data.minutes?.length) {
      return { traces: [], hasData: false, anomalyPoints: [] }
    }

    const xValues = data.minutes.map((s) => new Date(s))
    let series = data.series || []

    // Filter by selected groups if any
    if (selectedGroups.length > 0) {
      series = series.filter(
        (s) => selectedGroups.includes(s.name) || (config.showOthers && s.name === 'Others')
      )
    } else if (! config.showOthers) {
      series = series.filter((s) => s.name !== 'Others')
    }

    // Filter hidden series
    series = series.filter((s) => !hiddenSeries.has(s.name))

    // Sort series (Others last)
    const sortedSeries = [...series].sort((a, b) => {
      if (a.name === 'Others') return 1
      if (b.name === 'Others') return -1
      return 0
    })

    const traces:  Plotly.Data[] = []

    // Build series traces
    sortedSeries.forEach((s, i) => {
      const isOthers = s.name === 'Others'
      const color = getSeriesColor(i, isOthers)
      const userCount = topUsersQuery.data?.counts?.[s.name]

      const trace: Partial<Plotly.Data> = {
        name: userCount ? `${s.name} (${userCount})` : s.name,
        x: xValues,
        y: s.values || [],
        type: 'scatter',
        mode: 'lines',
        line: {
          width: isOthers ? 1.5 : 2.5,
          shape: 'spline',
          smoothing: 0.3,
          color,
        },
        fill: config.stacked || config.normalize ? 'tonexty' : 'none',
        stackgroup: config.stacked || config.normalize ? 'main' : undefined,
        groupnorm: config.normalize ? 'percent' : undefined,
        opacity: isOthers ? 0.4 : 0.85,
        hovertemplate: buildHoverTemplate(config, s.name),
        // Smooth transition
        // @ts-ignore
        transition: {
          duration: 500,
          easing: 'cubic-in-out',
        },
      }

      traces.push(trace as Plotly.Data)
    })

    // Build INLINE anomaly markers (on the curve, same Y axis)
    if (config.showAnomalyOverlay && config.analyticsMode !== 'none') {
      const anomScores = data.anom_by_minute_score || {}
      const anomBuckets = Object.keys(anomScores)

      if (anomBuckets.length > 0) {
        const anomX:  Date[] = []
        const anomY: number[] = []
        const anomColors: string[] = []
        const anomSizes: number[] = []
        const anomSymbols: string[] = []
        const anomCustomData: any[] = []

        anomBuckets.forEach((bucket) => {
          const score = anomScores[bucket]

          // FILTER BY SEVERITY AND MIN SCORE
          const severity = getAnomalySeverity(score)
          if (score < config.minAnomalyScore) return
          if (! config.selectedSeverities.includes(severity)) return

          const bucketIdx = data.minutes.findIndex((m) => m === bucket)

          if (bucketIdx !== -1) {
            // Calculate Y value at this bucket
            let totalY = 0
            if (config.stacked || config.normalize) {
              // For stacked, use the top of the stack
              sortedSeries.forEach((s) => {
                totalY += s.values?.[bucketIdx] || 0
              })
            } else {
              // For overlay, use the max value
              sortedSeries.forEach((s) => {
                const val = s.values?.[bucketIdx] || 0
                if (val > totalY) totalY = val
              })
            }

            const severityConfig = getSeverityConfig(score)

            anomX.push(new Date(bucket))
            anomY.push(totalY)
            anomColors.push(severityConfig.color)
            anomSizes.push(14 + score * 18) // Size based on severity
            anomSymbols.push(score >= 0.75 ? 'diamond' : 'circle')

            // Get user info from events
            const events = data.anomaly_events?.[bucket] || []
            const topUsers = events
              .slice(0, 5)
              .map((e:  any) => e.user_id)
              .filter(Boolean)

            anomCustomData.push({
              bucket,
              score,
              severity:  severityConfig.label,
              reasons: data.reasons_by_minute?.[bucket] || [],
              explain: data.explain_by_minute?.[bucket] || '',
              events: events,
              topUsers,
              eventCount: events.length,
            })
          }
        })

        if (anomX.length > 0) {
          traces.push({
            name: '⚠️ Anomalies',
            x: anomX,
            y: anomY,
            type: 'scatter',
            mode: 'markers',
            marker: {
              size: anomSizes,
              color: anomColors,
              symbol: anomSymbols,
              line: { width: 2, color: '#fff' },
              opacity: 0.9,
            },
            hovertemplate: anomCustomData.map((d) => {
              const reasons = d.reasons.length > 0 ? d.reasons.slice(0, 3).join(', ') : 'N/A'
              const usersStr = d.topUsers.length > 0 
                ? `<br><b>Users:</b> ${d.topUsers.slice(0, 3).join(', ')}${d.topUsers.length > 3 ? '...' : ''}`
                : ''
              return (
                `<b>⚠️ ANOMALY - ${d.severity}</b><br>` +
                `<b>Time:</b> %{x|%Y-%m-%d %H:%M}<br>` +
                `<b>Score:</b> ${(d.score * 100).toFixed(1)}%<br>` +
                `<b>Reasons:</b> ${reasons}` +
                usersStr +
                `<br><b>Events:</b> ${d.eventCount}` +
                `<br><i>🖱️ Click for full details</i><extra></extra>`
              )
            }),
            customdata: anomCustomData,
            showlegend: true,
            legendgroup: 'anomalies',
          } as any)
        }
      }
    }

    return { traces, hasData: true }
  }, [activeData, selectedGroups, hiddenSeries, config, topUsersQuery.data])

  // ---- Anomaly Statistics ----
  const anomalyStats = useMemo(() => {
    const scores = activeData?.anom_by_minute_score || {}
    const buckets = Object.keys(scores)
    
    let critical = 0, high = 0, medium = 0, low = 0
    buckets.forEach((b) => {
      const severity = getAnomalySeverity(scores[b])
      if (severity === 'critical') critical++
      else if (severity === 'high') high++
      else if (severity === 'medium') medium++
      else if (severity === 'low') low++
    })

    return {
      total: buckets.length,
      critical,
      high,
      medium,
      low,
      highAndCritical: critical + high,
    }
  }, [activeData])

  // ---- Handle Plot Click ----
  const handlePlotClick = useCallback(
    (event: Plotly.PlotMouseEvent) => {
      const point = event.points?.[0]
      if (! point) return

      const traceName = (point.data as any).name
      if (! traceName?.includes('Anomalies')) return

      const customData = (point as any).customdata
      if (!customData) return

      setSelectedAnomaly({
        bucketTime: customData.bucket,
        score: customData.score,
        reasons: customData.reasons,
        explain: customData.explain,
        events: customData.events,
      })
      setDrawerOpen(true)
    },
    []
  )

  // ---- Handle Legend Click ----
  const handleLegendClick = useCallback((event: Plotly.LegendClickEvent) => {
    const traceName = event.data[event.curveNumber]?.name
    if (!traceName || traceName.includes('Anomalies')) return false
    
    // Extract original name (without count)
    const originalName = traceName.split(' (')[0]
    
    setHiddenSeries((prev) => {
      const next = new Set(prev)
      if (next.has(originalName)) {
        next.delete(originalName)
      } else {
        next.add(originalName)
      }
      return next
    })
    
    return false // Prevent default plotly behavior
  }, [])

  // ---- Handle Zoom ----
  const handleRelayout = useCallback((event: Plotly.PlotRelayoutEvent) => {
    if (event['xaxis.range[0]'] && event['xaxis.range[1]']) {
      setZoomRange((prev) => ({
        ...prev,
        x: [event['xaxis.range[0]'], event['xaxis.range[1]']],
      }))
    }
    if (event['xaxis.autorange']) {
      setZoomRange(null)
    }
  }, [])

  const handleResetZoom = useCallback(() => {
    setZoomRange(null)
  }, [])

  // ---- Layout Configuration ----
  const layout = useMemo((): Partial<Plotly.Layout> => {
    const data = activeData
    const thresholds = data?.thresholds

    const baseLayout:  Partial<Plotly.Layout> = {
      height: chartHeight,
      margin: { l: 65, r: 30, t: 50, b: 50 },
      paper_bgcolor: 'transparent',
      plot_bgcolor: '#fafafa',
      hovermode: 'closest',
      dragmode: 'zoom',
      // Smooth transitions
      transition: {
        duration: 500,
        easing: 'cubic-in-out',
      },
      title: {
        text: buildChartTitle(config),
        font: { size: 14, color: '#333' },
        x: 0.01,
        xanchor: 'left',
      },
      xaxis: {
        type: 'date',
        rangeslider: { visible: ! compact, thickness: 0.08 },
        rangeselector:  compact ? undefined : {
          buttons: [
            { step: 'hour', stepmode: 'backward', count: 1, label: '1h' },
            { step: 'hour', stepmode: 'backward', count:  6, label: '6h' },
            { step: 'day', stepmode: 'backward', count: 1, label: '1d' },
            { step: 'day', stepmode:  'backward', count: 7, label: '7d' },
            { step: 'all', label: 'All' },
          ],
          x: 0,
          y: 1.12,
        },
        gridcolor: '#e0e0e0',
        ...(zoomRange?.x ? { range: zoomRange.x } : {}),
      },
      yaxis: {
        title: {
          text: buildYAxisTitle(config),
          font: { size: 11 },
        },
        rangemode: 'tozero',
        gridcolor: '#e0e0e0',
        zeroline: false,
        ...(zoomRange?.y ? { range: zoomRange.y } : {}),
      },
      legend: {
        orientation: 'h',
        y: 1.02,
        x: 0.5,
        xanchor: 'center',
        font: { size: 10 },
        itemclick: 'toggle',
        itemdoubleclick: 'toggleothers',
      },
      annotations: buildAnnotations(thresholds, config),
    }

    return baseLayout
  }, [activeData, config, chartHeight, compact, zoomRange])

  // ---- Plotly Config ----
  const plotConfig:  Partial<Plotly.Config> = useMemo(() => ({
    displaylogo: false,
    responsive: true,
    modeBarButtonsToRemove: ['select2d', 'lasso2d', 'autoScale2d'],
    toImageButtonOptions: {
      filename: `timeline_${config.bucket}_${new Date().toISOString().slice(0, 10)}`,
      format: 'png',
      scale: 2,
    },
  }), [config.bucket])

  // ---- Computed Values ----
  const dataSources = useMemo(() => {
    const sources = dataSourcesQuery.data || []
    return [
      { name: '', label: 'All Sources', type: 'all' as const, icon: null },
      ...sources.map((s) => ({
        name: s.name,
        label: `${s.name}`,
        sublabel: `${s.type}${s.row_count ? ` • ${s.row_count.toLocaleString()} rows` : ''}`,
        type: s.type,
        icon: s.type === 'view' ? '📊' : '📁',
      })),
    ]
  }, [dataSourcesQuery.data])

  const allUsers = useMemo(() => topUsersQuery.data?.users || [], [topUsersQuery.data])

  // ---- Summary Stats ----
  const summaryStats = useMemo(() => {
    if (!activeData?.series?.length) return null

    let totalEvents = 0
    let maxValue = 0
    let minValue = Infinity

    activeData.series.forEach((s) => {
      s.values?.forEach((v) => {
        totalEvents += v
        if (v > maxValue) maxValue = v
        if (v < minValue && v > 0) minValue = v
      })
    })

    return {
      totalEvents:  Math.round(totalEvents),
      maxValue: Math.round(maxValue),
      minValue: minValue === Infinity ? 0 : Math.round(minValue),
      bucketCount: activeData.minutes?.length || 0,
      groupCount: activeData.series?.length || 0,
    }
  }, [activeData])

  // ============================================================
  // Render
  // ============================================================

  return (
    <Stack sx={{ height: '100%', overflow: 'hidden' }} spacing={1}>
      {/* Loading Progress Bar */}
      {(timelineQuery.isFetching || isTransitioning) && (
        <LinearProgress 
          sx={{ 
            position: 'absolute', 
            top: 0, 
            left: 0, 
            right: 0, 
            zIndex: 20,
            height: 3,
          }} 
        />
      )}

      {/* Controls Panel */}
      <Paper variant="outlined" sx={{ p: 1.5 }}>
        <Stack spacing={1.5}>
          {/* Primary Controls Row */}
          <Stack direction="row" spacing={1.5} alignItems="center" flexWrap="wrap" useFlexGap>
            {/* Badge with anomaly count */}
            <Badge 
              badgeContent={anomalyStats.highAndCritical} 
              color="error" 
              invisible={anomalyStats.highAndCritical === 0}
            >
              <Chip
                icon={<TimelineIcon />}
                label="Timeline"
                color="primary"
                sx={{ fontWeight: 600 }}
              />
            </Badge>

            {/* Data Source Selector */}
            <FormControl size="small" sx={{ minWidth:  180 }}>
              <InputLabel>Data Source</InputLabel>
              <Select
                value={config.dataSource}
                onChange={(e) => updateConfig('dataSource', e.target.value)}
                label="Data Source"
                startAdornment={<StorageIcon sx={{ mr: 0.5, fontSize: 18, color: 'action.active' }} />}
              >
                {dataSources.map((src) => (
                  <MenuItem key={src.name} value={src.name}>
                    <Stack direction="row" spacing={1} alignItems="center">
                      {src.icon && <span>{src.icon}</span>}
                      <Box>
                        <Typography variant="body2">{src.label}</Typography>
                        {src.sublabel && (
                          <Typography variant="caption" color="text.secondary">
                            {src.sublabel}
                          </Typography>
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
              value={config.analyticsMode}
              onChange={(e) => updateConfig('analyticsMode', e.target.value as AnalyticsMode)}
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

            {/* NEW:  Threshold Preset Buttons (only show when analytics is active) */}
            {config.analyticsMode !== 'none' && (
              <Stack direction="row" spacing={1} alignItems="center">
                <Typography variant="caption" color="text.secondary" sx={{ mr: 0.5 }}>
                  Sensitivity:
                </Typography>
                <ToggleButtonGroup
                  value={config.thresholdPreset}
                  exclusive
                  onChange={(_, v) => v && handleThresholdPresetChange(v)}
                  size="small"
                >
                  {THRESHOLD_PRESETS.map((preset) => (
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
                          '&:hover': {
                            bgcolor: alpha(preset.color, 0.25),
                          }
                        }
                      }}
                    >
                      {preset.icon} {preset.label.split(' ')[0]}
                    </ToggleButton>
                  ))}
                </ToggleButtonGroup>

                {/* Live Anomaly Count Badge */}
                {anomalyStats.total > 0 && (
                  <Chip
                    size="small"
                    label={`${anomalyStats.total} anomalies`}
                    color={anomalyStats.highAndCritical > 0 ? 'error' : 'warning'}
                    sx={{ ml: 1, fontWeight: 600 }}
                  />
                )}
              </Stack>
            )}

            {/* Bucket Size */}
            <TextField
              select
              size="small"
              label="Bucket"
              value={config.bucket}
              onChange={(e) => updateConfig('bucket', e.target.value as Bucket)}
              sx={{ minWidth: 110 }}
            >
              {BUCKET_OPTIONS.map((opt) => (
                <MenuItem key={opt.value} value={opt.value}>
                  {opt.label}
                </MenuItem>
              ))}
            </TextField>

            {/* Metric */}
            <TextField
              select
              size="small"
              label="Metric"
              value={config.metric}
              onChange={(e) => updateConfig('metric', e.target.value as Metric)}
              sx={{ minWidth: 110 }}
            >
              {METRIC_OPTIONS.map((opt) => (
                <MenuItem key={opt.value} value={opt.value}>
                  <Tooltip title={opt.description} placement="right">
                    <span>{opt.label}</span>
                  </Tooltip>
                </MenuItem>
              ))}
            </TextField>

            {/* Value Field (for non-count metrics) */}
            {config.metric !== 'count' && (
              <TextField
                select
                size="small"
                label="Value Field"
                value={config.valueField}
                onChange={(e) => updateConfig('valueField', e.target.value)}
                sx={{ minWidth: 140 }}
              >
                {numericFields.map((f) => (
                  <MenuItem key={f} value={f}>{f}</MenuItem>
                ))}
              </TextField>
            )}

            {/* MULTI-SELECT GROUP BY FIELDS */}
            <FormControl size="small" sx={{ minWidth: 220, maxWidth: 400 }}>
              <InputLabel>Group By</InputLabel>
              <Select
                multiple
                value={config.groupFields}
                onChange={(e) => {
                  const value = e.target.value
                  handleGroupFieldsChange(typeof value === 'string' ? value.split(',') : value)
                }}
                input={<OutlinedInput label="Group By" />}
                renderValue={(selected) => (
                  <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                    {selected.map((value) => {
                      const field = ALL_GROUP_FIELDS.find((f) => f.value === value)
                      return (
                        <Chip
                          key={value}
                          label={`${field?.icon || ''} ${field?.label || value}`}
                          size="small"
                          onDelete={config.groupFields.length > 1 ? () => {
                            handleGroupFieldsChange(config.groupFields.filter((f) => f !== value))
                          } : undefined}
                          onMouseDown={(e) => e.stopPropagation()}
                          sx={{ height: 24 }}
                        />
                      )
                    })}
                  </Box>
                )}
                MenuProps={{
                  PaperProps: { sx: { maxHeight: 400 } },
                }}
              >
                {ALL_GROUP_FIELDS.map((field) => (
                  <MenuItem key={field.value} value={field.value}>
                    <Checkbox checked={config.groupFields.includes(field.value)} />
                    <ListItemText 
                      primary={`${field.icon} ${field.label}`}
                    />
                  </MenuItem>
                ))}
              </Select>
            </FormControl>

            {/* Top N */}
            <TextField
              size="small"
              label="Top N"
              type="number"
              value={config.topN}
              onChange={(e) => updateConfig('topN', Math.max(1, Math.min(50, parseInt(e.target.value) || 8)))}
              sx={{ width: 85 }}
              inputProps={{ min: 1, max: 50 }}
            />

            <Box sx={{ flex: 1 }} />

            {/* Zoom Reset */}
            {zoomRange && (
              <Tooltip title="Reset Zoom">
                <IconButton size="small" onClick={handleResetZoom}>
                  <RestartAltIcon />
                </IconButton>
              </Tooltip>
            )}

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
              onClick={handleRunQuery}
              disabled={timelineQuery.isFetching}
              startIcon={
                timelineQuery.isFetching ? (
                  <CircularProgress size={16} color="inherit" />
                ) : (
                  <RefreshIcon />
                )
              }
              sx={{ minWidth:  100 }}
            >
              {timelineQuery.isFetching ? 'Loading...' : 'Run'}
            </Button>
          </Stack>

          {/* Advanced Controls (Collapsible) */}
          <Collapse in={showAdvancedControls}>
            <Divider sx={{ my: 1 }} />
            <Stack direction="row" spacing={2} alignItems="center" flexWrap="wrap" useFlexGap>
              {/* User Selection */}
              <Autocomplete
                multiple
                size="small"
                options={allUsers}
                value={selectedGroups}
                onChange={(_, v) => setSelectedGroups(v)}
                renderInput={(params) => (
                  <TextField {...params} label="Filter Groups" placeholder="Select..." />
                )}
                sx={{ minWidth: 280 }}
                limitTags={2}
                ChipProps={{ size: 'small' }}
              />

              {/* Severity Filter - NEW */}
              {config.analyticsMode !== 'none' && (
                <>
                  <Typography variant="caption" color="text.secondary" sx={{ mr: -1 }}>
                    Show Severities: 
                  </Typography>
                  <ToggleButtonGroup
                    value={config.selectedSeverities}
                    onChange={(_, newSeverities) => {
                      if (newSeverities.length > 0) {
                        updateConfig('selectedSeverities', newSeverities)
                      }
                    }}
                    size="small"
                  >
                    {['critical', 'high', 'medium', 'low', 'normal'].map((sev) => (
                      <ToggleButton 
                        key={sev} 
                        value={sev}
                        sx={{
                          fontSize: '0.75rem',
                          px: 1,
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

              {/* Show Others Toggle */}
              <FormControlLabel
                control={
                  <Switch
                    checked={config.showOthers}
                    onChange={(_, v) => updateConfig('showOthers', v)}
                    size="small"
                  />
                }
                label="Show Others"
              />

              {/* Stacked/Overlay Toggle */}
              <ToggleButtonGroup
                value={config.stacked ? 'stacked' : 'overlay'}
                exclusive
                onChange={(_, v) => v && updateConfig('stacked', v === 'stacked')}
                size="small"
              >
                <ToggleButton value="stacked">Stacked</ToggleButton>
                <ToggleButton value="overlay">Overlay</ToggleButton>
              </ToggleButtonGroup>

              {/* Normalize Toggle */}
              <FormControlLabel
                control={
                  <Switch
                    checked={config.normalize}
                    onChange={(_, v) => updateConfig('normalize', v)}
                    size="small"
                  />
                }
                label="Normalize %"
              />

              {/* Timezone */}
              <TextField
                select
                size="small"
                label="Timezone"
                value={config.timezone}
                onChange={(e) => updateConfig('timezone', e.target.value)}
                sx={{ minWidth: 160 }}
              >
                {TIMEZONE_OPTIONS.map((tz) => (
                  <MenuItem key={tz.value} value={tz.value}>
                    {tz.label}
                  </MenuItem>
                ))}
              </TextField>

              {/* Smooth Slider */}
              <Box sx={{ width: 150 }}>
                <Typography variant="caption" color="text.secondary">
                  Smoothing: {config.smooth}
                </Typography>
                <Slider
                  size="small"
                  value={config.smooth}
                  onChange={(_, v) => updateConfig('smooth', v as number)}
                  min={0}
                  max={10}
                  step={1}
                  marks={[{ value: 0, label: '0' }, { value: 5, label: '5' }, { value: 10, label: '10' }]}
                />
              </Box>

              {/* Analytics-specific controls */}
                {config.analyticsMode === 'simple' && (
                  <Box sx={{ width: 220 }}>
                    <Stack direction="row" spacing={1} alignItems="center">
                      <Typography variant="caption" color="text.secondary">
                        Z-Threshold: <strong>{config.zThreshold.toFixed(1)}</strong>
                      </Typography>
                      <Tooltip title="Lower values = more sensitive (more anomalies detected)">
                        <InfoOutlinedIcon sx={{ fontSize: 14, color: 'text.secondary' }} />
                      </Tooltip>
                    </Stack>
                    <Slider
                      size="small"
                      value={config.zThreshold}
                      onChange={(_, v) => {
                        updateConfig('zThreshold', v as number)
                        updateConfig('thresholdPreset', null) // Clear preset when manually adjusted
                      }}
                      min={0.5}
                      max={10}
                      step={0.1}
                      marks={[
                        { value: 2, label: '2' }, 
                        { value: 3, label: '3' }, 
                        { value: 5, label: '5' }, 
                        { value: 10, label: '10' }
                      ]}
                      sx={{
                        '& .MuiSlider-track': {
                          background: 'linear-gradient(90deg, #D32F2F 0%, #F57C00 50%, #388E3C 100%)',
                        },
                        '& .MuiSlider-rail': {
                          opacity: 0.3,
                        },
                      }}
                    />
                    <Stack direction="row" justifyContent="space-between" sx={{ mt: 0.5 }}>
                      <Typography variant="caption" sx={{ fontSize: 10, color: 'text.secondary' }}>
                        More sensitive
                      </Typography>
                      <Typography variant="caption" sx={{ fontSize:  10, color: 'text.secondary' }}>
                        Less sensitive
                      </Typography>
                    </Stack>
                  </Box>
                )}
                
                {config.analyticsMode === 'advanced' && (
                  <>
                    <Box sx={{ width: 220 }}>
                      <Stack direction="row" spacing={1} alignItems="center">
                        <Typography variant="caption" color="text.secondary">
                          Contamination:  <strong>{(config.contamination * 100).toFixed(0)}%</strong>
                        </Typography>
                        <Tooltip title="Expected % of anomalies in data.Higher = more anomalies detected">
                          <InfoOutlinedIcon sx={{ fontSize:  14, color: 'text.secondary' }} />
                        </Tooltip>
                      </Stack>
                      <Slider
                        size="small"
                        value={config.contamination}
                        onChange={(_, v) => {
                          updateConfig('contamination', v as number)
                          updateConfig('thresholdPreset', null) // Clear preset when manually adjusted
                        }}
                        min={0.01}
                        max={0.3}
                        step={0.01}
                        marks={[
                          { value: 0.02, label: '2%' }, 
                          { value: 0.05, label: '5%' }, 
                          { value: 0.10, label: '10%' },
                          { value: 0.20, label: '20%' },
                        ]}
                        sx={{
                          '& .MuiSlider-track': {
                            background: 'linear-gradient(90deg, #388E3C 0%, #F57C00 50%, #D32F2F 100%)',
                          },
                          '& .MuiSlider-rail':  {
                            opacity: 0.3,
                          },
                        }}
                      />
                      <Stack direction="row" justifyContent="space-between" sx={{ mt: 0.5 }}>
                        <Typography variant="caption" sx={{ fontSize: 10, color: 'text.secondary' }}>
                          Less sensitive
                        </Typography>
                        <Typography variant="caption" sx={{ fontSize: 10, color: 'text.secondary' }}>
                          More sensitive
                        </Typography>
                      </Stack>
                    </Box>
                  </>
                )}

              {/* Anomaly Overlay Toggle */}
              {config.analyticsMode !== 'none' && (
                <FormControlLabel
                  control={
                    <Switch
                      checked={config.showAnomalyOverlay}
                      onChange={(_, v) => updateConfig('showAnomalyOverlay', v)}
                      size="small"
                      color="error"
                    />
                  }
                  label={
                    <Stack direction="row" spacing={0.5} alignItems="center">
                      <WarningAmberIcon fontSize="small" color="error" />
                      <span>Show Anomalies</span>
                    </Stack>
                  }
                />
              )}

              {/* Auto Refresh */}
              <FormControlLabel
                control={
                  <Switch
                    checked={config.autoRefresh}
                    onChange={(_, v) => updateConfig('autoRefresh', v)}
                    size="small"
                  />
                }
                label="Auto Refresh"
              />
              {config.autoRefresh && (
                <TextField
                  size="small"
                  label="Interval (s)"
                  type="number"
                  value={config.refreshInterval}
                  onChange={(e) => updateConfig('refreshInterval', Math.max(10, parseInt(e.target.value) || 60))}
                  sx={{ width: 100 }}
                  inputProps={{ min: 10 }}
                />
              )}
            </Stack>
          </Collapse>
        </Stack>
      </Paper>

      {/* Anomaly Summary Alert */}
      {config.analyticsMode !== 'none' && anomalyStats.total > 0 && (
        <Alert
          severity={anomalyStats.highAndCritical > 0 ? 'error' : 'warning'}
          icon={<WarningAmberIcon />}
          sx={{ py: 0.5 }}
        >
          <Stack direction="row" spacing={3} alignItems="center" flexWrap="wrap">
            <Typography variant="body2">
              <strong>{anomalyStats.total}</strong> anomalies detected
            </Typography>
            {anomalyStats.critical > 0 && (
              <Chip
                size="small"
                label={`${anomalyStats.critical} Critical`}
                sx={{ bgcolor:  SEVERITY_LEVELS.critical.color, color: '#fff' }}
              />
            )}
            {anomalyStats.high > 0 && (
              <Chip
                size="small"
                label={`${anomalyStats.high} High`}
                sx={{ bgcolor: SEVERITY_LEVELS.high.color, color: '#fff' }}
              />
            )}
            {anomalyStats.medium > 0 && (
              <Chip
                size="small"
                label={`${anomalyStats.medium} Medium`}
                sx={{ bgcolor:  SEVERITY_LEVELS.medium.color, color: '#fff' }}
              />
            )}
            <Typography variant="caption" color="text.secondary">
              🖱️ Click anomaly points for details
            </Typography>
          </Stack>
        </Alert>
      )}

      {/* Severity Legend */}
      {config.analyticsMode !== 'none' && config.showAnomalyOverlay && (
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
                      borderRadius: key === 'critical' || key === 'high' ? '3px' : '50%',
                      bgcolor: level.color,
                      transform: key === 'critical' ? 'rotate(45deg)' : 'none',
                    }}
                  />
                  <Typography variant="caption">{level.label}</Typography>
                </Stack>
              ))}
          </Stack>
        </Paper>
      )}

      {/* Summary Stats Bar */}
      {summaryStats && chartData.hasData && (
        <Paper variant="outlined" sx={{ px: 2, py: 0.5, bgcolor: '#f5f5f5' }}>
          <Stack direction="row" spacing={3} alignItems="center" flexWrap="wrap">
            <Typography variant="caption" color="text.secondary">
              <strong>{summaryStats.bucketCount}</strong> time buckets
            </Typography>
            <Typography variant="caption" color="text.secondary">
              <strong>{summaryStats.groupCount}</strong> groups
            </Typography>
            <Typography variant="caption" color="text.secondary">
              Total:  <strong>{summaryStats.totalEvents.toLocaleString()}</strong>
            </Typography>
            <Typography variant="caption" color="text.secondary">
              Max: <strong>{summaryStats.maxValue.toLocaleString()}</strong>
            </Typography>
          </Stack>
        </Paper>
      )}

      {/* Chart Area */}
      <Box sx={{ flex: 1, minHeight: 300, position: 'relative' }}>
        {! chartData.hasData && ! timelineQuery.isFetching ? (
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
            <TimelineIcon sx={{ fontSize: 64, opacity: 0.3 }} />
            <Typography variant="h6" color="text.secondary">
              No data available
            </Typography>
            <Typography variant="body2" color="text.secondary">
              Configure your parameters and click <strong>Run</strong> to load timeline data
            </Typography>
            <Button
              variant="outlined"
              startIcon={<RefreshIcon />}
              onClick={handleRunQuery}
              disabled={timelineQuery.isFetching}
            >
              Run Query
            </Button>
          </Box>
        ) : (
          <Plot
            ref={plotRef}
            data={chartData.traces}
            layout={layout}
            config={plotConfig}
            style={{ width:  '100%', height: '100%' }}
            useResizeHandler
            onClick={handlePlotClick}
            onLegendClick={handleLegendClick}
            onRelayout={handleRelayout}
          />
        )}

        {/* Transitioning overlay */}
        {isTransitioning && chartData.hasData && (
          <Box
            sx={{
              position:  'absolute',
              top:  0,
              left: 0,
              right: 0,
              bottom: 0,
              bgcolor: 'rgba(255,255,255,0.3)',
              display: 'flex',
              alignItems: 'center',
              justifyContent:  'center',
              zIndex: 10,
              pointerEvents: 'none',
            }}
          >
            <CircularProgress size={40} />
          </Box>
        )}
      </Box>

      {/* Anomaly Detail Drawer */}
      <AnomalyDetailDrawer
        open={drawerOpen}
        onClose={() => setDrawerOpen(false)}
        bucketTime={selectedAnomaly?.bucketTime || null}
        score={selectedAnomaly?.score || 0}
        reasons={selectedAnomaly?.reasons || []}
        explain={selectedAnomaly?.explain || ''}
        events={selectedAnomaly?.events || []}
        thresholds={activeData?.thresholds}
      />
    </Stack>
  )
}

// ============================================================
// Helper Functions
// ============================================================

function buildHoverTemplate(config: TimelineConfig, seriesName: string): string {
  const name = seriesName.split(' (')[0] // Remove count suffix
  if (config.normalize) {
    return `<b>%{x|%Y-%m-%d %H:%M}</b><br><b>${name}</b>:  %{y:.1f}%<extra></extra>`
  }
  if (config.metric === 'count') {
    return `<b>%{x|%Y-%m-%d %H:%M}</b><br><b>${name}</b>: %{y: ,.0f} events<extra></extra>`
  }
  return `<b>%{x|%Y-%m-%d %H:%M}</b><br><b>${name}</b>: %{y: ,.2f}<extra></extra>`
}

function buildChartTitle(config: TimelineConfig): string {
  const parts = ['<b>Timeline</b>']
  if (config.analyticsMode === 'simple') {
    parts.push('• Z-Score Analysis')
  } else if (config.analyticsMode === 'advanced') {
    parts.push('• IsolationForest Analysis')
  }
  if (config.dataSource) {
    parts.push(`• ${config.dataSource}`)
  }
  if (config.groupFields.length > 1) {
    parts.push(`• Grouped by ${config.groupFields.length} fields`)
  }
  return parts.join(' ')
}

function buildYAxisTitle(config: TimelineConfig): string {
  if (config.normalize) {
    return `Share (%) per ${config.bucket}`
  }
  if (config.metric === 'count') {
    return `Events per ${config.bucket}`
  }
  return `${config.metric}(${config.valueField}) per ${config.bucket}`
}

function buildAnnotations(
  thresholds: TimelineResponse['thresholds'] | undefined,
  config: TimelineConfig
): Partial<Plotly.Annotations>[] {
  if (!thresholds || config.analyticsMode === 'none') return []

  const annotations:  Partial<Plotly.Annotations>[] = []

  if (thresholds.mode === 'simple' && thresholds.z_thr) {
    annotations.push({
      xref: 'paper',
      yref: 'paper',
      x: 1,
      y: 1.08,
      text: `Z-threshold: ${thresholds.z_thr.toFixed(1)}`,
      showarrow: false,
      font: { size: 10, color: '#666' },
      bgcolor: '#f5f5f5',
      borderpad: 3,
    })
  } else if (thresholds.mode === 'advanced' && thresholds.contamination) {
    annotations.push({
      xref: 'paper',
      yref: 'paper',
      x: 1,
      y: 1.08,
      text: `Contamination: ${(thresholds.contamination * 100).toFixed(0)}%`,
      showarrow: false,
      font: { size: 10, color: '#666' },
      bgcolor: '#f5f5f5',
      borderpad:  3,
    })
  }

  return annotations
}