// ui-react/src/sections/BarChartPanel.tsx
// Production-grade Bar Chart Panel for categorical comparison analysis
//
// FEATURES:
// 1.Multi-field grouping (composite keys)
// 2.Proper metric computation (count, sum, avg, max, min) with value field
// 3.Unlimited top categories slider (3-200)
// 4.Show/Hide Others toggle
// 5.Drill-down on click with breadcrumb navigation
// 6.Data source filtering via WHERE clause (using 'eq' operator)
// 7.Advanced anomaly visualization with aggregation choice (max, avg, min)
// 8.Configurable anomaly threshold
// 9.Color by anomaly toggle with severity legend
// 10.Detail drawer on click with anomaly events table
// 11.Horizontal/Vertical orientation toggle

import React, { useState, useMemo, useCallback, useEffect } from 'react'
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
  Checkbox,
  ListItemText,
  OutlinedInput,
  Breadcrumbs,
  Link,
} from '@mui/material'
import BarChartIcon from '@mui/icons-material/BarChart'
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
import SwapHorizIcon from '@mui/icons-material/SwapHoriz'
import { useQuery } from '@tanstack/react-query'
import {
  vizGrid,
  fetchSchemaFields,
  fetchDataSources,
  type AnalyticsMode,
  type Metric,
  type FilterCond,
} from '../api'
import { useFilters } from '../context/FiltersContext'

// ============================================================
// Constants & Configuration
// ============================================================

const CHART_HEIGHT = 480

const SEVERITY_LEVELS = {
  critical: { min: 0.9, color: '#7B1FA2', bgColor: '#F3E5F5', label: 'Critical', icon: ErrorIcon },
  high: { min: 0.75, color: '#D32F2F', bgColor:  '#FFEBEE', label: 'High', icon: ErrorIcon },
  medium: { min: 0.5, color: '#F57C00', bgColor: '#FFF3E0', label: 'Medium', icon: WarningAmberIcon },
  low: { min: 0.25, color: '#FBC02D', bgColor: '#FFFDE7', label: 'Low', icon: WarningAmberIcon },
  normal: { min: 0, color: '#388E3C', bgColor: '#E8F5E9', label: 'Normal', icon: InfoIcon },
}

const THRESHOLD_PRESETS = [
  { label: 'High Sensitivity', zThreshold: 2.0, contamination: 0.10, color: '#D32F2F', icon: '🔴' },
  { label: 'Balanced', zThreshold: 3.0, contamination: 0.05, color: '#F57C00', icon: '🟡' },
  { label: 'Low Sensitivity', zThreshold: 5.0, contamination: 0.02, color: '#388E3C', icon: '🟢' },
]

const BAR_COLORS = [
  '#673AB7', '#1976D2', '#388E3C', '#F57C00', '#C2185B',
  '#0097A7', '#AFB42B', '#5D4037', '#455A64', '#E64A19',
  '#512DA8', '#00796B', '#FFA000', '#303F9F', '#689F38',
  '#0288D1', '#7CB342', '#8E24AA', '#FF7043', '#EC407A',
]

const ANOMALY_AGG_OPTIONS:  Array<{ value: 'max' | 'avg' | 'min'; label: string; description: string }> = [
  { value: 'max', label: 'Maximum', description: 'Highest anomaly score in category' },
  { value: 'avg', label: 'Average', description: 'Mean anomaly score across events' },
  { value: 'min', label: 'Minimum', description: 'Lowest anomaly score in category' },
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

function formatNumber(value: number, metric: Metric): string {
  if (metric === 'count') return Math.round(value).toLocaleString()
  if (metric === 'avg') return value.toFixed(2)
  return value.toLocaleString(undefined, { maximumFractionDigits: 2 })
}

function metricRequiresValueField(metric: Metric): boolean {
  return metric !== 'count'
}

// ============================================================
// Types
// ============================================================

export interface BarChartPanelProps {
  instanceId?:  string
  initialConfig?:  Partial<BarChartConfig>
  onConfigChange?: (config: BarChartConfig) => void
  compact?: boolean
}

export interface BarChartConfig {
  dataSource: string
  groupFields: string[]
  metric: Metric
  valueField: string
  analyticsMode: AnalyticsMode
  zThreshold: number
  contamination: number
  topN: number
  showOthers: boolean
  sortBy: 'value' | 'label' | 'anomaly'
  anomalyAggregation: 'max' | 'avg' | 'min'
  anomalyThreshold: number
  colorByAnomaly: boolean
  orientation: 'vertical' | 'horizontal'
  showValues: boolean
  showGrid: boolean
}

interface BarData {
  label: string
  rawLabel: string
  value: number
  percentage: number
  anomScore: number
  eventCount: number
  color: string
  severity: keyof typeof SEVERITY_LEVELS
  drillDownKey:  Record<string, string>
  isOthers: boolean
}

interface DrillDownLevel {
  filters: FilterCond[]
  label: string
  fieldValues: Record<string, string>
}

interface QueryResult {
  bars: BarData[]
  total: number
  totalEvents: number
  anomalyStats: { total: number; critical: number; high: number; medium: number; low: number }
  groupFields: string[]
  metric: Metric
}

const DEFAULT_CONFIG: BarChartConfig = {
  dataSource: '',
  groupFields: ['event_type'],
  metric: 'count',
  valueField:  'amount',
  analyticsMode: 'none',
  zThreshold: 3.0,
  contamination: 0.05,
  topN: 15,
  showOthers:  false,
  sortBy: 'value',
  anomalyAggregation: 'max',
  anomalyThreshold: 0.25,
  colorByAnomaly: false,
  orientation: 'vertical',
  showValues: true,
  showGrid: true,
}

// ============================================================
// Main Component
// ============================================================

export default function BarChartPanel({
  instanceId = 'default',
  initialConfig,
  onConfigChange,
  compact = false,
}: BarChartPanelProps) {
  const { startISO, endISO, filters:  globalFilters } = useFilters()

  const [config, setConfig] = useState<BarChartConfig>({
    ...DEFAULT_CONFIG,
    ...initialConfig,
  })

  // UI State
  const [showAdvancedControls, setShowAdvancedControls] = useState(false)
  const [isTransitioning, setIsTransitioning] = useState(false)
  const [thresholdPreset, setThresholdPreset] = useState<string | null>('Balanced')
  const [selectedBar, setSelectedBar] = useState<string | null>(null)

  // Drill-down state
  const [drillDownStack, setDrillDownStack] = useState<DrillDownLevel[]>([])

  // Detail drawer state
  const [drawerOpen, setDrawerOpen] = useState(false)
  const [selectedBarData, setSelectedBarData] = useState<BarData | null>(null)
  const [drawerEvents, setDrawerEvents] = useState<any[]>([])
  const [drawerEventsLoading, setDrawerEventsLoading] = useState(false)
  const [drawerReasons, setDrawerReasons] = useState<string[]>([])

  // Schema state
  const [availableFields, setAvailableFields] = useState<string[]>([])
  const [numericFields, setNumericFields] = useState<string[]>([])

  useEffect(() => {
    onConfigChange?.(config)
  }, [config, onConfigChange])

  // Fetch schema fields
  useEffect(() => {
    ;(async () => {
      try {
        const schema = await fetchSchemaFields()
        const allFields = Object.keys(schema.types || {}).filter(
          f => ! f.startsWith('_') && f !== 'id' && f !== 'ts' && f !== 'meta'
        )
        setAvailableFields(allFields.length > 0 ? allFields : [
          'event_type', 'user_id', 'account_id', 'device_id', 'ip', 'country', 'city', 'source'
        ])

        const nums = Object.entries(schema.types || {})
          .filter(([_, t]) => t === 'number')
          .map(([f]) => f)
        setNumericFields(Array.from(new Set([...nums, 'anom_score', 'zscore', 'amount'])))
      } catch (err) {
        console.error('Failed to fetch schema:', err)
        setAvailableFields(['event_type', 'user_id', 'account_id', 'device_id', 'ip', 'country', 'city', 'source'])
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
  const query = useQuery<QueryResult>({
    queryKey: [
      'barChart',
      instanceId,
      startISO,
      endISO,
      config.groupFields.join(','),
      config.metric,
      config.valueField,
      config.analyticsMode,
      config.zThreshold,
      config.contamination,
      config.topN,
      config.showOthers,
      config.anomalyAggregation,
      config.anomalyThreshold,
      config.sortBy,
      JSON.stringify(combinedFilters),
    ],
    queryFn: async (): Promise<QueryResult> => {
      const effectiveMetric = config.metric
      const effectiveValueField = metricRequiresValueField(effectiveMetric)
        ? config.valueField
        :  undefined

      // Query 1: Get aggregated values by group
      const valueResult = await vizGrid({
        start: startISO,
        end: endISO,
        analytics: 'none',
        aggregate: true,
        group_by: config.groupFields,
        metric: effectiveMetric,
        value_field: effectiveValueField,
        limit: 100000,
        where: combinedFilters.length > 0 ? combinedFilters :  undefined,
        source: config.dataSource || undefined,
      })

      const valueRows = Array.isArray(valueResult) ? valueResult : (valueResult?.rows || [])

      if (! valueRows || valueRows.length === 0) {
        return {
          bars: [],
          total: 0,
          totalEvents: 0,
          anomalyStats: { total: 0, critical: 0, high:  0, medium: 0, low: 0 },
          groupFields: config.groupFields,
          metric: config.metric,
        }
      }

      // Query 2: Get anomaly scores if analytics enabled
      const anomalyLookup = new Map<string, number>()
      if (config.analyticsMode !== 'none') {
        try {
          const anomResult = await vizGrid({
            start: startISO,
            end: endISO,
            analytics: config.analyticsMode,
            z_thr: config.analyticsMode === 'simple' ? config.zThreshold :  undefined,
            contamination: config.analyticsMode === 'advanced' ? config.contamination : undefined,
            aggregate: true,
            group_by: config.groupFields,
            metric: config.anomalyAggregation,
            value_field: 'anom_score',
            limit: 100000,
            where: combinedFilters.length > 0 ? combinedFilters : undefined,
            source: config.dataSource || undefined,
          })
          const anomRows = Array.isArray(anomResult) ? anomResult : (anomResult?.rows || [])

          anomRows.forEach((r: any) => {
            const key = config.groupFields.map(f => String(r[f] ?? '∅')).join('|')
            anomalyLookup.set(key, r.value ?? 0)
          })
        } catch (err) {
          console.error('Anomaly query failed:', err)
        }
      }

      // Query 3: Get event counts per group
      const countLookup = new Map<string, number>()
      try {
        const countResult = await vizGrid({
          start: startISO,
          end: endISO,
          analytics: 'none',
          aggregate: true,
          group_by: config.groupFields,
          metric: 'count',
          limit: 100000,
          where: combinedFilters.length > 0 ? combinedFilters : undefined,
        })
        const countRows = Array.isArray(countResult) ? countResult : (countResult?.rows || [])

        countRows.forEach((r: any) => {
          const key = config.groupFields.map(f => String(r[f] ?? '∅')).join('|')
          countLookup.set(key, r.value ?? 0)
        })
      } catch (err) {
        console.error('Count query failed:', err)
      }

      const buildKey = (row: any) => config.groupFields.map(f => String(row[f] ?? '∅')).join('|')

      // Sort rows
      const sorted = [...valueRows].sort((a, b) => {
        if (config.sortBy === 'value') return (b.value || 0) - (a.value || 0)
        if (config.sortBy === 'anomaly') {
          const aAnom = anomalyLookup.get(buildKey(a)) || 0
          const bAnom = anomalyLookup.get(buildKey(b)) || 0
          return bAnom - aAnom
        }
        const aLabel = config.groupFields.map(f => String(a[f] || '')).join(' | ')
        const bLabel = config.groupFields.map(f => String(b[f] || '')).join(' | ')
        return aLabel.localeCompare(bLabel)
      })

      const topItems = sorted.slice(0, config.topN)
      const otherItems = sorted.slice(config.topN)

      const totalValue = sorted.reduce((sum, r) => sum + (r.value || 0), 0)
      const totalEvents = sorted.reduce((sum, r) => sum + (countLookup.get(buildKey(r)) || 0), 0)

      // Build bars
      const bars:  BarData[] = topItems.map((r, idx) => {
        const key = buildKey(r)
        const anomScore = anomalyLookup.get(key) || 0
        const eventCount = countLookup.get(key) || 0
        const value = r.value || 0
        const percentage = totalValue > 0 ? (value / totalValue) * 100 : 0

        const labelParts = config.groupFields.map(f => String(r[f] ?? '∅'))
        const label = labelParts.join(' | ')

        const drillDownKey:  Record<string, string> = {}
        config.groupFields.forEach(f => {
          drillDownKey[f] = String(r[f] ?? '')
        })

        const severity = getAnomalySeverity(anomScore)

        let color:  string
        if (config.colorByAnomaly && anomScore >= config.anomalyThreshold) {
          color = getAnomalyColor(anomScore)
        } else {
          color = BAR_COLORS[idx % BAR_COLORS.length]
        }

        return {
          label,
          rawLabel: label,
          value,
          percentage,
          anomScore,
          eventCount,
          color,
          severity,
          drillDownKey,
          isOthers: false,
        }
      })

      // Add "Others" bar
      if (config.showOthers && otherItems.length > 0) {
        const othersValue = otherItems.reduce((sum, r) => sum + (r.value || 0), 0)
        const othersEventCount = otherItems.reduce((sum, r) => sum + (countLookup.get(buildKey(r)) || 0), 0)
        const othersMaxAnom = otherItems.reduce((max, r) => {
          const anom = anomalyLookup.get(buildKey(r)) || 0
          return Math.max(max, anom)
        }, 0)

        bars.push({
          label: `Others (${otherItems.length})`,
          rawLabel: 'Others',
          value: othersValue,
          percentage: totalValue > 0 ? (othersValue / totalValue) * 100 : 0,
          anomScore: othersMaxAnom,
          eventCount: othersEventCount,
          color: '#9E9E9E',
          severity: getAnomalySeverity(othersMaxAnom),
          drillDownKey: {},
          isOthers: true,
        })
      }

      // Calculate anomaly statistics
      const anomalyStats = { total: 0, critical: 0, high: 0, medium: 0, low: 0 }
      bars.forEach(bar => {
        if (bar.anomScore >= config.anomalyThreshold && ! bar.isOthers) {
          anomalyStats.total++
          if (bar.severity !== 'normal') {
            anomalyStats[bar.severity]++
          }
        }
      })

      return {
        bars,
        total: totalValue,
        totalEvents,
        anomalyStats,
        groupFields: config.groupFields,
        metric: config.metric,
      }
    },
    enabled: false,
  })

  // Run query
  const handleRunQuery = useCallback(() => {
    setIsTransitioning(true)
    setSelectedBar(null)
    query.refetch().finally(() => setIsTransitioning(false))
  }, [query])

  // Threshold preset change
  const handleThresholdPresetChange = useCallback((presetLabel: string) => {
    const preset = THRESHOLD_PRESETS.find(p => p.label === presetLabel)
    if (! preset) return
    setConfig(prev => ({
      ...prev,
      zThreshold: preset.zThreshold,
      contamination:  preset.contamination,
    }))
    setThresholdPreset(presetLabel)
  }, [])

  // Execute drill-down
  const executeDrillDown = useCallback((bar: BarData) => {
    if (bar.isOthers) return

    const newFilters:  FilterCond[] = Object.entries(bar.drillDownKey).map(([field, value]) => ({
      field,
      op: 'eq' as const,
      value,
    }))

    const newLevel: DrillDownLevel = {
      filters: newFilters,
      label: bar.label,
      fieldValues: bar.drillDownKey,
    }

    setDrillDownStack(prev => [...prev, newLevel])
    setSelectedBar(null)
    setDrawerOpen(false)
    setDrawerEvents([])

    setTimeout(() => {
      query.refetch()
    }, 100)
  }, [query])

  // Go back in drill-down
  const handleDrillUp = useCallback((toIndex: number) => {
    setDrillDownStack(prev => prev.slice(0, toIndex))
    setSelectedBar(null)
    setTimeout(() => {
      query.refetch()
    }, 100)
  }, [query])

  // Reset drill-down
  const handleResetDrillDown = useCallback(() => {
    setDrillDownStack([])
    setSelectedBar(null)
    setTimeout(() => {
      query.refetch()
    }, 100)
  }, [query])

  // Fetch anomaly events for drawer
  const fetchDrawerEvents = useCallback(async (bar: BarData) => {
    if (! bar || bar.isOthers) return

    setDrawerEventsLoading(true)
    setDrawerEvents([])
    setDrawerReasons([])

    try {
      const segmentFilters:  FilterCond[] = [
        ...combinedFilters,
        ...Object.entries(bar.drillDownKey).map(([field, value]) => ({
          field,
          op: 'eq' as const,
          value,
        })),
      ]

      const result = await vizGrid({
        start: startISO,
        end: endISO,
        analytics: config.analyticsMode,
        z_thr: config.analyticsMode === 'simple' ? config.zThreshold : undefined,
        contamination: config.analyticsMode === 'advanced' ? config.contamination : undefined,
        aggregate: false,
        limit: 100,
        sort_by: 'anom_score',
        sort_dir: 'desc',
        where: segmentFilters.length > 0 ? segmentFilters : undefined,
      })

      const rows = Array.isArray(result) ? result : (result?.rows || [])

      const anomalousEvents = rows
        .filter((r: any) => (r.anom_score || 0) >= config.anomalyThreshold)
        .sort((a: any, b: any) => (b.anom_score || 0) - (a.anom_score || 0))
        .slice(0, 50)

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
      bars: query.data.bars,
      total: query.data.total,
      totalEvents: query.data.totalEvents,
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
    a.download = `bar-chart-${config.groupFields.join('-')}-${new Date().toISOString().slice(0, 19).replace(/:/g, '-')}.json`
    a.click()
    URL.revokeObjectURL(url)
  }, [query.data, config, drillDownStack, combinedFilters, startISO, endISO])

  // Open drawer with bar data - SAME PATTERN AS PIECHART
  const handleOpenDetail = useCallback((bar: BarData) => {
    setSelectedBarData(bar)
    setSelectedBar(bar.label)
    setDrawerEvents([])
    setDrawerEventsLoading(false)
    setDrawerReasons([])
    setDrawerOpen(true)
  }, [])

  // Handle Plotly click event - SAME PATTERN AS PIECHART
  const handlePlotClick = useCallback((eventData: any) => {
    if (!eventData?.points?.length) {
      return
    }

    const point = eventData.points[0]
    const pointIndex = point.pointNumber ?? point.pointIndex ?? point.i

    // Access bars directly from query.data - SAME AS PIECHART
    const bars = query.data?.bars
    if (! bars || !bars.length) {
      return
    }

    if (pointIndex === undefined || pointIndex < 0 || pointIndex >= bars.length) {
      return
    }

    const bar = bars[pointIndex]
    if (! bar) {
      return
    }

    // Toggle selection
    setSelectedBar(selectedBar === bar.label ? null : bar.label)

    // Open detail drawer - SAME AS PIECHART
    handleOpenDetail(bar)
  }, [query.data, selectedBar, handleOpenDetail])

  // Plotly data - uses query.data?.bars directly
  const plotData = useMemo(() => {
    const bars = query.data?.bars
    if (!bars?.length) return []

    const isHorizontal = config.orientation === 'horizontal'

    return [{
      type: 'bar' as const,
      x: isHorizontal ? bars.map(b => b.value) : bars.map(b => b.label),
      y: isHorizontal ? bars.map(b => b.label) : bars.map(b => b.value),
      orientation: isHorizontal ? 'h' as const : 'v' as const,
      marker: {
        color: bars.map(b => b.color),
        line: { color: '#ffffff', width: 1 },
      },
      text: config.showValues ? bars.map(b => formatNumber(b.value, config.metric)) : undefined,
      textposition: 'auto' as const,
      hovertemplate: bars.map(b => {
        const metricLabel = config.metric === 'count' ? 'Count' : `${config.metric.toUpperCase()}(${config.valueField})`
        let hoverText = `<b>${b.label}</b><br>${metricLabel}: ${formatNumber(b.value, config.metric)}<br>Share: ${b.percentage.toFixed(1)}%<br>Events: ${b.eventCount.toLocaleString()}`

        if (config.analyticsMode !== 'none' && b.anomScore >= config.anomalyThreshold) {
          const severityConfig = SEVERITY_LEVELS[b.severity]
          hoverText += `<br><br><b style="color: ${severityConfig.color}">⚠ ${severityConfig.label} Anomaly</b>`
          hoverText += `<br>Score (${config.anomalyAggregation}): ${(b.anomScore * 100).toFixed(1)}%`
        }

        hoverText += `<br><br><i>Click for details${! b.isOthers ? ' & drill down' : ''}</i>`

        return hoverText + '<extra></extra>'
      }),
      pull: bars.map(b => selectedBar === b.label ? 0.02 : 0),
    }]
  }, [query.data, config, selectedBar])

  // Plotly layout
  const layout:  Partial<Plotly.Layout> = useMemo(() => {
    const isHorizontal = config.orientation === 'horizontal'

    return {
      margin: { l: isHorizontal ? 150 : 60, r: 30, t: 40, b: isHorizontal ? 60 : 120 },
      xaxis: {
        title: isHorizontal ? (config.metric === 'count' ? 'Count' : `${config.metric.toUpperCase()}(${config.valueField})`) : undefined,
        showgrid: config.showGrid,
        gridcolor: '#e0e0e0',
        tickangle: isHorizontal ? 0 : -45,
      },
      yaxis: {
        title: ! isHorizontal ? (config.metric === 'count' ? 'Count' : `${config.metric.toUpperCase()}(${config.valueField})`) : undefined,
        showgrid: config.showGrid,
        gridcolor:  '#e0e0e0',
        automargin: true,
      },
      paper_bgcolor: 'rgba(0,0,0,0)',
      plot_bgcolor: '#fafafa',
      bargap: 0.2,
    }
  }, [config])

  const plotConfig:  Partial<Plotly.Config> = {
    displaylogo: false,
    responsive: true,
    modeBarButtonsToRemove: ['select2d', 'lasso2d'],
  }

  const data = query.data
  const bars = data?.bars ?? []
  const anomalyStats = data?.anomalyStats ?? { total: 0, critical: 0, high: 0, medium: 0, low: 0 }

  return (
    <Box sx={{ height: '100%', display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
      <Stack spacing={1} sx={{ flex: 1, overflowY: 'auto', overflowX: 'hidden', p: 1 }}>
        {/* Loading Progress */}
        {(query.isFetching || isTransitioning) && (
          <LinearProgress sx={{ position: 'fixed', top: 0, left: 0, right: 0, zIndex: 20, height: 3 }} />
        )}

        {/* Drill-down Breadcrumb Navigation */}
        {drillDownStack.length > 0 && (
          <Paper variant="outlined" sx={{ p: 1, bgcolor: alpha('#673AB7', 0.05) }}>
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
                label={`${drillDownStack.length} level${drillDownStack.length > 1 ? 's' : ''} deep`}
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
                  icon={<BarChartIcon />}
                  label="Comparison"
                  sx={{ bgcolor: '#673AB7', color: '#fff', fontWeight: 600 }}
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

              {/* Multi-field Group By */}
              <FormControl size="small" sx={{ minWidth:  220, maxWidth: 400 }}>
                <InputLabel>Group By</InputLabel>
                <Select
                  multiple
                  value={config.groupFields}
                  onChange={(e) => {
                    const value = e.target.value
                    const newFields = typeof value === 'string' ? value.split(',') : value
                    if (newFields.length > 0) {
                      setConfig(prev => ({ ...prev, groupFields: newFields }))
                      setDrillDownStack([])
                    }
                  }}
                  input={<OutlinedInput label="Group By" />}
                  renderValue={(selected) => (
                    <Box sx={{ display:  'flex', flexWrap:  'wrap', gap: 0.5 }}>
                      {selected.map((value) => (
                        <Chip
                          key={value}
                          label={formatFieldLabel(value)}
                          size="small"
                          onDelete={config.groupFields.length > 1 ? () => {
                            setConfig(prev => ({
                              ...prev,
                              groupFields: prev.groupFields.filter(f => f !== value)
                            }))
                          } : undefined}
                          onMouseDown={(e) => e.stopPropagation()}
                          sx={{ height: 24 }}
                        />
                      ))}
                    </Box>
                  )}
                  MenuProps={{ PaperProps: { sx: { maxHeight: 400 } } }}
                >
                  {availableFields.map((field) => (
                    <MenuItem key={field} value={field}>
                      <Checkbox checked={config.groupFields.includes(field)} />
                      <ListItemText primary={formatFieldLabel(field)} />
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>

              {/* Metric */}
              <TextField
                select
                size="small"
                label="Metric"
                value={config.metric}
                onChange={e => setConfig(prev => ({ ...prev, metric: e.target.value as Metric }))}
                sx={{ minWidth: 110 }}
              >
                <MenuItem value="count">Count</MenuItem>
                <MenuItem value="sum">Sum</MenuItem>
                <MenuItem value="avg">Average</MenuItem>
                <MenuItem value="max">Maximum</MenuItem>
                <MenuItem value="min">Minimum</MenuItem>
              </TextField>

              {/* Value Field */}
              {metricRequiresValueField(config.metric) && (
                <TextField
                  select
                  size="small"
                  label="Value Field"
                  value={config.valueField}
                  onChange={e => setConfig(prev => ({ ...prev, valueField: e.target.value }))}
                  sx={{ minWidth: 140 }}
                >
                  {numericFields.map(f => (
                    <MenuItem key={f} value={f}>{formatFieldLabel(f)}</MenuItem>
                  ))}
                </TextField>
              )}

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
                      sx={{ ml: 1, fontWeight: 600 }}
                    />
                  )}
                </Stack>
              )}

              <Box sx={{ flex: 1 }} />

              {/* Orientation Toggle */}
              <Tooltip title="Toggle Orientation">
                <IconButton
                  size="small"
                  onClick={() => setConfig(prev => ({
                    ...prev,
                    orientation: prev.orientation === 'vertical' ? 'horizontal' : 'vertical'
                  }))}
                  color="primary"
                >
                  <SwapHorizIcon />
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
                sx={{ minWidth: 100, bgcolor: '#673AB7', '&:hover': { bgcolor: '#5E35B1' } }}
              >
                {query.isFetching ? 'Loading...' : 'Run'}
              </Button>
            </Stack>

            {/* Advanced Controls */}
            <Collapse in={showAdvancedControls}>
              <Divider sx={{ my: 1 }} />
              <Stack spacing={2}>
                <Stack direction="row" spacing={2} alignItems="center" flexWrap="wrap" useFlexGap>
                  <Box sx={{ width: 220 }}>
                    <Typography variant="caption" color="text.secondary">
                      Top Categories:  {config.topN}
                    </Typography>
                    <Slider
                      size="small"
                      value={config.topN}
                      min={3}
                      max={200}
                      step={1}
                      onChange={(_, v) => setConfig(prev => ({ ...prev, topN: v as number }))}
                      marks={[
                        { value: 3, label: '3' },
                        { value: 50, label: '50' },
                        { value: 100, label: '100' },
                        { value: 200, label: '200' },
                      ]}
                    />
                  </Box>

                  <FormControlLabel
                    control={
                      <Switch
                        checked={config.showOthers}
                        onChange={e => setConfig(prev => ({ ...prev, showOthers: e.target.checked }))}
                        size="small"
                      />
                    }
                    label={<Typography variant="caption">Show "Others"</Typography>}
                  />

                  <FormControlLabel
                    control={
                      <Switch
                        checked={config.showValues}
                        onChange={e => setConfig(prev => ({ ...prev, showValues: e.target.checked }))}
                        size="small"
                      />
                    }
                    label={<Typography variant="caption">Show Values</Typography>}
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

                  <Box>
                    <Typography variant="caption" color="text.secondary" sx={{ mr: 1 }}>Sort:</Typography>
                    <ToggleButtonGroup
                      value={config.sortBy}
                      exclusive
                      onChange={(_, v) => v && setConfig(prev => ({ ...prev, sortBy: v }))}
                      size="small"
                    >
                      <ToggleButton value="value" sx={{ fontSize: '0.75rem', px: 1 }}>Value</ToggleButton>
                      <ToggleButton value="label" sx={{ fontSize: '0.75rem', px: 1 }}>Label</ToggleButton>
                      {config.analyticsMode !== 'none' && (
                        <ToggleButton value="anomaly" sx={{ fontSize: '0.75rem', px: 1 }}>Anomaly</ToggleButton>
                      )}
                    </ToggleButtonGroup>
                  </Box>
                </Stack>

                {config.analyticsMode !== 'none' && (
                  <Stack direction="row" spacing={2} alignItems="center" flexWrap="wrap" useFlexGap>
                    <TextField
                      select
                      size="small"
                      label="Anomaly Aggregation"
                      value={config.anomalyAggregation}
                      onChange={e => setConfig(prev => ({ ...prev, anomalyAggregation: e.target.value as 'max' | 'avg' | 'min' }))}
                      sx={{ minWidth:  180 }}
                    >
                      {ANOMALY_AGG_OPTIONS.map(opt => (
                        <MenuItem key={opt.value} value={opt.value}>
                          <Stack>
                            <span>{opt.label}</span>
                            <Typography variant="caption" color="text.secondary">{opt.description}</Typography>
                          </Stack>
                        </MenuItem>
                      ))}
                    </TextField>

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
                        marks={[
                          { value: 0.1, label: '10%' },
                          { value: 0.5, label: '50%' },
                          { value: 0.9, label: '90%' },
                        ]}
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
                  </Stack>
                )}
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
                <strong>{anomalyStats.total}</strong> anomalous categories
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
                    <Box sx={{ width: 12, height: 12, borderRadius: '50%', bgcolor:  level.color }} />
                    <Typography variant="caption">{level.label} (≥{level.min * 100}%)</Typography>
                  </Stack>
                ))}
            </Stack>
          </Paper>
        )}

        {/* Chart */}
        <Paper variant="outlined" sx={{ minHeight:  CHART_HEIGHT, height: CHART_HEIGHT, position: 'relative', flexShrink: 0 }}>
          {! bars.length && ! query.isFetching ? (
            <Box
              sx={{
                height: '100%',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                flexDirection: 'column',
                gap: 2,
                color: 'text.secondary',
              }}
            >
              <BarChartIcon sx={{ fontSize: 64, opacity: 0.3 }} />
              <Typography variant="h6" color="text.secondary">
                No comparison data
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Click Run to generate bar chart
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
        {bars.length > 0 && (
          <Paper variant="outlined" sx={{ p: 1.5, bgcolor: '#fafafa', flexShrink: 0 }}>
            <Stack direction="row" spacing={3} alignItems="center" flexWrap="wrap">
              <Box>
                <Typography variant="caption" color="text.secondary">
                  {config.metric === 'count' ? 'Total Count' : `${config.metric.toUpperCase()}(${config.valueField})`}
                </Typography>
                <Typography variant="h6" fontWeight={700}>{formatNumber(data?.total || 0, config.metric)}</Typography>
              </Box>
              <Divider orientation="vertical" flexItem />
              <Box>
                <Typography variant="caption" color="text.secondary">Total Events</Typography>
                <Typography variant="h6" fontWeight={700}>{(data?.totalEvents || 0).toLocaleString()}</Typography>
              </Box>
              <Divider orientation="vertical" flexItem />
              <Box>
                <Typography variant="caption" color="text.secondary">Categories</Typography>
                <Typography variant="h6" fontWeight={700}>{bars.length}</Typography>
              </Box>
              <Divider orientation="vertical" flexItem />
              <Box>
                <Typography variant="caption" color="text.secondary">Grouped By</Typography>
                <Typography variant="body1" fontWeight={600}>
                  {config.groupFields.map(formatFieldLabel).join(' × ')}
                </Typography>
              </Box>
            </Stack>
          </Paper>
        )}
      </Stack>

      {/* Detail Drawer - SAME PATTERN AS PIECHART */}
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
        {selectedBarData && (
          <>
            <Box
              sx={{
                p: 2,
                background: `linear-gradient(135deg, ${SEVERITY_LEVELS[selectedBarData.severity].color}15 0%, ${SEVERITY_LEVELS[selectedBarData.severity].bgColor} 100%)`,
                borderBottom: `3px solid ${SEVERITY_LEVELS[selectedBarData.severity].color}`,
              }}
            >
              <Stack direction="row" justifyContent="space-between" alignItems="flex-start">
                <Stack spacing={1}>
                  <Stack direction="row" spacing={1} alignItems="center">
                    {React.createElement(SEVERITY_LEVELS[selectedBarData.severity].icon, {
                      sx: { color: SEVERITY_LEVELS[selectedBarData.severity].color, fontSize: 28 }
                    })}
                    <Typography variant="h6" fontWeight={700}>Category Details</Typography>
                  </Stack>
                  <Typography variant="body2" fontWeight={600} sx={{ maxWidth: 400, wordBreak: 'break-word' }}>
                    {selectedBarData.label}
                  </Typography>
                </Stack>
                <IconButton onClick={() => { setDrawerOpen(false); setDrawerEvents([]); }} size="small">
                  <CloseIcon />
                </IconButton>
              </Stack>
            </Box>

            <Box sx={{ p: 2, overflowY: 'auto', height: 'calc(100% - 140px)' }}>
              {/* Anomaly Score Card */}
              {config.analyticsMode !== 'none' && selectedBarData.anomScore >= config.anomalyThreshold && (
                <Card variant="outlined" sx={{ mb: 2, borderColor:  SEVERITY_LEVELS[selectedBarData.severity].color }}>
                  <CardContent>
                    <Stack direction="row" justifyContent="space-between" alignItems="center" mb={1}>
                      <Typography variant="subtitle2" color="text.secondary">
                        Anomaly Score ({config.anomalyAggregation})
                      </Typography>
                      <Chip
                        label={SEVERITY_LEVELS[selectedBarData.severity].label}
                        size="small"
                        sx={{ bgcolor: SEVERITY_LEVELS[selectedBarData.severity].color, color: '#fff', fontWeight: 600 }}
                      />
                    </Stack>
                    <Stack direction="row" alignItems="flex-end" spacing={1}>
                      <Typography variant="h3" fontWeight={700} color={SEVERITY_LEVELS[selectedBarData.severity].color}>
                        {(selectedBarData.anomScore * 100).toFixed(1)}%
                      </Typography>
                      <Typography variant="body2" color="text.secondary" pb={0.5}>
                        ({selectedBarData.anomScore.toFixed(4)})
                      </Typography>
                    </Stack>
                    <LinearProgress
                      variant="determinate"
                      value={Math.min(selectedBarData.anomScore * 100, 100)}
                      sx={{
                        mt: 1.5, height: 8, borderRadius: 4, bgcolor: '#e0e0e0',
                        '& .MuiLinearProgress-bar': { bgcolor: SEVERITY_LEVELS[selectedBarData.severity].color, borderRadius: 4 },
                      }}
                    />
                  </CardContent>
                </Card>
              )}

              {/* Category Statistics */}
              <Card variant="outlined" sx={{ mb: 2 }}>
                <CardContent>
                  <Typography variant="subtitle2" fontWeight={600} gutterBottom>Category Statistics</Typography>
                  <Table size="small">
                    <TableBody>
                      <TableRow>
                        <TableCell component="th" sx={{ fontWeight: 600, border: 'none', pl: 0 }}>
                          {config.metric === 'count' ? 'Count' : `${config.metric.toUpperCase()}(${config.valueField})`}
                        </TableCell>
                        <TableCell align="right" sx={{ border: 'none', fontWeight: 700 }}>
                          {formatNumber(selectedBarData.value, config.metric)}
                        </TableCell>
                      </TableRow>
                      <TableRow>
                        <TableCell component="th" sx={{ fontWeight: 600, border: 'none', pl: 0 }}>Share of Total</TableCell>
                        <TableCell align="right" sx={{ border: 'none', fontWeight: 700 }}>{selectedBarData.percentage.toFixed(2)}%</TableCell>
                      </TableRow>
                      <TableRow>
                        <TableCell component="th" sx={{ fontWeight: 600, border:  'none', pl: 0 }}>Total Events</TableCell>
                        <TableCell align="right" sx={{ border: 'none', fontWeight: 700 }}>{selectedBarData.eventCount.toLocaleString()}</TableCell>
                      </TableRow>
                      {config.analyticsMode !== 'none' && (
                        <TableRow>
                          <TableCell component="th" sx={{ fontWeight: 600, border:  'none', pl: 0 }}>Anomaly Score</TableCell>
                          <TableCell align="right" sx={{ border: 'none', fontWeight: 700, color: selectedBarData.anomScore >= config.anomalyThreshold ? SEVERITY_LEVELS[selectedBarData.severity].color : 'inherit' }}>
                            {(selectedBarData.anomScore * 100).toFixed(1)}%
                          </TableCell>
                        </TableRow>
                      )}
                    </TableBody>
                  </Table>
                </CardContent>
              </Card>

              {/* Field Breakdown */}
              {! selectedBarData.isOthers && Object.keys(selectedBarData.drillDownKey).length > 0 && (
                <Card variant="outlined" sx={{ mb: 2 }}>
                  <CardContent>
                    <Typography variant="subtitle2" fontWeight={600} gutterBottom>Field Values</Typography>
                    <Stack spacing={1}>
                      {Object.entries(selectedBarData.drillDownKey).map(([field, value]) => (
                        <Stack key={field} direction="row" justifyContent="space-between" alignItems="center">
                          <Typography variant="body2" color="text.secondary">{formatFieldLabel(field)}</Typography>
                          <Chip label={value || '(empty)'} size="small" variant="outlined" />
                        </Stack>
                      ))}
                    </Stack>
                  </CardContent>
                </Card>
              )}

              {/* Anomaly Events Section */}
              {config.analyticsMode !== 'none' && (
                <>
                  <Divider sx={{ my: 2 }} />
                  <Stack direction="row" justifyContent="space-between" alignItems="center" mb={1}>
                    <Typography variant="subtitle2" fontWeight={600}>Anomaly Events in Category</Typography>
                    {! drawerEventsLoading && drawerEvents.length === 0 && (
                      <Button
                        size="small"
                        variant="outlined"
                        onClick={() => fetchDrawerEvents(selectedBarData)}
                        startIcon={<RefreshIcon />}
                      >
                        Load Events
                      </Button>
                    )}
                  </Stack>

                  {drawerEventsLoading ? (
                    <Box sx={{ py: 3, textAlign: 'center' }}>
                      <CircularProgress size={32} />
                      <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>Loading anomaly events...</Typography>
                    </Box>
                  ) : drawerEvents.length === 0 ? (
                    <Alert severity="info" sx={{ mb:  2 }}>
                      Click "Load Events" to fetch anomaly events for this category.
                    </Alert>
                  ) : (
                    <>
                      {drawerReasons.length > 0 && (
                        <Box mb={2}>
                          <Typography variant="body2" fontWeight={600} gutterBottom>Anomaly Reasons</Typography>
                          <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
                            {drawerReasons.map((reason, i) => (
                              <Chip key={i} label={reason} size="small" variant="outlined" color="warning" sx={{ mb: 0.5 }} />
                            ))}
                          </Stack>
                        </Box>
                      )}

                      <Typography variant="body2" color="text.secondary" gutterBottom>
                        Showing {drawerEvents.length} anomalous events
                      </Typography>

                      <TableContainer component={Paper} variant="outlined" sx={{ maxHeight: 350, mb: 2 }}>
                        <Table size="small" stickyHeader>
                          <TableHead>
                            <TableRow>
                              <TableCell sx={{ fontWeight: 600, bgcolor: '#f5f5f5' }}>Time</TableCell>
                              <TableCell sx={{ fontWeight: 600, bgcolor:  '#f5f5f5' }}>User</TableCell>
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
                                    <Chip label={event.event_type || 'unknown'} size="small" variant="outlined" sx={{ fontSize: 11 }} />
                                  </TableCell>
                                  <TableCell>
                                    <Chip
                                      label={`${((event.anom_score || 0) * 100).toFixed(0)}%`}
                                      size="small"
                                      sx={{ bgcolor: evtConfig.color, color: '#fff', fontWeight: 600, fontSize: 11 }}
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
                                            icon={<LocationOnIcon sx={{ fontSize: '12px ! important' }} />}
                                            label={event.country}
                                            size="small"
                                            sx={{ fontSize:  10 }}
                                          />
                                        </Tooltip>
                                      )}
                                      {event.device_id && (
                                        <Tooltip title={`Device: ${event.device_id}`}>
                                          <Chip
                                            icon={<DevicesIcon sx={{ fontSize: '12px !important' }} />}
                                            label="Device"
                                            size="small"
                                            sx={{ fontSize: 10 }}
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

                      {drawerEvents.some((e) => e.reasons && e.reasons.length > 0) && (
                        <Box mt={2}>
                          <Typography variant="body2" fontWeight={600} gutterBottom>Detailed Reasons by Event</Typography>
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
                                          sx={{ fontSize: 10, mb: 0.5 }}
                                        />
                                      </Tooltip>
                                    ))}
                                  </Stack>
                                  {event.explain && (
                                    <Typography variant="caption" color="text.secondary">{event.explain}</Typography>
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
              {! selectedBarData.isOthers && (
                <Button
                  variant="contained"
                  fullWidth
                  size="large"
                  startIcon={<ZoomInIcon />}
                  onClick={() => executeDrillDown(selectedBarData)}
                  sx={{
                    bgcolor: '#673AB7',
                    '&: hover': { bgcolor: '#5E35B1' },
                    py: 1.5,
                    fontSize: '1rem',
                    fontWeight: 600,
                  }}
                >
                  Drill Down into "{selectedBarData.label}"
                </Button>
              )}

              {selectedBarData.isOthers && (
                <Alert severity="info" sx={{ mt: 2 }}>
                  <Typography variant="body2">
                    Cannot drill down into "Others". Increase the "Top Categories" limit in advanced settings to see more individual categories.
                  </Typography>
                </Alert>
              )}
            </Box>
          </>
        )}
      </Drawer>
    </Box>
  )
}