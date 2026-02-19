// ui-react/src/sections/PieChartPanel.tsx
// Production-grade Pie/Donut Chart Panel for categorical distribution analysis
//
// WORKING FEATURES:
// 1.Multi-field grouping (composite keys)
// 2.Proper metric computation (count, sum, avg, max, min) with value field
// 3.Unlimited top categories slider (3-200)
// 4.Show/Hide Others toggle
// 5.Drill-down on click with breadcrumb navigation
// 6.Data source filtering via WHERE clause (using 'eq' operator)
// 7.Advanced anomaly visualization with aggregation choice (max, avg, min)
// 8.Configurable anomaly threshold
// 9.Color by anomaly toggle with severity legend
// 10.Detail drawer on click with full statistics and drill-down button

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
  Checkbox,
  ListItemText,
  OutlinedInput,
  Breadcrumbs,
  Link,
} from '@mui/material'
import PieChartIcon from '@mui/icons-material/PieChart'
import DonutLargeIcon from '@mui/icons-material/DonutLarge'
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
import { useQuery } from '@tanstack/react-query'
import PersonIcon from '@mui/icons-material/Person'
import DevicesIcon from '@mui/icons-material/Devices'
import LocationOnIcon from '@mui/icons-material/LocationOn'
import TableContainer from '@mui/material/TableContainer'
import TableHead from '@mui/material/TableHead'
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

const PIE_COLORS = [
  '#1976D2', '#388E3C', '#7B1FA2', '#F57C00', '#C2185B',
  '#0097A7', '#AFB42B', '#5D4037', '#455A64', '#E64A19',
  '#512DA8', '#00796B', '#FFA000', '#303F9F', '#689F38',
  '#0288D1', '#7CB342', '#8E24AA', '#FF7043', '#EC407A',
]

const ANOMALY_AGG_OPTIONS:  Array<{ value: 'max' | 'avg' | 'min'; label: string; description: string }> = [
  { value: 'max', label: 'Maximum', description: 'Highest anomaly score in category' },
  { value:  'avg', label: 'Average', description: 'Mean anomaly score across events' },
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

export interface PieChartPanelProps {
  instanceId?:  string
  initialConfig?:  Partial<PieChartConfig>
  onConfigChange?: (config: PieChartConfig) => void
  compact?: boolean
}

export interface PieChartConfig {
  dataSource: string
  groupFields: string[]
  metric:  Metric
  valueField: string
  analyticsMode: AnalyticsMode
  zThreshold: number
  contamination: number
  topN: number
  showDonut: boolean
  showLabels: boolean
  showPercentages: boolean
  showLegend: boolean
  showOthers: boolean
  sortBy: 'value' | 'label' | 'anomaly'
  anomalyAggregation: 'max' | 'avg' | 'min'
  anomalyThreshold: number
  colorByAnomaly: boolean
}

interface SegmentData {
  label: string
  rawLabel: string
  value: number
  percentage: number
  anomScore: number
  eventCount:  number
  color: string
  severity: keyof typeof SEVERITY_LEVELS
  drillDownKey: Record<string, string>
  isOthers: boolean
}

interface DrillDownLevel {
  filters: FilterCond[]
  label: string
  fieldValues: Record<string, string>
}

const DEFAULT_CONFIG: PieChartConfig = {
  dataSource: '',
  groupFields: ['event_type'],
  metric: 'count',
  valueField:  'amount',
  analyticsMode: 'none',
  zThreshold: 3.0,
  contamination: 0.05,
  topN: 10,
  showDonut: true,
  showLabels: true,
  showPercentages:  true,
  showLegend: true,
  showOthers: true,
  sortBy: 'value',
  anomalyAggregation: 'max',
  anomalyThreshold:  0.25,
  colorByAnomaly: false,
}

// ============================================================
// Main Component
// ============================================================

export default function PieChartPanel({
  instanceId = 'default',
  initialConfig,
  onConfigChange,
  compact = false,
}: PieChartPanelProps) {
  const { startISO, endISO, filters:  globalFilters } = useFilters()

  const [config, setConfig] = useState<PieChartConfig>({
    ...DEFAULT_CONFIG,
    ...initialConfig,
  })

  // UI State
  const [showAdvancedControls, setShowAdvancedControls] = useState(false)
  const [isTransitioning, setIsTransitioning] = useState(false)
  const [thresholdPreset, setThresholdPreset] = useState<string | null>('Balanced')
  const [selectedSlice, setSelectedSlice] = useState<string | null>(null)

  // Drill-down state
  const [drillDownStack, setDrillDownStack] = useState<DrillDownLevel[]>([])

  // Detail drawer state
  const [drawerOpen, setDrawerOpen] = useState(false)
  const [selectedSegment, setSelectedSegment] = useState<SegmentData | null>(null)
  const [drawerEvents, setDrawerEvents] = useState<any[]>([])
  const [drawerEventsLoading, setDrawerEventsLoading] = useState(false)
  const [drawerReasons, setDrawerReasons] = useState<string[]>([])

  // Store segments for click handling
  const segmentsRef = useRef<SegmentData[]>([])

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
        setAvailableFields(allFields.length > 0 ? allFields :  [
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

  // Build combined filters (global + data source + drill-down)
  const combinedFilters = useMemo((): FilterCond[] => {
    const filters: FilterCond[] = [...(globalFilters || [])]


    // Add drill-down filters
    drillDownStack.forEach(level => {
      filters.push(...level.filters)
    })

    return filters
  }, [globalFilters, drillDownStack])

  // Main data query
  const query = useQuery({
    queryKey: [
      'pieChart',
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
    queryFn: async () => {
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
        where: combinedFilters.length > 0 ? combinedFilters : undefined,
        source: config.dataSource || undefined,
      })

      const valueRows = Array.isArray(valueResult) ? valueResult : (valueResult?.rows || [])

      if (! valueRows || valueRows.length === 0) {
        segmentsRef.current = []
        return {
          segments: [],
          total: 0,
          totalEvents: 0,
          anomalyStats: { total: 0, critical: 0, high: 0, medium: 0, low:  0 },
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
            limit:  100000,
            where:  combinedFilters.length > 0 ? combinedFilters :  undefined,
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

      // Build key helper
      const buildKey = (row: any) => config.groupFields.map(f => String(row[f] ?? '∅')).join('|')

      // Sort rows
      let sorted = [...valueRows].sort((a, b) => {
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

      // Split into top items and others
      const topItems = sorted.slice(0, config.topN)
      const otherItems = sorted.slice(config.topN)

      // Calculate totals from filtered data
      const totalValue = sorted.reduce((sum, r) => sum + (r.value || 0), 0)
      const totalEvents = sorted.reduce((sum, r) => sum + (countLookup.get(buildKey(r)) || 0), 0)

      // Build segments
      const segments: SegmentData[] = topItems.map((r, idx) => {
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
          color = PIE_COLORS[idx % PIE_COLORS.length]
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

      // Add "Others" segment
      if (config.showOthers && otherItems.length > 0) {
        const othersValue = otherItems.reduce((sum, r) => sum + (r.value || 0), 0)
        const othersEventCount = otherItems.reduce((sum, r) => sum + (countLookup.get(buildKey(r)) || 0), 0)
        const othersMaxAnom = otherItems.reduce((max, r) => {
          const anom = anomalyLookup.get(buildKey(r)) || 0
          return Math.max(max, anom)
        }, 0)

        segments.push({
          label: `Others (${otherItems.length} categories)`,
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

      // Store segments in ref for click handling
      segmentsRef.current = segments

      // Calculate anomaly statistics
      const anomalyStats = { total: 0, critical: 0, high: 0, medium: 0, low: 0 }
      segments.forEach(seg => {
        if (seg.anomScore >= config.anomalyThreshold && ! seg.isOthers) {
          anomalyStats.total++
          if (seg.severity !== 'normal') {
            anomalyStats[seg.severity]++
          }
        }
      })

      return {
        segments,
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
    setSelectedSlice(null)
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

  // Fetch anomaly events for the selected segment
const fetchDrawerEvents = useCallback(async (segment: SegmentData) => {
  if (!segment || segment.isOthers) return

  setDrawerEventsLoading(true)
  setDrawerEvents([])
  setDrawerReasons([])

  try {
    // Build filters for this segment
    const segmentFilters:  FilterCond[] = [
      ...combinedFilters,
      ...Object.entries(segment.drillDownKey).map(([field, value]) => ({
        field,
        op: 'eq' as const,
        value,
      })),
    ]

    // Fetch raw events with anomaly scores
    const result = await vizGrid({
      start: startISO,
      end: endISO,
      analytics: config.analyticsMode,
      z_thr: config.analyticsMode === 'simple' ? config.zThreshold : undefined,
      contamination: config.analyticsMode === 'advanced' ? config.contamination : undefined,
      aggregate: false,  // Get raw events, not aggregated
      limit: 100,  // Limit to top 100 events
      sort_by: 'anom_score',
      sort_dir: 'desc',
      where: segmentFilters.length > 0 ? segmentFilters : undefined,
    })

    const rows = Array.isArray(result) ? result : (result?.rows || [])

    // Filter to only anomalous events and sort by score
    const anomalousEvents = rows
      .filter((r: any) => (r.anom_score || 0) >= config.anomalyThreshold)
      .sort((a: any, b: any) => (b.anom_score || 0) - (a.anom_score || 0))
      .slice(0, 50)

    setDrawerEvents(anomalousEvents)

    // Extract unique reasons
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

  // Execute drill-down
  const executeDrillDown = useCallback((segment: SegmentData) => {
    if (segment.isOthers) return

    const newFilters:  FilterCond[] = Object.entries(segment.drillDownKey).map(([field, value]) => ({
      field,
      op: 'eq' as const,
      value,
    }))

    const newLevel: DrillDownLevel = {
      filters: newFilters,
      label: segment.label,
      fieldValues: segment.drillDownKey,
    }

    setDrillDownStack(prev => {
      const newStack = [...prev, newLevel]
      // Trigger refetch after state update
      setTimeout(() => {
        query.refetch()
      }, 50)
      return newStack
    })

    setSelectedSlice(null)
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
    setSelectedSlice(null)
  }, [query])

  // Reset drill-down
  const handleResetDrillDown = useCallback(() => {
    setDrillDownStack([])
    setSelectedSlice(null)
    setTimeout(() => {
      query.refetch()
    }, 50)
  }, [query])

  // Export data
  const handleExport = useCallback(() => {
    if (!query.data) return
    const exportData = {
      segments: query.data.segments,
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
    a.download = `pie-chart-${config.groupFields.join('-')}-${new Date().toISOString().slice(0, 19).replace(/:/g, '-')}.json`
    a.click()
    URL.revokeObjectURL(url)
  }, [query.data, config, drillDownStack, combinedFilters, startISO, endISO])

  // Handle Plotly click event - FIXED
  const handlePlotClick = useCallback((eventData: any) => {
    if (!eventData || !eventData.points || eventData.points.length === 0) {
      console.log('No click data')
      return
    }

    const point = eventData.points[0]
    const pointIndex = point.pointNumber ?? point.pointIndex ?? point.i

    console.log('Click detected:', { pointIndex, point })

    // Get segment from ref (most reliable)
    const segments = segmentsRef.current
    if (! segments || pointIndex >= segments.length || pointIndex < 0) {
      console.log('Invalid segment index:', pointIndex, 'segments:', segments?.length)
      return
    }

    const segment = segments[pointIndex]
    console.log('Selected segment:', segment)

    if (! segment) {
      console.log('No segment found at index:', pointIndex)
      return
    }

    // Set selected slice for visual feedback
    setSelectedSlice(prev => prev === segment.label ? null : segment.label)

    // Open detail drawer with the segment
    setSelectedSegment(segment)
    setDrawerOpen(true)
  }, [])

  // Plotly data
  const plotData = useMemo(() => {
    const segments = query.data?.segments
    if (!segments?.length) return []

    return [{
      type: 'pie' as const,
      labels: segments.map(s => s.label),
      values: segments.map(s => s.value),
      marker: {
        colors: segments.map(s => s.color),
        line: { color: '#ffffff', width: 2 },
      },
      hole: config.showDonut ? 0.45 : 0,
      textinfo: config.showLabels
        ? (config.showPercentages ? 'label+percent' : 'label')
        : (config.showPercentages ? 'percent' : 'none'),
      textposition: 'inside',
      insidetextorientation: 'radial',
      hovertemplate: segments.map(s => {
        const metricLabel = config.metric === 'count' ? 'Count' : `${config.metric.toUpperCase()}(${config.valueField})`
        let hoverText = `<b>${s.label}</b><br>${metricLabel}: ${formatNumber(s.value, config.metric)}<br>Share: ${s.percentage.toFixed(1)}%<br>Events: ${s.eventCount.toLocaleString()}`

        if (config.analyticsMode !== 'none' && s.anomScore >= config.anomalyThreshold) {
          const severityConfig = SEVERITY_LEVELS[s.severity]
          hoverText += `<br><br><b style="color: ${severityConfig.color}">⚠ ${severityConfig.label} Anomaly</b>`
          hoverText += `<br>Score (${config.anomalyAggregation}): ${(s.anomScore * 100).toFixed(1)}%`
        }

        hoverText += `<br><br><i>Click for details${! s.isOthers ? ' & drill down' : ''}</i>`

        return hoverText + '<extra></extra>'
      }),
      pull: segments.map(s => selectedSlice === s.label ? 0.08 : 0),
    }]
  }, [query.data, config, selectedSlice])

  // Plotly layout
  const layout:  Partial<Plotly.Layout> = useMemo(() => {
    const metricLabel = config.metric === 'count'
      ? 'Total Count'
      : `${config.metric.toUpperCase()}(${config.valueField})`

    return {
      margin: { l: 20, r: 20, t:  40, b: 20 },
      showlegend: config.showLegend,
      legend: {
        orientation: 'v' as const,
        x: 1.02,
        y: 0.5,
        font: { size: 11 },
      },
      paper_bgcolor: 'rgba(0,0,0,0)',
      annotations: config.showDonut && query.data ? [{
        text: `<b>${formatNumber(query.data.total, config.metric)}</b><br><span style="font-size: 10px">${metricLabel}</span>`,
        x: 0.5,
        y: 0.5,
        font: { size: 14, color: '#666' },
        showarrow:  false,
      }] : [],
    }
  }, [config, query.data])

  const plotConfig:  Partial<Plotly.Config> = {
    displaylogo: false,
    responsive: true,
    modeBarButtonsToRemove:  ['select2d', 'lasso2d'],
  }

  const data = query.data
  const anomalyStats = data?.anomalyStats ?? { total: 0, critical:  0, high: 0, medium: 0, low: 0 }

  return (
    <Box sx={{ height: '100%', display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
      <Stack spacing={1} sx={{ flex: 1, overflowY: 'auto', overflowX: 'hidden', p: 1 }}>
        {/* Loading Progress */}
        {(query.isFetching || isTransitioning) && (
          <LinearProgress sx={{ position: 'fixed', top: 0, left:  0, right: 0, zIndex: 20, height: 3 }} />
        )}

        {/* Drill-down Breadcrumb Navigation */}
        {drillDownStack.length > 0 && (
          <Paper variant="outlined" sx={{ p: 1, bgcolor: alpha('#E91E63', 0.05) }}>
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
                      cursor: idx === drillDownStack.length - 1 ? 'default' : 'pointer',
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
                  icon={config.showDonut ? <DonutLargeIcon /> : <PieChartIcon />}
                  label="Distribution"
                  sx={{ bgcolor: '#E91E63', color: '#fff', fontWeight: 600 }}
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
                  MenuProps={{ PaperProps: { sx:  { maxHeight: 400 } } }}
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
                            '&: hover': { bgcolor: alpha(preset.color, 0.25) }
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

              {/* Donut/Pie Toggle */}
              <Tooltip title={config.showDonut ? 'Switch to Pie' : 'Switch to Donut'}>
                <IconButton
                  size="small"
                  onClick={() => setConfig(prev => ({ ...prev, showDonut: !prev.showDonut }))}
                  color={config.showDonut ? 'primary' : 'default'}
                >
                  {config.showDonut ? <DonutLargeIcon /> : <PieChartIcon />}
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
                sx={{ minWidth: 100, bgcolor: '#E91E63', '&:hover': { bgcolor: '#C2185B' } }}
              >
                {query.isFetching ? 'Loading...' : 'Run'}
              </Button>
            </Stack>

            {/* Advanced Controls */}
            <Collapse in={showAdvancedControls}>
              <Divider sx={{ my: 1 }} />
              <Stack spacing={2}>
                {/* Row 1: Display options */}
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
                        { value:  3, label: '3' },
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
                        checked={config.showLabels}
                        onChange={e => setConfig(prev => ({ ...prev, showLabels: e.target.checked }))}
                        size="small"
                      />
                    }
                    label={<Typography variant="caption">Labels</Typography>}
                  />

                  <FormControlLabel
                    control={
                      <Switch
                        checked={config.showPercentages}
                        onChange={e => setConfig(prev => ({ ...prev, showPercentages: e.target.checked }))}
                        size="small"
                      />
                    }
                    label={<Typography variant="caption">Percentages</Typography>}
                  />

                  <FormControlLabel
                    control={
                      <Switch
                        checked={config.showLegend}
                        onChange={e => setConfig(prev => ({ ...prev, showLegend: e.target.checked }))}
                        size="small"
                      />
                    }
                    label={<Typography variant="caption">Legend</Typography>}
                  />

                  {/* Sort Options */}
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

                {/* Row 2: Anomaly options */}
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
                <strong>{anomalyStats.total}</strong> anomalous categories (score ≥ {(config.anomalyThreshold * 100).toFixed(0)}%)
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
        <Paper variant="outlined" sx={{ minHeight:  CHART_HEIGHT, height: CHART_HEIGHT, position: 'relative', flexShrink: 0 }}>
          {! data?.segments?.length && ! query.isFetching ? (
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
              <PieChartIcon sx={{ fontSize: 64, opacity: 0.3 }} />
              <Typography variant="h6" color="text.secondary">
                No distribution data
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Click Run to generate pie chart
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
              style={{ width: '100%', height:  '100%' }}
              useResizeHandler
              onClick={handlePlotClick}
            />
          )}
        </Paper>

        {/* Summary Stats */}
        {data?.segments?.length > 0 && (
          <Paper variant="outlined" sx={{ p: 1.5, bgcolor: '#fafafa', flexShrink: 0 }}>
            <Stack direction="row" spacing={3} alignItems="center" flexWrap="wrap">
              <Box>
                <Typography variant="caption" color="text.secondary">
                  {config.metric === 'count' ? 'Total Count' : `${config.metric.toUpperCase()}(${config.valueField})`}
                </Typography>
                <Typography variant="h6" fontWeight={700}>{formatNumber(data.total, config.metric)}</Typography>
              </Box>
              <Divider orientation="vertical" flexItem />
              <Box>
                <Typography variant="caption" color="text.secondary">Total Events</Typography>
                <Typography variant="h6" fontWeight={700}>{data.totalEvents.toLocaleString()}</Typography>
              </Box>
              <Divider orientation="vertical" flexItem />
              <Box>
                <Typography variant="caption" color="text.secondary">Categories</Typography>
                <Typography variant="h6" fontWeight={700}>{data.segments.length}</Typography>
              </Box>
              <Divider orientation="vertical" flexItem />
              <Box>
                <Typography variant="caption" color="text.secondary">Grouped By</Typography>
                <Typography variant="body1" fontWeight={600}>
                  {config.groupFields.map(formatFieldLabel).join(' × ')}
                </Typography>
              </Box>
              {data.segments.length > 0 && ! data.segments[0].isOthers && (
                <>
                  <Divider orientation="vertical" flexItem />
                  <Box>
                    <Typography variant="caption" color="text.secondary">Top Category</Typography>
                    <Typography variant="body1" fontWeight={600}>
                      {data.segments[0].label} ({data.segments[0].percentage.toFixed(1)}%)
                    </Typography>
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
          sx: { width: { xs: '100%', sm:   520, md: 620 }, p: 0 },
        }}
      >
        {selectedSegment && (
          <>
            {/* Header */}
            <Box
              sx={{
                p: 2,
                background: `linear-gradient(135deg, ${SEVERITY_LEVELS[selectedSegment.severity].color}15 0%, ${SEVERITY_LEVELS[selectedSegment.severity].bgColor} 100%)`,
                borderBottom: `3px solid ${SEVERITY_LEVELS[selectedSegment.severity].color}`,
              }}
            >
              <Stack direction="row" justifyContent="space-between" alignItems="flex-start">
                <Stack spacing={1}>
                  <Stack direction="row" spacing={1} alignItems="center">
                    {React.createElement(SEVERITY_LEVELS[selectedSegment.severity].icon, {
                      sx: { color:  SEVERITY_LEVELS[selectedSegment.severity].color, fontSize: 28 }
                    })}
                    <Typography variant="h6" fontWeight={700}>
                      Category Details
                    </Typography>
                  </Stack>
                  <Typography variant="body2" fontWeight={600} sx={{ maxWidth: 400, wordBreak: 'break-word' }}>
                    {selectedSegment.label}
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
              {config.analyticsMode !== 'none' && selectedSegment.anomScore >= config.anomalyThreshold && (
                <Card variant="outlined" sx={{ mb: 2, borderColor:   SEVERITY_LEVELS[selectedSegment.severity].color }}>
                  <CardContent>
                    <Stack direction="row" justifyContent="space-between" alignItems="center" mb={1}>
                      <Typography variant="subtitle2" color="text.secondary">
                        Anomaly Score ({config.anomalyAggregation})
                      </Typography>
                      <Chip
                        label={SEVERITY_LEVELS[selectedSegment.severity].label}
                        size="small"
                        sx={{
                          bgcolor: SEVERITY_LEVELS[selectedSegment.severity].color,
                          color: '#fff',
                          fontWeight: 600,
                        }}
                      />
                    </Stack>
                    <Stack direction="row" alignItems="flex-end" spacing={1}>
                      <Typography variant="h3" fontWeight={700} color={SEVERITY_LEVELS[selectedSegment.severity].color}>
                        {(selectedSegment.anomScore * 100).toFixed(1)}%
                      </Typography>
                      <Typography variant="body2" color="text.secondary" pb={0.5}>
                        ({selectedSegment.anomScore.toFixed(4)})
                      </Typography>
                    </Stack>
                    <LinearProgress
                      variant="determinate"
                      value={Math.min(selectedSegment.anomScore * 100, 100)}
                      sx={{
                        mt: 1.5,
                        height: 8,
                        borderRadius:   4,
                        bgcolor:  '#e0e0e0',
                        '& .MuiLinearProgress-bar': {
                          bgcolor: SEVERITY_LEVELS[selectedSegment.severity].color,
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

              {/* Category Statistics */}
              <Card variant="outlined" sx={{ mb:   2 }}>
                <CardContent>
                  <Typography variant="subtitle2" fontWeight={600} gutterBottom>
                    Category Statistics
                  </Typography>
                  <Table size="small">
                    <TableBody>
                      <TableRow>
                        <TableCell component="th" sx={{ fontWeight: 600, border: 'none', pl: 0 }}>
                          {config.metric === 'count' ? 'Count' : `${config.metric.toUpperCase()}(${config.valueField})`}
                        </TableCell>
                        <TableCell align="right" sx={{ border: 'none', fontWeight: 700 }}>
                          {formatNumber(selectedSegment.value, config.metric)}
                        </TableCell>
                      </TableRow>
                      <TableRow>
                        <TableCell component="th" sx={{ fontWeight:  600, border: 'none', pl: 0 }}>Share of Total</TableCell>
                        <TableCell align="right" sx={{ border: 'none', fontWeight: 700 }}>{selectedSegment.percentage.toFixed(2)}%</TableCell>
                      </TableRow>
                      <TableRow>
                        <TableCell component="th" sx={{ fontWeight: 600, border:   'none', pl: 0 }}>Total Events</TableCell>
                        <TableCell align="right" sx={{ border: 'none', fontWeight: 700 }}>{selectedSegment.eventCount.toLocaleString()}</TableCell>
                      </TableRow>
                      {config.analyticsMode !== 'none' && (
                        <TableRow>
                          <TableCell component="th" sx={{ fontWeight: 600, border: 'none', pl: 0 }}>Anomaly Score ({config.anomalyAggregation})</TableCell>
                          <TableCell align="right" sx={{ border: 'none', fontWeight: 700, color: selectedSegment.anomScore >= config.anomalyThreshold ? SEVERITY_LEVELS[selectedSegment.severity].color : 'inherit' }}>
                            {(selectedSegment.anomScore * 100).toFixed(1)}%
                          </TableCell>
                        </TableRow>
                      )}
                    </TableBody>
                  </Table>
                </CardContent>
              </Card>

              {/* Field Breakdown */}
              {!  selectedSegment.isOthers && Object.keys(selectedSegment.drillDownKey).length > 0 && (
                <Card variant="outlined" sx={{ mb: 2 }}>
                  <CardContent>
                    <Typography variant="subtitle2" fontWeight={600} gutterBottom>
                      Field Values
                    </Typography>
                    <Stack spacing={1}>
                      {Object.entries(selectedSegment.drillDownKey).map(([field, value]) => (
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
                    <Typography variant="subtitle2" fontWeight={600}>
                      Anomaly Events in Category
                    </Typography>
                    {! drawerEventsLoading && drawerEvents.length === 0 && (
                      <Button
                        size="small"
                        variant="outlined"
                        onClick={() => fetchDrawerEvents(selectedSegment)}
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
                        Loading anomaly events...
                      </Typography>
                    </Box>
                  ) : drawerEvents.length === 0 ? (
                    <Alert severity="info" sx={{ mb:  2 }}>
                      Click "Load Events" to fetch anomaly events for this category.
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
                        Showing {drawerEvents.length} anomalous events
                      </Typography>
                      
                      <TableContainer component={Paper} variant="outlined" sx={{ maxHeight: 350, mb: 2 }}>
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
              {! selectedSegment.isOthers && (
                <Button
                  variant="contained"
                  fullWidth
                  size="large"
                  startIcon={<ZoomInIcon />}
                  onClick={() => executeDrillDown(selectedSegment)}
                  sx={{ 
                    bgcolor: '#E91E63', 
                    '&:hover': { bgcolor: '#C2185B' },
                    py: 1.5,
                    fontSize: '1rem',
                    fontWeight: 600,
                  }}
                >
                  Drill Down into "{selectedSegment.label}"
                </Button>
              )}

              {selectedSegment.isOthers && (
                <Alert severity="info" sx={{ mt: 2 }}>
                  <Typography variant="body2">
                    Cannot drill down into "Others".Increase the "Top Categories" limit in advanced settings to see more individual categories.
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

