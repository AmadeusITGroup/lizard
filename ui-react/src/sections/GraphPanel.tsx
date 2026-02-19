// ui-react/src/sections/GraphPanel.tsx
// Enhanced Graph Panel with full UX parity and dynamic edge discovery
// - Manual Run button (no auto-load)
// - Dynamic edge type discovery from schema
// - Data source selection
// - Analytics modes with threshold presets
// - Severity filtering
// - Node detail drawer with anomaly information
// - Top nodes filter
// - Advanced controls collapse
// - Export functionality

import React, { useState, useCallback, useMemo, useRef } from 'react'
import {
  Box, Stack, Paper, Chip, Button, TextField, MenuItem, Slider, FormControlLabel, Switch,
  Typography, Tooltip, Divider, IconButton, Alert, Badge, Collapse, LinearProgress,
  FormControl, InputLabel, Select, ToggleButton, ToggleButtonGroup, Autocomplete,
  Drawer, Card, CardContent, List, ListItem, ListItemText, alpha,
} from '@mui/material'
import RefreshIcon from '@mui/icons-material/Refresh'
import CenterFocusStrongIcon from '@mui/icons-material/CenterFocusStrong'
import ScatterPlotIcon from '@mui/icons-material/ScatterPlot'
import WarningAmberIcon from '@mui/icons-material/WarningAmber'
import TuneIcon from '@mui/icons-material/Tune'
import StorageIcon from '@mui/icons-material/Storage'
import InfoOutlinedIcon from '@mui/icons-material/InfoOutlined'
import DownloadIcon from '@mui/icons-material/Download'
import CloseIcon from '@mui/icons-material/Close'
import AccountTreeIcon from '@mui/icons-material/AccountTree'
import CircularProgress from '@mui/material/CircularProgress'
import ForceGraph2D, { ForceGraphMethods } from 'react-force-graph-2d'
import { useQuery } from '@tanstack/react-query'
import { useFilters } from '../context/FiltersContext'
import { vizGraph, fetchDataSources, fetchSchemaFields, type AnalyticsMode, type Metric, type FilterCond } from '../api'

// ============================================================
// Constants & Configuration
// ============================================================

const DEFAULT_EDGES = [
  'user_id-account_id',
  'user_id-device_id',
  'user_id-ip',
  'user_id-card_hash',
  'device_id-ip',
] as const

// Entity fields that can form graph edges
const ENTITY_FIELDS = [
  'user_id',
  'account_id',
  'device_id',
  'ip',
  'card_hash',
  'country',
  'city',
  'event_type',
  'source',
  'carrier',
  'origin',
  'dest',
  'email',
  'phone',
  'session_id',
  'merchant_id',
  'product_id',
  'payment_method',
  'browser',
  'os',
]

const TYPE_COLORS:  Record<string, string> = {
  USER:  '#1976D2',
  ACCOUNT: '#00A1DE',
  DEVICE: '#7CB342',
  IP: '#EF6C00',
  CARD:  '#AA00FF',
  COUNTRY: '#00796B',
  EMAIL: '#C2185B',
  PHONE: '#5E35B1',
  SESSION: '#00897B',
  MERCHANT: '#6A1B9A',
  PRODUCT: '#D84315',
  DEFAULT: '#546E7A',
}

const SEVERITY_LEVELS = {
  critical: { min: 0.9, color: '#7B1FA2', bgColor: '#F3E5F5', label: 'Critical' },
  high: { min: 0.75, color: '#D32F2F', bgColor: '#FFEBEE', label: 'High' },
  medium: { min: 0.5, color: '#F57C00', bgColor: '#FFF3E0', label: 'Medium' },
  low: { min: 0.25, color: '#FBC02D', bgColor: '#FFFDE7', label: 'Low' },
  normal: { min: 0, color: '#388E3C', bgColor: '#E8F5E9', label: 'Normal' },
}

const THRESHOLD_PRESETS = [
  { label: 'High Sensitivity', zThreshold: 2.0, contamination: 0.10, color: '#D32F2F', icon: '🔴' },
  { label:  'Balanced', zThreshold: 3.0, contamination: 0.05, color: '#F57C00', icon: '🟡' },
  { label:  'Low Sensitivity', zThreshold: 5.0, contamination: 0.02, color: '#388E3C', icon: '🟢' },
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

function seededHash(str: string): number {
  let h = 2166136261 >>> 0
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i)
    h = Math.imul(h, 16777619)
  }
  h = (h ^ (h >>> 16)) >>> 0
  return (h & 0xffffff) / 0x1000000
}

function graphKey(params: any) {
  const pick = {
    start: params.start,
    end: params.end,
    edges: params.edges?.join(',') ?? '',
    metric: params.metric,
    value_field: params.value_field ?? '',
    min_link_value: params.min_link_value,
    whereLen: Array.isArray(params.where) ? params.where.length :  0,
  }
  const s = JSON.stringify(pick)
  return 'graph.layout.' + Math.floor(seededHash(s) * 1e9).toString(36)
}

// Generate human-readable labels
function formatEdgeLabel(field: string): string {
  return field
    .split('_')
    .map(w => w.charAt(0).toUpperCase() + w.slice(1))
    .join(' ')
}

// Generate all possible edge combinations from available fields
function generateEdgeOptions(availableFields: string[]): { value: string; label: string }[] {
  const edges: { value: string; label: string }[] = []
  const entityFields = availableFields.filter(f => ENTITY_FIELDS.includes(f))

  for (let i = 0; i < entityFields.length; i++) {
    for (let j = i + 1; j < entityFields.length; j++) {
      const a = entityFields[i]
      const b = entityFields[j]
      edges.push({
        value: `${a}-${b}`,
        label: `${formatEdgeLabel(a)} ↔ ${formatEdgeLabel(b)}`,
      })
    }
  }

  return edges.sort((a, b) => a.label.localeCompare(b.label))
}

// ============================================================
// Types
// ============================================================

type NodeT = {
  id: string
  type?:  string
  label?:  string
  value?: number
  degree?: number
  community?: number
  anom_max?: number
  reasons_top?: string[]
  x?: number
  y?: number
  vx?: number
  vy?:  number
  fx?: number
  fy?: number
}

type LinkT = { source: string; target: string; etype?:  string; value?: number }

// ============================================================
// Main Component
// ============================================================

export default function GraphPanel() {
  const { startISO, endISO, filters } = useFilters()

  // Configuration State
  const [dataSource, setDataSource] = useState<string>('')
  const [analytics, setAnalytics] = useState<AnalyticsMode>('none')
  const [zThreshold, setZThreshold] = useState<number>(3.0)
  const [contamination, setContamination] = useState<number>(0.05)
  const [thresholdPreset, setThresholdPreset] = useState<string | null>('Balanced')

  const [edges, setEdges] = useState<string[]>([])
  const [metric, setMetric] = useState<Metric>('count')
  const [valueField, setValueField] = useState<string>('anom_score')
  const [minLinkValue, setMinLinkValue] = useState<number>(1.0)
  const [maxNodes, setMaxNodes] = useState<number>(1500)
  const [maxLinks, setMaxLinks] = useState<number>(2500)

  const [selectedSeverities, setSelectedSeverities] = useState<string[]>([
    'critical',
    'high',
    'medium',
    'low',
    'normal',
  ])
  const [selectedNodes, setSelectedNodes] = useState<string[]>([])

  // Schema-derived state
  const [availableFields, setAvailableFields] = useState<string[]>([])
  const [edgeOptions, setEdgeOptions] = useState<{ value: string; label: string }[]>([])
  const [numericFields, setNumericFields] = useState<string[]>([])

  // Visualization State
  const [colorByCommunity, setColorByCommunity] = useState<boolean>(true)
  const [showLabels, setShowLabels] = useState<boolean>(false)
  const [showArrows, setShowArrows] = useState<boolean>(false)
  const [linkOpacity, setLinkOpacity] = useState<number>(0.55)
  const [nodeSize, setNodeSize] = useState<number>(6)
  const [cooldownTicks, setCooldownTicks] = useState<number>(160)
  const [freezeAfterWarmup, setFreezeAfterWarmup] = useState<boolean>(true)

  // UI State
  const [showAdvancedControls, setShowAdvancedControls] = useState(false)
  const [isTransitioning, setIsTransitioning] = useState(false)
  const [drawerOpen, setDrawerOpen] = useState(false)
  const [selectedNodeDetail, setSelectedNodeDetail] = useState<NodeT | null>(null)

  // Fetch schema fields and generate edge options
  React.useEffect(() => {
    ;(async () => {
      const schema = await fetchSchemaFields()

      // Numeric fields
      const nums = Object.entries(schema.types)
        .filter(([_, t]) => t === 'number')
        .map(([f]) => f)
      setNumericFields(Array.from(new Set([...nums, 'anom_score', 'zscore', 'amount'])))

      // All available fields
      const allFields = Object.keys(schema.types)
      setAvailableFields(allFields)

      // Generate edge options from available entity fields
      const generatedEdges = generateEdgeOptions(allFields)
      setEdgeOptions(generatedEdges)

      // Set smart defaults based on what's actually available
      const defaultEdges = Array.from(DEFAULT_EDGES).filter(edge => {
        const [a, b] = edge.split('-')
        return allFields.includes(a) && allFields.includes(b)
      })

      if (defaultEdges.length > 0) {
        setEdges(defaultEdges)
      } else if (generatedEdges.length > 0) {
        // Fallback:  use first 5 available edges
        setEdges(generatedEdges.slice(0, 5).map(e => e.value))
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
      { name: '', label: 'All Sources', type: 'all' as const, icon: null },
      ...sources.map((s: any) => ({
        name: s.name,
        label: s.name,
        sublabel: `${s.type}${s.row_count ? ` • ${s.row_count.toLocaleString()} rows` : ''}`,
        type: s.type,
        icon: s.type === 'view' ? '📊' : '📁',
      })),
    ]
  }, [dataSourcesQuery.data])

  // Query body
  const body = useMemo(
    () => ({
      start: startISO,
      end: endISO,
      analytics,
      z_thr: analytics === 'simple' ? zThreshold : undefined,
      contamination:  analytics === 'advanced' ? contamination : undefined,
      edges,
      metric,
      value_field: metric === 'count' ? undefined : valueField,
      min_link_value: minLinkValue,
      max_nodes: maxNodes,
      max_links: maxLinks,
      where: (filters ?? []) as FilterCond[],
      source: dataSource || undefined,
    }),
    [
      startISO,
      endISO,
      analytics,
      zThreshold,
      contamination,
      edges,
      metric,
      valueField,
      minLinkValue,
      maxNodes,
      maxLinks,
      filters,
      dataSource,
    ]
  )

  const gKey = useMemo(() => graphKey(body), [body])

  // Fetch graph data (manual trigger)
  const query = useQuery({
    queryKey: ['vizGraph', body],
    queryFn: () => vizGraph(body),
    enabled: false,
    keepPreviousData: true,
    staleTime: 30_000,
    onSettled: () => setIsTransitioning(false),
  })

  const handleRunQuery = useCallback(() => {
    setIsTransitioning(true)
    query.refetch()
  }, [query])

  const handleThresholdPresetChange = useCallback((presetLabel: string) => {
    const preset = THRESHOLD_PRESETS.find(p => p.label === presetLabel)
    if (!preset) return
    setZThreshold(preset.zThreshold)
    setContamination(preset.contamination)
    setThresholdPreset(presetLabel)
    setIsTransitioning(true)
  }, [])

  // Process graph data
  const rawNodes = (query.data?.nodes ?? []) as NodeT[]
  const rawLinks = (query.data?.links ?? []) as LinkT[]

  // Filter by severity and selected nodes
  const filteredNodes = useMemo(() => {
    let nodes = rawNodes

    // Filter by severity
    if (analytics !== 'none' && selectedSeverities.length < 5) {
      nodes = nodes.filter(n => {
        const score = n.anom_max ?? 0
        const severity = getAnomalySeverity(score)
        return selectedSeverities.includes(severity)
      })
    }

    // Filter by selected nodes
    if (selectedNodes.length > 0) {
      nodes = nodes.filter(n => selectedNodes.includes(n.id))
    }

    return nodes
  }, [rawNodes, analytics, selectedSeverities, selectedNodes])

  const filteredLinks = useMemo(() => {
    const nodeIds = new Set(filteredNodes.map(n => n.id))
    return rawLinks.filter(
      l => nodeIds.has(String(l.source)) && nodeIds.has(String(l.target))
    )
  }, [rawLinks, filteredNodes])

  // Anomaly statistics
  const anomalyStats = useMemo(() => {
    const stats = { total: 0, critical: 0, high: 0, medium: 0, low:  0 }
    filteredNodes.forEach(n => {
      const score = n.anom_max ?? 0
      const sev = getAnomalySeverity(score)
      if (sev === 'critical') stats.critical++
      else if (sev === 'high') stats.high++
      else if (sev === 'medium') stats.medium++
      else if (sev === 'low') stats.low++
    })
    stats.total = stats.critical + stats.high + stats.medium + stats.low
    return stats
  }, [filteredNodes])

  // Top nodes by anomaly score
  const topAnomalousNodes = useMemo(() => {
    return [...filteredNodes]
      .filter(n => (n.anom_max ?? 0) >= 0.25)
      .sort((a, b) => (b.anom_max ?? 0) - (a.anom_max ?? 0))
      .slice(0, 50)
      .map(n => n.id)
  }, [filteredNodes])

  // Graph rendering
  const graphRef = useRef<ForceGraphMethods>()
  const hoveredIdRef = useRef<string | null>(null)
  const neighRef = useRef<Map<string, Set<string>>>(new Map())

  const graphData = useMemo(() => {
    const neigh = new Map<string, Set<string>>()
    filteredNodes.forEach(n => neigh.set(n.id, new Set()))
    filteredLinks.forEach(l => {
      const a = String(l.source),
        b = String(l.target)
      neigh.get(a)?.add(b)
      neigh.get(b)?.add(a)
    })
    neighRef.current = neigh

    const saved = localStorage.getItem(gKey)
    let pos:  Record<string, { x: number; y: number }> | null = null
    try {
      pos = saved ? JSON.parse(saved) : null
    } catch {
      pos = null
    }

    const nodes = filteredNodes.map(n => {
      const nn:  NodeT = { ...n }
      if (pos?.[n.id]) {
        nn.x = pos[n.id].x
        nn.y = pos[n.id].y
        nn.vx = 0
        nn.vy = 0
      }
      return nn
    })

    const links = filteredLinks.map(l => ({ ...l, value: Number(l.value ?? 1) }))
    return { nodes, links }
  }, [filteredNodes, filteredLinks, gKey])

  const onEngineStop = useCallback(() => {
    if (! freezeAfterWarmup) return
    const fg = graphRef.current
    if (! fg) return
    const nodes = (graphData.nodes ?? []) as NodeT[]
    nodes.forEach(n => {
      n.fx = n.x
      n.fy = n.y
    })
    const out:  Record<string, { x: number; y: number }> = {}
    nodes.forEach(n => {
      if (n.x != null && n.y != null) out[n.id] = { x: n.x, y: n.y }
    })
    try {
      localStorage.setItem(gKey, JSON.stringify(out))
    } catch {
      /* ignore */
    }
  }, [graphData.nodes, gKey, freezeAfterWarmup])

  const colorOf = useCallback(
    (n: NodeT) => {
      const score = n.anom_max ?? 0
      if (analytics !== 'none' && score >= 0.25) {
        return getAnomalyColor(score)
      }
      if (colorByCommunity && typeof n.community === 'number') {
        const h = (n.community * 57) % 360
        return `hsl(${h} 80% 45%)`
      }
      const t = (n.type || '').toUpperCase()
      return TYPE_COLORS[t] || TYPE_COLORS.DEFAULT
    },
    [colorByCommunity, analytics]
  )

  const isNeighbor = useCallback((a: string, b: string) => {
    if (a === b) return true
    const s = neighRef.current.get(a)
    return !!s && s.has(b)
  }, [])

  const [hoveredNode, setHoveredNode] = useState<NodeT | null>(null)
  const onNodeHover = useCallback((n:  NodeT | null) => {
    hoveredIdRef.current = n?.id ?? null
    setHoveredNode(n)
  }, [])

  const onNodeClick = useCallback((n: NodeT) => {
    setSelectedNodeDetail(n)
    setDrawerOpen(true)
  }, [])

  const nodeRenderer = useCallback(
    (n: NodeT, ctx: CanvasRenderingContext2D, scale: number) => {
      const r = nodeSize
      const hovered = hoveredIdRef.current === n.id
      const isNeighborOfHovered = hoveredIdRef.current && isNeighbor(hoveredIdRef.current, n.id)
      const anomScore = (n as any).anom_max ?? 0
      const isAnomalous = analytics !== 'none' && anomScore >= 0.25

      // Anomaly halo
      if (isAnomalous) {
        const haloSize = r + 6 + anomScore * 8
        const alpha = Math.min(0.6, 0.2 + anomScore * 0.5)
        const anomColor = getAnomalyColor(anomScore)

        ctx.beginPath()
        ctx.arc(n.x!, n.y!, haloSize + 4, 0, 2 * Math.PI, false)
        ctx.fillStyle = `${anomColor}${Math.floor(alpha * 0.3 * 255)
          .toString(16)
          .padStart(2, '0')}`
        ctx.fill()

        ctx.beginPath()
        ctx.arc(n.x!, n.y!, haloSize, 0, 2 * Math.PI, false)
        ctx.fillStyle = `${anomColor}${Math.floor(alpha * 255)
          .toString(16)
          .padStart(2, '0')}`
        ctx.fill()
      }

      // Hover highlight
      if (hovered || isNeighborOfHovered) {
        ctx.beginPath()
        ctx.arc(n.x!, n.y!, r + 4, 0, 2 * Math.PI, false)
        ctx.fillStyle = 'rgba(25, 118, 210, 0.25)'
        ctx.fill()
      }

      // Node core
      ctx.beginPath()
      ctx.arc(n.x!, n.y!, r, 0, 2 * Math.PI, false)
      ctx.fillStyle = colorOf(n)
      ctx.fill()
      ctx.lineWidth = isAnomalous ? 2 : 0.8
      ctx.strokeStyle = isAnomalous ? getAnomalyColor(anomScore) : '#ffffff'
      ctx.stroke()

      // Label
      if (showLabels || hovered) {
        const label = n.label ?? n.id
        const fontSize = Math.max(10, 12 / scale)
        ctx.font = `${fontSize}px Inter, system-ui, sans-serif`
        ctx.fillStyle = hovered ? '#1976D2' : '#111'
        ctx.textAlign = 'left'
        ctx.textBaseline = 'middle'
        ctx.fillText(label, n.x!  + r + 3, n.y!)

        if (hovered && isAnomalous) {
          ctx.fillStyle = getAnomalyColor(anomScore)
          ctx.fillText(`(${anomScore.toFixed(2)})`, n.x! + r + 3, n.y! + fontSize + 2)
        }
      }
    },
    [nodeSize, colorOf, showLabels, isNeighbor, analytics]
  )

  const linkRenderer = useCallback(
    (l: any, ctx: CanvasRenderingContext2D) => {
      const src = l.source as NodeT,
        dst = l.target as NodeT
      const hovered = hoveredIdRef.current
      const active = hovered
        ? (src?.id && isNeighbor(hovered, src.id)) || (dst?.id && isNeighbor(hovered, dst.id))
        : true

      const srcAnom = (src as any)?.anom_max ?? 0
      const dstAnom = (dst as any)?.anom_max ?? 0
      const maxAnom = Math.max(srcAnom, dstAnom)
      const isAnomalousLink = analytics !== 'none' && maxAnom >= 0.5

      const baseW = 0.4 + Math.min(4, Math.log10((l.value ?? 1) + 1) + (l.value ?? 1) / 50)
      ctx.lineWidth = active ? baseW :  baseW * 0.4

      const a = active ? linkOpacity : linkOpacity * 0.25
      if (isAnomalousLink) {
        ctx.strokeStyle = `${getAnomalyColor(maxAnom)}${Math.floor(a * 255)
          .toString(16)
          .padStart(2, '0')}`
      } else {
        ctx.strokeStyle = `rgba(90, 90, 90, ${a})`
      }

      ctx.beginPath()
      ctx.moveTo(src.x!, src.y!)
      ctx.lineTo(dst.x!, dst.y!)
      ctx.stroke()

      if (showArrows) {
        const angle = Math.atan2(dst.y! - src.y!, dst.x! - src.x!)
        const len = 6 + Math.min(8, baseW * 2)
        ctx.beginPath()
        ctx.moveTo(dst.x!, dst.y!)
        ctx.lineTo(
          dst.x! - len * Math.cos(angle - Math.PI / 6),
          dst.y! - len * Math.sin(angle - Math.PI / 6)
        )
        ctx.lineTo(
          dst.x!  - len * Math.cos(angle + Math.PI / 6),
          dst.y! - len * Math.sin(angle + Math.PI / 6)
        )
        ctx.closePath()
        ctx.fillStyle = isAnomalousLink ? getAnomalyColor(maxAnom) : `rgba(90, 90, 90, ${a})`
        ctx.fill()
      }
    },
    [linkOpacity, showArrows, isNeighbor, analytics]
  )

  const zoomToFit = useCallback(() => {
    const fg = graphRef.current
    if (!fg) return
    fg.zoomToFit(600, 40)
  }, [])

  const reheatLayout = useCallback(() => {
    const fg = graphRef.current as any
    if (!fg) return
    graphData.nodes.forEach(n => {
      n.fx = undefined
      n.fy = undefined
    })
    setCooldownTicks(140)
    setTimeout(() => {}, 0)
  }, [graphData.nodes])

  const handleExport = useCallback(() => {
    const data = {
      nodes: filteredNodes,
      links: filteredLinks,
      stats: anomalyStats,
      edgeTypes: edges,
    }
    const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `graph-export-${new Date().toISOString().slice(0, 19).replace(/:/g, '-')}.json`
    a.click()
    URL.revokeObjectURL(url)
  }, [filteredNodes, filteredLinks, anomalyStats, edges])

  return (
    <Stack sx={{ height: '100%', overflow: 'hidden' }} spacing={1}>
      {/* Loading Progress */}
      {(query.isFetching || isTransitioning) && (
        <LinearProgress
          sx={{ position: 'absolute', top: 0, left: 0, right: 0, zIndex: 20, height: 3 }}
        />
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
              <Chip icon={<AccountTreeIcon />} label="Graph" color="primary" sx={{ fontWeight: 600 }} />
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
              value={analytics}
              onChange={e => setAnalytics(e.target.value as AnalyticsMode)}
              sx={{ minWidth: 160 }}
            >
              <MenuItem value="none">None</MenuItem>
              <MenuItem value="simple">
                <Stack>
                  <span>Simple (Z-Score)</span>
                  <Typography variant="caption" color="text.secondary">
                    Fast statistical detection
                  </Typography>
                </Stack>
              </MenuItem>
              <MenuItem value="advanced">
                <Stack>
                  <span>Advanced (IForest)</span>
                  <Typography variant="caption" color="text.secondary">
                    ML-based detection
                  </Typography>
                </Stack>
              </MenuItem>
            </TextField>

            {/* Threshold Presets */}
            {analytics !== 'none' && (
              <Stack direction="row" spacing={1} alignItems="center">
                <Typography variant="caption" color="text.secondary" sx={{ mr: 0.5 }}>
                  Sensitivity:
                </Typography>
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
                          '&:hover': {
                            bgcolor: alpha(preset.color, 0.25),
                          },
                        },
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

            {/* Export */}
            <Tooltip title="Export Graph Data">
              <IconButton size="small" onClick={handleExport} disabled={! filteredNodes.length}>
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
              onClick={handleRunQuery}
              disabled={query.isFetching}
              startIcon={query.isFetching ? <CircularProgress size={16} color="inherit" /> : <RefreshIcon />}
              sx={{ minWidth: 100 }}
            >
              {query.isFetching ? 'Loading...' : 'Run'}
            </Button>

            <Tooltip title="Zoom to fit">
              <IconButton onClick={zoomToFit}>
                <CenterFocusStrongIcon />
              </IconButton>
            </Tooltip>

            <Tooltip title="Reheat layout">
              <IconButton onClick={reheatLayout}>
                <ScatterPlotIcon />
              </IconButton>
            </Tooltip>
          </Stack>

          {/* Advanced Controls */}
          <Collapse in={showAdvancedControls}>
            <Divider sx={{ my: 1 }} />
            <Stack spacing={2}>
              {/* Row 1: Edge Selection & Metric */}
              <Stack direction="row" spacing={2} alignItems="center" flexWrap="wrap" useFlexGap>
                <Autocomplete
                  multiple
                  size="small"
                  options={edgeOptions.map(o => o.value)}
                  getOptionLabel={option => {
                    const found = edgeOptions.find(o => o.value === option)
                    if (found) return found.label
                    // Fallback:  format the edge value
                    const [a, b] = option.split('-')
                    return `${formatEdgeLabel(a)} ↔ ${formatEdgeLabel(b)}`
                  }}
                  value={edges}
                  onChange={(_, v) => setEdges(v)}
                  renderInput={params => (
                    <TextField
                      {...params}
                      label="Edge Types"
                      helperText={`${edgeOptions.length} combinations from ${availableFields.filter(f => ENTITY_FIELDS.includes(f)).length} entity fields`}
                    />
                  )}
                  sx={{ minWidth: 320 }}
                  limitTags={2}
                  ChipProps={{ size: 'small' }}
                  groupBy={option => {
                    const firstEntity = option.split('-')[0]
                    return formatEdgeLabel(firstEntity)
                  }}
                />

                <TextField
                  select
                  size="small"
                  label="Metric"
                  value={metric}
                  onChange={e => setMetric(e.target.value as Metric)}
                  sx={{ minWidth: 120 }}
                >
                  <MenuItem value="count">Count</MenuItem>
                  <MenuItem value="avg">Average</MenuItem>
                  <MenuItem value="max">Maximum</MenuItem>
                  <MenuItem value="sum">Sum</MenuItem>
                </TextField>

                <TextField
                  size="small"
                  label="Value Field"
                  value={valueField}
                  onChange={e => setValueField(e.target.value)}
                  disabled={metric === 'count'}
                  sx={{ minWidth: 160 }}
                />

                <Stack direction="row" spacing={1} alignItems="center" sx={{ minWidth: 220 }}>
                  <Typography variant="caption" color="text.secondary">
                    Min Link:  {minLinkValue}
                  </Typography>
                  <Slider
                    size="small"
                    value={minLinkValue}
                    min={1}
                    max={50}
                    step={1}
                    onChange={(_, v) => setMinLinkValue(v as number)}
                    sx={{ width: 120 }}
                  />
                </Stack>
              </Stack>

              {/* Row 2: Node Filters & Limits */}
              <Stack direction="row" spacing={2} alignItems="center" flexWrap="wrap" useFlexGap>
                {topAnomalousNodes.length > 0 && (
                  <Autocomplete
                    multiple
                    size="small"
                    options={topAnomalousNodes}
                    value={selectedNodes}
                    onChange={(_, v) => setSelectedNodes(v)}
                    renderInput={params => <TextField {...params} label="Filter Nodes (Top Anomalous)" />}
                    sx={{ minWidth: 280 }}
                    limitTags={2}
                    ChipProps={{ size: 'small' }}
                  />
                )}

                {analytics !== 'none' && (
                  <>
                    <Typography variant="caption" color="text.secondary" sx={{ mr: -1 }}>
                      Show Severities:
                    </Typography>
                    <ToggleButtonGroup
                      value={selectedSeverities}
                      onChange={(_, v) => {
                        if (v.length > 0) setSelectedSeverities(v)
                      }}
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
                              borderColor: SEVERITY_LEVELS[sev as keyof typeof SEVERITY_LEVELS]?.color,
                              color: SEVERITY_LEVELS[sev as keyof typeof SEVERITY_LEVELS]?.color,
                              fontWeight: 600,
                            },
                          }}
                        >
                          {sev.charAt(0).toUpperCase() + sev.slice(1)}
                        </ToggleButton>
                      ))}
                    </ToggleButtonGroup>
                  </>
                )}

                <Stack direction="row" spacing={1} alignItems="center" sx={{ minWidth: 220 }}>
                  <Typography variant="caption" color="text.secondary">
                    Max Nodes: {maxNodes}
                  </Typography>
                  <Slider
                    size="small"
                    value={maxNodes}
                    min={200}
                    max={5000}
                    step={100}
                    onChange={(_, v) => setMaxNodes(v as number)}
                    sx={{ width: 120 }}
                  />
                </Stack>

                <Stack direction="row" spacing={1} alignItems="center" sx={{ minWidth: 220 }}>
                  <Typography variant="caption" color="text.secondary">
                    Max Links: {maxLinks}
                  </Typography>
                  <Slider
                    size="small"
                    value={maxLinks}
                    min={500}
                    max={8000}
                    step={100}
                    onChange={(_, v) => setMaxLinks(v as number)}
                    sx={{ width: 120 }}
                  />
                </Stack>
              </Stack>

              {/* Row 3: Visual Controls & Thresholds */}
              <Stack direction="row" spacing={2} alignItems="center" flexWrap="wrap" useFlexGap>
                <FormControlLabel
                  control={<Switch checked={colorByCommunity} onChange={(_, v) => setColorByCommunity(v)} size="small" />}
                  label="Color by Community"
                />

                <FormControlLabel
                  control={<Switch checked={showLabels} onChange={(_, v) => setShowLabels(v)} size="small" />}
                  label="Show Labels"
                />

                <FormControlLabel
                  control={<Switch checked={showArrows} onChange={(_, v) => setShowArrows(v)} size="small" />}
                  label="Arrows"
                />

                <FormControlLabel
                  control={
                    <Switch checked={freezeAfterWarmup} onChange={(_, v) => setFreezeAfterWarmup(v)} size="small" />
                  }
                  label="Freeze After Warm-up"
                />

                <Stack direction="row" spacing={1} alignItems="center" sx={{ minWidth: 200 }}>
                  <Typography variant="caption" color="text.secondary">
                    Link Opacity: {linkOpacity.toFixed(2)}
                  </Typography>
                  <Slider
                    size="small"
                    value={linkOpacity}
                    min={0.1}
                    max={1.0}
                    step={0.05}
                    onChange={(_, v) => setLinkOpacity(v as number)}
                    sx={{ width: 100 }}
                  />
                </Stack>

                <Stack direction="row" spacing={1} alignItems="center" sx={{ minWidth: 200 }}>
                  <Typography variant="caption" color="text.secondary">
                    Node Size: {nodeSize}
                  </Typography>
                  <Slider
                    size="small"
                    value={nodeSize}
                    min={3}
                    max={14}
                    step={1}
                    onChange={(_, v) => setNodeSize(v as number)}
                    sx={{ width:  100 }}
                  />
                </Stack>

                {/* Thresholds */}
                {analytics === 'simple' && (
                  <Box sx={{ width: 220 }}>
                    <Stack direction="row" spacing={1} alignItems="center">
                      <Typography variant="caption" color="text.secondary">
                        Z-Threshold: <strong>{zThreshold.toFixed(1)}</strong>
                      </Typography>
                      <Tooltip title="Lower values = more sensitive">
                        <InfoOutlinedIcon sx={{ fontSize: 14, color: 'text.secondary' }} />
                      </Tooltip>
                    </Stack>
                    <Slider
                      size="small"
                      value={zThreshold}
                      onChange={(_, v) => {
                        setZThreshold(v as number)
                        setThresholdPreset(null)
                      }}
                      min={0.5}
                      max={10}
                      step={0.1}
                      marks={[
                        { value: 2, label: '2' },
                        { value: 3, label: '3' },
                        { value: 5, label: '5' },
                        { value: 10, label: '10' },
                      ]}
                      sx={{
                        '& .MuiSlider-track': {
                          background: 'linear-gradient(90deg, #D32F2F 0%, #F57C00 50%, #388E3C 100%)',
                        },
                      }}
                    />
                  </Box>
                )}

                {analytics === 'advanced' && (
                  <Box sx={{ width: 220 }}>
                    <Stack direction="row" spacing={1} alignItems="center">
                      <Typography variant="caption" color="text.secondary">
                        Contamination:  <strong>{(contamination * 100).toFixed(0)}%</strong>
                      </Typography>
                      <Tooltip title="Expected % of anomalies">
                        <InfoOutlinedIcon sx={{ fontSize:  14, color: 'text.secondary' }} />
                      </Tooltip>
                    </Stack>
                    <Slider
                      size="small"
                      value={contamination}
                      onChange={(_, v) => {
                        setContamination(v as number)
                        setThresholdPreset(null)
                      }}
                      min={0.01}
                      max={0.3}
                      step={0.01}
                      marks={[
                        { value: 0.02, label: '2%' },
                        { value: 0.05, label: '5%' },
                        { value:  0.10, label: '10%' },
                        { value: 0.20, label: '20%' },
                      ]}
                      sx={{
                        '& .MuiSlider-track':  {
                          background: 'linear-gradient(90deg, #388E3C 0%, #F57C00 50%, #D32F2F 100%)',
                        },
                      }}
                    />
                  </Box>
                )}
              </Stack>

              {/* Info Alert */}
              {edgeOptions.length > 0 && (
                <Alert severity="info" icon={<InfoOutlinedIcon />} sx={{ py: 0.5 }}>
                  <Typography variant="caption">
                    <strong>{edgeOptions.length} edge type(s)</strong> auto-discovered from your data schema.{' '}
                    {availableFields.filter(f => ENTITY_FIELDS.includes(f)).length > 0 && (
                      <>
                        Detected entity fields:{' '}
                        <strong>{availableFields.filter(f => ENTITY_FIELDS.includes(f)).join(', ')}</strong>
                      </>
                    )}
                  </Typography>
                </Alert>
              )}
            </Stack>
          </Collapse>
        </Stack>
      </Paper>

      {/* Anomaly Summary Alert */}
      {analytics !== 'none' && anomalyStats.total > 0 && (
        <Alert severity={anomalyStats.critical > 0 || anomalyStats.high > 0 ? 'error' : 'warning'} icon={<WarningAmberIcon />} sx={{ py: 0.5 }}>
          <Stack direction="row" spacing={3} alignItems="center" flexWrap="wrap">
            <Typography variant="body2">
              <strong>{anomalyStats.total}</strong> anomalous nodes detected
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
            <Typography variant="caption" color="text.secondary">
              🖱️ Click nodes for details
            </Typography>
          </Stack>
        </Alert>
      )}

      {/* Graph Canvas */}
      <Paper variant="outlined" sx={{ flex: 1, position: 'relative', overflow: 'hidden' }}>
        {/* Stats Header */}
        <Stack direction="row" spacing={1} alignItems="center" sx={{ p: 1, pb: 0 }}>
          {query.isFetching && <Chip size="small" label="Loading…" />}
          {!query.isFetching && (
            <Chip
              size="small"
              color="primary"
              label={`${filteredNodes.length.toLocaleString()} nodes • ${filteredLinks.length.toLocaleString()} links`}
            />
          )}
          <Box flex={1} />
          <Typography variant="caption" sx={{ opacity: 0.7, pr: 1 }}>
            Layout key: {gKey.replace('graph.layout.', '')}
          </Typography>
        </Stack>

        {/* Graph Rendering */}
        {filteredNodes.length === 0 && ! query.isFetching ? (
          <Box sx={{ height: '100%', display: 'flex', alignItems:  'center', justifyContent:  'center', flexDirection: 'column', gap: 2, color: 'text.secondary' }}>
            <AccountTreeIcon sx={{ fontSize: 64, opacity: 0.3 }} />
            <Typography variant="h6" color="text.secondary">
              No graph data available
            </Typography>
            <Typography variant="body2" color="text.secondary">
              Click <strong>Run</strong> to load graph network
            </Typography>
            <Button variant="outlined" startIcon={<RefreshIcon />} onClick={handleRunQuery} disabled={query.isFetching}>
              Run Query
            </Button>
          </Box>
        ) : (
          <Box sx={{ position: 'absolute', inset: 0, top: 40 }}>
            <ForceGraph2D
              ref={graphRef as any}
              graphData={graphData}
              nodeRelSize={1}
              cooldownTicks={cooldownTicks}
              onEngineStop={onEngineStop}
              enableNodeDrag={false}
              d3AlphaMin={0.02}
              d3VelocityDecay={0.25}
              onNodeHover={onNodeHover}
              onNodeClick={onNodeClick}
              linkDirectionalParticles={0}
              linkDirectionalArrowLength={showArrows ? 6 : 0}
              linkDirectionalArrowRelPos={0.92}
              d3Force={fg => {
                fg.force('charge')?.strength(-40)
                fg.force('link')?.distance((l: any) => 120 / Math.log2((l.value ?? 1) + 2))
              }}
              nodeCanvasObject={(node, ctx, scale) => nodeRenderer(node as NodeT, ctx, scale)}
              linkCanvasObject={(link, ctx) => linkRenderer(link, ctx)}
              linkCanvasObjectMode={() => 'after'}
              onZoomEnd={() => {}}
            />
          </Box>
        )}

        {/* Severity Legend */}
        {analytics !== 'none' && filteredNodes.length > 0 && (
          <Paper
            sx={{
              position: 'absolute',
              bottom: 16,
              right: 16,
              zIndex: 1000,
              p: 1.5,
              bgcolor: 'rgba(255,255,255,0.95)',
              backdropFilter: 'blur(4px)',
              borderRadius: 1,
              minWidth: 160,
            }}
          >
            <Typography variant="subtitle2" gutterBottom>
              Anomaly Severity
            </Typography>
            <Stack spacing={0.5}>
              {[
                { label: 'Critical', color:  SEVERITY_LEVELS.critical.color, range: '≥ 0.9' },
                { label: 'High', color: SEVERITY_LEVELS.high.color, range: '0.75+' },
                { label: 'Medium', color: SEVERITY_LEVELS.medium.color, range: '0.5+' },
                { label:  'Low', color: SEVERITY_LEVELS.low.color, range: '0.25+' },
              ].map(({ label, color, range }) => (
                <Stack key={label} direction="row" spacing={1} alignItems="center">
                  <Box
                    sx={{
                      width: 16,
                      height: 16,
                      borderRadius: '50%',
                      bgcolor: color,
                      boxShadow: `0 0 8px ${color}`,
                    }}
                  />
                  <Typography variant="caption" sx={{ flex: 1 }}>
                    {label}
                  </Typography>
                  <Typography variant="caption" sx={{ opacity: 0.6 }}>
                    {range}
                  </Typography>
                </Stack>
              ))}
            </Stack>
            <Divider sx={{ my: 1 }} />
            <Typography variant="caption" sx={{ opacity: 0.7 }}>
              Glow size = score intensity
            </Typography>
          </Paper>
        )}

        {/* Node Type Legend */}
        {filteredNodes.length > 0 && (
          <Paper
            sx={{
              position: 'absolute',
              bottom: 16,
              left: 16,
              zIndex: 1000,
              p: 1.5,
              bgcolor: 'rgba(255,255,255,0.95)',
              backdropFilter: 'blur(4px)',
              borderRadius: 1,
            }}
          >
            <Typography variant="subtitle2" gutterBottom>
              Node Types
            </Typography>
            <Stack spacing={0.5}>
              {Object.entries(TYPE_COLORS)
                .filter(([k]) => k !== 'DEFAULT')
                .map(([type, color]) => (
                  <Stack key={type} direction="row" spacing={1} alignItems="center">
                    <Box sx={{ width: 12, height: 12, borderRadius: '50%', bgcolor: color }} />
                    <Typography variant="caption">{type}</Typography>
                  </Stack>
                ))}
            </Stack>
          </Paper>
        )}
      </Paper>

      {/* Node Detail Drawer */}
      <Drawer anchor="right" open={drawerOpen} onClose={() => setDrawerOpen(false)} PaperProps={{ sx: { width: 400 } }}>
        <Box sx={{ p: 2 }}>
          <Stack direction="row" justifyContent="space-between" alignItems="center" sx={{ mb: 2 }}>
            <Typography variant="h6">Node Details</Typography>
            <IconButton size="small" onClick={() => setDrawerOpen(false)}>
              <CloseIcon />
            </IconButton>
          </Stack>

          {selectedNodeDetail && (
            <Stack spacing={2}>
              <Card variant="outlined">
                <CardContent>
                  <Typography variant="subtitle2" color="text.secondary" gutterBottom>
                    Node ID
                  </Typography>
                  <Typography variant="h6" sx={{ wordBreak: 'break-all' }}>
                    {selectedNodeDetail.id}
                  </Typography>
                </CardContent>
              </Card>

              <Card variant="outlined">
                <CardContent>
                  <List dense>
                    <ListItem>
                      <ListItemText primary="Type" secondary={selectedNodeDetail.type || 'Unknown'} />
                    </ListItem>
                    <ListItem>
                      <ListItemText primary="Label" secondary={selectedNodeDetail.label || selectedNodeDetail.id} />
                    </ListItem>
                    <ListItem>
                      <ListItemText primary="Value" secondary={selectedNodeDetail.value?.toLocaleString() || '—'} />
                    </ListItem>
                    <ListItem>
                      <ListItemText primary="Degree" secondary={selectedNodeDetail.degree || '—'} />
                    </ListItem>
                    <ListItem>
                      <ListItemText primary="Community" secondary={selectedNodeDetail.community ?? '—'} />
                    </ListItem>
                  </List>
                </CardContent>
              </Card>

              {analytics !== 'none' && selectedNodeDetail.anom_max !== undefined && (
                <Card variant="outlined" sx={{ bgcolor: SEVERITY_LEVELS[getAnomalySeverity(selectedNodeDetail.anom_max)].bgColor }}>
                  <CardContent>
                    <Stack spacing={1}>
                      <Typography variant="subtitle2" color="text.secondary">
                        Anomaly Information
                      </Typography>

                      <Stack direction="row" alignItems="center" spacing={1}>
                        <Typography variant="body2">Score: </Typography>
                        <Chip
                          size="small"
                          label={selectedNodeDetail.anom_max.toFixed(3)}
                          sx={{
                            bgcolor: SEVERITY_LEVELS[getAnomalySeverity(selectedNodeDetail.anom_max)].color,
                            color: '#fff',
                            fontWeight: 600,
                          }}
                        />
                        <Chip
                          size="small"
                          label={SEVERITY_LEVELS[getAnomalySeverity(selectedNodeDetail.anom_max)].label}
                          sx={{
                            bgcolor: SEVERITY_LEVELS[getAnomalySeverity(selectedNodeDetail.anom_max)].color,
                            color: '#fff',
                          }}
                        />
                      </Stack>

                      {selectedNodeDetail.reasons_top && selectedNodeDetail.reasons_top.length > 0 && (
                        <Box>
                          <Typography variant="caption" color="text.secondary">
                            Top Reasons: 
                          </Typography>
                          <Stack spacing={0.5} sx={{ mt: 0.5 }}>
                            {selectedNodeDetail.reasons_top.map((reason, idx) => (
                              <Chip key={idx} size="small" label={reason} variant="outlined" />
                            ))}
                          </Stack>
                        </Box>
                      )}
                    </Stack>
                  </CardContent>
                </Card>
              )}

              {neighRef.current.get(selectedNodeDetail.id) && (
                <Card variant="outlined">
                  <CardContent>
                    <Typography variant="subtitle2" color="text.secondary" gutterBottom>
                      Connected Nodes ({neighRef.current.get(selectedNodeDetail.id)?.size || 0})
                    </Typography>
                    <Stack spacing={0.5} sx={{ maxHeight: 200, overflow: 'auto' }}>
                      {Array.from(neighRef.current.get(selectedNodeDetail.id) || [])
                        .slice(0, 20)
                        .map(nid => (
                          <Chip key={nid} size="small" label={nid} variant="outlined" sx={{ fontSize: '0.75rem' }} />
                        ))}
                      {(neighRef.current.get(selectedNodeDetail.id)?.size || 0) > 20 && (
                        <Typography variant="caption" sx={{ opacity: 0.7, mt: 0.5 }}>
                          + {(neighRef.current.get(selectedNodeDetail.id)?.size || 0) - 20} more
                        </Typography>
                      )}
                    </Stack>
                  </CardContent>
                </Card>
              )}
            </Stack>
          )}
        </Box>
      </Drawer>
    </Stack>
  )
}