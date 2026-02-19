// ui-react/src/sections/FlowSankeyPanel.tsx
// Enhanced Flow Sankey Panel with full UX parity and dynamic field discovery
// - Manual Run button (no auto-load)
// - Dynamic field discovery from schema for flow path
// - Data source selection
// - Analytics modes with threshold presets
// - Severity filtering
// - Advanced controls collapse
// - Export functionality
// - Loading progress

import React, { useState, useMemo, useCallback } from 'react'
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
  Autocomplete,
} from '@mui/material'
import HelpOutlineIcon from '@mui/icons-material/HelpOutline'
import WarningAmberIcon from '@mui/icons-material/WarningAmber'
import RefreshIcon from '@mui/icons-material/Refresh'
import TuneIcon from '@mui/icons-material/Tune'
import StorageIcon from '@mui/icons-material/Storage'
import DownloadIcon from '@mui/icons-material/Download'
import InfoOutlinedIcon from '@mui/icons-material/InfoOutlined'
import TimelineIcon from '@mui/icons-material/Timeline'
import AddIcon from '@mui/icons-material/Add'
import RemoveIcon from '@mui/icons-material/Remove'
import { useQuery } from '@tanstack/react-query'
import { vizGrid, fetchSchemaFields, fetchDataSources, type Metric, type AnalyticsMode } from '../api'
import { useFilters } from '../context/FiltersContext'

// ============================================================
// Constants & Configuration
// ============================================================

const DEFAULT_PATH = ['ip', 'device_id', 'user_id', 'account_id'] as const
const LEVEL_COLORS = ['#1976D2', '#7CB342', '#F57C00', '#AB47BC', '#00ACC1', '#E91E63', '#795548', '#607D8B']

const SEVERITY_LEVELS = {
  critical: { min: 0.9, color: '#7B1FA2', bgColor: '#F3E5F5', label: 'Critical' },
  high: { min: 0.75, color: '#D32F2F', bgColor: '#FFEBEE', label: 'High' },
  medium: { min: 0.5, color: '#F57C00', bgColor: '#FFF3E0', label: 'Medium' },
  low:  { min: 0.25, color: '#FBC02D', bgColor: '#FFFDE7', label: 'Low' },
  normal: { min: 0, color: '#388E3C', bgColor: '#E8F5E9', label: 'Normal' },
}

const THRESHOLD_PRESETS = [
  { label: 'High Sensitivity', zThreshold: 2.0, contamination: 0.10, color: '#D32F2F', icon: '🔴' },
  { label:  'Balanced', zThreshold: 3.0, contamination: 0.05, color: '#F57C00', icon: '🟡' },
  { label: 'Low Sensitivity', zThreshold:  5.0, contamination: 0.02, color: '#388E3C', icon: '🟢' },
]

// Field descriptions for tooltips
const FIELD_DESCRIPTIONS:  Record<string, string> = {
  ip: 'Source IP address (NAT/VPN may aggregate many users)',
  device_id: 'Device fingerprint or unique identifier',
  user_id: 'Application user (login principal)',
  account_id: 'Business account or customer profile',
  card_hash: 'Hashed payment card number',
  country: 'Country (from geo/IP data)',
  city: 'City (from geo/IP data)',
  event_type: 'Event type (auth_success, auth_failure, payment, etc.)',
  source:  'Data ingestion source or file',
  carrier: 'Mobile carrier or network provider',
  origin: 'Origin location (airport, station, etc.)',
  dest: 'Destination location',
  email: 'Email address',
  phone: 'Phone number',
  session_id: 'Session identifier',
  merchant_id: 'Merchant identifier',
  product_id: 'Product identifier',
  browser: 'Web browser type',
  os: 'Operating system',
  payment_method: 'Payment method used',
}

type Level = string

// ============================================================
// Helper Functions
// ============================================================

function truncate(s: string, n: number) {
  if (n <= 0) return s
  return s.length > n ? `${s.slice(0, n)}…` : s
}

function hexToRgba(hex: string, alpha = 0.35) {
  const m = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex)
  if (! m) return `rgba(120,120,120,${alpha})`
  const r = parseInt(m[1], 16),
    g = parseInt(m[2], 16),
    b = parseInt(m[3], 16)
  return `rgba(${r},${g},${b},${alpha})`
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

// Format field name for display
function formatFieldLabel(field: string): string {
  return field
    .split('_')
    .map(w => w.charAt(0).toUpperCase() + w.slice(1))
    .join(' ')
}

// Categorize fields for better organization
function categorizeFields(fields: string[]): {
  identity: string[]
  location: string[]
  transaction:  string[]
  other: string[]
} {
  const identity = fields.filter(f =>
    ['user_id', 'account_id', 'device_id', 'ip', 'email', 'phone', 'session_id'].includes(f)
  )
  const location = fields.filter(f =>
    ['country', 'city', 'origin', 'dest', 'carrier'].includes(f)
  )
  const transaction = fields.filter(f =>
    ['event_type', 'source', 'card_hash', 'merchant_id', 'product_id', 'payment_method'].includes(f)
  )
  const other = fields.filter(
    f => ! identity.includes(f) && !location.includes(f) && !transaction.includes(f)
  )

  return { identity, location, transaction, other }
}

// ============================================================
// Main Component
// ============================================================

export default function FlowSankeyPanel() {
  const { startISO, endISO, filters } = useFilters()

  // Configuration State
  const [dataSource, setDataSource] = useState<string>('')
  const [analytics, setAnalytics] = useState<AnalyticsMode>('none')
  const [zThreshold, setZThreshold] = useState<number>(3.0)
  const [contamination, setContamination] = useState<number>(0.05)
  const [thresholdPreset, setThresholdPreset] = useState<string | null>('Balanced')

  const [levels, setLevels] = useState<Level[]>([])
  const [metric, setMetric] = useState<Metric>('count')
  const [valueField, setValueField] = useState<string>('anom_score')
  const [topPerLevel, setTopPerLevel] = useState<number>(20)
  const [minLink, setMinLink] = useState<number>(5)

  // Schema-derived state
  const [availableFields, setAvailableFields] = useState<string[]>([])
  const [categorizedFields, setCategorizedFields] = useState<{
    identity: string[]
    location: string[]
    transaction: string[]
    other:  string[]
  }>({ identity: [], location: [], transaction:  [], other: [] })
  const [numericFields, setNumericFields] = useState<string[]>([])

  // Visualization State
  const [truncateLen, setTruncateLen] = useState<number>(24)
  const [linkOpacity, setLinkOpacity] = useState<number>(0.35)

  // Severity Filtering
  const [selectedSeverities, setSelectedSeverities] = useState<string[]>([
    'critical',
    'high',
    'medium',
    'low',
    'normal',
  ])
  const [minAnomalyScore, setMinAnomalyScore] = useState<number>(0.25)

  // UI State
  const [showAdvancedControls, setShowAdvancedControls] = useState(false)
  const [isTransitioning, setIsTransitioning] = useState(false)

  // Fetch schema fields and categorize them
  React.useEffect(() => {
    ;(async () => {
      const schema = await fetchSchemaFields()

      // Numeric fields
      const nums = Object.entries(schema.types)
        .filter(([_, t]) => t === 'number')
        .map(([f]) => f)
      setNumericFields(Array.from(new Set([...nums, 'anom_score', 'zscore', 'amount'])))

      // All available fields (exclude internal/system fields)
      const allFields = Object.keys(schema.types).filter(
        f => !f.startsWith('_') && f !== 'id' && f !== 'ts' && f !== 'meta'
      )
      setAvailableFields(allFields)

      // Categorize fields
      const categorized = categorizeFields(allFields)
      setCategorizedFields(categorized)

      // Set smart defaults based on what's actually available
      const defaultFields = Array.from(DEFAULT_PATH).filter(f => allFields.includes(f))

      if (defaultFields.length >= 2) {
        setLevels(defaultFields)
      } else if (categorized.identity.length >= 2) {
        // Fallback:  use first available identity fields
        setLevels(categorized.identity.slice(0, 4))
      } else if (allFields.length >= 2) {
        // Last resort: use any available fields
        setLevels(allFields.slice(0, Math.min(4, allFields.length)))
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

  const pairs = useMemo(() => {
    const ps:  [Level, Level][] = []
    for (let i = 0; i < levels.length - 1; i++) ps.push([levels[i], levels[i + 1]])
    return ps
  }, [levels])

  // Query
  const q = useQuery({
    queryKey: [
      'sankey2',
      startISO,
      endISO,
      levels,
      metric,
      valueField,
      topPerLevel,
      minLink,
      filters,
      analytics,
      zThreshold,
      contamination,
      dataSource,
      selectedSeverities,
      minAnomalyScore,
    ],
    queryFn: async () => {
      // Fetch pairwise aggregations
      const results = await Promise.all(
        pairs.map(([a, b]) =>
          vizGrid({
            start: startISO,
            end: endISO,
            analytics:  'none',
            aggregate: true,
            group_by: [a, b],
            metric,
            value_field: valueField,
            limit: 50000,
            where: filters,
            source: dataSource || undefined,
          })
        )
      )

      // Fetch with anomaly scores
      let anomResults: any[][] = []
      if (analytics !== 'none') {
        anomResults = await Promise.all(
          pairs.map(([a, b]) =>
            vizGrid({
              start: startISO,
              end: endISO,
              analytics,
              z_thr: analytics === 'simple' ? zThreshold : undefined,
              contamination:  analytics === 'advanced' ? contamination : undefined,
              aggregate: true,
              group_by: [a, b],
              metric:  'max',
              value_field: 'anom_score',
              limit: 50000,
              where: filters,
              source:  dataSource || undefined,
            })
          )
        )
      }

      // Compute top-K per level
      const levelCounts = new Map<string, Map<string, number>>()
      const levelAnomScores = new Map<string, Map<string, number>>()

      pairs.forEach(([a, b], i) => {
        const rows = results[i] as any[]
        const anomRows = anomResults[i] as any[] || []

        const anomLookup = new Map<string, number>()
        anomRows.forEach(r => {
          const key = `${r[a]}|${r[b]}`
          anomLookup.set(key, r.value ??  0)
        })

        for (const r of rows) {
          const va = String(r[a] ?? '∅')
          const vb = String(r[b] ?? '∅')
          const w = Number(r.value ?? 0)
          const anomScore = anomLookup.get(`${va}|${vb}`) ?? 0

          if (!levelCounts.has(a)) levelCounts.set(a, new Map())
          if (!levelCounts.has(b)) levelCounts.set(b, new Map())
          if (!levelAnomScores.has(a)) levelAnomScores.set(a, new Map())
          if (!levelAnomScores.has(b)) levelAnomScores.set(b, new Map())

          levelCounts.get(a)!.set(va, (levelCounts.get(a)!.get(va) ?? 0) + w)
          levelCounts.get(b)!.set(vb, (levelCounts.get(b)!.get(vb) ?? 0) + w)

          const currentAnomA = levelAnomScores.get(a)!.get(va) ?? 0
          const currentAnomB = levelAnomScores.get(b)!.get(vb) ?? 0
          levelAnomScores.get(a)!.set(va, Math.max(currentAnomA, anomScore))
          levelAnomScores.get(b)!.set(vb, Math.max(currentAnomB, anomScore))
        }
      })

      const topSets = new Map<string, Set<string>>()
      for (const [lvl, m] of levelCounts) {
        const sorted = [...m.entries()]
          .sort((x, y) => y[1] - x[1])
          .slice(0, topPerLevel)
          .map(([k]) => k)
        topSets.set(lvl, new Set(sorted))
      }

      // Build Sankey nodes/links
      const nodeIndex = new Map<string, number>()
      const nodes: { name: string; raw: string; level: string; color: string; anomScore: number }[] = []
      const countsPerLevel = new Map<string, number>()
      const anomalyStats = { total: 0, critical: 0, high: 0, medium: 0, low:  0 }

      function nid(lvl: string, rawLabel: string, anomScore: number = 0) {
        const key = `${lvl}:${rawLabel}`
        if (!nodeIndex.has(key)) {
          nodeIndex.set(key, nodes.length)
          const idx = levels.indexOf(lvl)
          const maxAnom = levelAnomScores.get(lvl)?.get(rawLabel) ?? anomScore
          const baseColor = LEVEL_COLORS[idx % LEVEL_COLORS.length]

          // Filter by severity
          const severity = getAnomalySeverity(maxAnom)
          if (analytics !== 'none' && maxAnom >= minAnomalyScore) {
            if (! selectedSeverities.includes(severity)) {
              return -1 // Skip this node
            }
          }

          const nodeColor =
            analytics !== 'none' && maxAnom >= minAnomalyScore
              ? getAnomalyColor(maxAnom)
              : baseColor

          if (maxAnom >= minAnomalyScore) {
            anomalyStats.total++
            if (severity === 'critical') anomalyStats.critical++
            else if (severity === 'high') anomalyStats.high++
            else if (severity === 'medium') anomalyStats.medium++
            else if (severity === 'low') anomalyStats.low++
          }

          nodes.push({
            name: `${formatFieldLabel(lvl)} • ${truncate(rawLabel, truncateLen)}`,
            raw: `${formatFieldLabel(lvl)} • ${rawLabel}`,
            level: lvl,
            color: nodeColor,
            anomScore:  maxAnom,
          })
          countsPerLevel.set(lvl, (countsPerLevel.get(lvl) ?? 0) + 1)
        }
        return nodeIndex.get(key)!
      }

      const links: { source: number; target: number; value: number; label: string; anomScore: number }[] =
        []

      pairs.forEach(([a, b], i) => {
        const rows = results[i] as any[]
        const anomRows = anomResults[i] as any[] || []

        const anomLookup = new Map<string, number>()
        anomRows.forEach(r => {
          const key = `${r[a]}|${r[b]}`
          anomLookup.set(key, r.value ?? 0)
        })

        for (const r of rows) {
          const va = String(r[a] ?? '∅')
          const vb = String(r[b] ??  '∅')
          const w = Number(r.value ?? 0)
          const anomScore = anomLookup.get(`${va}|${vb}`) ?? 0

          if (w < minLink) continue
          if (! topSets.get(a)!.has(va) || !topSets.get(b)!.has(vb)) continue

          const s = nid(a, va, anomScore)
          const t = nid(b, vb, anomScore)

          if (s === -1 || t === -1) continue // Filtered out

          links.push({
            source: s,
            target: t,
            value: w,
            label: `${formatFieldLabel(a)}=${va} → ${formatFieldLabel(b)}=${vb}`,
            anomScore,
          })
        }
      })

      const totalsByTarget = new Map<number, number>()
      const levelIndexOfNode = new Map<number, number>()
      nodes.forEach((n, idx) => levelIndexOfNode.set(idx, levels.indexOf(n.level)))

      for (const lk of links) totalsByTarget.set(lk.target, (totalsByTarget.get(lk.target) ?? 0) + lk.value)

      return {
        nodes,
        links,
        totalsByTarget,
        levelIndexOfNode,
        countsPerLevel,
        anomalyStats,
      }
    },
    enabled: false,
    onSettled: () => setIsTransitioning(false),
  })

  const handleRunQuery = useCallback(() => {
    setIsTransitioning(true)
    q.refetch()
  }, [q])

  const handleThresholdPresetChange = useCallback((presetLabel: string) => {
    const preset = THRESHOLD_PRESETS.find(p => p.label === presetLabel)
    if (!preset) return
    setZThreshold(preset.zThreshold)
    setContamination(preset.contamination)
    setThresholdPreset(presetLabel)
    setIsTransitioning(true)
  }, [])

  const data = q.data
  const totalLinks = data?.links?.length ?? 0
  const totalNodes = data?.nodes?.length ?? 0
  const anomalyStats = data?.anomalyStats ??  { total: 0, critical: 0, high: 0, medium: 0, low: 0 }

  // Plotly payload
  const nodeLabels = data ?  data.nodes.map(n => n.name) : []
  const nodeHover = data
    ? data.nodes.map(n => {
        const anomText =
          n.anomScore >= minAnomalyScore
            ?  `<br><b style="color: ${getAnomalyColor(n.anomScore)}">⚠ Anomaly Score: ${n.anomScore.toFixed(
                3
              )}</b>`
            : ''
        return `${n.raw}${anomText}`
      })
    : []
  const nodeColors = data ? data.nodes.map(n => n.color) : []
  const linkSources = data ? data.links.map(l => l.source) : []
  const linkTargets = data ? data.links.map(l => l.target) : []
  const linkValues = data ? data.links.map(l => l.value) : []
  const linkColors = data
    ? data.links.map(l => {
        if (analytics !== 'none' && l.anomScore >= minAnomalyScore) {
          return hexToRgba(getAnomalyColor(l.anomScore), linkOpacity + 0.2)
        }
        const srcLevelIdx = data.levelIndexOfNode.get(l.source) ?? 0
        return hexToRgba(LEVEL_COLORS[srcLevelIdx % LEVEL_COLORS.length], linkOpacity)
      })
    : []
  const linkCustom = data
    ? data.links.map(l => {
        const tgtTotal = data.totalsByTarget.get(l.target) ?? 0
        const share = tgtTotal ?  l.value / tgtTotal : 0
        const anomText =
          l.anomScore >= minAnomalyScore
            ? `<br><b>⚠ Anomaly Score:  ${l.anomScore.toFixed(3)}</b>`
            : ''
        return `${l.label}<br>value=${l.value.toLocaleString()} • share to target=${(share * 100).toFixed(
          1
        )}%${anomText}`
      })
    : []

  const plotData = data
    ? [
        {
          type: 'sankey',
          orientation: 'h',
          arrangement: 'snap',
          valueformat: ',.0f',
          node: {
            label: nodeLabels,
            color: nodeColors,
            pad: 10,
            thickness: 14,
            line: { width: 0.6, color: '#8899A6' },
            hovertemplate: '%{customdata}<extra></extra>',
            customdata: nodeHover,
          },
          link: {
            source: linkSources,
            target: linkTargets,
            value: linkValues,
            color: linkColors,
            hovertemplate: '%{customdata}<extra></extra>',
            customdata: linkCustom,
          },
        } as any,
      ]
    : []

  const layout:  Partial<Plotly.Layout> = {
    margin: { l: 10, r: 10, t:  10, b: 10 },
    paper_bgcolor: 'rgba(0,0,0,0)',
    plot_bgcolor: '#ffffff',
    autosize: true,
  }

  const config: Partial<Plotly.Config> = { displaylogo: false, responsive: true }

  const legendItems = useMemo(() => {
    if (! data) return []
    return levels.map((lvl, idx) => ({
      lvl,
      color: LEVEL_COLORS[idx % LEVEL_COLORS.length],
      count: data.countsPerLevel.get(lvl) ?? 0,
    }))
  }, [data, levels])

  const handleExport = useCallback(() => {
    if (!data) return
    const exportData = {
      nodes: data.nodes,
      links: data.links,
      stats: anomalyStats,
      levels,
    }
    const blob = new Blob([JSON.stringify(exportData, null, 2)], { type: 'application/json' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `sankey-flow-${new Date().toISOString().slice(0, 19).replace(/:/g, '-')}.json`
    a.click()
    URL.revokeObjectURL(url)
  }, [data, anomalyStats, levels])

  return (
    <Box sx={{ height: '100%', display: 'flex', flexDirection:  'column', overflow: 'hidden' }}>
      <Stack spacing={1} sx={{ flex: 1, overflowY: 'auto', overflowX: 'hidden', p: 1 }}>
        {/* Loading Progress */}
        {(q.isFetching || isTransitioning) && (
          <LinearProgress sx={{ position: 'fixed', top: 0, left:  0, right: 0, zIndex: 20, height: 3 }} />
        )}

        {/* Controls Panel */}
        <Paper variant="outlined" sx={{ p: 1.5, flexShrink: 0 }}>
          <Stack spacing={1.5}>
            {/* Primary Controls Row */}
            <Stack direction="row" spacing={1.5} alignItems="center" flexWrap="wrap" useFlexGap>
              <Badge
                badgeContent={anomalyStats.critical + anomalyStats.high}
                color="error"
                invisible={anomalyStats.critical + anomalyStats.high === 0}
              >
                <Chip icon={<TimelineIcon />} label="Flow Sankey" color="primary" sx={{ fontWeight: 600 }} />
              </Badge>

              <Tooltip title="Shows how volume flows across selected dimensions to expose shared infrastructure or funnels">
                <HelpOutlineIcon fontSize="small" sx={{ color: 'text.secondary' }} />
              </Tooltip>

              {/* Data Source */}
              <FormControl size="small" sx={{ minWidth:  180 }}>
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
                      color={anomalyStats.critical + anomalyStats.high > 0 ? 'error' : 'warning'}
                      sx={{ ml:  1, fontWeight: 600 }}
                    />
                  )}
                </Stack>
              )}

              <Box sx={{ flex: 1 }} />

              {/* Export */}
              <Tooltip title="Export Flow Data">
                <IconButton size="small" onClick={handleExport} disabled={! data}>
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
                disabled={q.isFetching || levels.length < 2}
                startIcon={q.isFetching ? <CircularProgress size={16} color="inherit" /> : <RefreshIcon />}
                sx={{ minWidth: 100 }}
              >
                {q.isFetching ? 'Loading...' : 'Run'}
              </Button>
            </Stack>

            {/* Advanced Controls */}
            <Collapse in={showAdvancedControls}>
              <Divider sx={{ my: 1 }} />
              <Stack spacing={2}>
                {/* Row 1: Flow Levels */}
                <Stack direction="column" spacing={1.5}>
                  <Stack direction="row" spacing={1} alignItems="center" flexWrap="wrap" useFlexGap>
                    <Typography variant="caption" color="text.secondary" fontWeight={600}>
                      Flow Path ({levels.length} levels):
                    </Typography>
                    <Tooltip title="Define the sequence of dimensions to visualize flow patterns">
                      <HelpOutlineIcon sx={{ fontSize: 14, color: 'text.secondary' }} />
                    </Tooltip>
                  </Stack>

                  <Stack direction="row" spacing={1} alignItems="flex-start" flexWrap="wrap" useFlexGap>
                    {levels.map((lvl, i) => (
                      <Box key={i} sx={{ minWidth: 200, maxWidth: 250 }}>
                        <Autocomplete
                          size="small"
                          value={lvl}
                          options={availableFields}
                          onChange={(_, newValue) => {
                            if (newValue) {
                              const next = [...levels]
                              next[i] = newValue
                              setLevels(next)
                            }
                          }}
                          groupBy={option => {
                            if (categorizedFields.identity.includes(option)) return '👤 Identity'
                            if (categorizedFields.location.includes(option)) return '📍 Location'
                            if (categorizedFields.transaction.includes(option)) return '💳 Transaction'
                            return '📊 Other'
                          }}
                          getOptionLabel={option => formatFieldLabel(option)}
                          renderInput={params => (
                            <TextField
                              {...params}
                              label={`Level ${i + 1}`}
                              helperText={FIELD_DESCRIPTIONS[lvl] ?  truncate(FIELD_DESCRIPTIONS[lvl], 40) : undefined}
                            />
                          )}
                          renderOption={(props, option) => (
                            <li {...props}>
                              <Stack direction="row" spacing={1} alignItems="center" sx={{ width: '100%' }}>
                                <Typography variant="body2">{formatFieldLabel(option)}</Typography>
                                {FIELD_DESCRIPTIONS[option] && (
                                  <Tooltip title={FIELD_DESCRIPTIONS[option]} placement="right">
                                    <InfoOutlinedIcon sx={{ fontSize: 14, color: 'text.secondary', ml: 'auto' }} />
                                  </Tooltip>
                                )}
                              </Stack>
                            </li>
                          )}
                        />
                      </Box>
                    ))}

                    {/* Add/Remove Level Buttons */}
                    <Stack direction="row" spacing={0.5}>
                      {levels.length < 8 && availableFields.length > levels.length && (
                        <Tooltip title="Add another level to the flow path">
                          <IconButton
                            size="small"
                            color="primary"
                            onClick={() => {
                              // Find first available field not already in levels
                              const nextField = availableFields.find(f => !levels.includes(f))
                              if (nextField) {
                                setLevels([...levels, nextField])
                              }
                            }}
                            sx={{ border: '1px dashed', borderColor: 'primary.main' }}
                          >
                            <AddIcon fontSize="small" />
                          </IconButton>
                        </Tooltip>
                      )}
                      {levels.length > 2 && (
                        <Tooltip title="Remove last level">
                          <IconButton
                            size="small"
                            color="error"
                            onClick={() => setLevels(levels.slice(0, -1))}
                            sx={{ border: '1px dashed', borderColor: 'error.main' }}
                          >
                            <RemoveIcon fontSize="small" />
                          </IconButton>
                        </Tooltip>
                      )}
                    </Stack>
                  </Stack>

                  {/* Info Alert */}
                  {availableFields.length > 0 && (
                    <Alert severity="info" icon={<InfoOutlinedIcon />} sx={{ py: 0.5 }}>
                      <Typography variant="caption">
                        <strong>{availableFields.length} field(s)</strong> available from your data schema.
                        {categorizedFields.identity.length > 0 && (
                          <>
                            {' '}
                            Identity:  <strong>{categorizedFields.identity.join(', ')}</strong>.
                          </>
                        )}
                        {categorizedFields.location.length > 0 && (
                          <>
                            {' '}
                            Location: <strong>{categorizedFields.location.join(', ')}</strong>.
                          </>
                        )}
                        {categorizedFields.transaction.length > 0 && (
                          <>
                            {' '}
                            Transaction: <strong>{categorizedFields.transaction.join(', ')}</strong>.
                          </>
                        )}
                      </Typography>
                    </Alert>
                  )}
                </Stack>

                {/* Row 2: Metrics & Filtering */}
                <Stack direction="row" spacing={2} alignItems="center" flexWrap="wrap" useFlexGap>
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
                    select
                    size="small"
                    label="Field"
                    value={valueField}
                    onChange={e => setValueField(e.target.value)}
                    sx={{ minWidth:  160 }}
                    disabled={metric === 'count'}
                  >
                    {numericFields.map(f => (
                      <MenuItem key={f} value={f}>
                        {f}
                      </MenuItem>
                    ))}
                  </TextField>

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
                              px: 1,
                              '&.Mui-selected': {
                                bgcolor: `${SEVERITY_LEVELS[sev as keyof typeof SEVERITY_LEVELS]?.color}30`,
                                borderColor:  SEVERITY_LEVELS[sev as keyof typeof SEVERITY_LEVELS]?.color,
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
                </Stack>

                {/* Row 3: Visual Controls */}
                <Stack direction="row" spacing={2} alignItems="center" flexWrap="wrap" useFlexGap>
                  <Box sx={{ width: 200 }}>
                    <Typography variant="caption" color="text.secondary">
                      Top-K Nodes/Level:  {topPerLevel}
                    </Typography>
                    <Slider
                      size="small"
                      value={topPerLevel}
                      min={5}
                      max={100}
                      step={5}
                      onChange={(_, v) => setTopPerLevel(v as number)}
                    />
                  </Box>

                  <Box sx={{ width:  180 }}>
                    <Typography variant="caption" color="text.secondary">
                      Min Link:  {minLink}
                    </Typography>
                    <Slider
                      size="small"
                      value={minLink}
                      min={1}
                      max={200}
                      step={1}
                      onChange={(_, v) => setMinLink(v as number)}
                    />
                  </Box>

                  <Box sx={{ width: 180 }}>
                    <Typography variant="caption" color="text.secondary">
                      Label Length: {truncateLen}
                    </Typography>
                    <Slider
                      size="small"
                      value={truncateLen}
                      min={12}
                      max={48}
                      step={2}
                      onChange={(_, v) => setTruncateLen(v as number)}
                    />
                  </Box>

                  <Box sx={{ width: 180 }}>
                    <Typography variant="caption" color="text.secondary">
                      Link Opacity: {linkOpacity.toFixed(2)}
                    </Typography>
                    <Slider
                      size="small"
                      value={linkOpacity}
                      min={0.15}
                      max={0.7}
                      step={0.05}
                      onChange={(_, v) => setLinkOpacity(v as number)}
                    />
                  </Box>

                  {analytics !== 'none' && (
                    <Box sx={{ width: 200 }}>
                      <Typography variant="caption" color="text.secondary">
                        Min Anomaly Score: {minAnomalyScore.toFixed(2)}
                      </Typography>
                      <Slider
                        size="small"
                        value={minAnomalyScore}
                        min={0.0}
                        max={1.0}
                        step={0.05}
                        onChange={(_, v) => setMinAnomalyScore(v as number)}
                        color="error"
                      />
                    </Box>
                  )}
                </Stack>

                {/* Row 4: Thresholds */}
                {analytics !== 'none' && (
                  <Stack direction="row" spacing={2} alignItems="center" flexWrap="wrap" useFlexGap>
                    {analytics === 'simple' && (
                      <Box sx={{ width: 220 }}>
                        <Stack direction="row" spacing={1} alignItems="center">
                          <Typography variant="caption" color="text.secondary">
                            Z-Threshold: <strong>{zThreshold.toFixed(1)}</strong>
                          </Typography>
                          <Tooltip title="Lower values = more sensitive">
                            <InfoOutlinedIcon sx={{ fontSize:  14, color: 'text.secondary' }} />
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
                            Contamination: <strong>{(contamination * 100).toFixed(0)}%</strong>
                          </Typography>
                          <Tooltip title="Expected % of anomalies">
                            <InfoOutlinedIcon sx={{ fontSize: 14, color: 'text.secondary' }} />
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
                            { value: 0.10, label: '10%' },
                            { value: 0.20, label: '20%' },
                          ]}
                          sx={{
                            '& .MuiSlider-track': {
                              background: 'linear-gradient(90deg, #388E3C 0%, #F57C00 50%, #D32F2F 100%)',
                            },
                          }}
                        />
                      </Box>
                    )}
                  </Stack>
                )}
              </Stack>
            </Collapse>
          </Stack>
        </Paper>

        {/* Anomaly Summary Alert */}
        {analytics !== 'none' && anomalyStats.total > 0 && (
          <Alert
            severity={anomalyStats.critical > 0 || anomalyStats.high > 0 ? 'error' :  'warning'}
            icon={<WarningAmberIcon />}
            sx={{ py: 0.5, flexShrink: 0 }}
          >
            <Stack direction="row" spacing={3} alignItems="center" flexWrap="wrap">
              <Typography variant="body2">
                <strong>{anomalyStats.total}</strong> anomalous nodes detected
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
                  sx={{ bgcolor: SEVERITY_LEVELS.medium.color, color: '#fff' }}
                />
              )}
              {anomalyStats.low > 0 && (
                <Chip
                  size="small"
                  label={`${anomalyStats.low} Low`}
                  sx={{ bgcolor: SEVERITY_LEVELS.low.color, color: '#000' }}
                />
              )}
            </Stack>
          </Alert>
        )}

        {/* Legend */}
        <Paper variant="outlined" sx={{ p: 1.5, bgcolor: '#fafafa', flexShrink: 0 }}>
          <Stack spacing={1.5}>
            <Stack direction="row" spacing={2} alignItems="center" flexWrap="wrap">
              <Box sx={{ minWidth: 200 }}>
                <Typography variant="subtitle2" fontWeight={700}>
                  Flow Levels
                </Typography>
                <Typography variant="caption" color="text.secondary">
                  Each level has a distinct color. Bands show {metric}
                  {metric !== 'count' ?  ` of ${valueField}` : ''}.
                </Typography>
              </Box>

              <Box sx={{ flex: 1, display: 'flex', flexWrap: 'wrap', gap:  1.5 }}>
                {legendItems.map(({ lvl, color, count }) => (
                  <Stack key={lvl} direction="row" spacing={0.5} alignItems="center">
                    <Box sx={{ width: 12, height: 12, borderRadius:  '50%', bgcolor: color }} />
                    <Typography variant="body2" fontWeight={600}>
                      {formatFieldLabel(lvl)}
                    </Typography>
                    <Typography variant="caption" color="text.secondary">
                      ({count})
                    </Typography>
                    {FIELD_DESCRIPTIONS[lvl] && (
                      <Tooltip title={FIELD_DESCRIPTIONS[lvl]}>
                        <HelpOutlineIcon sx={{ fontSize: 14, color: 'text.secondary' }} />
                      </Tooltip>
                    )}
                  </Stack>
                ))}
              </Box>

              {data && (
                <Stack direction="row" spacing={1}>
                  <Chip size="small" label={`${totalNodes.toLocaleString()} nodes`} />
                  <Chip size="small" label={`${totalLinks.toLocaleString()} links`} />
                </Stack>
              )}
            </Stack>

            {/* Anomaly Legend */}
            {analytics !== 'none' && (
              <>
                <Divider />
                <Stack direction="row" spacing={2} flexWrap="wrap" alignItems="center">
                  <Typography variant="caption" fontWeight={600} color="text.secondary">
                    Anomaly Colors: 
                  </Typography>
                  {[
                    { label: 'Critical', color:  SEVERITY_LEVELS.critical.color },
                    { label: 'High', color: SEVERITY_LEVELS.high.color },
                    { label: 'Medium', color: SEVERITY_LEVELS.medium.color },
                    { label: 'Low', color: SEVERITY_LEVELS.low.color },
                  ].map(({ label, color }) => (
                    <Stack key={label} direction="row" spacing={0.5} alignItems="center">
                      <Box sx={{ width: 10, height: 10, borderRadius: '50%', bgcolor: color }} />
                      <Typography variant="caption">{label}</Typography>
                    </Stack>
                  ))}
                </Stack>
              </>
            )}
          </Stack>
        </Paper>

        {/* Chart */}
        <Paper variant="outlined" sx={{ minHeight: 560, height: 560, position: 'relative', flexShrink: 0 }}>
          {! data && ! q.isFetching ?  (
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
              <TimelineIcon sx={{ fontSize: 64, opacity: 0.3 }} />
              <Typography variant="h6" color="text.secondary">
                No flow data available
              </Typography>
              <Typography variant="body2" color="text.secondary">
                {levels.length < 2
                  ? 'Add at least 2 levels to the flow path'
                  : 'Click Run to generate Sankey diagram'}
              </Typography>
              <Button
                variant="outlined"
                startIcon={<RefreshIcon />}
                onClick={handleRunQuery}
                disabled={q.isFetching || levels.length < 2}
              >
                Run Query
              </Button>
            </Box>
          ) : (
            <Plot
              data={plotData as any}
              layout={layout as any}
              config={config}
              style={{ width: '100%', height: '100%' }}
              useResizeHandler
            />
          )}
        </Paper>
      </Stack>
    </Box>
  )
}