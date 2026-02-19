// path: ui-react/src/sections/MapPanel.tsx
// Enhanced Map Panel - FINAL VERSION
// - Critical anomalies now show in alert bar
// - Routes filtered by selected users/groups
// - Dynamic origin/dest field options based on data source

import React, { useState, useEffect, useMemo, useCallback } from 'react'
import {
  Box, Stack, Button, TextField, MenuItem, Slider, Chip, Typography,
  Autocomplete, Paper, FormControlLabel, Switch, Alert, Tooltip, Badge,
  IconButton, Collapse, Divider, CircularProgress, Select, InputLabel,
  FormControl, ToggleButton, ToggleButtonGroup, OutlinedInput, Checkbox,
  ListItemText, LinearProgress, alpha,
} from '@mui/material'
import WarningAmberIcon from '@mui/icons-material/WarningAmber'
import RefreshIcon from '@mui/icons-material/Refresh'
import TuneIcon from '@mui/icons-material/Tune'
import StorageIcon from '@mui/icons-material/Storage'
import MapIcon from '@mui/icons-material/Map'
import InfoOutlinedIcon from '@mui/icons-material/InfoOutlined'
import FlightIcon from '@mui/icons-material/Flight'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import DeckGL from '@deck.gl/react'
import { MapView, _GlobeView as GlobeView, PickingInfo } from '@deck.gl/core'
import { ScatterplotLayer, BitmapLayer, ArcLayer } from '@deck.gl/layers'
import { HexagonLayer } from '@deck.gl/aggregation-layers'
import { TileLayer } from '@deck.gl/geo-layers'
import MapGL from 'react-map-gl/maplibre'
import 'maplibre-gl/dist/maplibre-gl.css'
import {
  vizGlobe,
  fetchSchemaFields,
  fetchDataSources,
  fetchDataSourceInfo,
  fetchTopUsers,
  fetchDistinct,
  type AnalyticsMode,
  type Bucket,
  type Metric,
  type DataSource,
  type TimelineAnomalyEvent
} from '../api'
import { useFilters } from '../context/FiltersContext'
import { AIRPORTS, getAirportCoords } from '../data/airports'
import AnomalyDetailDrawer from '../components/AnomalyDetailDrawer'

// ============================================================
// Constants & Configuration
// ============================================================

const POSITRON = 'https://basemaps.cartocdn.com/gl/positron-gl-style/style.json'
const DARKMATTER = 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json'
const RASTER_TILES_LIGHT = 'https://a.basemaps.cartocdn.com/rastertiles/light_all/{z}/{x}/{y}.png'

const LS_PREFIX = 'lizard.map'

const BUCKET_OPTIONS:  { value: Bucket; label: string }[] = [
  { value: '30s', label: '30 sec' },
  { value: '1m', label: '1 min' },
  { value:  '2m', label: '2 min' },
  { value: '5m', label: '5 min' },
  { value:  '10m', label: '10 min' },
  { value: '15m', label: '15 min' },
  { value: '30m', label:  '30 min' },
  { value: '1h', label: '1 hour' },
  { value: '3h', label: '3 hours' },
  { value: '6h', label: '6 hours' },
  { value:  '12h', label: '12 hours' },
  { value: '1d', label: '1 day' },
]

const THRESHOLD_PRESETS = [
  { label: 'High Sensitivity', zThreshold: 2.0, contamination: 0.10, color: '#D32F2F', icon: '🔴' },
  { label: 'Balanced', zThreshold: 3.0, contamination: 0.05, color: '#F57C00', icon: '🟡' },
  { label: 'Low Sensitivity', zThreshold: 5.0, contamination: 0.02, color: '#388E3C', icon: '🟢' },
]

const METRIC_OPTIONS:  { value: Metric; label:  string }[] = [
  { value: 'count', label:  'Count' },
  { value: 'sum', label: 'Sum' },
  { value: 'avg', label: 'Average' },
  { value: 'max', label: 'Maximum' },
  { value: 'min', label: 'Minimum' },
]

// Anomaly severity matching TimelinePanel
const SEVERITY_LEVELS = {
  critical: { min: 0.9, color: '#7B1FA2', bgColor: '#F3E5F5', label: 'Critical' },
  high: { min: 0.75, color: '#D32F2F', bgColor:  '#FFEBEE', label: 'High' },
  medium: { min: 0.5, color: '#F57C00', bgColor: '#FFF3E0', label: 'Medium' },
  low:  { min: 0.25, color: '#FBC02D', bgColor: '#FFFDE7', label: 'Low' },
  normal: { min: 0, color: '#388E3C', bgColor: '#E8F5E9', label: 'Normal' },
}

const ANOMALY_COLORS_RGBA:  Record<string, [number, number, number, number]> = {
  none: [76, 175, 80, 220],
  low: [251, 192, 45, 220],
  medium:  [245, 124, 0, 220],
  high:  [211, 47, 47, 220],
  critical:  [123, 31, 162, 220],
}

// Default origin/dest field options (used when no data source selected)
const DEFAULT_ORIGIN_FIELDS = ['origin', 'meta.origin', 'departure', 'from']
const DEFAULT_DEST_FIELDS = ['dest', 'meta.dest', 'arrival', 'to']

function getAnomalySeverity(score: number): keyof typeof SEVERITY_LEVELS {
  if (score >= SEVERITY_LEVELS.critical.min) return 'critical'
  if (score >= SEVERITY_LEVELS.high.min) return 'high'
  if (score >= SEVERITY_LEVELS.medium.min) return 'medium'
  if (score >= SEVERITY_LEVELS.low.min) return 'low'
  return 'normal'
}

function getAnomalyColorRGBA(score: number): [number, number, number, number] {
  const severity = getAnomalySeverity(score)
  return ANOMALY_COLORS_RGBA[severity === 'normal' ? 'none' : severity]
}

// ============================================================
// Configuration Interface
// ============================================================

interface MapConfig {
  dataSource: string
  analyticsMode: AnalyticsMode
  viewType: '2d' | '3d'
  basemap: string

  zThreshold: number
  contamination: number
  speedThreshold: number
  distanceThreshold: number
  thresholdPreset: string | null

  groupField: string
  topN: number

  selectedSeverities: string[]
  minAnomalyScore: number

  routeMode: boolean
  routeMetric: Metric
  selectedCarriers: string[]
  originField: string
  destField: string

  metric: Metric
  valueField: string
  bucket: Bucket
  heat: boolean
  backendCluster: boolean
  localClustering: boolean
  clusterRadiusKm: number
  hexRadiusKm: number

  showAnomalyOverlay: boolean
}

const DEFAULT_CONFIG: MapConfig = {
  dataSource: '',
  analyticsMode: 'none',
  viewType: '2d',
  basemap:  POSITRON,

  zThreshold: 3.0,
  contamination: 0.05,
  speedThreshold: 900,
  distanceThreshold:  2000,
  thresholdPreset: 'Balanced',

  groupField: 'user_id',
  topN:  10,

  selectedSeverities: ['critical', 'high', 'medium', 'low', 'normal'],
  minAnomalyScore: 0.0,

  routeMode: true,
  routeMetric:  'count',
  selectedCarriers: [],
  originField:  'origin',
  destField: 'dest',

  metric: 'count',
  valueField: 'anom_score',
  bucket: '5m',
  heat: true,
  backendCluster:  false,
  localClustering:  false,
  clusterRadiusKm: 50,
  hexRadiusKm: 30,

  showAnomalyOverlay: true,
}

// ============================================================
// Persistence
// ============================================================

function loadPersistedConfig(): Partial<MapConfig> | null {
  try {
    const stored = localStorage.getItem(`${LS_PREFIX}.config`)
    return stored ? JSON.parse(stored) : null
  } catch {
    return null
  }
}

function savePersistedConfig(config: MapConfig) {
  try {
    localStorage.setItem(`${LS_PREFIX}.config`, JSON.stringify(config))
  } catch {}
}

// ============================================================
// Helper Functions
// ============================================================

function seededHash(str: string): number {
  let h = 2166136261 >>> 0
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i)
    h = Math.imul(h, 16777619)
  }
  h = (h ^ (h >>> 16)) >>> 0
  return (h & 0xFFFFFF) / 0x1000000
}

function clusterPoints(points: any[], radiusKm: number): Array<{ lat: number; lon: number; points: any[]; avgScore: number }> {
  if (! points.length) return []

  const clusters: Array<{ lat: number; lon: number; points: any[]; avgScore: number }> = []
  const visited = new Set<number>()
  const radiusDeg = radiusKm / 111.32

  for (let i = 0; i < points.length; i++) {
    if (visited.has(i)) continue

    const p1 = points[i]
    const lat1 = p1.geo_lat ?? p1.lat
    const lon1 = p1.geo_lon ?? p1.lon

    if (! Number.isFinite(lat1) || !Number.isFinite(lon1)) continue

    const cluster:  any[] = [p1]
    visited.add(i)

    for (let j = i + 1; j < points.length; j++) {
      if (visited.has(j)) continue

      const p2 = points[j]
      const lat2 = p2.geo_lat ?? p2.lat
      const lon2 = p2.geo_lon ?? p2.lon

      if (! Number.isFinite(lat2) || !Number.isFinite(lon2)) continue

      const latDiff = Math.abs(lat1 - lat2)
      const lonDiff = Math.abs(lon1 - lon2)
      const dist = Math.sqrt(latDiff ** 2 + lonDiff ** 2)

      if (dist <= radiusDeg) {
        cluster.push(p2)
        visited.add(j)
      }
    }

    const avgLat = cluster.reduce((sum, p) => sum + (p.geo_lat ?? p.lat), 0) / cluster.length
    const avgLon = cluster.reduce((sum, p) => sum + (p.geo_lon ?? p.lon), 0) / cluster.length
    const avgScore = cluster.reduce((sum, p) => sum + (p.anom_score ?? p.cwa_score ?? p.zscore ?? 0), 0) / cluster.length

    clusters.push({ lat: avgLat, lon:  avgLon, points: cluster, avgScore })
  }

  return clusters
}

function getCenterZoom(points: any[], airportsFromRoutes: { lon: number, lat: number }[]) {
  const coords = [
    ...points
      .map((d:  any) => [Number(d.geo_lon ?? d.lon ?? d.lng), Number(d.geo_lat ?? d.lat)])
      .filter(([x, y]) => Number.isFinite(x) && Number.isFinite(y)),
    ...airportsFromRoutes
      .map(a => [Number(a.lon), Number(a.lat)])
      .filter(([x, y]) => Number.isFinite(x) && Number.isFinite(y)),
  ]
  if (! coords.length) return { longitude: 2, latitude: 48.5, zoom: 2, bearing: 0, pitch: 30 }
  const lons = coords.map(c => c[0])
  const lats = coords.map(c => c[1])
  const lon = (Math.min(...lons) + Math.max(...lons)) / 2
  const lat = (Math.min(...lats) + Math.max(...lats)) / 2
  const spread = Math.max(Math.max(...lons) - Math.min(...lons), Math.max(...lats) - Math.min(...lats))
  const zoom = spread < 5 ? 6 : spread < 20 ? 4 : 2
  return { longitude: lon, latitude: lat, zoom, bearing: 0, pitch: 30 }
}

// Helper to check if a column name looks like an origin field
function isOriginLikeColumn(colName: string): boolean {
  const lower = colName.toLowerCase()
  return (
    lower.includes('origin') ||
    lower.includes('departure') ||
    lower.includes('from') ||
    lower === 'o' ||
    lower.endsWith('_origin') ||
    lower.endsWith('_from') ||
    lower.endsWith('_departure')
  )
}

// Helper to check if a column name looks like a dest field
function isDestLikeColumn(colName: string): boolean {
  const lower = colName.toLowerCase()
  return (
    lower.includes('dest') ||
    lower.includes('arrival') ||
    lower.includes('to') ||
    lower === 'd' ||
    lower.endsWith('_dest') ||
    lower.endsWith('_to') ||
    lower.endsWith('_arrival')
  )
}

// ============================================================
// Main Component
// ============================================================

export default function MapPanel() {
  const { startISO, endISO, filters } = useFilters()
  const queryClient = useQueryClient()

  // Load persisted config
  const [config, setConfig] = useState<MapConfig>(() => {
    const persisted = loadPersistedConfig()
    return { ...DEFAULT_CONFIG, ...persisted }
  })

  // Persist config changes
  useEffect(() => {
    savePersistedConfig(config)
  }, [config])

  const updateConfig = useCallback(<K extends keyof MapConfig>(key:  K, value: MapConfig[K]) => {
    setConfig((prev) => ({ ...prev, [key]: value }))
  }, [])

  // UI State
  const [showAdvancedControls, setShowAdvancedControls] = useState(false)
  const [selectedGroups, setSelectedGroups] = useState<string[]>([])
  const [isTransitioning, setIsTransitioning] = useState(false)
  const [err, setErr] = useState<string | null>(null)
  const [numericFields, setNumericFields] = useState<string[]>([])

  // Dynamic field options state
  const [availableColumns, setAvailableColumns] = useState<string[]>([])
  const [originFieldOptions, setOriginFieldOptions] = useState<string[]>(DEFAULT_ORIGIN_FIELDS)
  const [destFieldOptions, setDestFieldOptions] = useState<string[]>(DEFAULT_DEST_FIELDS)

  // Anomaly Drawer
  const [drawerOpen, setDrawerOpen] = useState(false)
  const [selectedAnomaly, setSelectedAnomaly] = useState<{
    bucketTime: string | null
    score: number
    reasons: string[]
    explain: string
    events: TimelineAnomalyEvent[]
    thresholds?:  any
  } | null>(null)

  const [viewState, setViewState] = useState<any>({
    longitude: 2, latitude: 48.5, zoom: 2, bearing: 0, pitch: 30
  })

  // Data Sources
  const dataSourcesQuery = useQuery({
    queryKey: ['dataSources'],
    queryFn: fetchDataSources,
    staleTime: 60000,
  })

  // Schema Fields
  const schemaQuery = useQuery({
    queryKey: ['schemaFields'],
    queryFn: fetchSchemaFields,
    staleTime: 60000,
  })

  useEffect(() => {
    if (!schemaQuery.data) return
    const nums = Object.entries(schemaQuery.data.types)
      .filter(([_, t]) => t === 'number')
      .map(([f]) => f)
    setNumericFields(Array.from(new Set([...nums, 'anom_score', 'zscore', 'amount', 'cwa_score'])))
  }, [schemaQuery.data])

  // Fetch columns when data source changes - for dynamic origin/dest field options
  useEffect(() => {
    if (!config.dataSource) {
      // Reset to defaults when no source selected
      setAvailableColumns([])
      setOriginFieldOptions(DEFAULT_ORIGIN_FIELDS)
      setDestFieldOptions(DEFAULT_DEST_FIELDS)
      return
    }

    // Fetch source info to get actual columns
    (async () => {
      try {
        const sourceInfo = await fetchDataSourceInfo(config.dataSource)
        if (sourceInfo.columns) {
          const cols = Object.keys(sourceInfo.columns)
          setAvailableColumns(cols)

          // Build origin field options from actual columns
          const originCols = cols.filter(isOriginLikeColumn)
          const newOriginOptions = [...new Set([...DEFAULT_ORIGIN_FIELDS, ...originCols])]
          setOriginFieldOptions(newOriginOptions)

          // Build dest field options from actual columns
          const destCols = cols.filter(isDestLikeColumn)
          const newDestOptions = [...new Set([...DEFAULT_DEST_FIELDS, ...destCols])]
          setDestFieldOptions(newDestOptions)

          // Auto-select origin field if current selection doesn't exist in new source
          if (! cols.includes(config.originField) && ! DEFAULT_ORIGIN_FIELDS.includes(config.originField)) {
            if (originCols.length > 0) {
              updateConfig('originField', originCols[0])
            }
          }

          // Auto-select dest field if current selection doesn't exist in new source
          if (!cols.includes(config.destField) && !DEFAULT_DEST_FIELDS.includes(config.destField)) {
            if (destCols.length > 0) {
              updateConfig('destField', destCols[0])
            }
          }
        }
      } catch (e) {
        console.warn('Failed to fetch source columns:', e)
        // Keep current options on error
      }
    })()
  }, [config.dataSource])

  // Top Users
  const topUsersQuery = useQuery({
    queryKey:  ['topUsers', startISO, endISO, config.groupField, filters],
    queryFn: () =>
      fetchTopUsers({
        start: startISO,
        end: endISO,
        n: 1000,
        group_field: config.groupField,
        where: filters,
      }),
    enabled: Boolean(startISO && endISO),
    staleTime: 30000,
  })

  const allUsers = useMemo(() => topUsersQuery.data?.users || [], [topUsersQuery.data])

  // Fetch Available Carriers
  const carriersQuery = useQuery({
    queryKey: ['carriers', startISO, endISO],
    queryFn: async () => {
      const result = await fetchDistinct('carrier', startISO, endISO, 100)
      return result.values.filter(Boolean).sort()
    },
    enabled: Boolean(startISO && endISO),
    staleTime: 60000,
  })

  const availableCarriers = useMemo(() => carriersQuery.data || [], [carriersQuery.data])

  // Query Configuration
  const queryBody = useMemo(() => ({
    start: startISO,
    end: endISO,
    analytics:  config.analyticsMode,
    z_thr: config.analyticsMode === 'simple' ? config.zThreshold : undefined,
    contamination: config.analyticsMode === 'advanced' ? config.contamination : undefined,
    speed_thr:  config.analyticsMode === 'advanced' ? config.speedThreshold : undefined,
    dist_thr: config.analyticsMode === 'advanced' ? config.distanceThreshold : undefined,
    cluster: config.backendCluster,
    bucket: config.bucket,
    metric: config.metric,
    value_field: config.metric !== 'count' ? config.valueField : undefined,
    where: filters,
    route_mode: config.routeMode,
    route_metric: config.routeMetric,
    carrier: config.selectedCarriers.length > 0 ? config.selectedCarriers.join(',') : undefined,
    source: config.dataSource || undefined
  }), [startISO, endISO, config, filters])

  const q = useQuery({
    queryKey: ['vizGlobe', queryBody],
    queryFn: async () => {
      setErr(null)
      return await vizGlobe(queryBody as any)
    },
    enabled: false,
    retry: 0,
    staleTime: 30000,
    keepPreviousData: true,
    onSettled: () => setIsTransitioning(false),
  })

  // Fix loading spinner stuck issue
  useEffect(() => {
    if (! q.isFetching && isTransitioning) {
      setIsTransitioning(false)
    }
  }, [q.isFetching, isTransitioning])

  const payload = q.data
  const eventsRaw:  any[] = Array.isArray(payload) ? (payload as any[]) : (payload?.events ?? [])

  // Update available columns when we get query results (works for views!)
useEffect(() => {
  if (eventsRaw.length > 0) {
    const cols = Object.keys(eventsRaw[0]).filter(k => !k.startsWith('_'))

    if (cols.length > 0) {
      setAvailableColumns(cols)

      // Build origin field options from actual columns
      const originCols = cols.filter(isOriginLikeColumn)
      const newOriginOptions = [...new Set([...DEFAULT_ORIGIN_FIELDS, ...originCols])]
      setOriginFieldOptions(newOriginOptions)

      // Build dest field options from actual columns
      const destCols = cols.filter(isDestLikeColumn)
      const newDestOptions = [...new Set([...DEFAULT_DEST_FIELDS, ...destCols])]
      setDestFieldOptions(newDestOptions)

      console.log('Updated field options from query results:', { originCols, destCols })
    }
  }
}, [eventsRaw])

  // Determine which users to filter by
  const activeFilteredUsers = useMemo(() => {
    if (selectedGroups.length > 0) {
      return selectedGroups
    } else if (config.topN < 9999 && allUsers.length > 0) {
      return allUsers.slice(0, config.topN)
    }
    return [] // No user filter
  }, [selectedGroups, config.topN, allUsers])

  // Process Events with ALL Filtering (Severity, Groups, Carriers)
  // Also handle dynamic origin/dest field names
  const events = useMemo(() => {
    let filtered = eventsRaw.map(e => {
      // Get origin value from configured field (handles prefixed columns like ticket_events_origin)
      let originValue = e[config.originField]
      if (originValue === undefined && config.originField.startsWith('meta.')) {
        originValue = e?.meta?.[config.originField.replace('meta.', '')]
      }
      if (originValue === undefined) {
        // Fallback:  check common origin field names
        originValue = e.origin ?? e?.meta?.origin
      }

      // Get dest value from configured field
      let destValue = e[config.destField]
      if (destValue === undefined && config.destField.startsWith('meta.')) {
        destValue = e?.meta?.[config.destField.replace('meta.', '')]
      }
      if (destValue === undefined) {
        // Fallback: check common dest field names
        destValue = e.dest ?? e?.meta?.dest
      }

      return {
        ...e,
        geo_lat: e.geo_lat ?? e?.meta?.geo_lat,
        geo_lon: e.geo_lon ?? e?.meta?.geo_lon,
        origin: originValue,
        dest:  destValue,
        carrier: e.carrier ?? e?.meta?.carrier,
        [config.groupField]: e[config.groupField] ?? e?.meta?.[config.groupField]
      }
    })

    // Filter by anomaly score and severity
    if (config.analyticsMode !== 'none') {
      filtered = filtered.filter(e => {
        const score = e.anom_score ?? e.cwa_score ?? e.zscore ?? 0
        if (score < config.minAnomalyScore) return false
        const severity = getAnomalySeverity(score)
        return config.selectedSeverities.includes(severity)
      })
    }

    // Filter by carrier
    if (config.selectedCarriers.length > 0) {
      filtered = filtered.filter(e => {
        const evtCarrier = (e.carrier || '').toUpperCase().trim()
        return config.selectedCarriers.some(c => evtCarrier.includes(c.toUpperCase()))
      })
    }

    // Filter by selected groups (Top N or manual selection)
    if (activeFilteredUsers.length > 0) {
      filtered = filtered.filter(e => {
        const groupValue = e[config.groupField]
        return activeFilteredUsers.includes(String(groupValue))
      })
    }

    return filtered
  }, [eventsRaw, activeFilteredUsers, config.groupField, config.minAnomalyScore, config.selectedSeverities, config.selectedCarriers, config.analyticsMode, config.originField, config.destField])

  const points = events.filter((d: any) => {
    const lon = Number(d.geo_lon ?? d.lon ?? d.lng)
    const lat = Number(d.geo_lat ?? d.lat)
    return Number.isFinite(lon) && Number.isFinite(lat)
  })

  // Anomaly Statistics - COUNT ALL SEVERITIES INCLUDING CRITICAL
  const anomalyStats = useMemo(() => {
    let critical = 0, high = 0, medium = 0, low = 0
    points.forEach(e => {
      const score = e.anom_score ?? e.cwa_score ?? e.zscore ?? 0
      const sev = getAnomalySeverity(score)
      if (sev === 'critical') critical++
      else if (sev === 'high') high++
      else if (sev === 'medium') medium++
      else if (sev === 'low') low++
    })
    return {
      total: critical + high + medium + low,
      critical,
      high,
      medium,
      low,
      highAndCritical: critical + high,
    }
  }, [points])

  // Clustering
  const clusteredPoints = useMemo(() => {
    if (! config.localClustering) return points
    return clusterPoints(points, config.clusterRadiusKm)
  }, [points, config.localClustering, config.clusterRadiusKm])

  // Build Routes from FILTERED Events (same as points)
  // Uses the normalized origin/dest values from events processing
  const routesFromData = useMemo(() => {
    if (!config.routeMode) return []

    const routeMap = new Map<string, { count: number; value: number; scores: number[]; users: Set<string> }>()

    // USE FILTERED EVENTS (same filtering as points)
    events.forEach(e => {
      // Use normalized origin/dest values (already extracted in events processing)
      const origin = e.origin
      const dest = e.dest
      const userId = String(e[config.groupField] || '')

      if (! origin || !dest || origin === dest) return

      const key = `${origin}-${dest}`
      const existing = routeMap.get(key)
      const score = e.anom_score ?? e.cwa_score ?? e.zscore ?? 0

      if (existing) {
        existing.count++
        existing.scores.push(score)
        if (userId) existing.users.add(userId)
      } else {
        const users = new Set<string>()
        if (userId) users.add(userId)
        routeMap.set(key, { count: 1, value: 0, scores: [score], users })
      }
    })

    const routes:  any[] = []
    routeMap.forEach((data, key) => {
      const [o, d] = key.split('-')
      let value = data.count

      if (config.routeMetric === 'avg') {
        value = data.scores.reduce((a, b) => a + b, 0) / data.scores.length
      } else if (config.routeMetric === 'max') {
        value = Math.max(...data.scores)
      } else if (config.routeMetric === 'sum') {
        value = data.scores.reduce((a, b) => a + b, 0)
      }

      const originCoords = getAirportCoords(o)
      const destCoords = getAirportCoords(d)

      if (originCoords && destCoords) {
        routes.push({
          o,
          d,
          count: data.count,
          value,
          avgScore: data.scores.reduce((a, b) => a + b, 0) / data.scores.length,
          userCount: data.users.size,
          sourcePosition: [originCoords.lng, originCoords.lat],
          targetPosition: [destCoords.lng, destCoords.lat]
        })
      }
    })

    return routes
  }, [events, config.routeMode, config.routeMetric, config.groupField])

  const airportsFromRoutes = useMemo(() => {
    if (!config.routeMode || !routesFromData.length) return []
    const setIata = new Set<string>()
    routesFromData.forEach(r => { setIata.add(r.o); setIata.add(r.d) })
    return [...setIata]
      .map(iata => AIRPORTS[iata])
      .filter((a:  any) => a && Number.isFinite(a.lat) && Number.isFinite(a.lon))
  }, [config.routeMode, routesFromData])

  // Auto-center
  useEffect(() => {
    if (!points.length && !airportsFromRoutes.length) return
    const target = getCenterZoom(points, airportsFromRoutes)
    setViewState((v:  any) => ({ ...v, ...target, pitch: config.viewType === '3d' ? 0 : 30 }))
  }, [q.data, config.viewType])

  // Globe Basemap
  const globeBasemap = new TileLayer({
    id: 'globe-basemap',
    data:  RASTER_TILES_LIGHT,
    minZoom: 0, maxZoom: 19, tileSize: 256, maxRequests: 8, pickable: false,
    loadOptions: { fetch: { mode: 'cors' as const } },
    renderSubLayers: (slProps: any) => {
      const { bbox:  { west, south, east, north } } = slProps.tile
      return new BitmapLayer({
        id: `bitmap-${slProps.tile.id}`,
        image: slProps.data,
        bounds: [west, south, east, north],
        opacity: 1
      })
    }
  })

  // Point Color
  function getPointColor(d: any): [number, number, number, number] {
    const s = Math.max(0, Math.min(1, Number(d.anom_score ?? d.cwa_score ?? d.zscore ?? 0)))
    return getAnomalyColorRGBA(s)
  }

  // Layers
  const layers = [
    ...(config.viewType === '3d' ? [globeBasemap] : []),
    ...(config.viewType === '2d' && config.heat ? [
      new HexagonLayer({
        id: 'hex-2d',
        data: points,
        getPosition: (d: any) => [
          Number(d.geo_lon ?? d.lon ?? d.lng),
          Number(d.geo_lat ?? d.lat)
        ],
        gpuAggregation: false,
        getElevationWeight: (d: any) => {
          if (config.metric === 'count') return 1
          const v = Number(d[config.valueField] ?? 0)
          return Number.isFinite(v) ? v : 0
        },
        elevationAggregation: config.metric === 'count' ? 'SUM' : (config.metric === 'avg' ? 'MEAN' : config.metric.toUpperCase()),
        extruded: true,
        radius: config.hexRadiusKm * 1000,
        elevationScale: 12,
        pickable: true,
        opacity: 0.6,
        colorAggregation: config.metric === 'count' ? 'SUM' : (config.metric === 'avg' ? 'MEAN' : config.metric.toUpperCase()),
        getColorWeight: (d: any) => {
          if (config.metric === 'count') return 1
          const v = Number(d[config.valueField] ?? 0)
          return Number.isFinite(v) ? v : 0
        }
      })
    ] : []),
    ...(config.routeMode && routesFromData.length > 0 ? [
      new ArcLayer({
        id: 'routes-arc',
        data: routesFromData,
        getSourcePosition: (d: any) => d.sourcePosition,
        getTargetPosition: (d: any) => d.targetPosition,
        getWidth: (d: any) => Math.min(12, 2 + Math.log1p(d.count) * 1.5),
        getSourceColor: (d: any) => {
          const score = d.avgScore ?? 0
          if (score >= 0.75) return [211, 47, 47, 180]
          if (score >= 0.5) return [245, 124, 0, 180]
          if (score >= 0.25) return [251, 192, 45, 180]
          return [41, 98, 255, 180]
        },
        getTargetColor: (d: any) => {
          const score = d.avgScore ?? 0
          if (score >= 0.75) return [123, 31, 162, 180]
          if (score >= 0.5) return [211, 47, 47, 180]
          if (score >= 0.25) return [245, 124, 0, 180]
          return [251, 192, 45, 180]
        },
        pickable: true,
        greatCircle: true
      }),
      new ScatterplotLayer({
        id:  'airports',
        data: Array.from(new Set([...routesFromData.map(r => r.o), ...routesFromData.map(r => r.d)]))
          .map((iata: string) => ({ iata, ...AIRPORTS[iata] }))
          .filter((a: any) => a && Number.isFinite(a.lat)),
        getPosition: (d:  any) => [d.lon, d.lat],
        radiusUnits: 'meters',
        getRadius: 6000,
        getFillColor: [60, 60, 60, 220],
        getLineColor: [255, 255, 255, 255],
        lineWidthUnits: 'pixels',
        getLineWidth: 2,
        pickable: true
      })
    ] : []),
    new ScatterplotLayer({
      id:  'scatter',
      data: config.localClustering ? clusteredPoints :  points,
      getPosition: config.localClustering
        ? (d: any) => [d.lon, d.lat]
        :  (d: any) => [Number(d.geo_lon ?? d.lon ?? d.lng), Number(d.geo_lat ?? d.lat)],
      getFillColor: config.localClustering
        ? (d: any) => getAnomalyColorRGBA(d.avgScore)
        : (d: any) => getPointColor(d),
      pickable: true,
      autoHighlight: true,
      radiusUnits: 'meters',
      getRadius: config.localClustering ? (d: any) => Math.min(15000, 3000 + d.points.length * 500) : (config.heat ? 3000 : 5000),
      getLineColor:  [255, 255, 255, 180],
      getLineWidth: config.localClustering ? 2 : 0.8,
      lineWidthUnits: 'pixels',
      highlightColor: [0, 0, 0, 40],
      onClick: (info: any) => {
        if (! info.object) return

        const obj = info.object
        if (config.localClustering && obj.points) {
          const events = obj.points.map((p: any) => ({
            ts: p.ts,
            user_id: p.user_id,
            event_type: p.event_type,
            anom_score: p.anom_score ?? p.cwa_score ?? p.zscore ?? 0,
            reasons: p.reasons || [],
            explain: p.explain || '',
            ip: p.ip,
            device_id: p.device_id,
            country: p.country,
            city: p.city
          }))

          setSelectedAnomaly({
            bucketTime: obj.points[0]?.ts || null,
            score: obj.avgScore,
            reasons: [],
            explain: `Cluster of ${obj.points.length} events`,
            events,
            thresholds: q.data?.thresholds
          })
          setDrawerOpen(true)
        } else {
          const score = obj.anom_score ?? obj.cwa_score ?? obj.zscore ?? 0
          if (score >= 0.25) {
            setSelectedAnomaly({
              bucketTime:  obj.ts || null,
              score,
              reasons: obj.reasons?.map((r: any) => r.code || r) || [],
              explain: obj.explain || '',
              events: [obj],
              thresholds:  q.data?.thresholds
            })
            setDrawerOpen(true)
          }
        }
      }
    })
  ]

  // Tooltip
  function fmt(v: any) { return v === null || v === undefined || v === '' ? 'n/a' : String(v) }

  function getTooltip(info: PickingInfo) {
    const { object, layer } = info
    if (!object) return null

    if (layer && layer.id === 'hex-2d') {
      const cell:  any = object
      const pts:  any[] = Array.isArray(cell.points) ? cell.points : []
      let maxAnom = 0
      for (const ev of pts) {
        const score = ev.anom_score ?? ev.cwa_score ?? 0
        if (score > maxAnom) maxAnom = score
      }
      const count = Number(cell.elevationValue ?? 0)
      const anomHtml = maxAnom >= 0.25
        ? `<br/><b style="color: ${SEVERITY_LEVELS[getAnomalySeverity(maxAnom)].color}">⚠ Max Anomaly: ${maxAnom.toFixed(3)}</b>`
        : ''
      return {
        html: `<div class="deck-tooltip"><b>Cluster</b><br/>events=${count}${anomHtml}</div>`
      }
    }

    if (config.routeMode && layer && layer.id === 'routes-arc') {
      const o:  any = object
      const originAirport = AIRPORTS[o.o]
      const destAirport = AIRPORTS[o.d]
      const anomalyHtml = o.avgScore >= 0.25
        ? `<br/><b style="color: ${SEVERITY_LEVELS[getAnomalySeverity(o.avgScore)].color}">⚠ Avg Anomaly: ${o.avgScore.toFixed(3)}</b>`
        : ''
      const userInfo = o.userCount > 0 ? `<br/><b>Users: </b> ${o.userCount}` : ''
      return {
        html: `<div class="deck-tooltip"><b>${o.o}</b> → <b>${o.d}</b><br/>
${originAirport?.city || o.o} → ${destAirport?.city || o.d}<br/>
trips=${o.count} • metric=${Number(o.value ?? 0).toFixed(2)}${userInfo}${anomalyHtml}</div>`
      }
    }

    if (layer && layer.id === 'airports') {
      const a: any = object
      return {
        html: `<div class="deck-tooltip"><b>${fmt(a.iata)}</b> — ${fmt(a.city)}<br/>lat=${fmt(a.lat)} lon=${fmt(a.lon)}</div>`
      }
    }

    const o: any = object

    if (config.localClustering && o.points) {
      const severity = getAnomalySeverity(o.avgScore)
      const severityColor = SEVERITY_LEVELS[severity].color
      const topUsers = o.points.slice(0, 3).map((p: any) => p.user_id).join(', ')
      return {
        html: `<div class="deck-tooltip"><b>Cluster (${o.points.length} events)</b><br/>
<span style="color: ${severityColor}; font-weight: bold;">⚠ Avg Anomaly: ${o.avgScore.toFixed(3)}</span><br/>
Top users: ${topUsers}</div>`
      }
    }

    const score = (o.cwa_score ?? o.anom_score ?? o.zscore)
    const scoreText = (score !== undefined && score !== null) ? Number(score).toFixed(3) : 'n/a'
    const severity = score != null ? getAnomalySeverity(score) : 'normal'
    const severityColor = SEVERITY_LEVELS[severity].color
    const city = o.city ?? o?.meta?.city
    const country = o.country ?? o?.meta?.country
    const origin = o.origin ?? o?.meta?.origin
    const dest = o.dest ?? o?.meta?.dest
    const reasons = o.reasons || []
    const reasonsHtml = reasons.length > 0
      ? `<br/><i>Reasons:</i> ${reasons.slice(0, 3).map((r: any) => r.code || r).join(', ')}`
      : ''

    return {
      html: `<div class="deck-tooltip"><b>${fmt(o.user_id)}</b> — ${fmt(o.event_type)}<br/>
${fmt(city)} / ${fmt(country)}<br/>ip=${fmt(o.ip)}<br/>
<span style="color: ${severityColor}; font-weight: bold;">⚠ Anomaly: ${scoreText}</span>${reasonsHtml}<br/>
origin=${fmt(origin)} dest=${fmt(dest)}</div>`
    }
  }

  // Handlers
  const handleThresholdPresetChange = useCallback((presetLabel: string) => {
    const preset = THRESHOLD_PRESETS.find(p => p.label === presetLabel)
    if (!preset) return
    setConfig((prev) => ({
      ...prev,
      zThreshold: preset.zThreshold,
      contamination: preset.contamination,
      thresholdPreset:  presetLabel,
    }))
    setIsTransitioning(true)
  }, [])

  const handleRunQuery = useCallback(() => {
    setIsTransitioning(true)
    q.refetch()
  }, [q])

  // Data Sources
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

  // ============================================================
  // Render
  // ============================================================

  return (
    <Stack sx={{ height: '100%', overflow: 'hidden' }} spacing={1}>
      {/* Loading Progress */}
      {(q.isFetching || isTransitioning) && (
        <LinearProgress sx={{ position: 'absolute', top: 0, left:  0, right: 0, zIndex: 20, height: 3 }} />
      )}

      {/* Controls Panel */}
      <Paper variant="outlined" sx={{ p:  1.5 }}>
        <Stack spacing={1.5}>
          {/* Primary Controls Row */}
          <Stack direction="row" spacing={1.5} alignItems="center" flexWrap="wrap" useFlexGap>
            <Badge
              badgeContent={anomalyStats.highAndCritical}
              color="error"
              invisible={anomalyStats.highAndCritical === 0}
            >
              <Chip
                icon={<MapIcon />}
                label="Map"
                color="primary"
                sx={{ fontWeight: 600 }}
              />
            </Badge>

            <FormControl size="small" sx={{ minWidth: 180 }}>
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

            <TextField
              select
              size="small"
              label="Bucket"
              value={config.bucket}
              onChange={(e) => updateConfig('bucket', e.target.value as Bucket)}
              sx={{ minWidth: 110 }}
            >
              {BUCKET_OPTIONS.map((opt) => (
                <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
              ))}
            </TextField>

            <TextField
              select
              size="small"
              label="Metric"
              value={config.metric}
              onChange={(e) => updateConfig('metric', e.target.value as Metric)}
              sx={{ minWidth: 110 }}
            >
              {METRIC_OPTIONS.map((opt) => (
                <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
              ))}
            </TextField>

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

            <TextField
              select
              size="small"
              label="Group By"
              value={config.groupField}
              onChange={(e) => {
                updateConfig('groupField', e.target.value)
                setSelectedGroups([])
              }}
              sx={{ minWidth: 140 }}
            >
              <MenuItem value="user_id">👤 User ID</MenuItem>
              <MenuItem value="event_type">📋 Event Type</MenuItem>
              <MenuItem value="device_id">📱 Device ID</MenuItem>
              <MenuItem value="ip">🌐 IP Address</MenuItem>
              <MenuItem value="country">🏳️ Country</MenuItem>
              <MenuItem value="city">🏙️ City</MenuItem>
              <MenuItem value="account_id">🏦 Account ID</MenuItem>
            </TextField>

            <TextField
              select
              size="small"
              label="Top N"
              value={config.topN}
              onChange={(e) => {
                const val = e.target.value
                updateConfig('topN', val === 'all' ? 9999 : parseInt(val as string))
              }}
              sx={{ width: 100 }}
            >
              <MenuItem value={5}>Top 5</MenuItem>
              <MenuItem value={10}>Top 10</MenuItem>
              <MenuItem value={20}>Top 20</MenuItem>
              <MenuItem value={50}>Top 50</MenuItem>
              <MenuItem value={100}>Top 100</MenuItem>
              <MenuItem value="all">All Users</MenuItem>
            </TextField>

            <Box sx={{ flex: 1 }} />

            <TextField
              select
              size="small"
              label="View"
              value={config.viewType}
              onChange={(e) => updateConfig('viewType', e.target.value as '2d' | '3d')}
              sx={{ minWidth: 100 }}
            >
              <MenuItem value="2d">2D</MenuItem>
              <MenuItem value="3d">3D</MenuItem>
            </TextField>

            <Tooltip title="Advanced Settings">
              <IconButton
                size="small"
                onClick={() => setShowAdvancedControls(!showAdvancedControls)}
                color={showAdvancedControls ? 'primary' : 'default'}
              >
                <TuneIcon />
              </IconButton>
            </Tooltip>

            <Button
              variant="contained"
              onClick={handleRunQuery}
              disabled={q.isFetching}
              startIcon={
                q.isFetching ? (
                  <CircularProgress size={16} color="inherit" />
                ) : (
                  <RefreshIcon />
                )
              }
              sx={{ minWidth:  100 }}
            >
              {q.isFetching ? 'Loading...' : 'Run'}
            </Button>
          </Stack>

          {/* Advanced Controls */}
          <Collapse in={showAdvancedControls}>
            <Divider sx={{ my: 1 }} />
            <Stack spacing={2}>
              <Stack direction="row" spacing={2} alignItems="center" flexWrap="wrap" useFlexGap>
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
                              borderColor: SEVERITY_LEVELS[sev as keyof typeof SEVERITY_LEVELS]?.color,
                              color: SEVERITY_LEVELS[sev as keyof typeof SEVERITY_LEVELS]?.color,
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

              <Stack direction="row" spacing={2} alignItems="center" flexWrap="wrap" useFlexGap>
                <FormControlLabel
                  control={
                    <Switch
                      checked={config.routeMode}
                      onChange={(_, v) => updateConfig('routeMode', v)}
                      size="small"
                    />
                  }
                  label="Show Routes"
                />

                {/* Dynamic Origin Field Selector */}
                <TextField
                  select
                  size="small"
                  label="Origin Field"
                  value={config.originField}
                  onChange={(e) => updateConfig('originField', e.target.value)}
                  sx={{ minWidth: 180 }}
                >
                  {originFieldOptions.map((field) => (
                    <MenuItem key={field} value={field}>
                      {field}
                    </MenuItem>
                  ))}
                </TextField>

                {/* Dynamic Dest Field Selector */}
                <TextField
                  select
                  size="small"
                  label="Dest Field"
                  value={config.destField}
                  onChange={(e) => updateConfig('destField', e.target.value)}
                  sx={{ minWidth: 180 }}
                >
                  {destFieldOptions.map((field) => (
                    <MenuItem key={field} value={field}>
                      {field}
                    </MenuItem>
                  ))}
                </TextField>

                {config.routeMode && (
                  <>
                    <Autocomplete
                      multiple
                      size="small"
                      options={availableCarriers}
                      value={config.selectedCarriers}
                      onChange={(_, v) => updateConfig('selectedCarriers', v)}
                      renderInput={(params) => (
                        <TextField
                          {...params}
                          label="Carriers"
                          placeholder={availableCarriers.length > 0 ? "Select..." : "Loading..."}
                          InputProps={{
                            ...params.InputProps,
                            startAdornment: (
                              <>
                                <FlightIcon sx={{ ml: 1, mr: -0.5, fontSize: 18, color: 'action.active' }} />
                                {params.InputProps.startAdornment}
                              </>
                            ),
                          }}
                        />
                      )}
                      sx={{ minWidth: 200 }}
                      limitTags={2}
                      ChipProps={{ size: 'small' }}
                      loading={carriersQuery.isLoading}
                    />

                    <TextField
                      select
                      size="small"
                      label="Route Metric"
                      value={config.routeMetric}
                      onChange={(e) => updateConfig('routeMetric', e.target.value as Metric)}
                      sx={{ width: 140 }}
                    >
                      <MenuItem value="count">Count</MenuItem>
                      <MenuItem value="avg">Avg (anom)</MenuItem>
                      <MenuItem value="max">Max (anom)</MenuItem>
                      <MenuItem value="sum">Sum (anom)</MenuItem>
                    </TextField>
                  </>
                )}
              </Stack>

              <Stack direction="row" spacing={2} alignItems="center" flexWrap="wrap" useFlexGap>
                <FormControlLabel
                  control={
                    <Switch
                      checked={config.heat}
                      onChange={(_, v) => updateConfig('heat', v)}
                      size="small"
                    />
                  }
                  label="Heat (hex)"
                />

                <FormControlLabel
                  control={
                    <Switch
                      checked={config.backendCluster}
                      onChange={(_, v) => updateConfig('backendCluster', v)}
                      size="small"
                    />
                  }
                  label="Backend clustering"
                />

                <FormControlLabel
                  control={
                    <Switch
                      checked={config.localClustering}
                      onChange={(_, v) => updateConfig('localClustering', v)}
                      size="small"
                    />
                  }
                  label="Local clustering"
                />

                {config.viewType === '2d' && (
                  <TextField
                    select
                    size="small"
                    label="Basemap"
                    value={config.basemap}
                    onChange={(e) => updateConfig('basemap', e.target.value)}
                    sx={{ minWidth:  160 }}
                  >
                    <MenuItem value={POSITRON}>Positron (light)</MenuItem>
                    <MenuItem value={DARKMATTER}>Dark Matter</MenuItem>
                  </TextField>
                )}

                {config.analyticsMode === 'simple' && (
                  <Box sx={{ width: 220 }}>
                    <Stack direction="row" spacing={1} alignItems="center">
                      <Typography variant="caption" color="text.secondary">
                        Z-Threshold:  <strong>{config.zThreshold.toFixed(1)}</strong>
                      </Typography>
                      <Tooltip title="Lower values = more sensitive">
                        <InfoOutlinedIcon sx={{ fontSize: 14, color: 'text.secondary' }} />
                      </Tooltip>
                    </Stack>
                    <Slider
                      size="small"
                      value={config.zThreshold}
                      onChange={(_, v) => {
                        updateConfig('zThreshold', v as number)
                        updateConfig('thresholdPreset', null)
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
                      }}
                    />
                  </Box>
                )}

                                {config.analyticsMode === 'advanced' && (
                  <Box sx={{ width: 220 }}>
                    <Stack direction="row" spacing={1} alignItems="center">
                      <Typography variant="caption" color="text.secondary">
                        Contamination: <strong>{(config.contamination * 100).toFixed(0)}%</strong>
                      </Typography>
                      <Tooltip title="Expected % of anomalies">
                        <InfoOutlinedIcon sx={{ fontSize:  14, color: 'text.secondary' }} />
                      </Tooltip>
                    </Stack>
                    <Slider
                      size="small"
                      value={config.contamination}
                      onChange={(_, v) => {
                        updateConfig('contamination', v as number)
                        updateConfig('thresholdPreset', null)
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

                {config.heat && (
                  <Box sx={{ width: 200 }}>
                    <Typography variant="caption" color="text.secondary">
                      Hex radius: {config.hexRadiusKm} km
                    </Typography>
                    <Slider
                      size="small"
                      value={config.hexRadiusKm}
                      onChange={(_, v) => updateConfig('hexRadiusKm', v as number)}
                      min={5}
                      max={200}
                      step={5}
                    />
                  </Box>
                )}

                {config.localClustering && (
                  <Box sx={{ width: 200 }}>
                    <Typography variant="caption" color="text.secondary">
                      Cluster radius: {config.clusterRadiusKm} km
                    </Typography>
                    <Slider
                      size="small"
                      value={config.clusterRadiusKm}
                      onChange={(_, v) => updateConfig('clusterRadiusKm', v as number)}
                      min={10}
                      max={200}
                      step={10}
                    />
                  </Box>
                )}
              </Stack>
            </Stack>
          </Collapse>
        </Stack>
      </Paper>

      {/* Anomaly Summary Alert - SHOWS ALL SEVERITIES INCLUDING CRITICAL */}
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
            {anomalyStats.low > 0 && (
              <Chip
                size="small"
                label={`${anomalyStats.low} Low`}
                sx={{ bgcolor: SEVERITY_LEVELS.low.color, color: '#000' }}
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

      {err && <Alert severity="error">{err}</Alert>}

      {/* Map Container */}
      <Box sx={{ flex: 1, minHeight: 300, position: 'relative' }}>
        <DeckGL
          views={config.viewType === '3d' ? [new (GlobeView as any)()] : [new MapView()]}
          controller={true}
          useDevicePixels={Math.min(2, (window as any).devicePixelRatio || 1)}
          viewState={viewState}
          onViewStateChange={({ viewState }) => setViewState(viewState)}
          layers={layers}
          parameters={config.viewType === '3d'
            ? { clearColor: [0.96, 0.98, 1, 1] as [number, number, number, number] }
            : { clearColor: [0, 0, 0, 0] as [number, number, number, number] }}
          getTooltip={getTooltip}
        >
          {config.viewType === '2d' && (
            <MapGL reuseMaps mapStyle={config.basemap} projection="mercator" antialias style={{ position: 'absolute', inset: 0 }} />
          )}
        </DeckGL>

        {/* Transitioning overlay */}
        {isTransitioning && (
          <Box
            sx={{
              position: 'absolute',
              top: 0,
              left: 0,
              right: 0,
              bottom:  0,
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
      {selectedAnomaly && (
        <AnomalyDetailDrawer
          open={drawerOpen}
          onClose={() => setDrawerOpen(false)}
          bucketTime={selectedAnomaly.bucketTime}
          score={selectedAnomaly.score}
          reasons={selectedAnomaly.reasons}
          explain={selectedAnomaly.explain}
          events={selectedAnomaly.events}
          thresholds={selectedAnomaly.thresholds}
        />
      )}
    </Stack>
  )
}