// path: ui-react/src/components/DataWorkbench/DataWorkbench.tsx
/**
 * DataWorkbench - Main container for data manipulation environment
 */
import React from 'react'
import {
  Box,
  Paper,
  Typography,
  Stack,
  Button,
  IconButton,
  Divider,
  Alert,
  CircularProgress,
  Snackbar,
  Drawer,
  Chip,
} from '@mui/material'
import {
  Add as AddIcon,
  PlayArrow as RunIcon,
  Save as SaveIcon,
  Refresh as RefreshIcon,
  Download as ExportIcon,
  Code as SqlIcon,
  History as HistoryIcon,
  Settings as SettingsIcon,
  Close as CloseIcon,
} from '@mui/icons-material'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import axios from 'axios'
import SourceSelector from './SourceSelector'
import PipelineBuilder from './PipelineBuilder'
import ResultsPreview from './ResultsPreview'
import SaveViewDialog from './SaveViewDialog'
import SQLPreview from './SQLPreview'

const API = import.meta.env.VITE_API_BASE || 'http://localhost:8000'

// ============================================================
// Types
// ============================================================

export interface PipelineStep {
  id: string
  type: string
  config: Record<string, any>
}

export interface DataSource {
  name: string
  type: 'table' | 'view'
  row_count?:  number
  description?: string
  columns?:  Record<string, any>
  view_id?: string
  is_materialized?: boolean
}

export interface QueryResult {
  data: any[]
  columns: string[]
  column_stats?:  Record<string, any>
  row_count:  number
  total_rows: number
}

export interface DerivedView {
  id: string
  name: string
  description: string
  pipeline:  PipelineStep[]
  source_tables: string[]
  is_materialized: boolean
  row_count?:  number
  created_at: string
  updated_at: string
}

// ============================================================
// API Functions
// ============================================================

async function fetchSources(): Promise<DataSource[]> {
  const { data } = await axios.get(`${API}/workbench/sources`)
  return data
}

async function fetchSourceInfo(name: string): Promise<DataSource & { columns: Record<string, any> }> {
  const { data } = await axios.get(`${API}/workbench/sources/${name}`)
  return data
}

async function executeQuery(pipeline: PipelineStep[], limit: number = 1000, offset: number = 0): Promise<QueryResult> {
  const { data } = await axios.post(`${API}/workbench/query`, {
    pipeline: pipeline.map(s => ({ type: s.type, config: s.config })),
    limit,
    offset,
  })
  return data
}

async function previewQuery(pipeline: PipelineStep[], limit:  number = 100): Promise<QueryResult & { column_stats: Record<string, any> }> {
  const { data } = await axios.post(`${API}/workbench/preview`, {
    pipeline: pipeline.map(s => ({ type: s.type, config: s.config })),
    limit,
  })
  return data
}

async function executeSQL(sql: string, limit: number = 1000): Promise<QueryResult> {
  const { data } = await axios.post(`${API}/workbench/sql`, { sql, limit })
  return data
}

async function saveView(view: {
  name: string
  description: string
  pipeline: PipelineStep[]
  is_materialized: boolean
  tags:  string[]
}): Promise<DerivedView> {
  const { data } = await axios.post(`${API}/workbench/views`, {
    ...view,
    pipeline: view.pipeline.map(s => ({ type: s.type, config: s.config })),
  })
  return data
}

async function fetchViews(): Promise<DerivedView[]> {
  const { data } = await axios.get(`${API}/workbench/views`)
  return data
}

async function deleteView(viewId: string): Promise<void> {
  await axios.delete(`${API}/workbench/views/${viewId}`)
}

async function materializeView(viewId: string): Promise<DerivedView> {
  const { data } = await axios.post(`${API}/workbench/views/${viewId}/materialize`)
  return data
}

// ============================================================
// Main Component
// ============================================================

export default function DataWorkbench() {
  const queryClient = useQueryClient()

  // State
  const [pipeline, setPipeline] = React.useState<PipelineStep[]>([])
  const [results, setResults] = React.useState<QueryResult | null>(null)
  const [columnStats, setColumnStats] = React.useState<Record<string, any>>({})
  const [selectedSource, setSelectedSource] = React.useState<string | null>(null)
  const [sourceColumns, setSourceColumns] = React.useState<string[]>([])
  const [isRunning, setIsRunning] = React.useState(false)
  const [error, setError] = React.useState<string | null>(null)
  const [saveDialogOpen, setSaveDialogOpen] = React.useState(false)
  const [sqlPreviewOpen, setSqlPreviewOpen] = React.useState(false)
  const [snackbar, setSnackbar] = React.useState<{ open: boolean; message: string; severity: 'success' | 'error' | 'info' }>({
    open:  false,
    message: '',
    severity: 'success',
  })

  // Queries
  const sourcesQuery = useQuery({
    queryKey: ['workbench-sources'],
    queryFn:  fetchSources,
  })

  const viewsQuery = useQuery({
    queryKey: ['workbench-views'],
    queryFn:  fetchViews,
  })

  // Get available columns from results
  const getAvailableColumns = React.useCallback((): string[] => {
    if (results?.columns) {
      return results.columns
    }
    return sourceColumns
  }, [results, sourceColumns])

  // Auto-run preview when pipeline changes
  const runPreview = React.useCallback(async () => {
    if (pipeline.length === 0) {
      setResults(null)
      setColumnStats({})
      return
    }

    // Must have at least a source step
    const hasSource = pipeline.some(s => s.type === 'source')
    if (!hasSource) {
      return
    }

    // Check if source has a table selected
    const sourceStep = pipeline.find(s => s.type === 'source')
    if (! sourceStep?.config?.table) {
      return
    }

    setIsRunning(true)
    setError(null)

    try {
      const result = await previewQuery(pipeline, 100)
      setResults(result)
      setColumnStats(result.column_stats || {})
    } catch (e:  any) {
      setError(e.response?.data?.detail || e.message || 'Query failed')
      setResults(null)
    } finally {
      setIsRunning(false)
    }
  }, [pipeline])

  // Debounced auto-preview
  React.useEffect(() => {
    const timer = setTimeout(() => {
      runPreview()
    }, 500)
    return () => clearTimeout(timer)
  }, [pipeline, runPreview])

  // Add source to pipeline
  const handleSelectSource = async (sourceName: string) => {
    setSelectedSource(sourceName)
    setError(null)

    // Fetch source columns
    try {
      const sourceInfo = await fetchSourceInfo(sourceName)
      if (sourceInfo.columns) {
        setSourceColumns(Object.keys(sourceInfo.columns))
      }
    } catch (e) {
      console.error('Failed to fetch source columns:', e)
      setSourceColumns([])
    }

    setPipeline([
      {
        id: `step-${Date.now()}`,
        type: 'source',
        config: { table: sourceName },
      },
    ])
  }

  // Add step to pipeline
  const handleAddStep = (type: string) => {
    const newStep:  PipelineStep = {
      id: `step-${Date.now()}`,
      type,
      config: {},
    }
    setPipeline([...pipeline, newStep])
  }

  // Update step in pipeline
  const handleUpdateStep = (stepId: string, config: Record<string, any>) => {
    setPipeline(pipeline.map(s =>
      s.id === stepId ? { ...s, config } :  s
    ))
  }

  // Remove step from pipeline
  const handleRemoveStep = (stepId: string) => {
    setPipeline(pipeline.filter(s => s.id !== stepId))
  }

  // Reorder steps
  const handleReorderSteps = (fromIndex: number, toIndex: number) => {
    const newPipeline = [...pipeline]
    const [removed] = newPipeline.splice(fromIndex, 1)
    newPipeline.splice(toIndex, 0, removed)
    setPipeline(newPipeline)
  }

  // Run full query
  const handleRunQuery = async () => {
    if (pipeline.length === 0) return

    setIsRunning(true)
    setError(null)

    try {
      const result = await executeQuery(pipeline, 1000, 0)
      setResults(result)
      setSnackbar({ open: true, message: `Query returned ${result.total_rows.toLocaleString()} rows`, severity: 'success' })
    } catch (e: any) {
      setError(e.response?.data?.detail || e.message || 'Query failed')
    } finally {
      setIsRunning(false)
    }
  }

  // Run SQL query
  const handleRunSQL = async (sql: string) => {
    setIsRunning(true)
    setError(null)

    try {
      const result = await executeSQL(sql, 1000)
      setResults(result)
      setColumnStats(result.column_stats || {})
      setSnackbar({ open: true, message: `Query returned ${result.total_rows.toLocaleString()} rows`, severity: 'success' })
    } catch (e: any) {
      setError(e.response?.data?.detail || e.message || 'SQL execution failed')
    } finally {
      setIsRunning(false)
    }
  }

  // Save as view
  const handleSaveView = async (viewData: { name: string; description: string; is_materialized: boolean; tags:  string[] }) => {
    try {
      await saveView({
        ...viewData,
        pipeline,
      })
      queryClient.invalidateQueries({ queryKey: ['workbench-sources'] })
      queryClient.invalidateQueries({ queryKey: ['workbench-views'] })
      setSnackbar({ open:  true, message: `View "${viewData.name}" saved successfully`, severity: 'success' })
      setSaveDialogOpen(false)
    } catch (e: any) {
      setSnackbar({ open: true, message: e.response?.data?.detail || 'Failed to save view', severity: 'error' })
    }
  }

  // Load view into pipeline
  const handleLoadView = async (view: DerivedView) => {
    // Load source columns for the view's source tables
    if (view.source_tables && view.source_tables.length > 0) {
      try {
        const sourceInfo = await fetchSourceInfo(view.source_tables[0])
        if (sourceInfo.columns) {
          setSourceColumns(Object.keys(sourceInfo.columns))
        }
        setSelectedSource(view.source_tables[0])
      } catch (e) {
        console.error('Failed to fetch source columns:', e)
      }
    }

    setPipeline(view.pipeline.map((s, i) => ({
      id: `step-${Date.now()}-${i}`,
      type: s.type,
      config: s.config,
    })))
    setSnackbar({ open: true, message: `Loaded view "${view.name}"`, severity: 'info' })
  }

  // Clear pipeline
  const handleClear = () => {
    setPipeline([])
    setResults(null)
    setColumnStats({})
    setSelectedSource(null)
    setSourceColumns([])
    setError(null)
  }

  // Get sources list
  const sources = sourcesQuery.data || []
  const views = viewsQuery.data || []

  // Memoize available columns
  const availableColumns = React.useMemo(() => getAvailableColumns(), [getAvailableColumns])

  return (
    <Box sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      {/* Header */}
      <Paper sx={{ p: 2, mb:  2 }}>
        <Stack direction="row" justifyContent="space-between" alignItems="center">
          <Box>
            <Typography variant="h5" gutterBottom>🔧 Data Workbench</Typography>
            <Typography variant="body2" color="text.secondary">
              Explore, transform, and correlate data from multiple sources
            </Typography>
          </Box>
          <Stack direction="row" spacing={1}>
            <Button
              variant="outlined"
              startIcon={<RefreshIcon />}
              onClick={() => sourcesQuery.refetch()}
              disabled={sourcesQuery.isFetching}
            >
              Refresh
            </Button>
            <Button
              variant="outlined"
              startIcon={<SqlIcon />}
              onClick={() => setSqlPreviewOpen(true)}
              disabled={pipeline.length === 0}
            >
              SQL Editor
            </Button>
            <Button
              variant="outlined"
              startIcon={<SaveIcon />}
              onClick={() => setSaveDialogOpen(true)}
              disabled={pipeline.length === 0}
            >
              Save View
            </Button>
            <Button
              variant="contained"
              startIcon={isRunning ? <CircularProgress size={18} /> : <RunIcon />}
              onClick={handleRunQuery}
              disabled={pipeline.length === 0 || isRunning}
            >
              Run Query
            </Button>
          </Stack>
        </Stack>
      </Paper>

      {/* Error Alert */}
      {error && (
        <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError(null)}>
          {error}
        </Alert>
      )}

      {/* Main Content */}
      <Box sx={{ flex: 1, display: 'flex', gap: 2, overflow: 'hidden' }}>
        {/* Left Panel - Source Selector */}
        <Paper sx={{ width: 280, p: 2, overflow: 'auto' }}>
          <SourceSelector
            sources={sources}
            views={views}
            selectedSource={selectedSource}
            onSelectSource={handleSelectSource}
            onLoadView={handleLoadView}
            onDeleteView={async (viewId) => {
              await deleteView(viewId)
              queryClient.invalidateQueries({ queryKey: ['workbench-sources'] })
              queryClient.invalidateQueries({ queryKey: ['workbench-views'] })
            }}
            onMaterializeView={async (viewId) => {
              await materializeView(viewId)
              queryClient.invalidateQueries({ queryKey: ['workbench-views'] })
              setSnackbar({ open: true, message: 'View materialized', severity: 'success' })
            }}
            isLoading={sourcesQuery.isLoading}
          />
        </Paper>

        {/* Center Panel - Pipeline Builder & Results */}
        <Box sx={{ flex: 1, display: 'flex', flexDirection: 'column', overflow:  'hidden' }}>
          {/* Pipeline Builder */}
          <Paper sx={{ p: 2, mb: 2, maxHeight: '40%', overflow: 'auto' }}>
            <PipelineBuilder
              pipeline={pipeline}
              availableColumns={availableColumns}
              sourceColumns={sourceColumns}
              sources={sources}
              onAddStep={handleAddStep}
              onUpdateStep={handleUpdateStep}
              onRemoveStep={handleRemoveStep}
              onReorderSteps={handleReorderSteps}
              onClear={handleClear}
            />
          </Paper>

          {/* Results Preview */}
          <Paper sx={{ flex: 1, p: 2, overflow: 'hidden', display: 'flex', flexDirection:  'column' }}>
            <ResultsPreview
              results={results}
              columnStats={columnStats}
              isLoading={isRunning}
              pipeline={pipeline}
            />
          </Paper>
        </Box>
      </Box>

      {/* Save View Dialog */}
      <SaveViewDialog
        open={saveDialogOpen}
        onClose={() => setSaveDialogOpen(false)}
        onSave={handleSaveView}
        pipeline={pipeline}
      />

      {/* SQL Editor Drawer */}
      <Drawer
        anchor="right"
        open={sqlPreviewOpen}
        onClose={() => setSqlPreviewOpen(false)}
        PaperProps={{ sx: { width: { xs: '100%', md: 600 } } }}
      >
        <Box sx={{ p: 2, height: '100%', display: 'flex', flexDirection: 'column' }}>
          <Stack direction="row" justifyContent="space-between" alignItems="center" mb={2}>
            <Typography variant="h6">SQL Editor</Typography>
            <IconButton onClick={() => setSqlPreviewOpen(false)}>
              <CloseIcon />
            </IconButton>
          </Stack>
          <Box sx={{ flex: 1, overflow: 'hidden' }}>
            <SQLPreview
              pipeline={pipeline}
              onRunSQL={handleRunSQL}
              isRunning={isRunning}
            />
          </Box>
        </Box>
      </Drawer>

      {/* Snackbar */}
      <Snackbar
        open={snackbar.open}
        autoHideDuration={4000}
        onClose={() => setSnackbar({ ...snackbar, open: false })}
        anchorOrigin={{ vertical: 'bottom', horizontal:  'right' }}
      >
        <Alert
          onClose={() => setSnackbar({ ...snackbar, open: false })}
          severity={snackbar.severity}
          variant="filled"
        >
          {snackbar.message}
        </Alert>
      </Snackbar>
    </Box>
  )
}