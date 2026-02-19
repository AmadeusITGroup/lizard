// path: ui-react/src/components/UploadDrawer.tsx
/**
 * UploadDrawer - File upload with full mapping editing capabilities
 * Supports template matching, auto-suggestion, manual editing, and save as template.
 * NEW: Cloud data source selection when in cloud mode with connectivity.
 */
import React from 'react'
import {
  Drawer,
  Box,
  Stack,
  Typography,
  IconButton,
  Divider,
  Button,
  MenuItem,
  TextField,
  Chip,
  Alert,
  CircularProgress,
  LinearProgress,
  Paper,
  Stepper,
  Step,
  StepLabel,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Tooltip,
  Tabs,
  Tab,
  ToggleButton,
  ToggleButtonGroup,
} from '@mui/material'
import CloseIcon from '@mui/icons-material/Close'
import CloudUploadIcon from '@mui/icons-material/CloudUpload'
import CloudIcon from '@mui/icons-material/Cloud'
import ComputerIcon from '@mui/icons-material/Computer'
import CheckCircleIcon from '@mui/icons-material/CheckCircle'
import AutoFixHighIcon from '@mui/icons-material/AutoFixHigh'
import SaveIcon from '@mui/icons-material/Save'
import ArrowBackIcon from '@mui/icons-material/ArrowBack'
import ArrowForwardIcon from '@mui/icons-material/ArrowForward'
import InfoIcon from '@mui/icons-material/Info'
import SchemaMapper from './SchemaMapper'
import CloudSourcePicker, { type CloudSourceSelection } from './CloudSourcePicker'
import { useCloud } from '../context/CloudContext'
import { useFilters } from '../context/FiltersContext'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import axios from 'axios'

const API = import.meta.env.VITE_API_BASE || 'http://localhost:8000'

// ============================================================
// Types
// ============================================================

interface MappingTemplate {
  id: string
  name: string
  description: string
  mapping: Record<string, string>
  expressions: Record<string, any>
  category: string
  use_count: number
  is_builtin: boolean
}

interface MatchResult {
  template_id: string
  template_name: string
  category: string
  score: number
  columns_matched: string[]
  columns_missing: string[]
}

interface SuggestionResult {
  filename: string
  total_rows: number
  columns: string[]
  suggested_mapping: Record<string, string>
  suggested_expressions: Record<string, any>
  candidates: Record<string, Array<{ column: string; score: number }>>
  column_analysis: Record<string, any>
  engine_used: string
}

// ============================================================
// API Functions
// ============================================================

async function fetchTemplates(): Promise<MappingTemplate[]> {
  const { data } = await axios.get(`${API}/mapping/templates`)
  return data
}

async function matchTemplates(file: File): Promise<MatchResult[]> {
  const formData = new FormData()
  formData.append('file', file)
  const { data } = await axios.post(`${API}/mapping/templates/match`, formData)
  return data
}

async function suggestMapping(file: File, engine: string): Promise<SuggestionResult> {
  const formData = new FormData()
  formData.append('file', file)
  const { data } = await axios.post(`${API}/mapping/templates/suggest`, formData, {
    params: { engine },
  })
  return data
}

/** NEW: Analyze a cloud dataset (preview → get columns → suggest mapping) */
async function analyzeCloudSource(
  connectionName: string,
  connectionType: 'storage' | 'dbfs',
  path: string,
  engine: string,
): Promise<SuggestionResult> {
  const { data } = await axios.post(`${API}/cloud/analyze`, null, {
    params: {
      connection_name: connectionName,
      connection_type: connectionType,
      path,
      engine,
    },
  })
  return data
}

async function previewMapping(file: File, mapping: Record<string, any>): Promise<any> {
  const formData = new FormData()
  formData.append('file', file)
  formData.append('mapping_json', JSON.stringify(mapping))
  const { data } = await axios.post(`${API}/mapping/preview`, formData)
  return data
}

async function recordTemplateUse(id: string): Promise<void> {
  await axios.post(`${API}/mapping/templates/${id}/use`)
}

// ============================================================
// Props
// ============================================================

type Props = {
  open: boolean
  onClose: () => void
}

// ============================================================
// Main Component
// ============================================================

export default function UploadDrawer({ open, onClose }: Props) {
  const { startISO, endISO } = useFilters()
  const queryClient = useQueryClient()

  // NEW: Cloud context for mode & connectivity check
  const { config, isCloudMode } = useCloud()
  const hasCloudConnections =
    (config.storage_connections?.length || 0) > 0 ||
    (config.databricks_connections?.length || 0) > 0
  const cloudAvailable = isCloudMode && hasCloudConnections

  // Wizard State
  const [activeStep, setActiveStep] = React.useState(0)

  // NEW: Source mode — 'local' (file upload) or 'cloud' (cloud picker)
  const [sourceMode, setSourceMode] = React.useState<'local' | 'cloud'>('local')

  // File State (local mode)
  const [file, setFile] = React.useState<File | null>(null)
  const [engine, setEngine] = React.useState<'heuristic' | 'openai' | 'ollama'>('heuristic')
  const [sourceName, setSourceName] = React.useState('uploaded_file')

  // NEW: Cloud source state
  const [cloudSelection, setCloudSelection] = React.useState<CloudSourceSelection | null>(null)

  // Analysis State
  const [suggestion, setSuggestion] = React.useState<SuggestionResult | null>(null)
  const [matches, setMatches] = React.useState<MatchResult[]>([])
  const [selectedTemplateId, setSelectedTemplateId] = React.useState<string | null>(null)
  const [previewData, setPreviewData] = React.useState<any>(null)

  // Mapping State
  const [mapping, setMapping] = React.useState<Record<string, any>>({})

  // UI State
  const [busy, setBusy] = React.useState(false)
  const [analyzing, setAnalyzing] = React.useState(false)
  const [result, setResult] = React.useState<{
    ingested: number
    rejected?: number
    source_name?: string
    mapping_used?: Record<string, any>
  } | null>(null)
  const [err, setErr] = React.useState<string | null>(null)
  const [mappingTab, setMappingTab] = React.useState(0)

  // Fetch templates
  const templatesQuery = useQuery({
    queryKey: ['mapping-templates'],
    queryFn: fetchTemplates,
    enabled: open,
  })

  // Reset when drawer closes
  React.useEffect(() => {
    if (!open) {
      setActiveStep(0)
      setSourceMode('local')
      setFile(null)
      setCloudSelection(null)
      setSuggestion(null)
      setMatches([])
      setSelectedTemplateId(null)
      setPreviewData(null)
      setMapping({})
      setResult(null)
      setErr(null)
      setSourceName('uploaded_file')
      setMappingTab(0)
    }
  }, [open])

  // Auto-set source name from file (local mode)
  React.useEffect(() => {
    if (file && sourceMode === 'local') {
      const baseName = file.name.replace(/\.[^/.]+$/, '').replace(/[^a-zA-Z0-9_]/g, '_')
      setSourceName(baseName)
    }
  }, [file, sourceMode])

  // NEW: Auto-set source name from cloud selection
  React.useEffect(() => {
    if (cloudSelection && sourceMode === 'cloud') {
      const baseName = cloudSelection.fileName
        .replace(/\.[^/.]+$/, '')
        .replace(/[^a-zA-Z0-9_]/g, '_')
      setSourceName(baseName)
    }
  }, [cloudSelection, sourceMode])

  // When template is selected, apply its mapping
  React.useEffect(() => {
    if (selectedTemplateId && templatesQuery.data) {
      const template = templatesQuery.data.find(t => t.id === selectedTemplateId)
      if (template) {
        setMapping({
          ...template.mapping,
          __expr__: template.expressions || {},
        })
      }
    }
  }, [selectedTemplateId, templatesQuery.data])

  // ============================================================
  // Handlers
  // ============================================================

  /** Analyze a local file (existing logic) */
  const analyzeLocalFile = async () => {
    if (!file) return
    setAnalyzing(true)
    setErr(null)

    try {
      const [suggestionResult, matchResults] = await Promise.all([
        suggestMapping(file, engine),
        matchTemplates(file),
      ])

      setSuggestion(suggestionResult)
      setMatches(matchResults)

      if (matchResults.length > 0 && matchResults[0].score >= 0.8) {
        setSelectedTemplateId(matchResults[0].template_id)
      } else {
        setSelectedTemplateId(null)
        setMapping({
          ...suggestionResult.suggested_mapping,
          __expr__: suggestionResult.suggested_expressions || {},
        })
      }

      setActiveStep(1)
    } catch (e: any) {
      setErr(e?.response?.data?.detail || e?.message || 'Analysis failed')
    } finally {
      setAnalyzing(false)
    }
  }

  /** NEW: Analyze a cloud source */
  const analyzeCloudFile = async () => {
    if (!cloudSelection) return
    setAnalyzing(true)
    setErr(null)

    try {
      const suggestionResult = await analyzeCloudSource(
        cloudSelection.connectionName,
        cloudSelection.connectionType,
        cloudSelection.path,
        engine,
      )

      setSuggestion(suggestionResult)
      // Template matching is not available for cloud sources (no File object),
      // so we rely on auto-suggestion only.
      setMatches([])
      setSelectedTemplateId(null)
      setMapping({
        ...suggestionResult.suggested_mapping,
        __expr__: suggestionResult.suggested_expressions || {},
      })

      setActiveStep(1)
    } catch (e: any) {
      setErr(e?.response?.data?.detail || e?.message || 'Cloud analysis failed')
    } finally {
      setAnalyzing(false)
    }
  }

  /** Unified analyze dispatcher */
  const analyzeFile = () => {
    if (sourceMode === 'cloud') return analyzeCloudFile()
    return analyzeLocalFile()
  }

  const refreshPreview = async () => {
    if (sourceMode === 'local' && (!file || !mapping)) return
    if (sourceMode === 'cloud' && !cloudSelection) return
    setBusy(true)
    try {
      if (sourceMode === 'local' && file) {
        const preview = await previewMapping(file, mapping)
        setPreviewData(preview)
      }
      // For cloud sources, preview is part of the suggestion already
    } catch (e: any) {
      console.warn('Preview failed:', e)
    } finally {
      setBusy(false)
    }
  }

  /** Import data — local file or cloud source */
  const doImport = async () => {
    setBusy(true)
    setErr(null)

    try {
      // Record template usage if using a template
      if (selectedTemplateId) {
        try {
          await recordTemplateUse(selectedTemplateId)
        } catch (e) {
          console.warn('Failed to record template usage:', e)
        }
      }

      let data: any

      if (sourceMode === 'local' && file) {
        // ---- Existing local file import ----
        const formData = new FormData()
        formData.append('file', file)
        formData.append('source_name', sourceName)
        formData.append('mapping_json', JSON.stringify(mapping))
        formData.append('engine_name', engine)
        if (selectedTemplateId) {
          formData.append('template_id', selectedTemplateId)
        }
        formData.append('validate', 'true')

        const res = await axios.post(`${API}/upload/events`, formData, {
          headers: { 'Content-Type': 'multipart/form-data' },
        })
        data = res.data
      } else if (sourceMode === 'cloud' && cloudSelection) {
        // ---- NEW: Cloud source import ----
        const res = await axios.post(`${API}/cloud/ingest`, {
          connection_name: cloudSelection.connectionName,
          connection_type: cloudSelection.connectionType,
          path: cloudSelection.path,
          source_name: sourceName,
          mapping_json: mapping,
          template_id: selectedTemplateId || undefined,
        })
        data = res.data
      } else {
        throw new Error('No source selected')
      }

      setResult({
        ingested: data.ingested,
        rejected: data.rejected || 0,
        source_name: data.source_name || sourceName,
        mapping_used: data.mapping_used,
      })

      setActiveStep(2)

      // Refresh related queries
      queryClient.invalidateQueries({ queryKey: ['ingestion-logs'] })
      queryClient.invalidateQueries({ queryKey: ['mapping-templates'] })
    } catch (e: any) {
      setErr(e?.response?.data?.detail || e?.message || 'Import failed')
    } finally {
      setBusy(false)
    }
  }

  const saveAsTemplate = async () => {
    const name = prompt('Enter template name:')
    if (!name?.trim()) return

    try {
      const expressions = (mapping.__expr__ as Record<string, any>) || {}
      const mappingWithoutExpr = Object.fromEntries(
        Object.entries(mapping).filter(([k]) => k !== '__expr__')
      )

      await axios.post(`${API}/mapping/templates`, {
        name: name.trim(),
        description: `Created from ${sourceMode === 'cloud' ? cloudSelection?.fileName : file?.name || 'upload'} on ${new Date().toLocaleDateString()}`,
        category: 'general',
        tags: ['user-created'],
        mapping: mappingWithoutExpr,
        expressions,
        sample_columns: suggestion?.columns || [],
        source_type: sourceMode === 'cloud' ? 'cloud' : 'csv',
        validation_rules: [],
      })

      queryClient.invalidateQueries({ queryKey: ['mapping-templates'] })
      alert('Template saved successfully!')
    } catch (e: any) {
      alert('Failed to save template: ' + (e.response?.data?.detail || e.message))
    }
  }

  const resetWizard = () => {
    setActiveStep(0)
    setSourceMode('local')
    setFile(null)
    setCloudSelection(null)
    setSuggestion(null)
    setMatches([])
    setSelectedTemplateId(null)
    setPreviewData(null)
    setMapping({})
    setResult(null)
    setErr(null)
    setSourceName('uploaded_file')
  }

  const applyTemplate = (templateId: string) => {
    setSelectedTemplateId(templateId)
  }

  const useSuggestion = () => {
    setSelectedTemplateId(null)
    if (suggestion) {
      setMapping({
        ...suggestion.suggested_mapping,
        __expr__: suggestion.suggested_expressions || {},
      })
    }
  }

  // ============================================================
  // Render Helpers
  // ============================================================

  const getMappedFieldCount = () => {
    return Object.keys(mapping).filter(k => !k.startsWith('_') && mapping[k]).length
  }

  const getExpressionCount = () => {
    const expr = (mapping.__expr__ as Record<string, any>) || {}
    return Object.keys(expr).length
  }

  /** Whether Step 1 has a valid source ready to analyze */
  const hasSourceReady =
    sourceMode === 'local' ? !!file : !!cloudSelection

  /** Display name of the selected source */
  const sourceDisplayName =
    sourceMode === 'cloud'
      ? cloudSelection?.fileName || 'Cloud Source'
      : file?.name || 'Local File'

  const steps = ['Select Source', 'Configure Mapping', 'Complete']

  // ============================================================
  // Render
  // ============================================================

  return (
    <Drawer
      anchor="right"
      open={open}
      onClose={onClose}
      PaperProps={{ sx: { width: { xs: '100%', md: 950 } } }}
    >
      <Box sx={{ p: 2, height: '100%', display: 'flex', flexDirection: 'column' }}>
        {/* Header */}
        <Stack direction="row" justifyContent="space-between" alignItems="center">
          <Stack direction="row" spacing={1} alignItems="center">
            {sourceMode === 'cloud' ? <CloudIcon color="primary" /> : <CloudUploadIcon color="primary" />}
            <Typography variant="h6" fontWeight={800}>
              {sourceMode === 'cloud' ? 'Import from Cloud' : 'Upload & Map Data'}
            </Typography>
            {sourceMode === 'cloud' && (
              <Chip label="Cloud" size="small" color="info" />
            )}
          </Stack>
          <IconButton onClick={onClose}><CloseIcon /></IconButton>
        </Stack>

        <Divider sx={{ my: 2 }} />

        {/* Stepper */}
        <Stepper activeStep={activeStep} sx={{ mb: 3 }}>
          {steps.map(label => (
            <Step key={label}>
              <StepLabel>{label}</StepLabel>
            </Step>
          ))}
        </Stepper>

        {/* Error Alert */}
        {err && (
          <Alert severity="error" sx={{ mb: 2 }} onClose={() => setErr(null)}>
            {err}
          </Alert>
        )}

        {/* Content Area */}
        <Box sx={{ flex: 1, overflow: 'auto' }}>

          {/* ============================================================ */}
          {/* STEP 1: Select Source                                        */}
          {/* ============================================================ */}
          {activeStep === 0 && (
            <Stack spacing={3}>
              {/* NEW: Source Mode Toggle — only shown when cloud is available */}
              {cloudAvailable && (
                <Box sx={{ display: 'flex', justifyContent: 'center' }}>
                  <ToggleButtonGroup
                    value={sourceMode}
                    exclusive
                    onChange={(_, val) => {
                      if (val) {
                        setSourceMode(val)
                        setFile(null)
                        setCloudSelection(null)
                        setErr(null)
                      }
                    }}
                    size="small"
                  >
                    <ToggleButton value="local">
                      <ComputerIcon sx={{ mr: 0.5 }} fontSize="small" />
                      Local File
                    </ToggleButton>
                    <ToggleButton value="cloud">
                      <CloudIcon sx={{ mr: 0.5 }} fontSize="small" />
                      Cloud Source
                    </ToggleButton>
                  </ToggleButtonGroup>
                </Box>
              )}

              {/* ---- LOCAL FILE MODE (existing) ---- */}
              {sourceMode === 'local' && (
                <Stack spacing={3} alignItems="center" justifyContent="center" sx={{ py: 2 }}>
                  <Paper
                    variant="outlined"
                    sx={{
                      p: 4,
                      textAlign: 'center',
                      borderStyle: 'dashed',
                      borderWidth: 2,
                      bgcolor: file ? 'success.50' : 'action.hover',
                      borderColor: file ? 'success.main' : 'divider',
                      cursor: 'pointer',
                      '&:hover': { borderColor: 'primary.main', bgcolor: 'action.selected' },
                      minWidth: 400,
                      transition: 'all 0.2s',
                    }}
                    component="label"
                  >
                    <CloudUploadIcon sx={{ fontSize: 48, color: file ? 'success.main' : 'primary.main', mb: 2 }} />
                    <Typography variant="h6" gutterBottom>
                      {file ? file.name : 'Choose a file to import'}
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      Supports CSV, JSON, and Parquet formats
                    </Typography>
                    {file && (
                      <Chip
                        label={`${(file.size / 1024).toFixed(1)} KB`}
                        size="small"
                        sx={{ mt: 1 }}
                      />
                    )}
                    <input
                      type="file"
                      hidden
                      accept=".csv,.json,.parquet"
                      onChange={(e) => {
                        const selectedFile = e.target.files?.[0] || null
                        setFile(selectedFile)
                        setErr(null)
                      }}
                    />
                  </Paper>
                </Stack>
              )}

              {/* ---- NEW: CLOUD SOURCE MODE ---- */}
              {sourceMode === 'cloud' && (
                <Box sx={{ py: 1 }}>
                  <CloudSourcePicker
                    selection={cloudSelection}
                    onSelect={(sel) => {
                      setCloudSelection(sel)
                      setErr(null)
                    }}
                  />
                </Box>
              )}

              {/* Options — shown once a source is ready (local or cloud) */}
              {hasSourceReady && (
                <Stack spacing={2} sx={{ width: '100%', maxWidth: 450, mx: 'auto' }}>
                  <TextField
                    select
                    size="small"
                    label="Mapping Engine"
                    value={engine}
                    onChange={(e) => setEngine(e.target.value as any)}
                    fullWidth
                    helperText="Choose how to auto-detect field mappings"
                  >
                    <MenuItem value="heuristic">Heuristic (Fast, Default)</MenuItem>
                    <MenuItem value="openai">OpenAI (If Configured)</MenuItem>
                    <MenuItem value="ollama">Ollama (Local LLM)</MenuItem>
                  </TextField>

                  <Button
                    variant="contained"
                    size="large"
                    onClick={analyzeFile}
                    disabled={analyzing}
                    startIcon={analyzing ? <CircularProgress size={20} /> : <AutoFixHighIcon />}
                    fullWidth
                  >
                    {analyzing ? 'Analyzing...' : 'Analyze & Continue'}
                  </Button>
                </Stack>
              )}
            </Stack>
          )}

          {/* ============================================================ */}
          {/* STEP 2: Configure Mapping (same for both modes)              */}
          {/* ============================================================ */}
          {activeStep === 1 && suggestion && (
            <Stack spacing={2}>
              {/* File/Source Summary */}
              <Paper variant="outlined" sx={{ p: 2 }}>
                <Stack direction="row" spacing={4} flexWrap="wrap" useFlexGap>
                  <Box>
                    <Typography variant="caption" color="text.secondary">
                      {sourceMode === 'cloud' ? 'Source' : 'File'}
                    </Typography>
                    <Typography variant="body2" fontWeight={500}>{sourceDisplayName}</Typography>
                  </Box>
                  {sourceMode === 'cloud' && cloudSelection && (
                    <Box>
                      <Typography variant="caption" color="text.secondary">Connection</Typography>
                      <Typography variant="body2" fontWeight={500}>
                        {cloudSelection.connectionName} ({cloudSelection.connectionType})
                      </Typography>
                    </Box>
                  )}
                  <Box>
                    <Typography variant="caption" color="text.secondary">Rows</Typography>
                    <Typography variant="body2" fontWeight={500}>
                      {suggestion.total_rows?.toLocaleString() || 'Unknown'}
                    </Typography>
                  </Box>
                  <Box>
                    <Typography variant="caption" color="text.secondary">Columns</Typography>
                    <Typography variant="body2" fontWeight={500}>{suggestion.columns?.length || 0}</Typography>
                  </Box>
                  <Box>
                    <Typography variant="caption" color="text.secondary">Mapped</Typography>
                    <Typography variant="body2" fontWeight={500} color="primary.main">
                      {getMappedFieldCount()} fields
                    </Typography>
                  </Box>
                  <Box>
                    <Typography variant="caption" color="text.secondary">Transforms</Typography>
                    <Typography variant="body2" fontWeight={500} color="info.main">
                      {getExpressionCount()}
                    </Typography>
                  </Box>
                </Stack>
              </Paper>

              {/* Template Matching (local mode only — requires File object) */}
              {matches.length > 0 && (
                <Paper variant="outlined" sx={{ p: 2 }}>
                  <Typography variant="subtitle2" gutterBottom>
                    Matching Templates
                  </Typography>
                  <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
                    {matches.slice(0, 5).map(match => (
                      <Tooltip
                        key={match.template_id}
                        title={`${match.columns_matched.length} columns matched, ${match.columns_missing.length} missing`}
                      >
                        <Chip
                          label={`${match.template_name} (${(match.score * 100).toFixed(0)}%)`}
                          onClick={() => applyTemplate(match.template_id)}
                          color={selectedTemplateId === match.template_id ? 'primary' : 'default'}
                          variant={selectedTemplateId === match.template_id ? 'filled' : 'outlined'}
                        />
                      </Tooltip>
                    ))}
                    <Chip
                      label="Use Auto-Suggestion"
                      onClick={useSuggestion}
                      color={!selectedTemplateId ? 'primary' : 'default'}
                      variant={!selectedTemplateId ? 'filled' : 'outlined'}
                      icon={<AutoFixHighIcon />}
                    />
                  </Stack>
                </Paper>
              )}

              {/* Source Name */}
              <TextField
                size="small"
                label="Source Name"
                value={sourceName}
                onChange={(e) => setSourceName(e.target.value.replace(/[^a-zA-Z0-9_]/g, '_'))}
                helperText="This identifier will be used in visualizations to filter by data source"
                fullWidth
              />

              {/* Mapping Editor Tabs */}
              <Paper variant="outlined">
                <Tabs value={mappingTab} onChange={(_, v) => setMappingTab(v)}>
                  <Tab label="Field Mapping" />
                  <Tab label="Source Columns" />
                  <Tab label="Mapping JSON" />
                </Tabs>
                <Box sx={{ p: 2 }}>
                  {mappingTab === 0 && (
                    <SchemaMapper
                      sourceColumns={suggestion.columns || []}
                      columnAnalysis={suggestion.column_analysis}
                      mapping={mapping}
                      onChange={setMapping}
                      suggestedMapping={suggestion.suggested_mapping}
                      suggestedExpressions={suggestion.suggested_expressions}
                      candidates={suggestion.candidates}
                      compact={false}
                    />
                  )}

                  {mappingTab === 1 && (
                    <Box>
                      <Typography variant="body2" color="text.secondary" gutterBottom>
                        Available source columns ({suggestion.columns?.length || 0})
                      </Typography>
                      <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap sx={{ mt: 1 }}>
                        {suggestion.columns?.map(col => {
                          const analysis = suggestion.column_analysis?.[col]
                          const isUsed = Object.values(mapping).includes(col)
                          return (
                            <Tooltip
                              key={col}
                              title={analysis
                                ? `Type: ${analysis.detected_type}, Unique: ${analysis.unique_count}, Null: ${analysis.null_percent?.toFixed(1)}%`
                                : col
                              }
                            >
                              <Chip
                                label={col}
                                size="small"
                                variant={isUsed ? 'filled' : 'outlined'}
                                color={isUsed ? 'primary' : 'default'}
                              />
                            </Tooltip>
                          )
                        })}
                      </Stack>
                    </Box>
                  )}

                  {mappingTab === 2 && (
                    <Box>
                      <Typography variant="body2" color="text.secondary" gutterBottom>
                        Raw mapping configuration (JSON)
                      </Typography>
                      <TextField
                        multiline
                        rows={12}
                        fullWidth
                        value={JSON.stringify(mapping, null, 2)}
                        onChange={(e) => {
                          try {
                            setMapping(JSON.parse(e.target.value))
                          } catch {
                            // Invalid JSON, ignore
                          }
                        }}
                        sx={{ fontFamily: 'monospace', fontSize: 12 }}
                      />
                    </Box>
                  )}
                </Box>
              </Paper>

              {/* Actions */}
              <Stack direction="row" spacing={2} justifyContent="space-between">
                <Button
                  startIcon={<ArrowBackIcon />}
                  onClick={() => setActiveStep(0)}
                >
                  Back
                </Button>
                <Stack direction="row" spacing={1}>
                  <Button
                    variant="outlined"
                    startIcon={<SaveIcon />}
                    onClick={saveAsTemplate}
                  >
                    Save as Template
                  </Button>
                  <Button
                    variant="contained"
                    endIcon={busy ? <CircularProgress size={18} /> : <ArrowForwardIcon />}
                    onClick={doImport}
                    disabled={busy || getMappedFieldCount() === 0}
                  >
                    {busy ? 'Importing...' : 'Import Data'}
                  </Button>
                </Stack>
              </Stack>
            </Stack>
          )}

          {/* ============================================================ */}
          {/* STEP 3: Complete                                             */}
          {/* ============================================================ */}
          {activeStep === 2 && result && (
            <Stack spacing={3} alignItems="center" justifyContent="center" sx={{ py: 4 }}>
              <CheckCircleIcon sx={{ fontSize: 72, color: 'success.main' }} />
              <Typography variant="h4" color="success.main" fontWeight={600}>
                Import Successful!
              </Typography>

              <Paper variant="outlined" sx={{ p: 3, minWidth: 350 }}>
                <Stack spacing={2}>
                  <Box>
                    <Typography variant="caption" color="text.secondary">Rows Imported</Typography>
                    <Typography variant="h3" color="success.main" fontWeight={700}>
                      {result.ingested.toLocaleString()}
                    </Typography>
                  </Box>
                  {result.rejected !== undefined && result.rejected > 0 && (
                    <Box>
                      <Typography variant="caption" color="text.secondary">Rows Rejected</Typography>
                      <Typography variant="h5" color="warning.main">
                        {result.rejected.toLocaleString()}
                      </Typography>
                    </Box>
                  )}
                  <Divider />
                  <Box>
                    <Typography variant="caption" color="text.secondary">Source Name</Typography>
                    <Typography variant="body1" fontWeight={500}>{sourceName}</Typography>
                  </Box>
                  <Box>
                    <Typography variant="caption" color="text.secondary">
                      {sourceMode === 'cloud' ? 'Cloud Path' : 'File'}
                    </Typography>
                    <Typography variant="body2">
                      {sourceMode === 'cloud' ? cloudSelection?.path : file?.name}
                    </Typography>
                  </Box>
                  {sourceMode === 'cloud' && cloudSelection && (
                    <Box>
                      <Typography variant="caption" color="text.secondary">Connection</Typography>
                      <Typography variant="body2">
                        {cloudSelection.connectionName} ({cloudSelection.connectionType})
                      </Typography>
                    </Box>
                  )}
                </Stack>
              </Paper>

              {/* Mapping Summary */}
              <Paper variant="outlined" sx={{ p: 2, maxWidth: 500, width: '100%' }}>
                <Typography variant="subtitle2" gutterBottom>Mapping Applied</Typography>
                <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
                  {Object.entries(mapping)
                    .filter(([k, v]) => !k.startsWith('_') && v)
                    .slice(0, 12)
                    .map(([target, source]) => (
                      <Chip
                        key={target}
                        label={`${target} ← ${source}`}
                        size="small"
                        variant="outlined"
                      />
                    ))}
                  {Object.keys(mapping).filter(k => !k.startsWith('_') && mapping[k]).length > 12 && (
                    <Chip
                      label={`+${Object.keys(mapping).filter(k => !k.startsWith('_') && mapping[k]).length - 12} more`}
                      size="small"
                    />
                  )}
                </Stack>
                {getExpressionCount() > 0 && (
                  <Box sx={{ mt: 1 }}>
                    <Typography variant="caption" color="text.secondary">
                      Transforms: {Object.keys((mapping.__expr__ as Record<string, any>) || {}).join(', ')}
                    </Typography>
                  </Box>
                )}
              </Paper>

              {/* Actions */}
              <Stack direction="row" spacing={2}>
                <Button variant="outlined" onClick={resetWizard} startIcon={<CloudUploadIcon />}>
                  Import Another
                </Button>
                <Button variant="contained" onClick={onClose}>
                  Done
                </Button>
              </Stack>
            </Stack>
          )}
        </Box>
      </Box>
    </Drawer>
  )
}