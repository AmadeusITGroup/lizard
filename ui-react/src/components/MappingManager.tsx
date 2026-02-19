// path: ui-react/src/components/MappingManager.tsx
/**
 * MappingManager - Complete mapping template management with full editing capabilities
 * - Create, edit, clone, delete templates
 * - Import data with editable mappings
 * - View ingestion history
 * - Re-map existing data
 *
 * FIXED: Import wizard now resets properly when opening for new import
 */
import React, { useState, useCallback, useEffect } from 'react'
import {
  Box,
  Paper,
  Typography,
  Button,
  IconButton,
  Chip,
  Stack,
  TextField,
  MenuItem,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Tooltip,
  Alert,
  Tabs,
  Tab,
  Divider,
  CircularProgress,
  Card,
  CardContent,
  CardActions,
  Grid,
  LinearProgress,
  Snackbar,
  Stepper,
  Step,
  StepLabel,
  StepContent,
  FormControlLabel,
  Switch,
} from '@mui/material'
import {
  Add as AddIcon,
  Edit as EditIcon,
  Delete as DeleteIcon,
  ContentCopy as CopyIcon,
  Upload as UploadIcon,
  Download as DownloadIcon,
  Search as SearchIcon,
  Refresh as RefreshIcon,
  Check as CheckIcon,
  Warning as WarningIcon,
  Error as ErrorIcon,
  Info as InfoIcon,
  AutoFixHigh as AutoFixIcon,
  PlayArrow as PreviewIcon,
  Save as SaveIcon,
  Close as CloseIcon,
  History as HistoryIcon,
  Transform as TransformIcon,
  RestartAlt as ResetIcon,
} from '@mui/icons-material'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import axios from 'axios'
import SchemaMapper from './SchemaMapper'
import RemapDataTab from './RemapDataTab'

const API = import.meta.env.VITE_API_BASE || 'http://localhost:8000'

// ============================================================
// Types
// ============================================================

interface MappingTemplate {
  id: string
  name: string
  description: string
  mapping:  Record<string, string>
  expressions: Record<string, any>
  source_type: string
  category: string
  tags: string[]
  sample_columns: string[]
  validation_rules: any[]
  use_count: number
  last_used_at: string | null
  created_at: string
  updated_at: string
  is_builtin: boolean
  is_active: boolean
  created_by: string
}

interface MatchResult {
  template_id: string
  template_name: string
  category: string
  description: string
  score: number
  columns_matched: string[]
  columns_missing: string[]
}

interface SuggestionResult {
  filename: string
  total_rows: number
  columns:  string[]
  suggested_mapping: Record<string, string>
  suggested_expressions: Record<string, any>
  candidates: Record<string, Array<{ column: string; score: number }>>
  column_analysis: Record<string, any>
  engine_used: string
}

interface IngestionLog {
  id: string
  filename: string
  source_name:  string
  template_id: string | null
  template_name: string | null
  mapping_used: Record<string, any>
  status: string
  rows_total: number
  rows_ingested: number
  rows_rejected:  number
  started_at: string
  completed_at: string | null
}

// ============================================================
// Constants
// ============================================================

const CATEGORY_COLORS:  Record<string, 'primary' | 'secondary' | 'success' | 'warning' | 'error' | 'info' | 'default'> = {
  travel: 'primary',
  auth: 'secondary',
  payment: 'success',
  analytics: 'info',
  general: 'default',
}

const CATEGORY_OPTIONS = ['general', 'auth', 'travel', 'payment', 'analytics', 'fraud', 'security']

// ============================================================
// API Functions
// ============================================================

async function fetchTemplates(params?: { category?: string; search?: string }): Promise<MappingTemplate[]> {
  const { data } = await axios.get(`${API}/mapping/templates`, { params })
  return data
}

async function createTemplate(template: Partial<MappingTemplate>): Promise<MappingTemplate> {
  const { data } = await axios.post(`${API}/mapping/templates`, template)
  return data
}

async function updateTemplate(id:  string, updates: Partial<MappingTemplate>): Promise<MappingTemplate> {
  const { data } = await axios.put(`${API}/mapping/templates/${id}`, updates)
  return data
}

async function deleteTemplate(id: string): Promise<void> {
  await axios.delete(`${API}/mapping/templates/${id}`)
}

async function cloneTemplate(id: string, newName?:  string): Promise<MappingTemplate> {
  const { data } = await axios.post(`${API}/mapping/templates/${id}/clone`, null, {
    params: { new_name: newName },
  })
  return data
}

async function recordTemplateUse(id: string): Promise<{ template_id: string; use_count:  number }> {
  const { data } = await axios.post(`${API}/mapping/templates/${id}/use`)
  return data
}

async function matchTemplates(file: File): Promise<MatchResult[]> {
  const formData = new FormData()
  formData.append('file', file)
  const { data } = await axios.post(`${API}/mapping/templates/match`, formData)
  return data
}

async function suggestMapping(file: File, engine: string = 'heuristic'): Promise<SuggestionResult> {
  const formData = new FormData()
  formData.append('file', file)
  const { data } = await axios.post(`${API}/mapping/templates/suggest`, formData, {
    params: { engine },
  })
  return data
}

async function fetchFields(): Promise<{ fields: string[]; field_info: Record<string, any> }> {
  const { data } = await axios.get(`${API}/mapping/fields`)
  return data
}

async function fetchIngestionLogs(): Promise<IngestionLog[]> {
  const { data } = await axios.get(`${API}/mapping/ingestion-logs`)
  return data
}

async function importData(
  file: File,
  mapping: Record<string, any>,
  sourceName: string,
  templateId?:  string,
  templateName?: string
): Promise<{ ingested: number; source: string; log_id: string }> {
  const formData = new FormData()
  formData.append('file', file)
  formData.append('source_name', sourceName)
  formData.append('mapping_json', JSON.stringify(mapping))
  if (templateId) formData.append('template_id', templateId)
  if (templateName) formData.append('template_name', templateName)

  const { data } = await axios.post(`${API}/upload/events`, formData, {
    headers: { 'Content-Type':  'multipart/form-data' },
  })
  return data
}

// ============================================================
// Template Card Component
// ============================================================

function TemplateCard({
  template,
  onEdit,
  onClone,
  onDelete,
  onUse,
}:  {
  template: MappingTemplate
  onEdit: () => void
  onClone:  () => void
  onDelete:  () => void
  onUse: () => void
}) {
  const mappedFieldCount = Object.keys(template.mapping).filter(k => ! k.startsWith('_')).length
  const exprCount = Object.keys(template.expressions || {}).length

  return (
    <Card variant="outlined" sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      <CardContent sx={{ flex: 1 }}>
        <Stack direction="row" justifyContent="space-between" alignItems="flex-start" mb={1}>
          <Typography variant="h6" component="div" noWrap sx={{ flex: 1 }}>
            {template.name}
          </Typography>
          {template.is_builtin && (
            <Chip label="Built-in" size="small" variant="outlined" color="info" />
          )}
        </Stack>

        <Stack direction="row" spacing={1} mb={1} flexWrap="wrap" useFlexGap>
          <Chip
            label={template.category}
            size="small"
            color={CATEGORY_COLORS[template.category] || 'default'}
          />
          {template.tags.slice(0, 2).map((tag) => (
            <Chip key={tag} label={tag} size="small" variant="outlined" />
          ))}
        </Stack>

        <Typography variant="body2" color="text.secondary" sx={{ mb: 1, minHeight: 40 }}>
          {template.description || 'No description'}
        </Typography>

        <Stack direction="row" spacing={2} sx={{ opacity: 0.7 }}>
          <Typography variant="caption">{mappedFieldCount} fields</Typography>
          {exprCount > 0 && <Typography variant="caption">{exprCount} transforms</Typography>}
          <Typography variant="caption">Used {template.use_count}x</Typography>
        </Stack>
      </CardContent>

      <CardActions>
        <Button size="small" startIcon={<PreviewIcon />} onClick={onUse} variant="contained">
          Use
        </Button>
        <Tooltip title={template.is_builtin ? "Clone to edit" : "Edit"}>
          <IconButton size="small" onClick={template.is_builtin ? onClone : onEdit}>
            {template.is_builtin ? <CopyIcon fontSize="small" /> : <EditIcon fontSize="small" />}
          </IconButton>
        </Tooltip>
        {! template.is_builtin && (
          <>
            <IconButton size="small" onClick={onClone}>
              <CopyIcon fontSize="small" />
            </IconButton>
            <IconButton size="small" color="error" onClick={onDelete}>
              <DeleteIcon fontSize="small" />
            </IconButton>
          </>
        )}
      </CardActions>
    </Card>
  )
}

// ============================================================
// Import Wizard Component - WITH RESET FUNCTIONALITY
// ============================================================

interface ImportWizardProps {
  open: boolean
  onClose:  () => void
  onSuccess:  (result: { ingested: number; logId: string }) => void
  initialFile?: File | null
  initialTemplateId?: string
}

function ImportWizard({ open, onClose, onSuccess, initialFile, initialTemplateId }: ImportWizardProps) {
  const queryClient = useQueryClient()

  // Session ID to force re-render on reset
  const [sessionId, setSessionId] = useState<string>(() => Date.now().toString())

  // Wizard state
  const [activeStep, setActiveStep] = useState(0)
  const [file, setFile] = useState<File | null>(null)
  const [sourceName, setSourceName] = useState('')
  const [suggestion, setSuggestion] = useState<SuggestionResult | null>(null)
  const [matches, setMatches] = useState<MatchResult[]>([])
  const [selectedTemplateId, setSelectedTemplateId] = useState<string | null>(null)
  const [mapping, setMapping] = useState<Record<string, any>>({})
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [importing, setImporting] = useState(false)
  const [engine, setEngine] = useState<'heuristic' | 'openai' | 'ollama'>('heuristic')
  const [importComplete, setImportComplete] = useState(false)
  const [importResult, setImportResult] = useState<{ ingested: number; logId:  string } | null>(null)

  const templatesQuery = useQuery({
    queryKey: ['mapping-templates'],
    queryFn: () => fetchTemplates(),
    enabled: open,
  })

  // RESET FUNCTION - Clears all wizard state
  const resetWizard = useCallback(() => {
    setActiveStep(0)
    setFile(null)
    setSourceName('')
    setSuggestion(null)
    setMatches([])
    setSelectedTemplateId(null)
    setMapping({})
    setLoading(false)
    setError(null)
    setImporting(false)
    setImportComplete(false)
    setImportResult(null)
    setEngine('heuristic')
    setSessionId(Date.now().toString()) // Force re-render
  }, [])

  // Reset wizard when dialog opens
  useEffect(() => {
    if (open) {
      resetWizard()
      // Apply initial values if provided
      if (initialFile) {
        setFile(initialFile)
      }
      if (initialTemplateId) {
        setSelectedTemplateId(initialTemplateId)
      }
    }
  }, [open, resetWizard, initialFile, initialTemplateId])

  // Analyze file when selected
  useEffect(() => {
    if (file && !importComplete) {
      setSourceName(file.name.replace(/\.[^/.]+$/, '').replace(/[^a-zA-Z0-9]/g, '_'))
      analyzeFile()
    }
  }, [file])

  // Apply template when selected
  useEffect(() => {
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

  const analyzeFile = async () => {
    if (!file) return
    setLoading(true)
    setError(null)
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
    } catch (e:  any) {
      setError(e.response?.data?.detail || e.message || 'Analysis failed')
    } finally {
      setLoading(false)
    }
  }

  const handleImport = async () => {
    if (!file) return
    setImporting(true)
    setError(null)
    try {
      const template = selectedTemplateId ? templatesQuery.data?.find(t => t.id === selectedTemplateId) : null

      // Record template usage
      if (selectedTemplateId) {
        await recordTemplateUse(selectedTemplateId)
      }

      const result = await importData(
        file,
        mapping,
        sourceName,
        selectedTemplateId || undefined,
        template?.name
      )

      // Create ingestion log
      try {
        await axios.post(`${API}/mapping/ingestion-logs`, {
          filename: file.name,
          source_name: sourceName,
          template_id: selectedTemplateId,
          template_name: template?.name,
          mapping_used:  mapping,
          status: 'completed',
          rows_ingested: result.ingested,
        })
      } catch (logErr) {
        console.warn('Failed to create ingestion log:', logErr)
      }

      queryClient.invalidateQueries({ queryKey: ['mapping-templates'] })
      queryClient.invalidateQueries({ queryKey: ['ingestion-logs'] })

      // Set import complete state
      setImportComplete(true)
      setImportResult({ ingested: result.ingested, logId: result.log_id || '' })

      // Notify parent
      onSuccess({ ingested: result.ingested, logId: result.log_id || '' })
    } catch (e: any) {
      setError(e.response?.data?.detail || e.message || 'Import failed')
    } finally {
      setImporting(false)
    }
  }

  const handleSaveAsTemplate = async () => {
    const name = prompt('Template name:')
    if (!name) return

    try {
      await createTemplate({
        name,
        description: `Created from ${file?.name || 'import'}`,
        category: 'general',
        tags: [],
        mapping:  Object.fromEntries(
          Object.entries(mapping).filter(([k]) => k !== '__expr__')
        ) as Record<string, string>,
        expressions: (mapping.__expr__ as Record<string, any>) || {},
        sample_columns: suggestion?.columns || [],
        source_type: 'csv',
        validation_rules: [],
      })
      queryClient.invalidateQueries({ queryKey: ['mapping-templates'] })
      alert('Template saved!')
    } catch (e: any) {
      alert('Failed to save template: ' + (e.response?.data?.detail || e.message))
    }
  }

  // Handle "Import More Data" button
  const handleImportMoreData = useCallback(() => {
    resetWizard()
  }, [resetWizard])

  // Handle close - always reset
  const handleClose = useCallback(() => {
    resetWizard()
    onClose()
  }, [resetWizard, onClose])

  const steps = [
    { label: 'Select File', description: 'Choose a CSV, JSON, or Parquet file to import' },
    { label: 'Configure Mapping', description: 'Edit field mappings and transformations' },
    { label:  'Review & Import', description: 'Verify settings and import data' },
  ]

  return (
    <Dialog
      open={open}
      onClose={handleClose}
      maxWidth="lg"
      fullWidth
      key={sessionId} // Force re-render on reset
    >
      <DialogTitle>
        <Stack direction="row" justifyContent="space-between" alignItems="center">
          <Typography variant="h6">
            {importComplete ? '✅ Import Complete' : 'Import Data'}
          </Typography>
          <Stack direction="row" spacing={1}>
            {! importComplete && (
              <Tooltip title="Reset wizard">
                <IconButton onClick={resetWizard} size="small">
                  <ResetIcon />
                </IconButton>
              </Tooltip>
            )}
            <IconButton onClick={handleClose}>
              <CloseIcon />
            </IconButton>
          </Stack>
        </Stack>
      </DialogTitle>
      <DialogContent dividers>
        {error && (
          <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError(null)}>
            {error}
          </Alert>
        )}

        {/* Import Complete State */}
        {importComplete && importResult ? (
          <Box sx={{ textAlign: 'center', py: 4 }}>
            <CheckIcon sx={{ fontSize: 64, color: 'success.main', mb: 2 }} />
            <Typography variant="h5" gutterBottom>
              Import Successful!
            </Typography>
            <Typography variant="body1" color="text.secondary" gutterBottom>
              Successfully imported <strong>{importResult.ingested.toLocaleString()}</strong> rows
              into <strong>{sourceName}</strong>
            </Typography>
            <Stack direction="row" spacing={2} justifyContent="center" mt={4}>
              <Button
                variant="contained"
                onClick={handleImportMoreData}
                startIcon={<AddIcon />}
                size="large"
              >
                Import More Data
              </Button>
              <Button
                variant="outlined"
                onClick={handleClose}
              >
                Close
              </Button>
            </Stack>
          </Box>
        ) : (
          /* Import Wizard Steps */
          <Stepper activeStep={activeStep} orientation="vertical">
            {/* Step 1: Select File */}
            <Step>
              <StepLabel>{steps[0].label}</StepLabel>
              <StepContent>
                <Typography variant="body2" color="text.secondary" mb={2}>
                  {steps[0].description}
                </Typography>

                <Stack spacing={2}>
                  <Button
                    component="label"
                    variant="contained"
                    startIcon={<UploadIcon />}
                    sx={{ alignSelf: 'flex-start' }}
                  >
                    {file ? file.name : 'Choose File'}
                    <input
                      type="file"
                      hidden
                      accept=".csv,.json,.parquet"
                      onChange={(e) => {
                        const selectedFile = e.target.files?.[0] || null
                        setFile(selectedFile)
                        // Reset the input so same file can be selected again
                        e.target.value = ''
                      }}
                    />
                  </Button>

                  {loading && <LinearProgress />}

                  {file && ! loading && (
                    <Stack direction="row" spacing={2}>
                      <Button variant="outlined" onClick={() => setActiveStep(1)}>
                        Continue
                      </Button>
                      <Button
                        variant="text"
                        color="secondary"
                        onClick={() => {
                          setFile(null)
                          setSuggestion(null)
                          setMatches([])
                          setMapping({})
                          setSelectedTemplateId(null)
                        }}
                      >
                        Clear Selection
                      </Button>
                    </Stack>
                  )}
                </Stack>
              </StepContent>
            </Step>

            {/* Step 2: Configure Mapping */}
            <Step>
              <StepLabel>{steps[1].label}</StepLabel>
              <StepContent>
                <Typography variant="body2" color="text.secondary" mb={2}>
                  {steps[1].description}
                </Typography>

                {/* Engine Selection & Template Matching */}
                <Paper variant="outlined" sx={{ p: 2, mb: 2 }}>
                  <Stack spacing={2}>
                    {/* Mapping Engine Selector */}
                    <Stack direction="row" spacing={2} alignItems="center">
                      <TextField
                        select
                        size="small"
                        label="Mapping Engine"
                        value={engine}
                        onChange={(e) => {
                          const newEngine = e.target.value as 'heuristic' | 'openai' | 'ollama'
                          setEngine(newEngine)
                          // Re-analyze with new engine
                          if (file) {
                            setLoading(true)
                            suggestMapping(file, newEngine)
                              .then((result) => {
                                setSuggestion(result)
                                if (! selectedTemplateId) {
                                  setMapping({
                                    ...result.suggested_mapping,
                                    __expr__: result.suggested_expressions || {},
                                  })
                                }
                              })
                              .catch((err) => setError(err.message))
                              .finally(() => setLoading(false))
                          }
                        }}
                        sx={{ minWidth: 200 }}
                      >
                        <MenuItem value="heuristic">Heuristic (Fast, Default)</MenuItem>
                        <MenuItem value="openai">OpenAI (If Configured)</MenuItem>
                        <MenuItem value="ollama">Ollama (Local LLM)</MenuItem>
                      </TextField>

                      {suggestion && (
                        <Chip
                          label={`Engine: ${suggestion.engine_used}`}
                          size="small"
                          color="info"
                          variant="outlined"
                        />
                      )}

                      {loading && <CircularProgress size={20} />}
                    </Stack>

                    {/* Template Matches */}
                    {matches.length > 0 && (
                      <Box>
                        <Typography variant="subtitle2" gutterBottom>
                          Matching Templates ({matches.length})
                        </Typography>
                        <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
                          {matches.slice(0, 5).map(match => (
                            <Chip
                              key={match.template_id}
                              label={`${match.template_name} (${(match.score * 100).toFixed(0)}%)`}
                              onClick={() => setSelectedTemplateId(match.template_id)}
                              color={selectedTemplateId === match.template_id ? 'primary' : 'default'}
                              variant={selectedTemplateId === match.template_id ? 'filled' :  'outlined'}
                            />
                          ))}
                          <Chip
                            label="Use Auto-Suggestion"
                            onClick={() => {
                              setSelectedTemplateId(null)
                              if (suggestion) {
                                setMapping({
                                  ...suggestion.suggested_mapping,
                                  __expr__: suggestion.suggested_expressions || {},
                                })
                              }
                            }}
                            color={! selectedTemplateId ? 'primary' : 'default'}
                            variant={!selectedTemplateId ? 'filled' : 'outlined'}
                            icon={<AutoFixIcon />}
                          />
                        </Stack>
                      </Box>
                    )}

                    {/* Current Selection Info */}
                    <Alert severity="info" icon={false}>
                      {selectedTemplateId ? (
                        <span>
                          Using template:{' '}
                          <strong>
                            {matches.find(m => m.template_id === selectedTemplateId)?.template_name}
                          </strong>
                        </span>
                      ) : (
                        <span>
                          Using auto-suggestion from <strong>{suggestion?.engine_used || engine}</strong> engine
                        </span>
                      )}
                    </Alert>
                  </Stack>
                </Paper>

                {/* Source Name */}
                <TextField
                  fullWidth
                  size="small"
                  label="Source Name"
                  value={sourceName}
                  onChange={(e) => setSourceName(e.target.value)}
                  sx={{ mb: 2 }}
                  helperText="This name will identify the data source in visualizations"
                />

                {/* Mapping Editor */}
                {suggestion && (
                  <SchemaMapper
                    sourceColumns={suggestion.columns}
                    columnAnalysis={suggestion.column_analysis}
                    mapping={mapping}
                    onChange={setMapping}
                    suggestedMapping={suggestion.suggested_mapping}
                    suggestedExpressions={suggestion.suggested_expressions}
                    candidates={suggestion.candidates}
                  />
                )}

                {/* Actions */}
                <Stack direction="row" spacing={2} mt={2}>
                  <Button onClick={() => setActiveStep(0)}>Back</Button>
                  <Button variant="outlined" onClick={handleSaveAsTemplate} startIcon={<SaveIcon />}>
                    Save as Template
                  </Button>
                  <Box flex={1} />
                  <Button variant="contained" onClick={() => setActiveStep(2)}>
                    Continue
                  </Button>
                </Stack>
              </StepContent>
            </Step>

            {/* Step 3: Review & Import */}
            <Step>
              <StepLabel>{steps[2].label}</StepLabel>
              <StepContent>
                <Typography variant="body2" color="text.secondary" mb={2}>
                  {steps[2].description}
                </Typography>

                <Paper variant="outlined" sx={{ p: 2, mb: 2 }}>
                  <Stack spacing={1}>
                    <Typography variant="body2">
                      <strong>File:</strong> {file?.name}
                    </Typography>
                    <Typography variant="body2">
                      <strong>Rows:</strong> {suggestion?.total_rows.toLocaleString() || 'Unknown'}
                    </Typography>
                    <Typography variant="body2">
                      <strong>Source Name:</strong> {sourceName}
                    </Typography>
                    <Typography variant="body2">
                      <strong>Mapped Fields:</strong>{' '}
                      {Object.keys(mapping).filter(k => ! k.startsWith('_')).length}
                    </Typography>
                    <Typography variant="body2">
                      <strong>Transforms:</strong>{' '}
                      {Object.keys((mapping.__expr__ as Record<string, any>) || {}).length}
                    </Typography>
                  </Stack>
                </Paper>

                {/* Mapping Summary */}
                <Paper variant="outlined" sx={{ p: 2, mb: 2, maxHeight: 200, overflow: 'auto' }}>
                  <Typography variant="subtitle2" gutterBottom>
                    Mapping Summary
                  </Typography>
                  <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
                    {Object.entries(mapping)
                      .filter(([k]) => !k.startsWith('_'))
                      .map(([target, source]) => (
                        <Chip
                          key={target}
                          label={`${target} ← ${source}`}
                          size="small"
                          variant="outlined"
                        />
                      ))}
                  </Stack>
                </Paper>

                <Stack direction="row" spacing={2}>
                  <Button onClick={() => setActiveStep(1)}>Back</Button>
                  <Box flex={1} />
                  <Button
                    variant="contained"
                    color="primary"
                    onClick={handleImport}
                    disabled={importing}
                    startIcon={importing ? <CircularProgress size={18} /> : <CheckIcon />}
                  >
                    {importing ? 'Importing...' : 'Import Data'}
                  </Button>
                </Stack>
              </StepContent>
            </Step>
          </Stepper>
        )}
      </DialogContent>
    </Dialog>
  )
}

// ============================================================
// Template Editor Dialog
// ============================================================

interface TemplateEditorProps {
  open: boolean
  onClose: () => void
  template?:  MappingTemplate | null
  onSave: (template:  Partial<MappingTemplate>) => void
}

function TemplateEditor({ open, onClose, template, onSave }: TemplateEditorProps) {
  const [name, setName] = useState('')
  const [description, setDescription] = useState('')
  const [category, setCategory] = useState('general')
  const [tags, setTags] = useState<string[]>([])
  const [tagInput, setTagInput] = useState('')
  const [mapping, setMapping] = useState<Record<string, any>>({})
  const [sampleColumns, setSampleColumns] = useState<string[]>([])
  const [sampleColumnInput, setSampleColumnInput] = useState('')

  const fieldsQuery = useQuery({
    queryKey: ['mapping-fields'],
    queryFn: fetchFields,
    enabled: open,
  })

  useEffect(() => {
    if (template) {
      setName(template.name || '')
      setDescription(template.description || '')
      setCategory(template.category || 'general')
      setTags(template.tags || [])
      setMapping({
        ...template.mapping,
        __expr__: template.expressions || {},
      })
      setSampleColumns(template.sample_columns || [])
    } else {
      setName('')
      setDescription('')
      setCategory('general')
      setTags([])
      setMapping({})
      setSampleColumns([])
    }
  }, [template, open])

  const handleSave = () => {
    if (!name.trim()) return

    const expressions = (mapping.__expr__ as Record<string, any>) || {}
    const mappingWithoutExpr = Object.fromEntries(
      Object.entries(mapping).filter(([k]) => k !== '__expr__')
    ) as Record<string, string>

    onSave({
      name:  name.trim(),
      description: description.trim(),
      category,
      tags,
      mapping: mappingWithoutExpr,
      expressions,
      sample_columns: sampleColumns,
      source_type: 'csv',
      validation_rules: [],
    })
    onClose()
  }

  return (
    <Dialog open={open} onClose={onClose} maxWidth="lg" fullWidth>
      <DialogTitle>{template ? 'Edit Template' :  'Create Template'}</DialogTitle>
      <DialogContent dividers>
        <Stack spacing={3}>
          {/* Basic Info */}
          <Box>
            <Typography variant="subtitle2" gutterBottom>
              Basic Information
            </Typography>
            <Stack spacing={2}>
              <TextField
                label="Template Name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                fullWidth
                required
              />
              <TextField
                label="Description"
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                fullWidth
                multiline
                rows={2}
              />
              <Stack direction="row" spacing={2}>
                <TextField
                  select
                  label="Category"
                  value={category}
                  onChange={(e) => setCategory(e.target.value)}
                  sx={{ minWidth: 150 }}
                >
                  {CATEGORY_OPTIONS.map(cat => (
                    <MenuItem key={cat} value={cat}>
                      {cat}
                    </MenuItem>
                  ))}
                </TextField>
                <TextField
                  label="Add Tag"
                  value={tagInput}
                  onChange={(e) => setTagInput(e.target.value)}
                  onKeyPress={(e) => {
                    if (e.key === 'Enter' && tagInput.trim()) {
                      setTags([...tags, tagInput.trim()])
                      setTagInput('')
                    }
                  }}
                  sx={{ flex: 1 }}
                />
              </Stack>
              {tags.length > 0 && (
                <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
                  {tags.map(tag => (
                    <Chip
                      key={tag}
                      label={tag}
                      onDelete={() => setTags(tags.filter(t => t !== tag))}
                    />
                  ))}
                </Stack>
              )}
            </Stack>
          </Box>

          <Divider />

          {/* Sample Columns */}
          <Box>
            <Typography variant="subtitle2" gutterBottom>
              Expected Source Columns (for template matching)
            </Typography>
            <Stack direction="row" spacing={2} mb={1}>
              <TextField
                label="Add Column Name"
                value={sampleColumnInput}
                onChange={(e) => setSampleColumnInput(e.target.value)}
                onKeyPress={(e) => {
                  if (e.key === 'Enter' && sampleColumnInput.trim()) {
                    setSampleColumns([...sampleColumns, sampleColumnInput.trim()])
                    setSampleColumnInput('')
                  }
                }}
                sx={{ flex: 1 }}
              />
              <Button
                onClick={() => {
                  if (sampleColumnInput.trim()) {
                    setSampleColumns([...sampleColumns, sampleColumnInput.trim()])
                    setSampleColumnInput('')
                  }
                }}
              >
                Add
              </Button>
            </Stack>
            {sampleColumns.length > 0 && (
              <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
                {sampleColumns.map(col => (
                  <Chip
                    key={col}
                    label={col}
                    onDelete={() => setSampleColumns(sampleColumns.filter(c => c !== col))}
                    variant="outlined"
                  />
                ))}
              </Stack>
            )}
          </Box>

          <Divider />

          {/* Mapping Editor */}
          <Box>
            <Typography variant="subtitle2" gutterBottom>
              Field Mapping
            </Typography>
            <SchemaMapper
              sourceColumns={sampleColumns}
              mapping={mapping}
              onChange={setMapping}
              showAllFields
            />
          </Box>
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button onClick={handleSave} variant="contained" disabled={!name.trim()}>
          {template ? 'Save Changes' : 'Create Template'}
        </Button>
      </DialogActions>
    </Dialog>
  )
}

// ============================================================
// Main Component
// ============================================================

export default function MappingManager() {
  const queryClient = useQueryClient()
  const fileInputRef = React.useRef<HTMLInputElement>(null)

  // Custom field dialog state
  const [customFieldDialogOpen, setCustomFieldDialogOpen] = useState(false)
  const [newFieldName, setNewFieldName] = useState('')
  const [newFieldType, setNewFieldType] = useState('string')
  const [newFieldDescription, setNewFieldDescription] = useState('')
  const [newFieldRequired, setNewFieldRequired] = useState(false)

  // Main state
  const [activeTab, setActiveTab] = useState(0)
  const [searchQuery, setSearchQuery] = useState('')
  const [categoryFilter, setCategoryFilter] = useState<string>('')
  const [importWizardOpen, setImportWizardOpen] = useState(false)
  const [templateEditorOpen, setTemplateEditorOpen] = useState(false)
  const [editingTemplate, setEditingTemplate] = useState<MappingTemplate | null>(null)
  const [snackbar, setSnackbar] = useState<{
    open: boolean
    message: string
    severity: 'success' | 'error' | 'info'
  }>({
    open: false,
    message:  '',
    severity: 'success',
  })

  // Queries
  const templatesQuery = useQuery({
    queryKey: ['mapping-templates', categoryFilter, searchQuery],
    queryFn: () =>
      fetchTemplates({
        category: categoryFilter || undefined,
        search: searchQuery || undefined,
      }),
  })

  const fieldsQuery = useQuery({
    queryKey:  ['mapping-fields'],
    queryFn: fetchFields,
  })

  const logsQuery = useQuery({
    queryKey: ['ingestion-logs'],
    queryFn: fetchIngestionLogs,
  })

  const customFieldsQuery = useQuery({
    queryKey: ['custom-fields'],
    queryFn: async () => {
      const { data } = await axios.get(`${API}/mapping/custom-fields`)
      return data
    },
  })

  // Mutations
  const deleteMutation = useMutation({
    mutationFn: deleteTemplate,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['mapping-templates'] })
      setSnackbar({ open: true, message: 'Template deleted', severity: 'success' })
    },
    onError: (e:  any) => {
      setSnackbar({
        open: true,
        message: e.response?.data?.detail || 'Delete failed',
        severity: 'error',
      })
    },
  })

  const cloneMutation = useMutation({
    mutationFn: ({ id, name }: { id: string; name?:  string }) => cloneTemplate(id, name),
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey:  ['mapping-templates'] })
      setSnackbar({ open: true, message: `Template cloned:  ${data.name}`, severity: 'success' })
      // Open editor for the cloned template
      setEditingTemplate(data)
      setTemplateEditorOpen(true)
    },
  })

  const createMutation = useMutation({
    mutationFn: createTemplate,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['mapping-templates'] })
      setSnackbar({ open: true, message: 'Template created', severity: 'success' })
    },
  })

  const updateMutation = useMutation({
    mutationFn: ({ id, updates }: { id: string; updates: Partial<MappingTemplate> }) =>
      updateTemplate(id, updates),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['mapping-templates'] })
      setSnackbar({ open:  true, message: 'Template updated', severity: 'success' })
    },
  })

  const createFieldMutation = useMutation({
    mutationFn: async (field: {
      name: string
      type: string
      description:  string
      required: boolean
    }) => {
      const { data } = await axios.post(`${API}/mapping/custom-fields`, field)
      return data
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['mapping-fields'] })
      queryClient.invalidateQueries({ queryKey: ['custom-fields'] })
      setCustomFieldDialogOpen(false)
      setNewFieldName('')
      setNewFieldType('string')
      setNewFieldDescription('')
      setNewFieldRequired(false)
      setSnackbar({ open:  true, message: 'Custom field created', severity: 'success' })
    },
    onError: (e: any) => {
      setSnackbar({
        open: true,
        message: e.response?.data?.detail || 'Failed to create field',
        severity: 'error',
      })
    },
  })

  const deleteFieldMutation = useMutation({
    mutationFn: async (fieldName: string) => {
      await axios.delete(`${API}/mapping/custom-fields/${fieldName}`)
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['mapping-fields'] })
      queryClient.invalidateQueries({ queryKey: ['custom-fields'] })
      setSnackbar({ open:  true, message: 'Custom field deleted', severity: 'success' })
    },
  })

  // Handlers
  const handleDelete = (template: MappingTemplate) => {
    if (window.confirm(`Delete template "${template.name}"?`)) {
      deleteMutation.mutate(template.id)
    }
  }

  const handleClone = (template: MappingTemplate) => {
    cloneMutation.mutate({ id: template.id })
  }

  const handleEdit = (template: MappingTemplate) => {
    if (template.is_builtin) {
      handleClone(template)
    } else {
      setEditingTemplate(template)
      setTemplateEditorOpen(true)
    }
  }

  const handleSaveTemplate = (templateData: Partial<MappingTemplate>) => {
    if (editingTemplate && ! editingTemplate.is_builtin) {
      updateMutation.mutate({ id: editingTemplate.id, updates: templateData })
    } else {
      createMutation.mutate(templateData)
    }
    setEditingTemplate(null)
  }

  const handleUseTemplate = (template: MappingTemplate) => {
    recordTemplateUse(template.id)
    queryClient.invalidateQueries({ queryKey: ['mapping-templates'] })
    setImportWizardOpen(true)
  }

  const handleImportSuccess = (result: { ingested: number; logId: string }) => {
    setSnackbar({
      open: true,
      message: `Successfully imported ${result.ingested.toLocaleString()} rows`,
      severity: 'success',
    })
    queryClient.invalidateQueries({ queryKey: ['ingestion-logs'] })
  }

  // Handler for opening import wizard - ensures fresh state
  const handleOpenImportWizard = useCallback(() => {
    setImportWizardOpen(true)
  }, [])

  const templates = templatesQuery.data || []
  const categories = [...new Set(templates.map(t => t.category))].sort()

  return (
    <Box sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      {/* Header */}
      <Paper sx={{ p: 2, mb:  2 }}>
        <Stack
          direction={{ xs: 'column', md: 'row' }}
          justifyContent="space-between"
          alignItems={{ md: 'center' }}
          spacing={2}
        >
          <Box>
            <Typography variant="h5" gutterBottom>
              📋 Data Mapping Manager
            </Typography>
            <Typography variant="body2" color="text.secondary">
              Create and manage mapping templates for flexible data ingestion
            </Typography>
          </Box>
          <Stack direction="row" spacing={1}>
            <Button
              variant="outlined"
              startIcon={<RefreshIcon />}
              onClick={() => templatesQuery.refetch()}
              disabled={templatesQuery.isFetching}
            >
              Refresh
            </Button>
            <Button
              variant="outlined"
              startIcon={<UploadIcon />}
              onClick={handleOpenImportWizard}
            >
              Import Data
            </Button>
            <Button
              variant="contained"
              startIcon={<AddIcon />}
              onClick={() => {
                setEditingTemplate(null)
                setTemplateEditorOpen(true)
              }}
            >
              New Template
            </Button>
          </Stack>
        </Stack>
      </Paper>

      {/* Tabs */}
      <Paper sx={{ mb: 2 }}>
        <Tabs value={activeTab} onChange={(_, v) => setActiveTab(v)}>
          <Tab label="Templates" />
          <Tab label="Target Fields" />
          <Tab label={`Ingestion History (${logsQuery.data?.length || 0})`} />
          <Tab label="Re-map Data" />
        </Tabs>
      </Paper>

      {/* Tab Content */}
      <Box sx={{ flex: 1, overflow: 'auto' }}>
        {/* Templates Tab */}
        {activeTab === 0 && (
          <Box>
            <Stack direction="row" spacing={2} sx={{ mb: 2 }}>
              <TextField
                size="small"
                placeholder="Search templates..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                InputProps={{ startAdornment: <SearchIcon sx={{ mr: 1, opacity: 0.5 }} /> }}
                sx={{ minWidth: 250 }}
              />
              <TextField
                select
                size="small"
                label="Category"
                value={categoryFilter}
                onChange={(e) => setCategoryFilter(e.target.value)}
                sx={{ minWidth: 150 }}
              >
                <MenuItem value="">All</MenuItem>
                {categories.map(cat => (
                  <MenuItem key={cat} value={cat}>
                    {cat}
                  </MenuItem>
                ))}
              </TextField>
            </Stack>

            {templatesQuery.isLoading ? (
              <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
                <CircularProgress />
              </Box>
            ) : (
              <Grid container spacing={2}>
                {templates.map(template => (
                  <Grid item xs={12} sm={6} md={4} lg={3} key={template.id}>
                    <TemplateCard
                      template={template}
                      onEdit={() => handleEdit(template)}
                      onClone={() => handleClone(template)}
                      onDelete={() => handleDelete(template)}
                      onUse={() => handleUseTemplate(template)}
                    />
                  </Grid>
                ))}
                {templates.length === 0 && (
                  <Grid item xs={12}>
                    <Alert severity="info">
                      No templates found.Create one to get started.
                    </Alert>
                  </Grid>
                )}
              </Grid>
            )}
          </Box>
        )}

        {/* Target Fields Tab */}
        {activeTab === 1 && (
          <Box>
            <Stack direction="row" justifyContent="space-between" alignItems="center" mb={2}>
              <Box>
                <Typography variant="h6" gutterBottom>
                  Target Schema Fields
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Standard and custom fields that your data can be mapped to.
                </Typography>
              </Box>
              <Button
                variant="contained"
                startIcon={<AddIcon />}
                onClick={() => setCustomFieldDialogOpen(true)}
              >
                Add Custom Field
              </Button>
            </Stack>

            {fieldsQuery.isLoading ? (
              <CircularProgress />
            ) : (
              <TableContainer component={Paper} variant="outlined">
                <Table size="small">
                  <TableHead>
                    <TableRow>
                      <TableCell>Field Name</TableCell>
                      <TableCell>Type</TableCell>
                      <TableCell>Required</TableCell>
                      <TableCell>Description</TableCell>
                      <TableCell>Source</TableCell>
                      <TableCell>Actions</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {fieldsQuery.data?.fields.map(field => {
                      const info = fieldsQuery.data?.field_info[field] || {}
                      const isCustom = info.is_custom === true
                      return (
                        <TableRow
                          key={field}
                          sx={{ bgcolor: isCustom ? 'action.hover' : 'transparent' }}
                        >
                          <TableCell>
                            <Typography
                              variant="body2"
                              fontFamily="monospace"
                              fontWeight={info.required ? 600 : 400}
                            >
                              {field}
                            </Typography>
                          </TableCell>
                          <TableCell>
                            <Chip
                              label={info.type || 'string'}
                              size="small"
                              variant="outlined"
                              color={
                                info.type === 'number'
                                  ? 'primary'
                                  : info.type === 'boolean'
                                  ? 'secondary'
                                  : 'default'
                              }
                            />
                          </TableCell>
                          <TableCell>
                            {info.required ? (
                              <Chip label="Required" size="small" color="error" />
                            ) : (
                              <span style={{ opacity: 0.4 }}>Optional</span>
                            )}
                          </TableCell>
                          <TableCell>
                            <Typography
                              variant="body2"
                              color="text.secondary"
                              sx={{ maxWidth: 300 }}
                            >
                              {info.description || '-'}
                            </Typography>
                          </TableCell>
                          <TableCell>
                            <Chip
                              label={isCustom ? 'Custom' : 'Built-in'}
                              size="small"
                              color={isCustom ? 'info' : 'default'}
                              variant="outlined"
                            />
                          </TableCell>
                          <TableCell>
                            {isCustom && (
                              <IconButton
                                size="small"
                                color="error"
                                onClick={() => {
                                  if (window.confirm(`Delete custom field "${field}"?`)) {
                                    deleteFieldMutation.mutate(field)
                                  }
                                }}
                              >
                                <DeleteIcon fontSize="small" />
                              </IconButton>
                            )}
                          </TableCell>
                        </TableRow>
                      )
                    })}
                  </TableBody>
                </Table>
              </TableContainer>
            )}

            {/* Custom Field Dialog */}
            <Dialog
              open={customFieldDialogOpen}
              onClose={() => setCustomFieldDialogOpen(false)}
              maxWidth="sm"
              fullWidth
            >
              <DialogTitle>Add Custom Target Field</DialogTitle>
              <DialogContent>
                <Stack spacing={2} sx={{ mt: 1 }}>
                  <TextField
                    label="Field Name"
                    value={newFieldName}
                    onChange={(e) =>
                      setNewFieldName(e.target.value.toLowerCase().replace(/[^a-z0-9_]/g, '_'))
                    }
                    fullWidth
                    required
                    helperText="Use lowercase letters, numbers, and underscores only"
                    placeholder="e.g., risk_score, custom_category"
                  />
                  <TextField
                    select
                    label="Data Type"
                    value={newFieldType}
                    onChange={(e) => setNewFieldType(e.target.value)}
                    fullWidth
                  >
                    <MenuItem value="string">String (text)</MenuItem>
                    <MenuItem value="number">Number (integer or decimal)</MenuItem>
                    <MenuItem value="boolean">Boolean (true/false)</MenuItem>
                    <MenuItem value="datetime">DateTime (timestamp)</MenuItem>
                    <MenuItem value="json">JSON (complex object)</MenuItem>
                  </TextField>
                  <TextField
                    label="Description"
                    value={newFieldDescription}
                    onChange={(e) => setNewFieldDescription(e.target.value)}
                    fullWidth
                    multiline
                    rows={2}
                    placeholder="Describe what this field is used for..."
                  />
                  <FormControlLabel
                    control={
                      <Switch
                        checked={newFieldRequired}
                        onChange={(e) => setNewFieldRequired(e.target.checked)}
                      />
                    }
                    label="Required field"
                  />
                </Stack>
              </DialogContent>
              <DialogActions>
                <Button onClick={() => setCustomFieldDialogOpen(false)}>Cancel</Button>
                <Button
                  variant="contained"
                  onClick={() =>
                    createFieldMutation.mutate({
                      name:  newFieldName,
                      type: newFieldType,
                      description: newFieldDescription,
                      required: newFieldRequired,
                    })
                  }
                  disabled={!newFieldName.trim() || createFieldMutation.isPending}
                >
                  {createFieldMutation.isPending ? 'Creating...' : 'Create Field'}
                </Button>
              </DialogActions>
            </Dialog>
          </Box>
        )}

        {/* Ingestion History Tab */}
        {activeTab === 2 && (
          <Box>
            {logsQuery.isLoading ? (
              <CircularProgress />
            ) : logsQuery.data && logsQuery.data.length > 0 ? (
              <TableContainer component={Paper} variant="outlined">
                <Table size="small">
                  <TableHead>
                    <TableRow>
                      <TableCell>Date</TableCell>
                      <TableCell>File</TableCell>
                      <TableCell>Source</TableCell>
                      <TableCell>Template</TableCell>
                      <TableCell>Rows</TableCell>
                      <TableCell>Status</TableCell>
                      <TableCell>Mapping</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {logsQuery.data.map(log => (
                      <TableRow key={log.id}>
                        <TableCell>
                          <Typography variant="body2">
                            {new Date(log.started_at).toLocaleString()}
                          </Typography>
                        </TableCell>
                        <TableCell>{log.filename}</TableCell>
                        <TableCell>{log.source_name}</TableCell>
                        <TableCell>
                          {log.template_name ? (
                            <Chip label={log.template_name} size="small" />
                          ) : (
                            <em style={{ opacity: 0.5 }}>Auto-mapped</em>
                          )}
                        </TableCell>
                        <TableCell>
                          {log.rows_ingested?.toLocaleString() || '-'}
                          {log.rows_rejected > 0 && (
                            <span style={{ color: 'red' }}> ({log.rows_rejected} rejected)</span>
                          )}
                        </TableCell>
                        <TableCell>
                          <Chip
                            label={log.status}
                            size="small"
                            color={
                              log.status === 'completed'
                                ? 'success'
                                : log.status === 'failed'
                                ? 'error'
                                : 'default'
                            }
                          />
                        </TableCell>
                        <TableCell>
                          <Tooltip title={JSON.stringify(log.mapping_used, null, 2)}>
                            <IconButton size="small">
                              <InfoIcon fontSize="small" />
                            </IconButton>
                          </Tooltip>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            ) : (
              <Alert severity="info">
                No ingestion history yet.Import some data to see the history here.
              </Alert>
            )}
          </Box>
        )}

        {/* Re-map Data Tab */}
        {activeTab === 3 && (
          <RemapDataTab
            ingestionLogs={logsQuery.data || []}
            onSuccess={() => {
              setSnackbar({
                open:  true,
                message: 'Data re-mapped successfully',
                severity:  'success',
              })
              queryClient.invalidateQueries({ queryKey: ['ingestion-logs'] })
            }}
          />
        )}
      </Box>

      {/* Import Wizard - key prop forces re-mount on open */}
      <ImportWizard
        open={importWizardOpen}
        onClose={() => setImportWizardOpen(false)}
        onSuccess={handleImportSuccess}
      />

      {/* Template Editor */}
      <TemplateEditor
        open={templateEditorOpen}
        onClose={() => {
          setTemplateEditorOpen(false)
          setEditingTemplate(null)
        }}
        template={editingTemplate}
        onSave={handleSaveTemplate}
      />

      {/* Snackbar */}
      <Snackbar
        open={snackbar.open}
        autoHideDuration={4000}
        onClose={() => setSnackbar({ ...snackbar, open: false })}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
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