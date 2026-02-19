// path: ui-react/src/components/RemapDataTab.tsx
/**
 * RemapDataTab - Component for re-mapping already imported data
 */
import React from 'react'
import {
  Box,
  Paper,
  Typography,
  Stack,
  TextField,
  MenuItem,
  Button,
  Alert,
  Chip,
  Divider,
  CircularProgress,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  FormControlLabel,
  Switch,
  Stepper,
  Step,
  StepLabel,
  StepContent,
  IconButton,
  Tooltip,
  Collapse,
} from '@mui/material'
import {
  Refresh as RefreshIcon,
  PlayArrow as PreviewIcon,
  Check as ApplyIcon,
  History as HistoryIcon,
  ExpandMore as ExpandMoreIcon,
  ExpandLess as ExpandLessIcon,
  ContentCopy as CopyIcon,
} from '@mui/icons-material'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import axios from 'axios'
import SchemaMapper from './SchemaMapper'

const API = import.meta.env.VITE_API_BASE || 'http://localhost:8000'

interface IngestionLog {
  id: string
  filename: string
  source_name:  string
  template_id: string | null
  template_name:  string | null
  mapping_used:  Record<string, any>
  status: string
  rows_total: number
  rows_ingested: number
  rows_rejected:  number
  started_at: string
  completed_at: string | null
}

interface RemapDataTabProps {
  ingestionLogs: IngestionLog[]
  onSuccess: () => void
}

export default function RemapDataTab({ ingestionLogs, onSuccess }: RemapDataTabProps) {
  const queryClient = useQueryClient()

  // State
  const [selectedLog, setSelectedLog] = React.useState<IngestionLog | null>(null)
  const [selectedSourceName, setSelectedSourceName] = React.useState<string>('')
  const [mapping, setMapping] = React.useState<Record<string, any>>({})
  const [dryRun, setDryRun] = React.useState(true)
  const [activeStep, setActiveStep] = React.useState(0)
  const [previewResult, setPreviewResult] = React.useState<any>(null)
  const [error, setError] = React.useState<string | null>(null)
  const [showHistory, setShowHistory] = React.useState(false)

  // Get unique source names from logs
  const sourceNames = React.useMemo(() => {
    const names = new Set<string>()
    ingestionLogs.forEach(log => {
      if (log.source_name) names.add(log.source_name)
    })
    return Array.from(names).sort()
  }, [ingestionLogs])

  // Get logs for selected source
  const logsForSource = React.useMemo(() => {
    if (!selectedSourceName) return []
    return ingestionLogs
      .filter(log => log.source_name === selectedSourceName)
      .sort((a, b) => new Date(b.started_at).getTime() - new Date(a.started_at).getTime())
  }, [ingestionLogs, selectedSourceName])

  // When source is selected, load the last used mapping
  React.useEffect(() => {
    if (logsForSource.length > 0) {
      const latestLog = logsForSource[0]
      setSelectedLog(latestLog)
      setMapping(latestLog.mapping_used || {})
    } else {
      setSelectedLog(null)
      setMapping({})
    }
  }, [logsForSource])

  // Preview mutation
  const previewMutation = useMutation({
    mutationFn: async () => {
      const formData = new FormData()
      formData.append('source_name', selectedSourceName)
      formData.append('mapping_json', JSON.stringify(mapping))
      formData.append('limit', '10')

      const { data } = await axios.post(`${API}/mapping/remap/preview`, formData)
      return data
    },
    onSuccess: (data) => {
      setPreviewResult(data)
      setActiveStep(2)
    },
    onError: (e: any) => {
      setError(e.response?.data?.detail || e.message || 'Preview failed')
    },
  })

  // Apply mutation
  const applyMutation = useMutation({
    mutationFn:  async () => {
      const { data } = await axios.post(`${API}/mapping/remap`, {
        source_name: selectedSourceName,
        mapping: Object.fromEntries(
          Object.entries(mapping).filter(([k]) => k !== '__expr__')
        ),
        expressions:  (mapping.__expr__ as Record<string, any>) || {},
        dry_run: dryRun,
      })
      return data
    },
    onSuccess:  (data) => {
      if (data.status === 'dry_run') {
        setPreviewResult(data)
        setError(null)
      } else {
        onSuccess()
        setActiveStep(3)
      }
    },
    onError: (e: any) => {
      setError(e.response?.data?.detail || e.message || 'Re-mapping failed')
    },
  })

  const handleSelectSource = (sourceName: string) => {
    setSelectedSourceName(sourceName)
    setActiveStep(1)
    setPreviewResult(null)
    setError(null)
    setShowHistory(false)
  }

  const handlePreview = () => {
    setError(null)
    previewMutation.mutate()
  }

  const handleApply = () => {
    setError(null)
    applyMutation.mutate()
  }

  const handleUsePreviousMapping = (log: IngestionLog) => {
    setSelectedLog(log)
    setMapping(log.mapping_used || {})
    setShowHistory(false)
  }

  const handleReset = () => {
    setActiveStep(0)
    setSelectedSourceName('')
    setSelectedLog(null)
    setMapping({})
    setPreviewResult(null)
    setError(null)
    setDryRun(true)
  }

  return (
    <Box>
      <Alert severity="info" sx={{ mb: 2 }}>
        <Typography variant="body2">
          <strong>Re-map Data</strong> allows you to change how previously imported data is interpreted.
          Select a data source, modify the mapping, and apply the changes to update your data.
        </Typography>
      </Alert>

      {error && (
        <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError(null)}>
          {error}
        </Alert>
      )}

      <Stepper activeStep={activeStep} orientation="vertical">
        {/* Step 1: Select Source */}
        <Step>
          <StepLabel>Select Data Source</StepLabel>
          <StepContent>
            <Typography variant="body2" color="text.secondary" mb={2}>
              Choose a data source to re-map. Sources are identified by the name used during import.
            </Typography>

            {sourceNames.length === 0 ? (
              <Alert severity="warning">
                No imported data sources found.Import some data first.
              </Alert>
            ) : (
              <Stack spacing={2}>
                <TextField
                  select
                  size="small"
                  label="Data Source"
                  value={selectedSourceName}
                  onChange={(e) => handleSelectSource(e.target.value)}
                  sx={{ minWidth: 350 }}
                >
                  <MenuItem value="">-- Select a source --</MenuItem>
                  {sourceNames.map(name => {
                    const logs = ingestionLogs.filter(l => l.source_name === name)
                    const totalRows = logs.reduce((sum, l) => sum + (l.rows_ingested || 0), 0)
                    const lastImport = logs.length > 0 
                      ? new Date(Math.max(...logs.map(l => new Date(l.started_at).getTime())))
                      : null
                    return (
                      <MenuItem key={name} value={name}>
                        <Stack direction="row" spacing={2} alignItems="center" sx={{ width: '100%' }}>
                          <Typography variant="body2" sx={{ flex: 1 }}>{name}</Typography>
                          <Chip label={`${totalRows.toLocaleString()} rows`} size="small" />
                          {lastImport && (
                            <Typography variant="caption" color="text.secondary">
                              {lastImport.toLocaleDateString()}
                            </Typography>
                          )}
                        </Stack>
                      </MenuItem>
                    )
                  })}
                </TextField>
              </Stack>
            )}
          </StepContent>
        </Step>

        {/* Step 2: Edit Mapping */}
        <Step>
          <StepLabel>Edit Mapping</StepLabel>
          <StepContent>
            {selectedLog && (
              <Stack spacing={2}>
                {/* Current mapping info */}
                <Paper variant="outlined" sx={{ p: 2 }}>
                  <Stack direction="row" justifyContent="space-between" alignItems="flex-start">
                    <Box>
                      <Typography variant="subtitle2" gutterBottom>Current Mapping Info</Typography>
                      <Stack direction="row" spacing={3} flexWrap="wrap" useFlexGap>
                        <Box>
                          <Typography variant="caption" color="text.secondary">Source</Typography>
                          <Typography variant="body2" fontWeight={500}>{selectedLog.source_name}</Typography>
                        </Box>
                        <Box>
                          <Typography variant="caption" color="text.secondary">File</Typography>
                          <Typography variant="body2">{selectedLog.filename}</Typography>
                        </Box>
                        <Box>
                          <Typography variant="caption" color="text.secondary">Last Import</Typography>
                          <Typography variant="body2">
                            {new Date(selectedLog.started_at).toLocaleString()}
                          </Typography>
                        </Box>
                        <Box>
                          <Typography variant="caption" color="text.secondary">Rows</Typography>
                          <Typography variant="body2">{selectedLog.rows_ingested?.toLocaleString()}</Typography>
                        </Box>
                        {selectedLog.template_name && (
                          <Box>
                            <Typography variant="caption" color="text.secondary">Template</Typography>
                            <Chip label={selectedLog.template_name} size="small" />
                          </Box>
                        )}
                      </Stack>
                    </Box>
                    {logsForSource.length > 1 && (
                      <Button
                        size="small"
                        startIcon={showHistory ? <ExpandLessIcon /> : <HistoryIcon />}
                        onClick={() => setShowHistory(!showHistory)}
                      >
                        {showHistory ? 'Hide' : 'Show'} History ({logsForSource.length})
                      </Button>
                    )}
                  </Stack>

                  {/* Import history collapse */}
                  <Collapse in={showHistory}>
                    <Divider sx={{ my: 2 }} />
                    <Typography variant="subtitle2" gutterBottom>Import History</Typography>
                    <Typography variant="body2" color="text.secondary" gutterBottom>
                      Click on a previous import to use its mapping configuration.
                    </Typography>
                    <TableContainer sx={{ maxHeight: 200 }}>
                      <Table size="small">
                        <TableHead>
                          <TableRow>
                            <TableCell>Date</TableCell>
                            <TableCell>File</TableCell>
                            <TableCell>Rows</TableCell>
                            <TableCell>Template</TableCell>
                            <TableCell>Action</TableCell>
                          </TableRow>
                        </TableHead>
                        <TableBody>
                          {logsForSource.map(log => (
                            <TableRow 
                              key={log.id}
                              sx={{ 
                                bgcolor: log.id === selectedLog?.id ? 'action.selected' : 'transparent',
                                cursor: 'pointer',
                                '&:hover':  { bgcolor: 'action.hover' },
                              }}
                              onClick={() => handleUsePreviousMapping(log)}
                            >
                              <TableCell>
                                <Typography variant="body2">
                                  {new Date(log.started_at).toLocaleString()}
                                </Typography>
                              </TableCell>
                              <TableCell>{log.filename}</TableCell>
                              <TableCell>{log.rows_ingested?.toLocaleString()}</TableCell>
                              <TableCell>
                                {log.template_name ? (
                                  <Chip label={log.template_name} size="small" />
                                ) : (
                                  <em style={{ opacity: 0.5 }}>Auto</em>
                                )}
                              </TableCell>
                              <TableCell>
                                <Tooltip title="Use this mapping">
                                  <IconButton size="small">
                                    <CopyIcon fontSize="small" />
                                  </IconButton>
                                </Tooltip>
                              </TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    </TableContainer>
                  </Collapse>
                </Paper>

                {/* Mapping editor */}
                <Box>
                  <Typography variant="subtitle2" gutterBottom>Edit Field Mapping</Typography>
                  <Typography variant="body2" color="text.secondary" gutterBottom>
                    Modify the mapping below. Changes will be applied to all data from this source.
                  </Typography>
                </Box>
                
                <SchemaMapper
                  mapping={mapping}
                  onChange={setMapping}
                  compact={false}
                  showAllFields={true}
                />

                {/* Actions */}
                <Stack direction="row" spacing={2} alignItems="center">
                  <Button onClick={() => setActiveStep(0)}>Back</Button>
                  <Box flex={1} />
                  <FormControlLabel
                    control={<Switch checked={dryRun} onChange={(_, v) => setDryRun(v)} />}
                    label="Dry run (preview only)"
                  />
                  <Button
                    variant="contained"
                    onClick={handlePreview}
                    disabled={previewMutation.isPending}
                    startIcon={previewMutation.isPending ? <CircularProgress size={18} /> : <PreviewIcon />}
                  >
                    Preview Changes
                  </Button>
                </Stack>
              </Stack>
            )}
          </StepContent>
        </Step>

        {/* Step 3: Preview & Apply */}
        <Step>
          <StepLabel>Review & Apply</StepLabel>
          <StepContent>
            {previewResult && (
              <Stack spacing={2}>
                <Alert severity={dryRun ? 'info' : 'warning'}>
                  {dryRun
                    ? 'This is a preview.No changes have been made yet.'
                    : 'Warning: This will modify your existing data.Make sure the mapping is correct.'}
                </Alert>

                {/* Preview result info */}
                <Paper variant="outlined" sx={{ p: 2 }}>
                  <Typography variant="subtitle2" gutterBottom>Preview Result</Typography>
                  <Stack spacing={1}>
                    <Typography variant="body2" color="text.secondary">
                      {previewResult.message || 'Preview completed.'}
                    </Typography>
                    {previewResult.affected_rows_estimate !== undefined && (
                      <Typography variant="body2">
                        <strong>Estimated affected rows:</strong> {previewResult.affected_rows_estimate.toLocaleString()}
                      </Typography>
                    )}
                    <Typography variant="body2">
                      <strong>Source:</strong> {previewResult.source_name || selectedSourceName}
                    </Typography>
                  </Stack>
                </Paper>

                {/* New mapping summary */}
                <Paper variant="outlined" sx={{ p: 2 }}>
                  <Typography variant="subtitle2" gutterBottom>New Mapping</Typography>
                  <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
                    {Object.entries(mapping)
                      .filter(([k, v]) => !k.startsWith('_') && v)
                      .slice(0, 15)
                      .map(([target, source]) => (
                        <Chip
                          key={target}
                          label={`${target} ← ${source}`}
                          size="small"
                          variant="outlined"
                        />
                      ))}
                    {Object.keys(mapping).filter(k => ! k.startsWith('_') && mapping[k]).length > 15 && (
                      <Chip
                        label={`+${Object.keys(mapping).filter(k => !k.startsWith('_') && mapping[k]).length - 15} more`}
                        size="small"
                      />
                    )}
                  </Stack>
                  {mapping.__expr__ && Object.keys(mapping.__expr__ as Record<string, any>).length > 0 && (
                    <Box mt={1}>
                      <Typography variant="caption" color="text.secondary">
                        Transforms: {Object.keys(mapping.__expr__ as Record<string, any>).join(', ')}
                      </Typography>
                    </Box>
                  )}
                </Paper>

                {/* Preview rows if available */}
                {previewResult.preview_rows && previewResult.preview_rows.length > 0 && (
                  <Box>
                    <Typography variant="subtitle2" gutterBottom>
                      Sample Transformed Rows ({previewResult.preview_rows.length})
                    </Typography>
                    <TableContainer component={Paper} variant="outlined" sx={{ maxHeight: 250 }}>
                      <Table size="small" stickyHeader>
                        <TableHead>
                          <TableRow>
                            {Object.keys(previewResult.preview_rows[0]).slice(0, 8).map(col => (
                              <TableCell key={col}>{col}</TableCell>
                            ))}
                          </TableRow>
                        </TableHead>
                        <TableBody>
                          {previewResult.preview_rows.map((row:  any, idx: number) => (
                            <TableRow key={idx}>
                              {Object.keys(row).slice(0, 8).map(col => (
                                <TableCell key={col}>
                                  <Typography variant="body2" noWrap sx={{ maxWidth: 100 }}>
                                    {row[col] != null ? String(row[col]) : <em style={{ opacity: 0.4 }}>null</em>}
                                  </Typography>
                                </TableCell>
                              ))}
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    </TableContainer>
                  </Box>
                )}

                {/* Actions */}
                <Stack direction="row" spacing={2}>
                  <Button onClick={() => setActiveStep(1)}>Back to Edit</Button>
                  <Box flex={1} />
                  <Button
                    variant="contained"
                    color="warning"
                    onClick={() => {
                      setDryRun(false)
                      handleApply()
                    }}
                    disabled={applyMutation.isPending}
                    startIcon={applyMutation.isPending ? <CircularProgress size={18} /> : <ApplyIcon />}
                  >
                    {applyMutation.isPending ? 'Applying...' : 'Apply Changes'}
                  </Button>
                </Stack>
              </Stack>
            )}
          </StepContent>
        </Step>

        {/* Step 4: Complete */}
        <Step>
          <StepLabel>Complete</StepLabel>
          <StepContent>
            <Alert severity="success" sx={{ mb:  2 }} icon={<ApplyIcon />}>
              <Typography variant="body1" fontWeight={500}>
                Data has been re-mapped successfully!
              </Typography>
              <Typography variant="body2">
                The mapping for "{selectedSourceName}" has been updated. Visualizations will now use the new field mappings.
              </Typography>
            </Alert>
            <Stack direction="row" spacing={2}>
              <Button variant="contained" onClick={handleReset}>
                Re-map Another Source
              </Button>
              <Button variant="outlined" onClick={() => setActiveStep(1)}>
                Make More Changes
              </Button>
            </Stack>
          </StepContent>
        </Step>
      </Stepper>
    </Box>
  )
}