// path: ui-react/src/components/SchemaMapper.tsx
/**
 * SchemaMapper - Interactive field mapping component with full expression support
 * Allows editing mappings, adding expressions, and testing transformations
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
  IconButton,
  Chip,
  Alert,
  Tooltip,
  Autocomplete,
  Divider,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  TableContainer,
  Switch,
  FormControlLabel,
  CircularProgress,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Tabs,
  Tab,
} from '@mui/material'
import {
  Add as AddIcon,
  Delete as DeleteIcon,
  AutoFixHigh as AutoFixIcon,
  ExpandMore as ExpandMoreIcon,
  Info as InfoIcon,
  Warning as WarningIcon,
  Check as CheckIcon,
  Edit as EditIcon,
  Code as CodeIcon,
  Save as SaveIcon,
} from '@mui/icons-material'
import { useQuery } from '@tanstack/react-query'
import axios from 'axios'
import ExpressionBuilder, { Expression, expressionToBackend } from './ExpressionBuilder'

const API = import.meta.env.VITE_API_BASE || 'http://localhost:8000'

// ============================================================
// Types
// ============================================================

interface ColumnAnalysis {
  detected_type: string
  null_count: number
  null_percent: number
  unique_count: number
  sample_values: string[]
}

interface FieldInfo {
  type: string
  required: boolean
  description: string
}

interface Candidate {
  column: string
  score: number
}

interface FieldMapping {
  sourceColumn: string | null
  expression: Expression | string | null
  enabled: boolean
}

interface SchemaMapperProps {
  // Source data
  sample?:  any[]
  sourceColumns?:  string[]
  columnAnalysis?: Record<string, ColumnAnalysis>

  // Current mapping state (backend format)
  mapping: Record<string, any>
  onChange: (mapping: Record<string, any>) => void

  // Optional suggestions
  suggestedMapping?: Record<string, string>
  suggestedExpressions?: Record<string, any>
  candidates?: Record<string, Candidate[]>

  // UI options
  readOnly?: boolean
  compact?: boolean
  showAllFields?: boolean
}

// Simple transforms for quick access
const QUICK_TRANSFORMS = [
  { value: '', label: 'None' },
  { value: 'try_float', label: 'To Number' },
  { value: 'try_int', label: 'To Integer' },
  { value: 'upper', label: 'Uppercase' },
  { value:  'lower', label: 'Lowercase' },
  { value: 'trim', label: 'Trim' },
  { value: 'bool', label: 'To Boolean' },
]

// Fetch target fields from API
async function fetchFields(): Promise<{ fields: string[]; field_info: Record<string, FieldInfo> }> {
  const { data } = await axios.get(`${API}/mapping/fields`)
  return data
}

// ============================================================
// Main Component
// ============================================================

export default function SchemaMapper({
  sample,
  sourceColumns:  explicitSourceColumns,
  columnAnalysis:  explicitColumnAnalysis,
  mapping,
  onChange,
  suggestedMapping = {},
  suggestedExpressions = {},
  candidates = {},
  readOnly = false,
  compact = false,
  showAllFields = true,
}: SchemaMapperProps) {
  const [showUnmapped, setShowUnmapped] = React.useState(showAllFields)
  const [showAdvanced, setShowAdvanced] = React.useState(false)
  const [editingField, setEditingField] = React.useState<string | null>(null)
  const [activeTab, setActiveTab] = React.useState(0)

  // Derive source columns from sample if not provided
  const sourceColumns = React.useMemo(() => {
    if (explicitSourceColumns?.length) return explicitSourceColumns
    if (sample?.length) return Object.keys(sample[0])
    return []
  }, [explicitSourceColumns, sample])

  // Derive column analysis from sample if not provided
  const columnAnalysis = React.useMemo(() => {
    if (explicitColumnAnalysis) return explicitColumnAnalysis
    if (! sample?.length) return {}

    const analysis: Record<string, ColumnAnalysis> = {}
    for (const col of sourceColumns) {
      const values = sample.map(row => row[col])
      const nonNull = values.filter(v => v != null && v !== '')
      const uniqueValues = new Set(nonNull.map(String))

      let detectedType = 'string'
      if (nonNull.length > 0) {
        const firstVal = nonNull[0]
        if (typeof firstVal === 'number') {
          detectedType = 'number'
        } else if (typeof firstVal === 'boolean') {
          detectedType = 'boolean'
        } else if (typeof firstVal === 'string') {
          if (nonNull.every(v => ! isNaN(Number(v)))) {
            detectedType = 'number'
          }
        }
      }

      analysis[col] = {
        detected_type: detectedType,
        null_count: values.length - nonNull.length,
        null_percent: ((values.length - nonNull.length) / values.length) * 100,
        unique_count:  uniqueValues.size,
        sample_values: Array.from(uniqueValues).slice(0, 5).map(String),
      }
    }
    return analysis
  }, [explicitColumnAnalysis, sample, sourceColumns])

  // Fetch target fields
  const fieldsQuery = useQuery({
    queryKey: ['mapping-fields'],
    queryFn:  fetchFields,
  })

  const targetFields = fieldsQuery.data?.fields || []
  const fieldInfo = fieldsQuery.data?.field_info || {}

  // Parse current mapping
  const currentMapping = React.useMemo(() => {
    const result: Record<string, string> = {}
    for (const [key, value] of Object.entries(mapping)) {
      if (key !== '__expr__' && typeof value === 'string') {
        result[key] = value
      }
    }
    return result
  }, [mapping])

  const currentExpressions = React.useMemo(() => {
    return (mapping.__expr__ as Record<string, any>) || {}
  }, [mapping])

  // Update mapping
  const updateMapping = (target: string, source: string | null) => {
    const newMapping = { ...mapping }
    if (source) {
      newMapping[target] = source
    } else {
      delete newMapping[target]
    }
    onChange(newMapping)
  }

  // Update expression
  const updateExpression = (target: string, expr: Expression | string | null) => {
    const newMapping = { ...mapping }
    const expressions = { ...(newMapping.__expr__ || {}) }

    const backendExpr = expressionToBackend(expr)
    if (backendExpr !== undefined) {
      expressions[target] = backendExpr
    } else {
      delete expressions[target]
    }

    if (Object.keys(expressions).length > 0) {
      newMapping.__expr__ = expressions
    } else {
      delete newMapping.__expr__
    }

    onChange(newMapping)
  }

  // Apply all suggestions
  const applySuggestions = () => {
    const newMapping = { ...mapping }

    // Apply column mappings
    for (const [target, source] of Object.entries(suggestedMapping)) {
      if (source && ! newMapping[target]) {
        newMapping[target] = source
      }
    }

    // Apply expressions
    if (Object.keys(suggestedExpressions).length > 0) {
      newMapping.__expr__ = { ...(newMapping.__expr__ || {}), ...suggestedExpressions }
    }

    onChange(newMapping)
  }

  // Add custom field
  const [customFieldName, setCustomFieldName] = React.useState('')
  const addCustomField = () => {
    if (customFieldName && ! targetFields.includes(customFieldName)) {
      updateMapping(customFieldName, '')
      setCustomFieldName('')
    }
  }

  // Calculate stats
  const mappedTargets = Object.keys(currentMapping).filter(k => currentMapping[k])
  const unmappedTargets = targetFields.filter(f => !mappedTargets.includes(f))
  const requiredFields = targetFields.filter(f => fieldInfo[f]?.required)
  const missingRequired = requiredFields.filter(f => !mappedTargets.includes(f))
  const usedSourceColumns = new Set(Object.values(currentMapping).filter(Boolean))
  const unmappedSourceColumns = sourceColumns.filter(c => !usedSourceColumns.has(c))

  // Get expression display
  const getExpressionDisplay = (target: string): string => {
    const expr = currentExpressions[target]
    if (!expr) return ''
    if (typeof expr === 'string') return expr
    if (typeof expr === 'object' && expr.op) return `${expr.op}(...)`
    return 'custom'
  }

  if (fieldsQuery.isLoading) {
    return <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}><CircularProgress /></Box>
  }

  return (
    <Box>
      {/* Header */}
      <Stack direction="row" justifyContent="space-between" alignItems="center" mb={2}>
        <Typography variant={compact ? "subtitle1" : "h6"}>Field Mapping</Typography>
        <Stack direction="row" spacing={1} alignItems="center">
          {Object.keys(suggestedMapping).length > 0 && (
            <Button
              size="small"
              startIcon={<AutoFixIcon />}
              onClick={applySuggestions}
              disabled={readOnly}
              variant="outlined"
            >
              Apply All Suggestions
            </Button>
          )}
          <FormControlLabel
            control={<Switch checked={showUnmapped} onChange={(_, v) => setShowUnmapped(v)} size="small" />}
            label={<Typography variant="body2">Show unmapped</Typography>}
          />
        </Stack>
      </Stack>

      {/* Warnings */}
      {missingRequired.length > 0 && (
        <Alert severity="warning" sx={{ mb: 2 }}>
          Missing required fields: {missingRequired.join(', ')}
        </Alert>
      )}

      {/* Stats */}
      <Stack direction="row" spacing={2} mb={2} flexWrap="wrap">
        <Chip
          label={`${mappedTargets.length} / ${targetFields.length} fields mapped`}
          color={mappedTargets.length > 0 ? 'primary' : 'default'}
          variant="outlined"
          size="small"
        />
        <Chip
          label={`${Object.keys(currentExpressions).length} transforms`}
          color={Object.keys(currentExpressions).length > 0 ? 'info' : 'default'}
          variant="outlined"
          size="small"
        />
        {sourceColumns.length > 0 && (
          <Chip
            label={`${unmappedSourceColumns.length} source columns unused`}
            variant="outlined"
            size="small"
          />
        )}
      </Stack>

      {/* Tabs */}
      {! compact && (
        <Tabs value={activeTab} onChange={(_, v) => setActiveTab(v)} sx={{ mb: 2 }}>
          <Tab label="Mapping" />
          <Tab label="Expressions" />
          <Tab label="Add Custom Field" />
        </Tabs>
      )}

      {/* Main Mapping Table */}
      {(activeTab === 0 || compact) && (
        <TableContainer component={Paper} variant="outlined" sx={{ mb: 2, maxHeight: compact ? 350 : 500 }}>
          <Table size="small" stickyHeader>
            <TableHead>
              <TableRow>
                <TableCell width="25%">Target Field</TableCell>
                <TableCell width="30%">Source Column</TableCell>
                <TableCell width="25%">Transform</TableCell>
                <TableCell width="20%">Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {targetFields
                .filter(target => showUnmapped || currentMapping[target])
                .map(target => {
                  const info = fieldInfo[target] || {}
                  const source = currentMapping[target] || ''
                  const exprDisplay = getExpressionDisplay(target)
                  const candidate = candidates[target]?.[0]
                  const suggested = suggestedMapping[target]
                  const analysis = source ? columnAnalysis[source] : null

                  return (
                    <TableRow
                      key={target}
                      sx={{
                        bgcolor: currentMapping[target] ? 'transparent' : 'action.hover',
                        opacity: currentMapping[target] ? 1 : 0.7,
                      }}
                    >
                      {/* Target Field */}
                      <TableCell>
                        <Stack direction="row" spacing={1} alignItems="center">
                          <Typography variant="body2" fontFamily="monospace" fontWeight={info.required ? 600 : 400}>
                            {target}
                          </Typography>
                          {info.required && (
                            <Chip label="*" size="small" color="error" sx={{ height: 16, fontSize: 10 }} />
                          )}
                        </Stack>
                        <Tooltip title={info.description || ''}>
                          <Typography variant="caption" color="text.secondary" sx={{ cursor: 'help' }}>
                            {info.type || 'string'}
                          </Typography>
                        </Tooltip>
                      </TableCell>

                      {/* Source Column */}
                      <TableCell>
                        <Autocomplete
                          size="small"
                          options={sourceColumns}
                          value={source || null}
                          onChange={(_, v) => updateMapping(target, v)}
                          disabled={readOnly}
                          freeSolo
                          renderInput={(params) => (
                            <TextField
                              {...params}
                              placeholder={suggested ? `Suggested: ${suggested}` : 'Select or type...'}
                              sx={{ minWidth: 150 }}
                            />
                          )}
                          renderOption={(props, option) => (
                            <li {...props}>
                              <Stack direction="row" justifyContent="space-between" width="100%">
                                <span>{option}</span>
                                {columnAnalysis[option] && (
                                  <Chip label={columnAnalysis[option].detected_type} size="small" sx={{ height: 18, fontSize: 10 }} />
                                )}
                              </Stack>
                            </li>
                          )}
                        />
                        {suggested && ! source && (
                          <Button
                            size="small"
                            onClick={() => updateMapping(target, suggested)}
                            sx={{ mt: 0.5, fontSize: 10 }}
                            disabled={readOnly}
                          >
                            Use suggestion
                          </Button>
                        )}
                      </TableCell>

                      {/* Transform (Quick Select) */}
                      <TableCell>
                        <Stack direction="row" spacing={0.5} alignItems="center">
                          <TextField
                            select
                            size="small"
                            value={typeof currentExpressions[target] === 'string' ? currentExpressions[target] : (currentExpressions[target] ? 'custom' : '')}
                            onChange={(e) => {
                              const val = e.target.value
                              if (val === 'custom') {
                                setEditingField(target)
                              } else {
                                updateExpression(target, val || null)
                              }
                            }}
                            disabled={readOnly || !source}
                            sx={{ minWidth: 100 }}
                          >
                            {QUICK_TRANSFORMS.map(t => (
                              <MenuItem key={t.value} value={t.value}>{t.label}</MenuItem>
                            ))}
                            <MenuItem value="custom">Custom...</MenuItem>
                          </TextField>
                          {exprDisplay && exprDisplay !== currentExpressions[target] && (
                            <Chip label={exprDisplay} size="small" sx={{ height: 20, fontSize: 10 }} />
                          )}
                        </Stack>
                      </TableCell>

                      {/* Actions */}
                      <TableCell>
                        <Stack direction="row" spacing={0.5}>
                          <Tooltip title="Edit expression">
                            <IconButton size="small" onClick={() => setEditingField(target)} disabled={readOnly || !source}>
                              <CodeIcon fontSize="small" />
                            </IconButton>
                          </Tooltip>
                          {analysis && (
                            <Tooltip title={`${analysis.unique_count} unique, ${analysis.null_percent.toFixed(1)}% null.Samples: ${analysis.sample_values.slice(0,3).join(', ')}`}>
                              <IconButton size="small">
                                <InfoIcon fontSize="small" />
                              </IconButton>
                            </Tooltip>
                          )}
                          <Tooltip title="Clear mapping">
                            <IconButton
                              size="small"
                              onClick={() => {
                                updateMapping(target, null)
                                updateExpression(target, null)
                              }}
                              disabled={readOnly || !source}
                            >
                              <DeleteIcon fontSize="small" />
                            </IconButton>
                          </Tooltip>
                        </Stack>
                      </TableCell>
                    </TableRow>
                  )
                })}
            </TableBody>
          </Table>
        </TableContainer>
      )}

      {/* Expressions Tab */}
      {activeTab === 1 && ! compact && (
        <Paper variant="outlined" sx={{ p: 2 }}>
          <Typography variant="subtitle2" gutterBottom>
            Current Expressions ({Object.keys(currentExpressions).length})
          </Typography>
          {Object.keys(currentExpressions).length === 0 ? (
            <Typography color="text.secondary">No expressions defined. Add transforms from the Mapping tab.</Typography>
          ) : (
            <TableContainer>
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>Target Field</TableCell>
                    <TableCell>Expression</TableCell>
                    <TableCell>Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {Object.entries(currentExpressions).map(([field, expr]) => (
                    <TableRow key={field}>
                      <TableCell>
                        <Typography variant="body2" fontFamily="monospace">{field}</Typography>
                      </TableCell>
                      <TableCell>
                        <Box sx={{ fontFamily: 'monospace', fontSize: 12 }}>
                          {typeof expr === 'string' ? expr : JSON.stringify(expr)}
                        </Box>
                      </TableCell>
                      <TableCell>
                        <IconButton size="small" onClick={() => setEditingField(field)}>
                          <EditIcon fontSize="small" />
                        </IconButton>
                        <IconButton size="small" onClick={() => updateExpression(field, null)}>
                          <DeleteIcon fontSize="small" />
                        </IconButton>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          )}
        </Paper>
      )}

      {/* Add Custom Field Tab */}
      {activeTab === 2 && !compact && (
        <Paper variant="outlined" sx={{ p: 2 }}>
          <Typography variant="subtitle2" gutterBottom>Add Custom Target Field</Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            Create custom target fields for your specific use case. These fields will be available in visualizations.
          </Typography>
          <Stack direction="row" spacing={2}>
            <TextField
              size="small"
              label="Field Name"
              value={customFieldName}
              onChange={(e) => setCustomFieldName(e.target.value.toLowerCase().replace(/[^a-z0-9_]/g, '_'))}
              placeholder="e.g., custom_risk_score"
              sx={{ minWidth: 250 }}
            />
            <Button variant="contained" onClick={addCustomField} disabled={!customFieldName}>
              Add Field
            </Button>
          </Stack>
        </Paper>
      )}

      {/* Unused Source Columns */}
      {unmappedSourceColumns.length > 0 && ! compact && (
        <Box mt={2}>
          <Typography variant="subtitle2" gutterBottom>
            Unmapped Source Columns ({unmappedSourceColumns.length})
          </Typography>
          <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
            {unmappedSourceColumns.slice(0, 25).map(col => (
              <Tooltip key={col} title={columnAnalysis[col] ? `Type: ${columnAnalysis[col].detected_type}, Samples: ${columnAnalysis[col].sample_values.join(', ')}` : col}>
                <Chip label={col} size="small" variant="outlined" />
              </Tooltip>
            ))}
            {unmappedSourceColumns.length > 25 && (
              <Chip label={`+${unmappedSourceColumns.length - 25} more`} size="small" />
            )}
          </Stack>
        </Box>
      )}

      {/* Expression Editor Dialog */}
      <Dialog
        open={!!editingField}
        onClose={() => setEditingField(null)}
        maxWidth="md"
        fullWidth
      >
        <DialogTitle>
          Edit Expression:  {editingField}
        </DialogTitle>
        <DialogContent dividers>
          {editingField && (
            <ExpressionBuilder
              sourceColumns={sourceColumns}
              value={
                currentExpressions[editingField]
                  ? (typeof currentExpressions[editingField] === 'string'
                      ? currentExpressions[editingField]
                      : { type: 'custom' as const, config: { json: JSON.stringify(currentExpressions[editingField]) } })
                  : null
              }
              onChange={(expr) => updateExpression(editingField, expr)}
              sampleData={sample}
              disabled={readOnly}
            />
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setEditingField(null)}>Close</Button>
        </DialogActions>
      </Dialog>
    </Box>
  )
}