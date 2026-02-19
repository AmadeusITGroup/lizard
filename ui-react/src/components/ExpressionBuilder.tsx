// path: ui-react/src/components/ExpressionBuilder.tsx
/**
 * ExpressionBuilder - Visual builder for complex field expressions
 * Supports concat, regex extraction, conditionals, math operations, lookups
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
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Divider,
  FormControlLabel,
  Switch,
  Tabs,
  Tab,
} from '@mui/material'
import {
  Add as AddIcon,
  Delete as DeleteIcon,
  Help as HelpIcon,
  Code as CodeIcon,
  PlayArrow as TestIcon,
} from '@mui/icons-material'

// ============================================================
// Types
// ============================================================

export type ExpressionType = 
  | 'direct'      // Direct column copy
  | 'unary'       // Simple transform (try_float, upper, etc.)
  | 'concat'      // Concatenate columns
  | 'coalesce'    // First non-null
  | 'regex'       // Regex extraction
  | 'conditional' // If-else logic
  | 'math'        // Math operations
  | 'lookup'      // Value mapping
  | 'template'    // Template string ${col}
  | 'custom'      // Raw JSON expression

export interface Expression {
  type: ExpressionType
  config: Record<string, any>
}

interface ExpressionBuilderProps {
  sourceColumns: string[]
  value: Expression | string | null
  onChange: (expr:  Expression | string | null) => void
  onTest?:  (expr: Expression | string) => Promise<any[]>
  sampleData?: any[]
  disabled?: boolean
}

// ============================================================
// Unary operations
// ============================================================

const UNARY_OPS = [
  { value: 'try_float', label: 'Convert to Number', description: 'Parse as float, handles comma decimals' },
  { value:  'try_int', label: 'Convert to Integer', description: 'Parse as integer' },
  { value: 'bool', label: 'Convert to Boolean', description: 'true/1/yes → true, false/0/no → false' },
  { value:  'upper', label: 'Uppercase', description: 'Convert to uppercase' },
  { value: 'lower', label: 'Lowercase', description: 'Convert to lowercase' },
  { value: 'trim', label:  'Trim Whitespace', description: 'Remove leading/trailing spaces' },
  { value: 'str', label: 'Convert to String', description: 'Convert any value to string' },
]

// ============================================================
// Helper to convert Expression to backend format
// ============================================================

export function expressionToBackend(expr: Expression | string | null): any {
  if (!expr) return undefined
  if (typeof expr === 'string') return expr
  
  switch (expr.type) {
    case 'direct':
      return undefined // No transform needed
    case 'unary':
      return expr.config.operation
    case 'concat': 
      return { op: 'concat', cols: expr.config.columns, sep: expr.config.separator || '' }
    case 'coalesce':
      return { op: 'coalesce', cols: expr.config.columns, default: expr.config.defaultValue }
    case 'regex':
      return { op: 'regex_extract', col: expr.config.sourceColumn, pattern: expr.config.pattern, group: expr.config.group || 0 }
    case 'conditional':
      return {
        op: 'if_else',
        col: expr.config.sourceColumn,
        condition: expr.config.condition,
        value: expr.config.compareValue,
        then: expr.config.thenValue,
        else: expr.config.elseValue,
      }
    case 'math': 
      if (expr.config.operation === 'add') {
        return { op: 'add', cols:  expr.config.columns, constant: expr.config.constant || 0 }
      }
      if (expr.config.operation === 'multiply') {
        return { op:  'multiply', cols: expr.config.columns, constant: expr.config.constant || 1 }
      }
      if (expr.config.operation === 'divide') {
        return { op: 'divide', numerator: expr.config.numerator, denominator: expr.config.denominator }
      }
      return undefined
    case 'lookup': 
      return { op: 'map', col: expr.config.sourceColumn, mapping: expr.config.mapping, default: expr.config.defaultValue }
    case 'template': 
      return expr.config.template // Template strings like "${col1}-${col2}"
    case 'custom':
      try {
        return JSON.parse(expr.config.json)
      } catch {
        return undefined
      }
    default:
      return undefined
  }
}

// ============================================================
// Main Component
// ============================================================

export default function ExpressionBuilder({
  sourceColumns,
  value,
  onChange,
  onTest,
  sampleData,
  disabled = false,
}: ExpressionBuilderProps) {
  // Parse existing value
  const [exprType, setExprType] = React.useState<ExpressionType>('direct')
  const [config, setConfig] = React.useState<Record<string, any>>({})
  const [testResults, setTestResults] = React.useState<any[] | null>(null)
  const [showHelp, setShowHelp] = React.useState(false)

  // Initialize from value
  React.useEffect(() => {
    if (! value) {
      setExprType('direct')
      setConfig({})
      return
    }
    
    if (typeof value === 'string') {
      if (value.includes('${')) {
        setExprType('template')
        setConfig({ template: value })
      } else if (UNARY_OPS.find(op => op.value === value)) {
        setExprType('unary')
        setConfig({ operation: value })
      } else {
        setExprType('custom')
        setConfig({ json: JSON.stringify(value) })
      }
      return
    }

    setExprType(value.type)
    setConfig(value.config || {})
  }, [value])

  // Update parent when config changes
  const updateExpression = (type: ExpressionType, newConfig: Record<string, any>) => {
    setExprType(type)
    setConfig(newConfig)
    
    if (type === 'direct') {
      onChange(null)
    } else {
      onChange({ type, config: newConfig })
    }
  }

  // Test expression against sample data
  const handleTest = async () => {
    if (!onTest || !value) return
    try {
      const results = await onTest(value)
      setTestResults(results)
    } catch (e) {
      console.error('Test failed:', e)
    }
  }

  return (
    <Box>
      {/* Expression Type Selector */}
      <Stack direction="row" spacing={2} alignItems="center" mb={2}>
        <TextField
          select
          size="small"
          label="Expression Type"
          value={exprType}
          onChange={(e) => updateExpression(e.target.value as ExpressionType, {})}
          disabled={disabled}
          sx={{ minWidth: 180 }}
        >
          <MenuItem value="direct">Direct Copy</MenuItem>
          <MenuItem value="unary">Simple Transform</MenuItem>
          <MenuItem value="concat">Concatenate Columns</MenuItem>
          <MenuItem value="coalesce">First Non-Null</MenuItem>
          <MenuItem value="regex">Regex Extract</MenuItem>
          <MenuItem value="conditional">Conditional (If/Else)</MenuItem>
          <MenuItem value="math">Math Operation</MenuItem>
          <MenuItem value="lookup">Value Lookup/Map</MenuItem>
          <MenuItem value="template">Template String</MenuItem>
          <MenuItem value="custom">Custom JSON</MenuItem>
        </TextField>
        
        <IconButton size="small" onClick={() => setShowHelp(true)}>
          <HelpIcon />
        </IconButton>
        
        {onTest && value && (
          <Button size="small" startIcon={<TestIcon />} onClick={handleTest}>
            Test
          </Button>
        )}
      </Stack>

      {/* Expression Config */}
      <Paper variant="outlined" sx={{ p: 2 }}>
        {/* Direct - No config needed */}
        {exprType === 'direct' && (
          <Typography color="text.secondary">
            Value will be copied directly from the source column without transformation.
          </Typography>
        )}

        {/* Unary Transform */}
        {exprType === 'unary' && (
          <TextField
            select
            fullWidth
            size="small"
            label="Operation"
            value={config.operation || ''}
            onChange={(e) => updateExpression('unary', { operation: e.target.value })}
            disabled={disabled}
          >
            {UNARY_OPS.map(op => (
              <MenuItem key={op.value} value={op.value}>
                <Stack>
                  <Typography variant="body2">{op.label}</Typography>
                  <Typography variant="caption" color="text.secondary">{op.description}</Typography>
                </Stack>
              </MenuItem>
            ))}
          </TextField>
        )}

        {/* Concatenate */}
        {exprType === 'concat' && (
          <Stack spacing={2}>
            <Autocomplete
              multiple
              size="small"
              options={sourceColumns}
              value={config.columns || []}
              onChange={(_, v) => updateExpression('concat', { ...config, columns: v })}
              disabled={disabled}
              renderInput={(params) => <TextField {...params} label="Columns to Concatenate" />}
            />
            <TextField
              size="small"
              label="Separator"
              value={config.separator || ''}
              onChange={(e) => updateExpression('concat', { ...config, separator: e.target.value })}
              disabled={disabled}
              placeholder="e.g., -, _, :"
              helperText="Leave empty for no separator"
            />
            {config.columns?.length > 0 && (
              <Alert severity="info">
                Preview: {config.columns.join(config.separator || '')}
              </Alert>
            )}
          </Stack>
        )}

        {/* Coalesce */}
        {exprType === 'coalesce' && (
          <Stack spacing={2}>
            <Autocomplete
              multiple
              size="small"
              options={sourceColumns}
              value={config.columns || []}
              onChange={(_, v) => updateExpression('coalesce', { ...config, columns: v })}
              disabled={disabled}
              renderInput={(params) => <TextField {...params} label="Columns (in priority order)" />}
            />
            <TextField
              size="small"
              label="Default Value (if all null)"
              value={config.defaultValue || ''}
              onChange={(e) => updateExpression('coalesce', { ...config, defaultValue: e.target.value })}
              disabled={disabled}
            />
            <Alert severity="info">
              Returns the first non-null value from the selected columns.
            </Alert>
          </Stack>
        )}

        {/* Regex Extract */}
        {exprType === 'regex' && (
          <Stack spacing={2}>
            <Autocomplete
              size="small"
              options={sourceColumns}
              value={config.sourceColumn || null}
              onChange={(_, v) => updateExpression('regex', { ...config, sourceColumn: v })}
              disabled={disabled}
              renderInput={(params) => <TextField {...params} label="Source Column" />}
            />
            <TextField
              size="small"
              label="Regex Pattern"
              value={config.pattern || ''}
              onChange={(e) => updateExpression('regex', { ...config, pattern: e.target.value })}
              disabled={disabled}
              placeholder="e.g., (\d{4})-(\d{2})-(\d{2})"
              helperText="Use capture groups () to extract parts"
            />
            <TextField
              size="small"
              type="number"
              label="Capture Group"
              value={config.group ?? 0}
              onChange={(e) => updateExpression('regex', { ...config, group: parseInt(e.target.value) || 0 })}
              disabled={disabled}
              helperText="0 = entire match, 1 = first group, etc."
            />
          </Stack>
        )}

        {/* Conditional */}
        {exprType === 'conditional' && (
          <Stack spacing={2}>
            <Autocomplete
              size="small"
              options={sourceColumns}
              value={config.sourceColumn || null}
              onChange={(_, v) => updateExpression('conditional', { ...config, sourceColumn: v })}
              disabled={disabled}
              renderInput={(params) => <TextField {...params} label="Source Column" />}
            />
            <Stack direction="row" spacing={2}>
              <TextField
                select
                size="small"
                label="Condition"
                value={config.condition || 'eq'}
                onChange={(e) => updateExpression('conditional', { ...config, condition: e.target.value })}
                disabled={disabled}
                sx={{ minWidth: 150 }}
              >
                <MenuItem value="eq">Equals</MenuItem>
                <MenuItem value="ne">Not Equals</MenuItem>
                <MenuItem value="contains">Contains</MenuItem>
                <MenuItem value="startswith">Starts With</MenuItem>
                <MenuItem value="endswith">Ends With</MenuItem>
                <MenuItem value="gt">Greater Than</MenuItem>
                <MenuItem value="gte">Greater or Equal</MenuItem>
                <MenuItem value="lt">Less Than</MenuItem>
                <MenuItem value="lte">Less or Equal</MenuItem>
                <MenuItem value="is_null">Is Null</MenuItem>
                <MenuItem value="is_not_null">Is Not Null</MenuItem>
                <MenuItem value="regex">Matches Regex</MenuItem>
              </TextField>
              {! ['is_null', 'is_not_null'].includes(config.condition) && (
                <TextField
                  size="small"
                  label="Compare Value"
                  value={config.compareValue || ''}
                  onChange={(e) => updateExpression('conditional', { ...config, compareValue: e.target.value })}
                  disabled={disabled}
                  sx={{ flex: 1 }}
                />
              )}
            </Stack>
            <Stack direction="row" spacing={2}>
              <TextField
                size="small"
                label="Then (if true)"
                value={config.thenValue || ''}
                onChange={(e) => updateExpression('conditional', { ...config, thenValue: e.target.value })}
                disabled={disabled}
                sx={{ flex: 1 }}
              />
              <TextField
                size="small"
                label="Else (if false)"
                value={config.elseValue || ''}
                onChange={(e) => updateExpression('conditional', { ...config, elseValue: e.target.value })}
                disabled={disabled}
                sx={{ flex:  1 }}
              />
            </Stack>
          </Stack>
        )}

        {/* Math Operation */}
        {exprType === 'math' && (
          <Stack spacing={2}>
            <TextField
              select
              size="small"
              label="Operation"
              value={config.operation || 'add'}
              onChange={(e) => updateExpression('math', { ...config, operation: e.target.value })}
              disabled={disabled}
            >
              <MenuItem value="add">Add / Sum</MenuItem>
              <MenuItem value="multiply">Multiply</MenuItem>
              <MenuItem value="divide">Divide</MenuItem>
            </TextField>
            
            {config.operation === 'divide' ? (
              <Stack direction="row" spacing={2}>
                <Autocomplete
                  size="small"
                  options={sourceColumns}
                  value={config.numerator || null}
                  onChange={(_, v) => updateExpression('math', { ...config, numerator: v })}
                  disabled={disabled}
                  renderInput={(params) => <TextField {...params} label="Numerator Column" />}
                  sx={{ flex: 1 }}
                />
                <Autocomplete
                  size="small"
                  options={sourceColumns}
                  value={config.denominator || null}
                  onChange={(_, v) => updateExpression('math', { ...config, denominator: v })}
                  disabled={disabled}
                  renderInput={(params) => <TextField {...params} label="Denominator Column" />}
                  sx={{ flex: 1 }}
                />
              </Stack>
            ) : (
              <>
                <Autocomplete
                  multiple
                  size="small"
                  options={sourceColumns}
                  value={config.columns || []}
                  onChange={(_, v) => updateExpression('math', { ...config, columns: v })}
                  disabled={disabled}
                  renderInput={(params) => <TextField {...params} label="Columns" />}
                />
                <TextField
                  size="small"
                  type="number"
                  label="Constant"
                  value={config.constant ?? (config.operation === 'multiply' ? 1 : 0)}
                  onChange={(e) => updateExpression('math', { ...config, constant: parseFloat(e.target.value) || 0 })}
                  disabled={disabled}
                  helperText={config.operation === 'add' ? 'Added to sum' : 'Multiplied with result'}
                />
              </>
            )}
          </Stack>
        )}

        {/* Lookup/Map */}
        {exprType === 'lookup' && (
          <Stack spacing={2}>
            <Autocomplete
              size="small"
              options={sourceColumns}
              value={config.sourceColumn || null}
              onChange={(_, v) => updateExpression('lookup', { ...config, sourceColumn: v })}
              disabled={disabled}
              renderInput={(params) => <TextField {...params} label="Source Column" />}
            />
            <Typography variant="body2">Value Mapping:</Typography>
            {Object.entries(config.mapping || {}).map(([key, val], idx) => (
              <Stack key={idx} direction="row" spacing={1} alignItems="center">
                <TextField
                  size="small"
                  label="From"
                  value={key}
                  onChange={(e) => {
                    const newMapping = { ...config.mapping }
                    delete newMapping[key]
                    newMapping[e.target.value] = val
                    updateExpression('lookup', { ...config, mapping: newMapping })
                  }}
                  disabled={disabled}
                  sx={{ flex: 1 }}
                />
                <Typography>→</Typography>
                <TextField
                  size="small"
                  label="To"
                  value={val as string}
                  onChange={(e) => {
                    updateExpression('lookup', { 
                      ...config, 
                      mapping: { ...config.mapping, [key]: e.target.value } 
                    })
                  }}
                  disabled={disabled}
                  sx={{ flex: 1 }}
                />
                <IconButton 
                  size="small" 
                  onClick={() => {
                    const newMapping = { ...config.mapping }
                    delete newMapping[key]
                    updateExpression('lookup', { ...config, mapping: newMapping })
                  }}
                  disabled={disabled}
                >
                  <DeleteIcon />
                </IconButton>
              </Stack>
            ))}
            <Button
              size="small"
              startIcon={<AddIcon />}
              onClick={() => {
                updateExpression('lookup', { 
                  ...config, 
                  mapping: { ...config.mapping, '': '' } 
                })
              }}
              disabled={disabled}
            >
              Add Mapping
            </Button>
            <TextField
              size="small"
              label="Default (if no match)"
              value={config.defaultValue || ''}
              onChange={(e) => updateExpression('lookup', { ...config, defaultValue: e.target.value })}
              disabled={disabled}
            />
          </Stack>
        )}

        {/* Template String */}
        {exprType === 'template' && (
          <Stack spacing={2}>
            <TextField
              fullWidth
              size="small"
              label="Template"
              value={config.template || ''}
              onChange={(e) => updateExpression('template', { template: e.target.value })}
              disabled={disabled}
              placeholder="${firstName} ${lastName}"
              helperText="Use ${columnName} to insert column values"
            />
            <Stack direction="row" spacing={1} flexWrap="wrap">
              {sourceColumns.slice(0, 10).map(col => (
                <Chip
                  key={col}
                  label={col}
                  size="small"
                  onClick={() => {
                    updateExpression('template', { 
                      template: (config.template || '') + '${' + col + '}' 
                    })
                  }}
                  disabled={disabled}
                />
              ))}
            </Stack>
          </Stack>
        )}

        {/* Custom JSON */}
        {exprType === 'custom' && (
          <Stack spacing={2}>
            <TextField
              fullWidth
              multiline
              rows={4}
              size="small"
              label="JSON Expression"
              value={config.json || ''}
              onChange={(e) => updateExpression('custom', { json: e.target.value })}
              disabled={disabled}
              placeholder='{"op": "concat", "cols": ["col1", "col2"], "sep": "-"}'
              sx={{ fontFamily: 'monospace' }}
            />
            <Alert severity="info">
              Advanced: Enter a raw JSON expression object.See documentation for available operations.
            </Alert>
          </Stack>
        )}
      </Paper>

      {/* Test Results */}
      {testResults && (
        <Paper variant="outlined" sx={{ p: 2, mt: 2 }}>
          <Typography variant="subtitle2" gutterBottom>Test Results (first 5 rows)</Typography>
          <Box sx={{ fontFamily: 'monospace', fontSize: 12, overflow: 'auto', maxHeight:  150 }}>
            {testResults.slice(0, 5).map((r, i) => (
              <div key={i}>{JSON.stringify(r)}</div>
            ))}
          </Box>
        </Paper>
      )}

      {/* Help Dialog */}
      <Dialog open={showHelp} onClose={() => setShowHelp(false)} maxWidth="md" fullWidth>
        <DialogTitle>Expression Builder Help</DialogTitle>
        <DialogContent dividers>
          <Stack spacing={2}>
            <Typography variant="subtitle2">Available Expression Types:</Typography>
            
            <Box>
              <Typography variant="body2" fontWeight={600}>Direct Copy</Typography>
              <Typography variant="body2" color="text.secondary">
                Copies the value from the source column without any transformation.
              </Typography>
            </Box>
            
            <Box>
              <Typography variant="body2" fontWeight={600}>Simple Transform</Typography>
              <Typography variant="body2" color="text.secondary">
                Apply a single transformation:  convert to number, uppercase, lowercase, trim, boolean, etc.
              </Typography>
            </Box>
            
            <Box>
              <Typography variant="body2" fontWeight={600}>Concatenate</Typography>
              <Typography variant="body2" color="text.secondary">
                Combine multiple columns with an optional separator. Example: "John" + "-" + "Doe" = "John-Doe"
              </Typography>
            </Box>
            
            <Box>
              <Typography variant="body2" fontWeight={600}>Regex Extract</Typography>
              <Typography variant="body2" color="text.secondary">
                Extract parts of a string using regular expressions.Use capture groups () to specify what to extract.
              </Typography>
            </Box>
            
            <Box>
              <Typography variant="body2" fontWeight={600}>Conditional</Typography>
              <Typography variant="body2" color="text.secondary">
                Return different values based on a condition.Example: if status == "active" then "1" else "0"
              </Typography>
            </Box>
            
            <Box>
              <Typography variant="body2" fontWeight={600}>Template String</Typography>
              <Typography variant="body2" color="text.secondary">
                Build a string from multiple columns using {'${columnName}'} syntax. Example: {'${firstName} ${lastName}'}
              </Typography>
            </Box>
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setShowHelp(false)}>Close</Button>
        </DialogActions>
      </Dialog>
    </Box>
  )
}