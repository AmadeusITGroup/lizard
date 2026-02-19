// path: ui-react/src/components/DataWorkbench/PipelineStepCard.tsx
/**
 * PipelineStepCard - Individual step card in the pipeline builder
 * ENHANCED: Added time-window join support for correlating events
 */
import React from 'react'
import {
  Box,
  Paper,
  Typography,
  Stack,
  IconButton,
  Chip,
  Collapse,
  TextField,
  MenuItem,
  Button,
  Autocomplete,
  Tooltip,
  FormControlLabel,
  Switch,
  Alert,
  Divider,
} from '@mui/material'
import {
  ExpandMore as ExpandMoreIcon,
  ExpandLess as ExpandLessIcon,
  Delete as DeleteIcon,
  ArrowUpward as MoveUpIcon,
  ArrowDownward as MoveDownIcon,
  Add as AddIcon,
  Remove as RemoveIcon,
  Help as HelpIcon,
  Schedule as TimeIcon,
} from '@mui/icons-material'
import { PipelineStep, DataSource } from './DataWorkbench'

interface PipelineStepCardProps {
  step: PipelineStep
  index: number
  totalSteps:  number
  availableColumns: string[]
  sourceColumns: string[]
  sources: DataSource[]
  icon: React.ReactNode
  label: string
  onUpdate: (config: Record<string, any>) => void
  onRemove: () => void
  onMoveUp?:  () => void
  onMoveDown?: () => void
  isSource?: boolean
}

// Filter operators
const FILTER_OPERATORS = [
  { value:  'eq', label: '= Equals' },
  { value:  'ne', label: '≠ Not Equals' },
  { value: 'gt', label: '> Greater Than' },
  { value:  'gte', label: '≥ Greater or Equal' },
  { value:  'lt', label: '< Less Than' },
  { value: 'lte', label:  '≤ Less or Equal' },
  { value: 'contains', label: 'Contains' },
  { value: 'startswith', label: 'Starts With' },
  { value:  'endswith', label: 'Ends With' },
  { value: 'in', label: 'In List' },
  { value: 'nin', label: 'Not In List' },
  { value: 'isnull', label: 'Is Null' },
  { value: 'notnull', label: 'Is Not Null' },
  { value: 'regex', label: 'Matches Regex' },
  { value:  'between', label: 'Between' },
]

// Aggregate functions
const AGG_FUNCTIONS = [
  { value: 'count', label: 'Count' },
  { value: 'count_distinct', label: 'Count Distinct' },
  { value: 'sum', label: 'Sum' },
  { value: 'avg', label: 'Average' },
  { value: 'min', label: 'Min' },
  { value: 'max', label: 'Max' },
  { value: 'first', label: 'First' },
  { value: 'last', label: 'Last' },
]

// Join types
const JOIN_TYPES = [
  { value: 'inner', label: 'Inner Join' },
  { value: 'left', label: 'Left Join' },
  { value: 'right', label: 'Right Join' },
  { value:  'outer', label: 'Full Outer Join' },
]

// Time window units
const TIME_UNITS = [
  { value: 'minutes', label: 'Minutes' },
  { value: 'hours', label: 'Hours' },
  { value: 'days', label: 'Days' },
]

// Join condition operators (for advanced conditions)
const JOIN_CONDITION_OPERATORS = [
  { value: 'eq', label: '= Equals' },
  { value: 'gt', label: '> Greater Than' },
  { value: 'gte', label: '≥ Greater or Equal' },
  { value: 'lt', label: '< Less Than' },
  { value: 'lte', label: '≤ Less or Equal' },
  { value: 'between', label: 'Between (time window)' },
]

export default function PipelineStepCard({
  step,
  index,
  totalSteps,
  availableColumns,
  sourceColumns,
  sources,
  icon,
  label,
  onUpdate,
  onRemove,
  onMoveUp,
  onMoveDown,
  isSource = false,
}: PipelineStepCardProps) {
  const [expanded, setExpanded] = React.useState(true)
  const config = step.config || {}

  const updateConfig = (updates:  Record<string, any>) => {
    onUpdate({ ...config, ...updates })
  }

  // Get step summary for collapsed view
  const getStepSummary = (): string => {
    switch (step.type) {
      case 'source':
        return config.table || 'No table selected'
      case 'filter':
        const conditions = config.conditions || []
        return conditions.length > 0 ? `${conditions.length} condition(s)` : 'No conditions'
      case 'select':
        const cols = config.columns || []
        return cols.length > 0 ? `${cols.length} column(s)` : 'All columns'
      case 'join':
        if (! config.table) return 'Select a table'
        const joinConds = config.on || []
        const hasTimeWindow = config.time_window?.enabled
        let summary = `${config.type || 'inner'} join with ${config.table}`
        if (joinConds.length > 0) summary += ` (${joinConds.length} condition(s))`
        if (hasTimeWindow) summary += ` [${config.time_window.value}${config.time_window.unit[0]}]`
        return summary
      case 'aggregate':
        const groupBy = config.group_by || []
        const aggs = config.aggregations || []
        return `Group by ${groupBy.length} field(s), ${aggs.length} aggregation(s)`
      case 'transform':
        const transforms = config.transforms || []
        if (transforms.length === 0 && config.column) {
          return `${config.column} = ${config.expression || ''}`
        }
        return transforms.length > 0 ? `${transforms.length} transform(s)` : 'No transforms'
      case 'sort': 
        const sortBy = config.by || []
        if (sortBy.length === 0 && config.field) {
          return `${config.field} ${config.direction || 'asc'}`
        }
        return sortBy.length > 0 ? `${sortBy.length} sort field(s)` : 'No sorting'
      case 'distinct':
        const distinctCols = config.columns || []
        return distinctCols.length > 0 ? `On ${distinctCols.length} column(s)` : 'All columns'
      case 'rename':
        const mappings = config.mappings || {}
        return `${Object.keys(mappings).length} rename(s)`
      case 'drop':
        const dropCols = config.columns || []
        return `${dropCols.length} column(s)`
      case 'union':
        const tables = config.tables || []
        return `${tables.length} table(s)`
      default:
        return ''
    }
  }

  // Render step-specific configuration UI
  const renderConfig = () => {
    switch (step.type) {
      case 'source':
        return renderSourceConfig()
      case 'filter':
        return renderFilterConfig()
      case 'select': 
        return renderSelectConfig()
      case 'join':
        return renderJoinConfig()
      case 'aggregate':
        return renderAggregateConfig()
      case 'transform':
        return renderTransformConfig()
      case 'sort':
        return renderSortConfig()
      case 'distinct':
        return renderDistinctConfig()
      case 'rename':
        return renderRenameConfig()
      case 'drop':
        return renderDropConfig()
      case 'union':
        return renderUnionConfig()
      default:
        return <Typography color="text.secondary">Unknown step type</Typography>
    }
  }

  // Source configuration
  const renderSourceConfig = () => (
    <TextField
      select
      size="small"
      label="Table"
      value={config.table || ''}
      onChange={(e) => updateConfig({ table: e.target.value })}
      fullWidth
    >
      {sources.filter(s => s.type === 'table').map(s => (
        <MenuItem key={s.name} value={s.name}>
          {s.name} ({s.row_count?.toLocaleString() || '? '} rows)
        </MenuItem>
      ))}
    </TextField>
  )

  // Filter configuration
  const renderFilterConfig = () => {
    const conditions = config.conditions || []

    const addCondition = () => {
      updateConfig({
        conditions: [...conditions, { field: '', op: 'eq', value: '' }]
      })
    }

    const updateCondition = (idx: number, updates: any) => {
      const newConditions = conditions.map((c:  any, i: number) =>
        i === idx ? { ...c, ...updates } : c
      )
      updateConfig({ conditions:  newConditions })
    }

    const removeCondition = (idx: number) => {
      updateConfig({ conditions: conditions.filter((_: any, i: number) => i !== idx) })
    }

    return (
      <Stack spacing={1}>
        {conditions.map((cond: any, idx: number) => (
          <Stack key={idx} direction="row" spacing={1} alignItems="center">
            <Autocomplete
              size="small"
              options={availableColumns}
              value={cond.field || null}
              onChange={(_, v) => updateCondition(idx, { field: v })}
              renderInput={(params) => <TextField {...params} label="Field" />}
              sx={{ minWidth: 150 }}
              freeSolo
            />
            <TextField
              select
              size="small"
              label="Op"
              value={cond.op || 'eq'}
              onChange={(e) => updateCondition(idx, { op: e.target.value })}
              sx={{ minWidth: 130 }}
            >
              {FILTER_OPERATORS.map(op => (
                <MenuItem key={op.value} value={op.value}>{op.label}</MenuItem>
              ))}
            </TextField>
            {! ['isnull', 'notnull'].includes(cond.op) && (
              <TextField
                size="small"
                label="Value"
                value={cond.value || ''}
                onChange={(e) => updateCondition(idx, { value: e.target.value })}
                sx={{ flex: 1 }}
              />
            )}
            <IconButton size="small" onClick={() => removeCondition(idx)}>
              <RemoveIcon fontSize="small" />
            </IconButton>
          </Stack>
        ))}
        <Button size="small" startIcon={<AddIcon />} onClick={addCondition}>
          Add Condition
        </Button>
      </Stack>
    )
  }

  // Select configuration
  const renderSelectConfig = () => (
    <Autocomplete
      multiple
      size="small"
      options={availableColumns}
      value={config.columns || []}
      onChange={(_, v) => updateConfig({ columns: v })}
      renderInput={(params) => <TextField {...params} label="Columns to keep" />}
      fullWidth
    />
  )

  // ============================================================
  // ENHANCED JOIN CONFIGURATION with Time Window Support
  // ============================================================
  const renderJoinConfig = () => {
    const joinTable = config.table || ''
    const joinType = config.type || 'left'
    const joinConditions = config.on || []
    const timeWindow = config.time_window || { enabled: false, left_col: '', right_col: '', value: 1, unit: 'hours', direction: 'after' }

    // Get columns for the selected join table
    const joinTableSource = sources.find(s => s.name === joinTable)
    const joinTableColumns = joinTableSource?.columns
      ? Object.keys(joinTableSource.columns)
      : []

    // All columns (current + join table) for autocomplete
    const allColumns = [...new Set([...availableColumns, ...joinTableColumns])]

    const addJoinCondition = () => {
      updateConfig({
        on: [...joinConditions, { left:  '', right: '', op: 'eq' }]
      })
    }

    const updateJoinCondition = (idx: number, updates: any) => {
      const newConditions = joinConditions.map((c: any, i: number) =>
        i === idx ? { ...c, ...updates } : c
      )
      updateConfig({ on: newConditions })
    }

    const removeJoinCondition = (idx: number) => {
      updateConfig({ on: joinConditions.filter((_: any, i: number) => i !== idx) })
    }

    const updateTimeWindow = (updates: any) => {
      updateConfig({ time_window: { ...timeWindow, ...updates } })
    }

    return (
      <Stack spacing={3}>
        {/* Basic Join Settings */}
        <Stack direction="row" spacing={2}>
          <TextField
            select
            size="small"
            label="Join with table"
            value={joinTable}
            onChange={(e) => updateConfig({ table: e.target.value })}
            sx={{ flex: 1 }}
          >
            <MenuItem value="">-- Select table --</MenuItem>
            {sources.filter(s => s.type === 'table').map(s => (
              <MenuItem key={s.name} value={s.name}>
                {s.name} ({s.row_count?.toLocaleString() || '?'} rows)
              </MenuItem>
            ))}
          </TextField>
          <TextField
            select
            size="small"
            label="Join Type"
            value={joinType}
            onChange={(e) => updateConfig({ type: e.target.value })}
            sx={{ minWidth: 150 }}
          >
            {JOIN_TYPES.map(jt => (
              <MenuItem key={jt.value} value={jt.value}>{jt.label}</MenuItem>
            ))}
          </TextField>
        </Stack>

        {/* Standard Join Conditions */}
        <Box>
          <Typography variant="subtitle2" gutterBottom sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <span>Join Conditions</span>
            <Chip label="Match rows where..." size="small" variant="outlined" />
          </Typography>
          <Stack spacing={1}>
            {joinConditions.map((cond: any, idx: number) => (
              <Stack key={idx} direction="row" spacing={1} alignItems="center">
                <Autocomplete
                  size="small"
                  options={availableColumns}
                  value={cond.left || null}
                  onChange={(_, v) => updateJoinCondition(idx, { left: v })}
                  renderInput={(params) => <TextField {...params} label="Left column (base table)" placeholder="e.g., user_id" />}
                  sx={{ flex: 1 }}
                  freeSolo
                />
                <Typography sx={{ color: 'primary.main', fontWeight: 600 }}>=</Typography>
                <Autocomplete
                  size="small"
                  options={joinTableColumns.length > 0 ? joinTableColumns : allColumns}
                  value={cond.right || null}
                  onChange={(_, v) => updateJoinCondition(idx, { right: v })}
                  renderInput={(params) => (
                    <TextField
                      {...params}
                      label={`Right column${joinTable ? ` (${joinTable})` : ''}`}
                      placeholder="e.g., user_id"
                    />
                  )}
                  sx={{ flex: 1 }}
                  freeSolo
                />
                <IconButton size="small" onClick={() => removeJoinCondition(idx)} color="error">
                  <RemoveIcon fontSize="small" />
                </IconButton>
              </Stack>
            ))}
            <Button size="small" startIcon={<AddIcon />} onClick={addJoinCondition} variant="outlined">
              Add Join Condition
            </Button>
          </Stack>
        </Box>

        <Divider />

        {/* TIME WINDOW JOIN - NEW FEATURE */}
        <Box>
          <Stack direction="row" alignItems="center" spacing={1} mb={1}>
            <TimeIcon color="primary" />
            <Typography variant="subtitle2">Time Window Join</Typography>
            <Tooltip title="Match rows from the right table that fall within a time window relative to the left table's timestamp. Essential for correlating events like 'auth before booking'.">
              <HelpIcon fontSize="small" color="action" />
            </Tooltip>
          </Stack>

          <FormControlLabel
            control={
              <Switch
                checked={timeWindow.enabled}
                onChange={(e) => updateTimeWindow({ enabled: e.target.checked })}
                color="primary"
              />
            }
            label="Enable time window matching"
          />

          {timeWindow.enabled && (
            <Paper variant="outlined" sx={{ p: 2, mt: 1, bgcolor: 'action.hover' }}>
              <Stack spacing={2}>
                {/* Timestamp columns */}
                <Stack direction="row" spacing={2}>
                  <Autocomplete
                    size="small"
                    options={availableColumns.filter(c => c.toLowerCase().includes('ts') || c.toLowerCase().includes('time') || c.toLowerCase().includes('date'))}
                    value={timeWindow.left_col || null}
                    onChange={(_, v) => updateTimeWindow({ left_col: v })}
                    renderInput={(params) => <TextField {...params} label="Base table timestamp" placeholder="e.g., ts" />}
                    sx={{ flex: 1 }}
                    freeSolo
                  />
                  <Autocomplete
                    size="small"
                    options={(joinTableColumns.length > 0 ? joinTableColumns : allColumns).filter(c => 
                      c.toLowerCase().includes('ts') || c.toLowerCase().includes('time') || c.toLowerCase().includes('date')
                    )}
                    value={timeWindow.right_col || null}
                    onChange={(_, v) => updateTimeWindow({ right_col:  v })}
                    renderInput={(params) => <TextField {...params} label={`${joinTable || 'Join table'} timestamp`} placeholder="e.g., ts" />}
                    sx={{ flex: 1 }}
                    freeSolo
                  />
                </Stack>

                {/* Time window configuration */}
                <Stack direction="row" spacing={2} alignItems="center">
                  <Typography variant="body2" color="text.secondary" sx={{ minWidth: 200 }}>
                    Match {joinTable || 'right table'} rows where timestamp is: 
                  </Typography>
                  <TextField
                    select
                    size="small"
                    value={timeWindow.direction || 'after'}
                    onChange={(e) => updateTimeWindow({ direction: e.target.value })}
                    sx={{ minWidth: 180 }}
                  >
                    <MenuItem value="after">After (within next)</MenuItem>
                    <MenuItem value="before">Before (within previous)</MenuItem>
                    <MenuItem value="around">Around (±)</MenuItem>
                  </TextField>
                  <TextField
                    size="small"
                    type="number"
                    label="Duration"
                    value={timeWindow.value || 1}
                    onChange={(e) => updateTimeWindow({ value: parseInt(e.target.value) || 1 })}
                    sx={{ width: 100 }}
                    inputProps={{ min: 1 }}
                  />
                  <TextField
                    select
                    size="small"
                    value={timeWindow.unit || 'hours'}
                    onChange={(e) => updateTimeWindow({ unit: e.target.value })}
                    sx={{ minWidth: 100 }}
                  >
                    {TIME_UNITS.map(u => (
                      <MenuItem key={u.value} value={u.value}>{u.label}</MenuItem>
                    ))}
                  </TextField>
                </Stack>

                {/* Preview of the condition */}
                {timeWindow.left_col && timeWindow.right_col && (
                  <Alert severity="info" icon={false}>
                    <Typography variant="body2" fontFamily="monospace" fontSize={12}>
                      {timeWindow.direction === 'after' && (
                        <>
                          <strong>{joinTable}.{timeWindow.right_col}</strong> BETWEEN{' '}
                          <strong>base.{timeWindow.left_col}</strong> AND{' '}
                          <strong>base.{timeWindow.left_col}</strong> + {timeWindow.value} {timeWindow.unit}
                        </>
                      )}
                      {timeWindow.direction === 'before' && (
                        <>
                          <strong>{joinTable}.{timeWindow.right_col}</strong> BETWEEN{' '}
                          <strong>base.{timeWindow.left_col}</strong> - {timeWindow.value} {timeWindow.unit} AND{' '}
                          <strong>base.{timeWindow.left_col}</strong>
                        </>
                      )}
                      {timeWindow.direction === 'around' && (
                        <>
                          <strong>{joinTable}.{timeWindow.right_col}</strong> BETWEEN{' '}
                          <strong>base.{timeWindow.left_col}</strong> - {timeWindow.value} {timeWindow.unit} AND{' '}
                          <strong>base.{timeWindow.left_col}</strong> + {timeWindow.value} {timeWindow.unit}
                        </>
                      )}
                    </Typography>
                  </Alert>
                )}

                {/* Common presets */}
                <Box>
                  <Typography variant="caption" color="text.secondary" gutterBottom display="block">
                    Quick presets:
                  </Typography>
                  <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
                    <Chip
                      label="1 hour after"
                      size="small"
                      onClick={() => updateTimeWindow({ value:  1, unit: 'hours', direction: 'after' })}
                      variant={timeWindow.value === 1 && timeWindow.unit === 'hours' && timeWindow.direction === 'after' ? 'filled' : 'outlined'}
                      color="primary"
                    />
                    <Chip
                      label="30 min after"
                      size="small"
                      onClick={() => updateTimeWindow({ value: 30, unit: 'minutes', direction: 'after' })}
                      variant={timeWindow.value === 30 && timeWindow.unit === 'minutes' && timeWindow.direction === 'after' ? 'filled' : 'outlined'}
                    />
                    <Chip
                      label="1 day around"
                      size="small"
                      onClick={() => updateTimeWindow({ value: 1, unit: 'days', direction: 'around' })}
                      variant={timeWindow.value === 1 && timeWindow.unit === 'days' && timeWindow.direction === 'around' ? 'filled' : 'outlined'}
                    />
                    <Chip
                      label="15 min before"
                      size="small"
                      onClick={() => updateTimeWindow({ value: 15, unit: 'minutes', direction: 'before' })}
                      variant={timeWindow.value === 15 && timeWindow.unit === 'minutes' && timeWindow.direction === 'before' ? 'filled' : 'outlined'}
                    />
                  </Stack>
                </Box>
              </Stack>
            </Paper>
          )}
        </Box>

        {/* Validation messages */}
        {!joinTable && (
          <Alert severity="warning" variant="outlined">
            Please select a table to join with
          </Alert>
        )}
        {joinTable && joinConditions.length === 0 && ! timeWindow.enabled && (
          <Alert severity="warning" variant="outlined">
            Add at least one join condition or enable time window matching
          </Alert>
        )}
        {timeWindow.enabled && (! timeWindow.left_col || !timeWindow.right_col) && (
          <Alert severity="warning" variant="outlined">
            Select timestamp columns for both tables to use time window join
          </Alert>
        )}
      </Stack>
    )
  }

  // Aggregate configuration - FIXED
  const renderAggregateConfig = () => {
    const groupBy = config.group_by || []
    const aggregations = config.aggregations || []

    const addAggregation = () => {
      updateConfig({
        aggregations: [...aggregations, { name: '', function: 'count', field: '' }]
      })
    }

    const updateAggregation = (idx: number, updates: any) => {
      const newAggs = aggregations.map((a: any, i: number) =>
        i === idx ? { ...a, ...updates } : a
      )
      updateConfig({ aggregations: newAggs })
    }

    const removeAggregation = (idx: number) => {
      updateConfig({ aggregations:  aggregations.filter((_: any, i: number) => i !== idx) })
    }

    return (
      <Stack spacing={2}>
        {/* Group By */}
        <Box>
          <Typography variant="caption" color="text.secondary" gutterBottom display="block">
            Group by fields (rows will be combined):
          </Typography>
          <Autocomplete
            multiple
            size="small"
            options={availableColumns}
            value={groupBy}
            onChange={(_, v) => updateConfig({ group_by: v })}
            renderInput={(params) => <TextField {...params} label="Group by fields" placeholder="Select fields..." />}
            fullWidth
          />
        </Box>

        {/* Aggregations */}
        <Box>
          <Typography variant="caption" color="text.secondary" gutterBottom display="block">
            Aggregations (calculations to perform):
          </Typography>
          <Stack spacing={1}>
            {aggregations.map((agg: any, idx: number) => (
              <Stack key={idx} direction="row" spacing={1} alignItems="center">
                <TextField
                  size="small"
                  label="Output column name"
                  value={agg.name || ''}
                  onChange={(e) => updateAggregation(idx, { name: e.target.value })}
                  placeholder="e.g., total_amount"
                  sx={{ minWidth: 150 }}
                />
                <TextField
                  select
                  size="small"
                  label="Function"
                  value={agg.function || 'count'}
                  onChange={(e) => updateAggregation(idx, { function: e.target.value })}
                  sx={{ minWidth: 130 }}
                >
                  {AGG_FUNCTIONS.map(fn => (
                    <MenuItem key={fn.value} value={fn.value}>{fn.label}</MenuItem>
                  ))}
                </TextField>
                {! ['count'].includes(agg.function) && (
                  <Autocomplete
                    size="small"
                    options={availableColumns}
                    value={agg.field || null}
                    onChange={(_, v) => updateAggregation(idx, { field: v })}
                    renderInput={(params) => <TextField {...params} label="Field" />}
                    sx={{ flex: 1 }}
                    freeSolo
                  />
                )}
                <IconButton size="small" onClick={() => removeAggregation(idx)}>
                  <RemoveIcon fontSize="small" />
                </IconButton>
              </Stack>
            ))}
            <Button size="small" startIcon={<AddIcon />} onClick={addAggregation}>
              Add Aggregation
            </Button>
          </Stack>
        </Box>

        {/* Help text */}
        {groupBy.length === 0 && aggregations.length === 0 && (
          <Typography variant="caption" color="text.secondary">
            Select fields to group by, then add aggregations like COUNT, SUM, AVG, etc.
          </Typography>
        )}
      </Stack>
    )
  }

  // Transform configuration - FIXED
  const renderTransformConfig = () => {
    const transforms = config.transforms || []

    const addTransform = () => {
      updateConfig({
        transforms: [...transforms, { column: '', expression: '' }]
      })
    }

    const updateTransform = (idx: number, updates:  any) => {
      const newTransforms = transforms.map((t: any, i: number) =>
        i === idx ? { ...t, ...updates } : t
      )
      updateConfig({ transforms: newTransforms })
    }

    const removeTransform = (idx: number) => {
      updateConfig({ transforms: transforms.filter((_: any, i: number) => i !== idx) })
    }

    return (
      <Stack spacing={1}>
        <Typography variant="caption" color="text.secondary">
          Add computed columns using expressions like:  UPPER(name), price * 1.2, CONCAT(first, " ", last)
        </Typography>
        {transforms.map((t: any, idx: number) => (
          <Stack key={idx} direction="row" spacing={1} alignItems="center">
            <TextField
              size="small"
              label="New column name"
              value={t.column || ''}
              onChange={(e) => updateTransform(idx, { column: e.target.value })}
              sx={{ minWidth: 150 }}
              placeholder="e.g., full_name"
            />
            <Typography>=</Typography>
            <TextField
              size="small"
              label="Expression"
              value={t.expression || ''}
              onChange={(e) => updateTransform(idx, { expression: e.target.value })}
              sx={{ flex: 1 }}
              placeholder="e.g., CONCAT(first_name, ' ', last_name)"
            />
            <IconButton size="small" onClick={() => removeTransform(idx)}>
              <RemoveIcon fontSize="small" />
            </IconButton>
          </Stack>
        ))}
        <Button size="small" startIcon={<AddIcon />} onClick={addTransform}>
          Add Transform
        </Button>
      </Stack>
    )
  }

  // Sort configuration
  const renderSortConfig = () => {
    const sortBy = config.by || []

    const addSort = () => {
      updateConfig({
        by: [...sortBy, { field: '', direction: 'asc' }]
      })
    }

    const updateSort = (idx: number, updates: any) => {
      const newSorts = sortBy.map((s: any, i: number) =>
        i === idx ? { ...s, ...updates } : s
      )
      updateConfig({ by:  newSorts })
    }

    const removeSort = (idx:  number) => {
      updateConfig({ by: sortBy.filter((_: any, i: number) => i !== idx) })
    }

    return (
      <Stack spacing={1}>
        {sortBy.map((sort: any, idx: number) => (
          <Stack key={idx} direction="row" spacing={1} alignItems="center">
            <Autocomplete
              size="small"
              options={availableColumns}
              value={sort.field || null}
              onChange={(_, v) => updateSort(idx, { field: v })}
              renderInput={(params) => <TextField {...params} label="Field" />}
              sx={{ flex: 1 }}
              freeSolo
            />
            <TextField
              select
              size="small"
              label="Direction"
              value={sort.direction || 'asc'}
              onChange={(e) => updateSort(idx, { direction: e.target.value })}
              sx={{ minWidth: 120 }}
            >
              <MenuItem value="asc">Ascending</MenuItem>
              <MenuItem value="desc">Descending</MenuItem>
            </TextField>
            <IconButton size="small" onClick={() => removeSort(idx)}>
              <RemoveIcon fontSize="small" />
            </IconButton>
          </Stack>
        ))}
        <Button size="small" startIcon={<AddIcon />} onClick={addSort}>
          Add Sort Field
        </Button>
      </Stack>
    )
  }

  // Distinct configuration
  const renderDistinctConfig = () => (
    <Box>
      <Typography variant="caption" color="text.secondary" gutterBottom display="block">
        Select columns to check for duplicates (leave empty for all columns):
      </Typography>
      <Autocomplete
        multiple
        size="small"
        options={availableColumns}
        value={config.columns || []}
        onChange={(_, v) => updateConfig({ columns:  v })}
        renderInput={(params) => <TextField {...params} label="Columns for uniqueness" />}
        fullWidth
      />
    </Box>
  )

  // Rename configuration
  const renderRenameConfig = () => {
    const mappings = config.mappings || {}
    const entries = Object.entries(mappings)

    const addMapping = () => {
      updateConfig({
        mappings: { ...mappings, '': '' }
      })
    }

    const updateMapping = (oldKey: string, newKey: string, newValue: string) => {
      const newMappings = { ...mappings }
      if (oldKey !== newKey) {
        delete newMappings[oldKey]
      }
      newMappings[newKey] = newValue
      updateConfig({ mappings: newMappings })
    }

    const removeMapping = (key: string) => {
      const newMappings = { ...mappings }
      delete newMappings[key]
      updateConfig({ mappings: newMappings })
    }

    return (
      <Stack spacing={1}>
        {entries.map(([oldName, newName], idx) => (
          <Stack key={idx} direction="row" spacing={1} alignItems="center">
            <Autocomplete
              size="small"
              options={availableColumns}
              value={oldName || null}
              onChange={(_, v) => updateMapping(oldName, v || '', newName as string)}
              renderInput={(params) => <TextField {...params} label="Current name" />}
              sx={{ flex: 1 }}
              freeSolo
            />
            <Typography>→</Typography>
            <TextField
              size="small"
              label="New name"
              value={newName as string}
              onChange={(e) => updateMapping(oldName, oldName, e.target.value)}
              sx={{ flex: 1 }}
            />
            <IconButton size="small" onClick={() => removeMapping(oldName)}>
              <RemoveIcon fontSize="small" />
            </IconButton>
          </Stack>
        ))}
        <Button size="small" startIcon={<AddIcon />} onClick={addMapping}>
          Add Rename
        </Button>
      </Stack>
    )
  }

  // Drop configuration
  const renderDropConfig = () => (
    <Autocomplete
      multiple
      size="small"
      options={availableColumns}
      value={config.columns || []}
      onChange={(_, v) => updateConfig({ columns: v })}
      renderInput={(params) => <TextField {...params} label="Columns to drop" />}
      fullWidth
    />
  )

  // Union configuration
  const renderUnionConfig = () => (
    <Box>
      <Typography variant="caption" color="text.secondary" gutterBottom display="block">
        Select tables to combine with the current result (must have matching columns):
      </Typography>
      <Autocomplete
        multiple
        size="small"
        options={sources.filter(s => s.type === 'table').map(s => s.name)}
        value={config.tables || []}
        onChange={(_, v) => updateConfig({ tables: v })}
        renderInput={(params) => <TextField {...params} label="Tables to union" />}
        fullWidth
      />
    </Box>
  )

  return (
    <Paper
      variant="outlined"
      sx={{
        borderColor: isSource ? 'primary.main' :  'divider',
        borderWidth: isSource ? 2 : 1,
      }}
    >
      {/* Header */}
      <Stack
        direction="row"
        alignItems="center"
        spacing={1}
        sx={{
          px: 2,
          py:  1,
          bgcolor: isSource ? 'primary.50' : 'transparent',
          cursor: 'pointer',
        }}
        onClick={() => setExpanded(! expanded)}
      >
        {icon}
        <Typography variant="subtitle2" sx={{ flex: 1 }}>
          {label}
        </Typography>
        <Chip
          label={getStepSummary()}
          size="small"
          variant="outlined"
          sx={{ maxWidth: 300 }}
        />
        {! isSource && (
          <>
            <Tooltip title="Move up">
              <span>
                <IconButton
                  size="small"
                  onClick={(e) => { e.stopPropagation(); onMoveUp?.() }}
                  disabled={index <= 1}
                >
                  <MoveUpIcon fontSize="small" />
                </IconButton>
              </span>
            </Tooltip>
            <Tooltip title="Move down">
              <span>
                <IconButton
                  size="small"
                  onClick={(e) => { e.stopPropagation(); onMoveDown?.() }}
                  disabled={index >= totalSteps - 1}
                >
                  <MoveDownIcon fontSize="small" />
                </IconButton>
              </span>
            </Tooltip>
            <Tooltip title="Remove step">
              <IconButton
                size="small"
                onClick={(e) => { e.stopPropagation(); onRemove() }}
                color="error"
              >
                <DeleteIcon fontSize="small" />
              </IconButton>
            </Tooltip>
          </>
        )}
        <IconButton size="small">
          {expanded ? <ExpandLessIcon /> : <ExpandMoreIcon />}
        </IconButton>
      </Stack>

      {/* Expanded content */}
      <Collapse in={expanded}>
        <Box sx={{ p: 2, pt: 1, borderTop: '1px solid', borderColor: 'divider' }}>
          {renderConfig()}
        </Box>
      </Collapse>
    </Paper>
  )
}