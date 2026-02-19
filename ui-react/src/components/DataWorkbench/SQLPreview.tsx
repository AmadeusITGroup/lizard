// path: ui-react/src/components/DataWorkbench/SQLPreview.tsx
/**
 * SQLEditor - Editable SQL editor with ability to run queries
 */
import React from 'react'
import {
  Box,
  Typography,
  Stack,
  Tabs,
  Tab,
  Paper,
  IconButton,
  Tooltip,
  Snackbar,
  Alert,
  Button,
  CircularProgress,
  Divider,
  TextField,
} from '@mui/material'
import {
  ContentCopy as CopyIcon,
  PlayArrow as RunIcon,
  Refresh as ResetIcon,
  Code as CodeIcon,
} from '@mui/icons-material'
import { PipelineStep } from './DataWorkbench'

interface SQLPreviewProps {
  pipeline: PipelineStep[]
  onRunSQL?:  (sql: string) => Promise<void>
  isRunning?: boolean
}

export default function SQLPreview({ pipeline, onRunSQL, isRunning = false }: SQLPreviewProps) {
  const [tab, setTab] = React.useState(0)
  const [copied, setCopied] = React.useState(false)
  const [sqlContent, setSqlContent] = React.useState('')
  const [jsonContent, setJsonContent] = React.useState('')
  const [isEdited, setIsEdited] = React.useState(false)

  // Generate pseudo-SQL from pipeline
  const generateSQL = React.useCallback((): string => {
    if (pipeline.length === 0) return '-- No pipeline defined\n-- Select a data source to start'

    let fromClause = ''
    const joins:  string[] = []
    const wheres: string[] = []
    let selects: string[] = ['*']
    const groupBys: string[] = []
    const orderBys: string[] = []
    const transforms: string[] = []
    let limitClause = ''
    let isDistinct = false

    for (const step of pipeline) {
      const config = step.config

      switch (step.type) {
        case 'source':
          fromClause = config.table || 'unknown_table'
          break

        case 'filter':
          const conditions = config.conditions || []
          for (const cond of conditions) {
            if (! cond.field) continue
            const field = cond.field
            const op = cond.op || 'eq'
            const value = cond.value

            let whereClause = ''
            switch (op) {
              case 'eq':
                whereClause = `${field} = '${value}'`
                break
              case 'ne':
                whereClause = `${field} != '${value}'`
                break
              case 'gt': 
                whereClause = `${field} > ${value}`
                break
              case 'gte':
                whereClause = `${field} >= ${value}`
                break
              case 'lt':
                whereClause = `${field} < ${value}`
                break
              case 'lte':
                whereClause = `${field} <= ${value}`
                break
              case 'contains':
                whereClause = `${field} LIKE '%${value}%'`
                break
              case 'startswith':
                whereClause = `${field} LIKE '${value}%'`
                break
              case 'endswith': 
                whereClause = `${field} LIKE '%${value}'`
                break
              case 'isnull':
                whereClause = `${field} IS NULL`
                break
              case 'notnull':
                whereClause = `${field} IS NOT NULL`
                break
              case 'in':
                const inValues = Array.isArray(value) ? value.map(v => `'${v}'`).join(', ') : `'${value}'`
                whereClause = `${field} IN (${inValues})`
                break
              case 'between':
                if (Array.isArray(value) && value.length === 2) {
                  whereClause = `${field} BETWEEN ${value[0]} AND ${value[1]}`
                }
                break
              default:
                whereClause = `${field} ${op} '${value}'`
            }
            if (whereClause) wheres.push(whereClause)
          }
          break

        case 'select':
          if (config.columns && config.columns.length > 0) {
            selects = config.columns
          }
          break

        case 'join':
          if (config.table) {
            const joinType = (config.type || 'inner').toUpperCase()
            const joinTable = config.table
            const joinConditions = config.on || []
            const onClauses = joinConditions
              .filter((c: any) => c.left && c.right)
              .map((c: any) => `t1.${c.left} = ${joinTable}.${c.right}`)
              .join(' AND ')
            if (onClauses) {
              joins.push(`${joinType} JOIN ${joinTable} ON ${onClauses}`)
            }
          }
          break

        case 'aggregate':
          const groupByFields = config.group_by || []
          const aggregations = config.aggregations || []

          if (groupByFields.length > 0) {
            groupBys.push(...groupByFields)
            selects = [...groupByFields]
          }

          for (const agg of aggregations) {
            if (agg.column) {
              const func = (agg.func || 'count').toUpperCase()
              const field = agg.field || '*'
              const alias = agg.column
              selects.push(`${func}(${field}) AS ${alias}`)
            }
          }
          break

        case 'transform':
          const transformsList = config.transforms || []
          for (const t of transformsList) {
            if (t.column && t.expression) {
              transforms.push(`${t.expression} AS ${t.column}`)
            }
          }
          break

        case 'sort':
          const sortBy = config.by || []
          for (const s of sortBy) {
            if (s.field) {
              const dir = (s.direction || 'asc').toUpperCase()
              orderBys.push(`${s.field} ${dir}`)
            }
          }
          break

        case 'limit':
          limitClause = `LIMIT ${config.n || 1000}`
          break

        case 'distinct':
          isDistinct = true
          const distinctCols = config.columns || []
          if (distinctCols.length > 0) {
            selects = distinctCols
          }
          break

        case 'drop':
          const dropCols = config.columns || []
          if (dropCols.length > 0 && selects[0] !== '*') {
            selects = selects.filter(s => !dropCols.includes(s))
          }
          break

        case 'rename':
          const renameMappings = config.mappings || {}
          for (const [from, to] of Object.entries(renameMappings)) {
            if (from && to && ! from.startsWith('__new_')) {
              const idx = selects.indexOf(from)
              if (idx >= 0) {
                selects[idx] = `${from} AS ${to}`
              }
            }
          }
          break
      }
    }

    // Build SQL
    const distinctKeyword = isDistinct ? 'DISTINCT ' : ''
    const selectClause = selects.length > 0 ? selects.join(',\n       ') : '*'
    const transformClause = transforms.length > 0 ? ',\n       ' + transforms.join(',\n       ') : ''

    // Add table alias if there are joins
    const tableAlias = joins.length > 0 ? ' t1' : ''

    let sql = `SELECT ${distinctKeyword}${selectClause}${transformClause}\nFROM   ${fromClause}${tableAlias}`

    if (joins.length > 0) {
      sql += '\n' + joins.join('\n')
    }

    if (wheres.length > 0) {
      sql += '\nWHERE  ' + wheres.join('\n   AND ')
    }

    if (groupBys.length > 0) {
      sql += '\nGROUP BY ' + groupBys.join(', ')
    }

    if (orderBys.length > 0) {
      sql += '\nORDER BY ' + orderBys.join(', ')
    }

    if (limitClause) {
      sql += '\n' + limitClause
    } else {
      sql += '\nLIMIT 1000'
    }

    sql += ';'

    return sql
  }, [pipeline])

  // Generate JSON representation
  const generateJSON = React.useCallback((): string => {
    return JSON.stringify(
      pipeline.map(s => ({ type: s.type, config: s.config })),
      null,
      2
    )
  }, [pipeline])

  // Update content when pipeline changes
  React.useEffect(() => {
    if (!isEdited) {
      setSqlContent(generateSQL())
      setJsonContent(generateJSON())
    }
  }, [pipeline, generateSQL, generateJSON, isEdited])

  // Initialize on mount
  React.useEffect(() => {
    setSqlContent(generateSQL())
    setJsonContent(generateJSON())
  }, [])

  const handleCopy = () => {
    const content = tab === 0 ? sqlContent :  jsonContent
    navigator.clipboard.writeText(content)
    setCopied(true)
  }

  const handleReset = () => {
    setSqlContent(generateSQL())
    setJsonContent(generateJSON())
    setIsEdited(false)
  }

  const handleRun = async () => {
    if (onRunSQL) {
      await onRunSQL(sqlContent)
    }
  }

  const handleSQLChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    setSqlContent(e.target.value)
    setIsEdited(true)
  }

  return (
    <Box sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      {/* Header */}
      <Stack direction="row" justifyContent="space-between" alignItems="center" mb={2}>
        <Tabs value={tab} onChange={(_, v) => setTab(v)}>
          <Tab label="SQL Editor" icon={<CodeIcon />} iconPosition="start" />
          <Tab label="Pipeline JSON" />
        </Tabs>
        <Stack direction="row" spacing={1}>
          {isEdited && (
            <Tooltip title="Reset to generated SQL">
              <Button
                size="small"
                startIcon={<ResetIcon />}
                onClick={handleReset}
                color="warning"
              >
                Reset
              </Button>
            </Tooltip>
          )}
          <Tooltip title="Copy to clipboard">
            <IconButton onClick={handleCopy}>
              <CopyIcon />
            </IconButton>
          </Tooltip>
        </Stack>
      </Stack>

      {/* SQL Editor Tab */}
      {tab === 0 && (
        <Box sx={{ flex: 1, display: 'flex', flexDirection:  'column' }}>
          {/* Editable SQL */}
          <Paper
            variant="outlined"
            sx={{
              flex: 1,
              bgcolor: 'grey.900',
              borderRadius: 1,
              overflow: 'hidden',
              display: 'flex',
              flexDirection: 'column',
            }}
          >
            <Box
              component="textarea"
              value={sqlContent}
              onChange={handleSQLChange}
              sx={{
                flex: 1,
                width: '100%',
                p: 2,
                m: 0,
                border: 'none',
                outline: 'none',
                resize: 'none',
                bgcolor: 'transparent',
                color: 'grey.100',
                fontFamily: '"Fira Code", "Consolas", "Monaco", monospace',
                fontSize:  13,
                lineHeight: 1.6,
                '&::placeholder': {
                  color: 'grey.500',
                },
              }}
              placeholder="-- Enter your SQL query here..."
              spellCheck={false}
            />
          </Paper>

          {/* Run Button */}
          <Stack direction="row" spacing={2} alignItems="center" mt={2}>
            <Button
              variant="contained"
              startIcon={isRunning ? <CircularProgress size={18} color="inherit" /> : <RunIcon />}
              onClick={handleRun}
              disabled={isRunning || ! sqlContent.trim() || !onRunSQL}
            >
              {isRunning ? 'Running...' : 'Run Query'}
            </Button>
            
            {isEdited && (
              <Typography variant="caption" color="warning.main">
                ⚠️ SQL has been manually edited
              </Typography>
            )}
            
            {! onRunSQL && (
              <Typography variant="caption" color="text.secondary">
                SQL execution not available in this view
              </Typography>
            )}
          </Stack>

          {/* Help */}
          <Typography variant="caption" color="text.secondary" sx={{ mt: 1 }}>
            Edit the SQL query above and click "Run Query" to execute. 
            Use "Reset" to regenerate from the visual pipeline.
          </Typography>
        </Box>
      )}

      {/* JSON Tab - Read Only */}
      {tab === 1 && (
        <Box sx={{ flex: 1, display: 'flex', flexDirection:  'column' }}>
          <Paper
            variant="outlined"
            sx={{
              flex:  1,
              p: 2,
              bgcolor:  'grey.900',
              borderRadius: 1,
              overflow: 'auto',
            }}
          >
            <Box
              component="pre"
              sx={{
                m: 0,
                color: 'grey.100',
                fontFamily: '"Fira Code", "Consolas", "Monaco", monospace',
                fontSize: 13,
                lineHeight: 1.6,
                whiteSpace: 'pre-wrap',
              }}
            >
              {jsonContent}
            </Box>
          </Paper>
          <Typography variant="caption" color="text.secondary" sx={{ mt: 1 }}>
            This is the pipeline configuration in JSON format (read-only).
          </Typography>
        </Box>
      )}

      <Snackbar
        open={copied}
        autoHideDuration={2000}
        onClose={() => setCopied(false)}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
      >
        <Alert severity="success" variant="filled">
          Copied to clipboard! 
        </Alert>
      </Snackbar>
    </Box>
  )
}