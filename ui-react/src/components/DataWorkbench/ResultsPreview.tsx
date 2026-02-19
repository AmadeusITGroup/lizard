// path: ui-react/src/components/DataWorkbench/ResultsPreview.tsx
/**
 * ResultsPreview - Live preview of pipeline results
 */
import React from 'react'
import {
  Box,
  Typography,
  Stack,
  Chip,
  CircularProgress,
  Alert,
  Tooltip,
  IconButton,
  Popover,
} from '@mui/material'
import { DataGrid, GridColDef, GridToolbar } from '@mui/x-data-grid'
import {
  Info as InfoIcon,
  Analytics as StatsIcon,
} from '@mui/icons-material'
import { PipelineStep, QueryResult } from './DataWorkbench'

interface ResultsPreviewProps {
  results: QueryResult | null
  columnStats: Record<string, any>
  isLoading:  boolean
  pipeline: PipelineStep[]
}

export default function ResultsPreview({
  results,
  columnStats,
  isLoading,
  pipeline,
}: ResultsPreviewProps) {
  const [statsAnchor, setStatsAnchor] = React.useState<{ anchor: HTMLElement; column: string } | null>(null)

  // Build grid columns
  const columns: GridColDef[] = React.useMemo(() => {
    if (!results?.columns) return []

    return results.columns.map(col => ({
      field: col,
      headerName: col,
      flex: 1,
      minWidth: 120,
      renderHeader: (params) => (
        <Stack direction="row" alignItems="center" spacing={0.5}>
          <Typography variant="body2" fontWeight={500} noWrap>
            {col}
          </Typography>
          {columnStats[col] && (
            <IconButton
              size="small"
              onClick={(e) => {
                e.stopPropagation()
                setStatsAnchor({ anchor: e.currentTarget, column: col })
              }}
              sx={{ opacity: 0.5, '&:hover': { opacity: 1 } }}
            >
              <StatsIcon sx={{ fontSize: 14 }} />
            </IconButton>
          )}
        </Stack>
      ),
      renderCell: (params) => {
        const value = params.value
        if (value === null || value === undefined) {
          return <Typography variant="body2" color="text.disabled" fontStyle="italic">null</Typography>
        }
        if (typeof value === 'boolean') {
          return <Chip label={value ? 'true' :  'false'} size="small" color={value ? 'success' : 'default'} />
        }
        if (typeof value === 'object') {
          return <Typography variant="body2" noWrap>{JSON.stringify(value)}</Typography>
        }
        return <Typography variant="body2" noWrap>{String(value)}</Typography>
      },
    }))
  }, [results?.columns, columnStats])

  // Build grid rows
  const rows = React.useMemo(() => {
    if (!results?.data) return []
    return results.data.map((row, idx) => ({
      id: idx,
      ...row,
    }))
  }, [results?.data])

  // Format number with commas
  const formatNumber = (n: number) => {
    if (n === undefined || n === null) return '?'
    return n.toLocaleString()
  }

  // Get column type chip color
  const getTypeColor = (type: string): "default" | "primary" | "secondary" | "success" | "warning" => {
    if (type.includes('int') || type.includes('float')) return 'primary'
    if (type.includes('datetime') || type.includes('date')) return 'secondary'
    if (type.includes('bool')) return 'success'
    return 'default'
  }

  // Render column stats popover
  const renderStatsPopover = () => {
    if (!statsAnchor) return null
    const stats = columnStats[statsAnchor.column]
    if (!stats) return null

    return (
      <Popover
        open={Boolean(statsAnchor)}
        anchorEl={statsAnchor.anchor}
        onClose={() => setStatsAnchor(null)}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'left' }}
      >
        <Box sx={{ p: 2, minWidth: 200 }}>
          <Typography variant="subtitle2" gutterBottom>{statsAnchor.column}</Typography>
          <Stack spacing={0.5}>
            <Stack direction="row" justifyContent="space-between">
              <Typography variant="caption" color="text.secondary">Type</Typography>
              <Chip label={stats.type} size="small" color={getTypeColor(stats.type)} />
            </Stack>
            <Stack direction="row" justifyContent="space-between">
              <Typography variant="caption" color="text.secondary">Null Count</Typography>
              <Typography variant="caption">{formatNumber(stats.null_count)}</Typography>
            </Stack>
            <Stack direction="row" justifyContent="space-between">
              <Typography variant="caption" color="text.secondary">Unique</Typography>
              <Typography variant="caption">{formatNumber(stats.unique_count)}</Typography>
            </Stack>
            {stats.min !== undefined && (
              <Stack direction="row" justifyContent="space-between">
                <Typography variant="caption" color="text.secondary">Min</Typography>
                <Typography variant="caption">{stats.min}</Typography>
              </Stack>
            )}
            {stats.max !== undefined && (
              <Stack direction="row" justifyContent="space-between">
                <Typography variant="caption" color="text.secondary">Max</Typography>
                <Typography variant="caption">{stats.max}</Typography>
              </Stack>
            )}
            {stats.mean !== undefined && (
              <Stack direction="row" justifyContent="space-between">
                <Typography variant="caption" color="text.secondary">Mean</Typography>
                <Typography variant="caption">{stats.mean.toFixed(2)}</Typography>
              </Stack>
            )}
            {stats.samples && stats.samples.length > 0 && (
              <Box>
                <Typography variant="caption" color="text.secondary">Samples: </Typography>
                <Stack direction="row" spacing={0.5} flexWrap="wrap" sx={{ mt: 0.5 }}>
                  {stats.samples.slice(0, 3).map((s:  string, i: number) => (
                    <Chip key={i} label={s} size="small" variant="outlined" sx={{ fontSize: 10 }} />
                  ))}
                </Stack>
              </Box>
            )}
          </Stack>
        </Box>
      </Popover>
    )
  }

  return (
    <Box sx={{ height: '100%', display: 'flex', flexDirection:  'column' }}>
      {/* Header */}
      <Stack direction="row" justifyContent="space-between" alignItems="center" mb={1}>
        <Stack direction="row" spacing={1} alignItems="center">
          <Typography variant="subtitle1" fontWeight={600}>
            Results Preview
          </Typography>
          {results && (
            <>
              <Chip 
                label={`${formatNumber(results.row_count)} / ${formatNumber(results.total_rows)} rows`}
                size="small"
                variant="outlined"
              />
              <Chip 
                label={`${results.columns.length} columns`}
                size="small"
                variant="outlined"
              />
            </>
          )}
        </Stack>
        {isLoading && <CircularProgress size={20} />}
      </Stack>

      {/* Content */}
      <Box sx={{ flex: 1, minHeight: 0 }}>
        {pipeline.length === 0 ? (
          <Alert severity="info">
            Select a data source and build your pipeline to see results here.
          </Alert>
        ) : !results ? (
          isLoading ? (
            <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100%' }}>
              <CircularProgress />
            </Box>
          ) : (
            <Alert severity="warning">
              No results. Check your pipeline configuration.
            </Alert>
          )
        ) : results.data.length === 0 ? (
          <Alert severity="info">
            Query returned no results.Try adjusting your filters.
          </Alert>
        ) : (
          <DataGrid
            rows={rows}
            columns={columns}
            density="compact"
            disableRowSelectionOnClick
            pageSizeOptions={[25, 50, 100]}
            initialState={{
              pagination: { paginationModel: { pageSize: 25 } },
            }}
            slots={{ toolbar: GridToolbar }}
            slotProps={{
              toolbar:  {
                showQuickFilter:  true,
                quickFilterProps:  { debounceMs: 500 },
              },
            }}
            sx={{
              '& .MuiDataGrid-cell': {
                fontSize: 13,
              },
            }}
          />
        )}
      </Box>

      {/* Stats Popover */}
      {renderStatsPopover()}
    </Box>
  )
}