// path: src/components/DataGridView.tsx
import React from 'react'
import { Box } from '@mui/material'
import {
  DataGrid,
  GridToolbarContainer,
  GridToolbarQuickFilter,
  GridToolbar,
  type GridColDef
} from '@mui/x-data-grid'

export type GridParams = {
  enabled: boolean
  minScore?: number // NEW: filter on anomaly score
}

export default function DataGridView({ data, params }: { data: any[]; params: GridParams }) {
  // Prepare rows
  const rowsAll = data.map((d, i) => ({ id: i, ...d }))
  const rows =
    typeof params.minScore === 'number'
      ? rowsAll.filter((r) => (r.anom_score ?? 0) >= params.minScore!)
      : rowsAll

  // Define columns
  const cols: GridColDef[] = [
    { field: 'ts', headerName: 'Timestamp', width: 180 },
    { field: 'user_id', headerName: 'User', width: 140 },
    { field: 'event_type', headerName: 'Event', width: 160 },
    { field: 'anom_score', headerName: 'Score', width: 110, type: 'number' },
    { field: 'anomaly', headerName: 'Anomaly?', width: 110, type: 'boolean' },
    { field: 'is_unusual', headerName: 'Unusual?', width: 110, type: 'boolean' },
    {
      field: 'reasons',
      headerName: 'Reasons',
      width: 220,
      sortable: false,
      valueGetter: (p) =>
        Array.isArray(p.row?.reasons)
          ? p.row.reasons.map((r: any) => r.code).join(', ')
          : ''
    },
    { field: 'cluster', headerName: 'Cluster', width: 110, type: 'number' },
    { field: 'explain', headerName: 'Explanation', width: 420, sortable: false }
  ]

  function CustomToolbar() {
    return (
      <GridToolbarContainer>
        <GridToolbarQuickFilter placeholder="Filter rows…" />
        <GridToolbar />
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, ml: 2 }}>
          <span style={{ fontSize: 12, opacity: 0.8 }}>Min score</span>
          {/* slider or input UI belongs to parent panel */}
        </Box>
      </GridToolbarContainer>
    )
  }

  return (
    <Box sx={{ height: '100%', width: '100%' }}>
      <DataGrid
        rows={rows}
        columns={cols}
        density="compact"
        disableRowSelectionOnClick
        slots={{ toolbar: CustomToolbar }}
      />
    </Box>
  )
}
