// path: ui-react/src/pages/WorkbenchPage.tsx
/**
 * WorkbenchPage - Page wrapper for Data Workbench
 */
import React from 'react'
import { Box } from '@mui/material'
import { DataWorkbench } from '../components/DataWorkbench'

export default function WorkbenchPage() {
  return (
    <Box sx={{ height: 'calc(100vh - 120px)', p: 2 }}>
      <DataWorkbench />
    </Box>
  )
}