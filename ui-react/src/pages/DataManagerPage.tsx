// ui-react/src/pages/DataManagerPage.tsx
import React from 'react'
import { Box } from '@mui/material'
import DataManager from '../components/DataManager'

export default function DataManagerPage() {
  return (
    <Box sx={{ height: 'calc(100vh - 64px)', overflow: 'auto' }}>
      <DataManager />
    </Box>
  )
}