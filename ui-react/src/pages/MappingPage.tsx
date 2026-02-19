// path: ui-react/src/pages/MappingPage.tsx
/**
 * MappingPage - Full page for managing data mapping templates
 */
import React from 'react'
import { Box } from '@mui/material'
import MappingManager from '../components/MappingManager'

export default function MappingPage() {
  return (
    <Box
      sx={{
        height: 'calc(100vh - 64px)',
        p: 2,
        bgcolor: 'background.default',
        overflow: 'hidden',
      }}
    >
      <MappingManager />
    </Box>
  )
}