// path: ui-react/src/pages/RulesPage.tsx
import React from 'react'
import { Box } from '@mui/material'
import RulesManager from '../components/RulesManager'

export default function RulesPage() {
  return (
    <Box sx={{ height: 'calc(100vh - 64px)', p: 2, bgcolor: 'background.default' }}>
      <RulesManager />
    </Box>
  )
}