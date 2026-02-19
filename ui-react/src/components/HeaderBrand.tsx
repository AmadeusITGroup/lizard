// src/components/HeaderBrand.tsx
import React from 'react'
import { AppBar, Toolbar, Typography, Box, Stack, Button } from '@mui/material'
import lizardLogo from '../assets/lizard-logo.svg'

export default function HeaderBrand({ onOpenUpload }: { onOpenUpload: () => void }) {
  return (
    <AppBar color="inherit" position="sticky" elevation={0} sx={{ borderBottom: '1px solid #E6ECF3' }}>
      <Toolbar sx={{ gap: 2 }}>
        <Stack direction="row" alignItems="center" spacing={1.2}>
          <img src={lizardLogo} alt="Lizard" height={28} />
          <Typography variant="h6" sx={{ fontWeight: 800, color: 'primary.main' }}>LIZARD</Typography>
          <Typography variant="h6" sx={{ fontWeight: 700, opacity: 0.85 }}>Fraud Pattern Visualizer</Typography>
        </Stack>
        <Box flex={1} />
        <Button variant="contained" onClick={onOpenUpload}>Upload & Map</Button>
      </Toolbar>
    </AppBar>
  )
}