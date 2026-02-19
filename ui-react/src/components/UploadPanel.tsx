// src/components/UploadPanel.tsx
import React from 'react'
import {
  Box, Stack, Paper, Typography, Button, MenuItem, TextField, Divider, Chip
} from '@mui/material'
import CloudUploadIcon from '@mui/icons-material/CloudUpload'
import CheckCircleIcon from '@mui/icons-material/CheckCircle'
import { uploadPreview, uploadCommit } from '../api'

type Props = {
  onIngested?: (count: number) => void
}

export default function UploadPanel({ onIngested }: Props) {
  const [file, setFile] = React.useState<File | null>(null)
  const [engine, setEngine] = React.useState<'heuristic'|'openai'|'ollama'>('heuristic')
  const [sourceName, setSourceName] = React.useState('uploaded_file')
  const [preview, setPreview] = React.useState<{ mapping: Record<string,string>; sample: any[] }|null>(null)
  const [loading, setLoading] = React.useState(false)
  const [done, setDone] = React.useState<{ ingested: number }|null>(null)

  async function doPreview() {
    if (!file) return
    setLoading(true)
    try {
      const p = await uploadPreview(file, engine, 25)
      setPreview(p)
    } finally {
      setLoading(false)
    }
  }

  async function doCommit() {
    if (!file) return
    setLoading(true)
    try {
      const res = await uploadCommit({ file, engine, sourceName, mappingJson: preview?.mapping })
      setDone({ ingested: res.ingested })
      onIngested?.(res.ingested)
    } finally {
      setLoading(false)
    }
  }

  return (
    <Paper variant="outlined" sx={{ p: 2, bgcolor: '#fff' }}>
      <Stack spacing={2}>
        <Stack direction="row" spacing={1} alignItems="center">
          <Chip color="secondary" label="Upload" />
          <Typography variant="body2" sx={{ opacity: 0.7 }}>
            Ingest CSV/Parquet as-is.Analytics are applied later, per visualization.
          </Typography>
        </Stack>

        <Stack direction={{ xs: 'column', md: 'row' }} spacing={2} alignItems={{ md: 'center' }}>
          <Button
            component="label"
            variant="contained"
            startIcon={<CloudUploadIcon />}
          >
            {file ? `Selected: ${file.name}` : 'Choose file'}
            <input type="file" hidden accept=".csv,.parquet" onChange={e => setFile(e.target.files?.[0] ?? null)} />
          </Button>

          <TextField
            select size="small" label="Mapping engine" sx={{ minWidth: 180 }}
            value={engine} onChange={e => setEngine(e.target.value as any)}
          >
            <MenuItem value="heuristic">Heuristic (fast, default)</MenuItem>
            <MenuItem value="openai">OpenAI (optional)</MenuItem>
            <MenuItem value="ollama">Ollama (optional)</MenuItem>
          </TextField>

          <TextField
            size="small" label="Source name" sx={{ minWidth: 240 }}
            value={sourceName} onChange={e => setSourceName(e.target.value)}
          />

          <Box flex={1} />
          <Button variant="outlined" onClick={doPreview} disabled={!file || loading}>
            Preview mapping
          </Button>
          <Button variant="contained" onClick={doCommit} disabled={!file || loading}>
            Ingest
          </Button>
        </Stack>

        {preview && (
          <>
            <Divider />
            <Typography variant="subtitle2" sx={{ fontWeight: 700 }}>Proposed mapping</Typography>
            <Box sx={{
              p: 1.5, fontFamily: 'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace',
              fontSize: 12, bgcolor: '#f8fafc', border: '1px solid #E6ECF3', borderRadius: 1
            }}>
              {JSON.stringify(preview.mapping, null, 2)}
            </Box>
            <Typography variant="subtitle2" sx={{ fontWeight: 700, mt: 1 }}>Sample rows</Typography>
            <Box sx={{
              p: 1.5, fontFamily: 'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace',
              fontSize: 12, bgcolor: '#f8fafc', border: '1px solid #E6ECF3', borderRadius: 1, maxHeight: 220, overflow: 'auto'
            }}>
              {JSON.stringify(preview.sample, null, 2)}
            </Box>
          </>
        )}

        {done && (
          <Stack direction="row" spacing={1} alignItems="center" sx={{ color: 'success.main' }}>
            <CheckCircleIcon fontSize="small" />
            <Typography variant="body2">Ingested {done.ingested.toLocaleString()} events</Typography>
          </Stack>
        )}
      </Stack>
    </Paper>
  )
}