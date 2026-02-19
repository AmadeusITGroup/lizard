// src/components/AdvancedFilterBuilder.tsx
import React from 'react'
import { Box, Stack, MenuItem, TextField, IconButton, Button, Chip, Typography } from '@mui/material'
import DeleteIcon from '@mui/icons-material/Delete'
import AddIcon from '@mui/icons-material/Add'
import { fetchSchemaFields, fetchSchemaOperators, fetchDistinct, type FilterCond, type FieldType } from '../api'

type Props = {
  startISO: string
  endISO: string
  value: FilterCond[]
  onChange: (next: FilterCond[]) => void
}

export default function AdvancedFilterBuilder({ startISO, endISO, value, onChange }: Props) {
  const [fields, setFields] = React.useState<string[]>([])
  const [types, setTypes] = React.useState<Record<string, FieldType>>({})
  const [opsMap, setOpsMap] = React.useState<Record<FieldType, string[]>>({} as any)
  const [distinctCache, setDistinctCache] = React.useState<Record<string, string[]>>({})

  React.useEffect(() => {
    (async () => {
      const schema = await fetchSchemaFields()
      setFields(schema.fields)
      setTypes(schema.types)
      const ops = await fetchSchemaOperators()
      setOpsMap(ops)
    })()
  }, [])

  function setItem(i: number, patch: Partial<FilterCond>) {
    const next = value.map((row, idx) => (idx === i ? { ...row, ...patch } : row))
    onChange(next)
  }

  async function loadDistinct(field: string) {
    if (distinctCache[field]) return
    const res = await fetchDistinct(field, startISO, endISO, 50)
    setDistinctCache(s => ({ ...s, [field]: res.values }))
  }

  return (
    <Stack spacing={1.5}>
      <Stack direction="row" spacing={1} alignItems="center">
        <Typography variant="subtitle2" sx={{ fontWeight: 700, color: 'primary.dark' }}>
          Advanced filters
        </Typography>
        {value.length > 0 && <Chip size="small" color="primary" label={`${value.length}`} />}
        <Box flex={1} />
        <Button
          size="small"
          variant="outlined"
          startIcon={<AddIcon />}
          onClick={() => onChange([...value, { field: 'user_id', op: 'eq', value: '' }])}
        >
          Add rule
        </Button>
      </Stack>

      {value.length === 0 && (
        <Typography variant="body2" sx={{ opacity: 0.7 }}>
          No rules.Click <b>Add rule</b> to start building your filter.
        </Typography>
      )}

      {value.map((row, i) => {
        const ft = types[row.field] ?? 'string'
        const ops = opsMap[ft] ?? []
        const distinct = distinctCache[row.field]

        return (
          <Stack key={i} direction={{ xs: 'column', md: 'row' }} spacing={1} alignItems="center">
            <TextField
              select size="small" label="Field" sx={{ minWidth: 180 }}
              value={row.field}
              onChange={e => {
                const f = e.target.value
                setItem(i, { field: f, op: 'eq', value: '' })
                loadDistinct(f)
              }}
            >
              {fields.map(f => (<MenuItem key={f} value={f}>{f}</MenuItem>))}
            </TextField>

            <TextField
              select size="small" label="Operator" sx={{ minWidth: 160 }}
              value={row.op}
              onChange={e => setItem(i, { op: e.target.value as any })}
            >
              {ops.map(op => (<MenuItem key={op} value={op}>{op}</MenuItem>))}
            </TextField>

            {/* Value input with assist from "distinct" */}
            <TextField
              size="small" label="Value" sx={{ minWidth: 220, flex: 1 }}
              value={row.value ?? ''}
              onChange={e => setItem(i, { value: e.target.value })}
              placeholder={distinct && distinct.length ? `Try: ${distinct.slice(0, 3).join(', ')}` : 'e.g., user42'}
            />

            <IconButton color="error" onClick={() => onChange(value.filter((_, idx) => idx !== i))}>
              <DeleteIcon />
            </IconButton>
          </Stack>
        )
      })}
    </Stack>
  )
}