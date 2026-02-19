// src/components/GlobalFiltersBar.tsx
import React from 'react'
import { Paper, Stack, TextField, FormControlLabel, Switch, Button } from '@mui/material'
import { DateTimePicker } from '@mui/x-date-pickers/DateTimePicker'
import dayjs, { Dayjs } from 'dayjs'
import utc from 'dayjs/plugin/utc'
import timezone from 'dayjs/plugin/timezone'
import AdvancedFilterBuilder from './AdvancedFilterBuilder'
import { useFilters } from '../context/FiltersContext'

dayjs.extend(utc)
dayjs.extend(timezone)

function toDayjs(iso?: string): Dayjs | null {
  if (!iso) return null
  const d = dayjs(iso)
  return d.isValid() ? d : null
}
function toIsoZ(d: Dayjs | null): string {
  if (!d) return ''
  return d.toDate().toISOString()
}

export default function GlobalFiltersBar() {
  const { startISO, endISO, setStartISO, setEndISO, filters, setFilters } = useFilters()

  // UI state: allow manual ISO editing if the user prefers
  const [manual, setManual] = React.useState<boolean>(() => {
    try {
      return JSON.parse(localStorage.getItem('filters.manualIso') || 'false')
    } catch {
      return false
    }
  })
  React.useEffect(() => {
    localStorage.setItem('filters.manualIso', JSON.stringify(manual))
  }, [manual])

  // Bridge ISO strings <-> Dayjs values for pickers
  const startVal = React.useMemo(() => toDayjs(startISO), [startISO])
  const endVal   = React.useMemo(() => toDayjs(endISO),   [endISO])

  // Quick ranges
  const applyQuick = (hoursBack: number) => {
    const now = dayjs()
    const st  = now.subtract(hoursBack, 'hour')
    setStartISO(toIsoZ(st))
    setEndISO(toIsoZ(now))
  }

  // Advanced filters state (split from filters array)
  const [advanced, setAdvanced] = React.useState(filters.filter(f => f.field !== 'ts'))
  // Sync advanced filters with context
  React.useEffect(() => {
    // Build the full filters array: date filters + advanced
    const dateFilters = [
      { field: 'ts', op: 'gte', value: startISO },
      { field: 'ts', op: 'lte', value: endISO }
    ]
    setFilters([...dateFilters, ...advanced])
    // eslint-disable-next-line
  }, [startISO, endISO, advanced])

  return (
    <Paper variant="outlined" sx={{ p: 2, bgcolor: '#fff', mb: 2 }}>
      <Stack direction={{ xs: 'column', md: 'row' }} spacing={2} alignItems={{ md: 'center' }}>
        <FormControlLabel
          control={<Switch checked={manual} onChange={(_, v) => setManual(v)} />}
          label="Manual ISO"
        />

        {!manual ? (
          <>
            <DateTimePicker
              label="Start"
              value={startVal}
              onChange={(v) => setStartISO(toIsoZ(v))}
              slotProps={{ textField: { size: 'small', sx: { minWidth: 260 } } }}
            />
            <DateTimePicker
              label="End"
              value={endVal}
              onChange={(v) => setEndISO(toIsoZ(v))}
              slotProps={{ textField: { size: 'small', sx: { minWidth: 260 } } }}
            />
            <Stack direction="row" spacing={1} sx={{ flexWrap: 'wrap' }}>
              <Button size="small" variant="outlined" onClick={() => applyQuick(6)}>Last 6h</Button>
              <Button size="small" variant="outlined" onClick={() => applyQuick(24)}>Last 24h</Button>
              <Button size="small" variant="outlined" onClick={() => applyQuick(72)}>Last 3d</Button>
              <Button size="small" variant="outlined" onClick={() => applyQuick(168)}>Last 7d</Button>
            </Stack>
          </>
        ) : (
          <>
            <TextField
              label="Start (ISO)"
              size="small"
              sx={{ minWidth: 340 }}
              value={startISO}
              onChange={(e) => setStartISO(e.target.value)}
              placeholder="e.g., 2025-11-06T12:00:00Z"
            />
            <TextField
              label="End (ISO)"
              size="small"
              sx={{ minWidth: 340 }}
              value={endISO}
              onChange={(e) => setEndISO(e.target.value)}
              placeholder="e.g., 2025-11-13T12:00:00Z"
            />
          </>
        )}

        {/* Advanced field/value filters */}
        <AdvancedFilterBuilder
          startISO={startISO}
          endISO={endISO}
          value={advanced}
          onChange={setAdvanced}
        />
      </Stack>
    </Paper>
  )
}
