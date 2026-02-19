// src/context/FiltersContext.tsx
import React from 'react'
import type { FilterCond } from '../api'

type FiltersState = {
  startISO: string
  endISO: string
  filters: FilterCond[]
  setStartISO: (v: string) => void
  setEndISO: (v: string) => void
  setFilters: (v: FilterCond[]) => void
}

const FiltersContext = React.createContext<FiltersState | null>(null)

export function FiltersProvider({ children }: { children: React.ReactNode }) {
  const [startISO, setStartISO] = React.useState(new Date(Date.now() - 7 * 24 * 3600 * 1000).toISOString())
  const [endISO, setEndISO] = React.useState(new Date().toISOString())
  const [filters, setFilters] = React.useState<FilterCond[]>([])

  const value = React.useMemo(() => ({ startISO, endISO, filters, setStartISO, setEndISO, setFilters }), [startISO, endISO, filters])
  return <FiltersContext.Provider value={value}>{children}</FiltersContext.Provider>
}

export function useFilters() {
  const ctx = React.useContext(FiltersContext)
  if (!ctx) throw new Error('useFilters must be used within FiltersProvider')
  return ctx
}