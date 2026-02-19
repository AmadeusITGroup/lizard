// path: ui-react/src/App.tsx
import React from 'react'
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import MappingPage from './pages/MappingPage'
import { FiltersProvider } from './context/FiltersContext'

// Pages
import Dashboard from './pages/Dashboard'
import RulesPage from './pages/RulesPage'
import WorkbenchPage from './pages/WorkbenchPage'
import DataManagerPage from './pages/DataManagerPage'

// === Cloud pages (Phase 1/2) ===
import CloudBrowserPage from './pages/CloudBrowserPage'
import SettingsPage from './pages/SettingsPage'

// Layout
import AppLayout from './components/AppLayout'

// === Cloud context (Phase 3 — mode toggle + scheduler polling) ===
import { CloudProvider } from './context/CloudContext'

export default function App() {
  return (
    <CloudProvider>
      <FiltersProvider>
        <BrowserRouter>
          <Routes>
            <Route path="/" element={<AppLayout />}>
              <Route index element={<Navigate to="/dashboard" replace />} />
              <Route path="dashboard" element={<Dashboard />} />
              <Route path="rules" element={<RulesPage />} />
              <Route path="mapping" element={<MappingPage />} />
              <Route path="workbench" element={<WorkbenchPage />} />
              <Route path="data" element={<DataManagerPage />} />
              <Route path="cloud-browser" element={<CloudBrowserPage />} />
              <Route path="cloud-settings" element={<SettingsPage />} />
            </Route>
          </Routes>
        </BrowserRouter>
      </FiltersProvider>
    </CloudProvider>
  )
}