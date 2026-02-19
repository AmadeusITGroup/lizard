// ui-react/src/context/CloudContext.tsx
/**
 * Cloud context — provides mode, config, scheduler state, and cloud API helpers.
 * Serves: SettingsPage, CloudBrowserPage, CloudModeToggle, JobProgressPanel.
 */
import React, { createContext, useContext, useState, useEffect, useCallback, useRef } from 'react'
import apiClient from '../api'

// ── Exported types (used by SettingsPage, CloudBrowserPage, etc.) ────

export interface AuthConfig {
  type: 'service_principal' | 'developer_token' | 'username_password'
  tenant_id?: string
  client_id?: string
  client_secret?: string
  token?: string
  username?: string
  password?: string
}

export interface ClusterConfig {
  cluster_id?: string
  cluster_name?: string
  spark_config?: Record<string, string>
  auto_start?: boolean
}

export interface GatewayConfig {
  name: string
  fqdn: string
  environment?: string
  exposed_workspaces: string[]
  exposed_storage_accounts: string[]
}

export interface DatabricksConnectionConfig {
  name: string
  workspace_id: string
  workspace_url?: string
  connectivity: 'direct' | 'gateway'
  gateway_name?: string
  auth: AuthConfig
  cluster: ClusterConfig
}

export interface StorageConnectionConfig {
  name: string
  account_name: string
  container?: string
  endpoint_type?: 'blob' | 'dfs'
  connectivity: 'direct' | 'gateway'
  gateway_name?: string
  auth: AuthConfig
}

export interface CloudConfig {
  mode: 'local' | 'cloud'
  gateways: GatewayConfig[]
  databricks_connections: DatabricksConnectionConfig[]
  storage_connections: StorageConnectionConfig[]
}

export interface TestConnectionStep {
  step: string
  status: string
  host?: string
  url?: string
  detail?: { message?: string; action?: string; count?: number }
}

export interface TestConnectionResult {
  connection_type: string
  connection_name: string
  overall: string
  steps: TestConnectionStep[]
  error?: { message: string; action?: string }
}

export interface SchedulerJob {
  name: string
  interval_seconds: number
  enabled: boolean
  last_run: string | null
  last_status: string | null
  last_duration_ms: number | null
  last_error: string | null
  run_count: number
  error_count: number
}

export interface SchedulerStatus {
  running: boolean
  job_count: number
  jobs: SchedulerJob[]
}

// ── Default config ───────────────────────────────────────────────────

const DEFAULT_CONFIG: CloudConfig = {
  mode: 'local',
  gateways: [],
  databricks_connections: [],
  storage_connections: [],
}

// ── Context shape ────────────────────────────────────────────────────

interface CloudState {
  // Original fields (CloudModeToggle, JobProgressPanel)
  mode: 'local' | 'cloud'
  loading: boolean
  error: string | null
  scheduler: SchedulerStatus | null
  switchMode: (mode: 'local' | 'cloud') => Promise<void>
  refreshScheduler: () => Promise<void>
  toggleScheduler: () => Promise<void>

  // Extended fields (SettingsPage, CloudBrowserPage)
  config: CloudConfig
  isCloudMode: boolean
  setMode: (mode: 'local' | 'cloud') => Promise<void>
  updateConfig: (cfg: CloudConfig) => Promise<void>
  testConnection: (type: 'databricks' | 'storage', name: string) => Promise<TestConnectionResult>
}

const CloudContext = createContext<CloudState | null>(null)

export function CloudProvider({ children }: { children: React.ReactNode }) {
  const [config, setConfig] = useState<CloudConfig>(DEFAULT_CONFIG)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [scheduler, setScheduler] = useState<SchedulerStatus | null>(null)
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null)

  // ── Fetch full cloud config ──
  const fetchConfig = useCallback(async () => {
    try {
      const { data } = await apiClient.get('/cloud/config')
      const cfg: CloudConfig = {
        mode: data?.mode ?? 'local',
        gateways: data?.gateways ?? [],
        databricks_connections: data?.databricks_connections ?? [],
        storage_connections: data?.storage_connections ?? [],
      }
      setConfig(cfg)
      return cfg
    } catch {
      setConfig(DEFAULT_CONFIG)
      return DEFAULT_CONFIG
    }
  }, [])

  // ── Fetch scheduler status ──
  const refreshScheduler = useCallback(async () => {
    try {
      const { data } = await apiClient.get('/scheduler/status')
      setScheduler(data)
    } catch {
      setScheduler(null)
    }
  }, [])

  // ── Initial load ──
  useEffect(() => {
    setLoading(true)
    setError(null)
    fetchConfig()
      .then(() => setLoading(false))
      .catch((err) => {
        setError(err?.message ?? 'Failed to load cloud config')
        setLoading(false)
      })
  }, [fetchConfig])

  // ── Poll scheduler when in cloud mode ──
  useEffect(() => {
    if (config.mode === 'cloud') {
      refreshScheduler() // immediate
      pollRef.current = setInterval(refreshScheduler, 10_000)
      return () => {
        if (pollRef.current) clearInterval(pollRef.current)
      }
    } else {
      setScheduler(null)
      if (pollRef.current) {
        clearInterval(pollRef.current)
        pollRef.current = null
      }
    }
  }, [config.mode, refreshScheduler])

  // ── switchMode / setMode ──
  const switchMode = useCallback(async (newMode: 'local' | 'cloud') => {
    // Prevent switching to cloud without connections
    if (newMode === 'cloud') {
      const hasConns =
        config.databricks_connections.length > 0 || config.storage_connections.length > 0
      if (!hasConns) {
        const msg =
          'Cannot switch to Cloud mode: no Databricks or Storage connections configured. ' +
          'Add at least one connection in Cloud Settings first.'
        setError(msg)
        throw new Error(msg)
      }
    }

    setLoading(true)
    setError(null)
    try {
      await apiClient.post('/cloud/mode', { mode: newMode })
      setConfig(prev => ({ ...prev, mode: newMode }))
      // auto start/stop scheduler
      if (newMode === 'cloud') {
        try { await apiClient.post('/scheduler/start') } catch {}
      } else {
        try { await apiClient.post('/scheduler/stop') } catch {}
      }
    } catch (e: any) {
      const msg = e?.response?.data?.detail || e.message || 'Failed to switch mode'
      setError(msg)
      throw new Error(msg)
    } finally {
      setLoading(false)
    }
  }, [config])

  // ── toggleScheduler ──
  const toggleScheduler = useCallback(async () => {
    try {
      if (scheduler?.running) {
        await apiClient.post('/scheduler/stop')
      } else {
        await apiClient.post('/scheduler/start')
      }
      await refreshScheduler()
    } catch (e: any) {
      console.warn('toggleScheduler failed', e?.message)
    }
  }, [scheduler, refreshScheduler])

  // ── updateConfig (save full config) ──
  const updateConfig = useCallback(async (newCfg: CloudConfig) => {
    try {
      const { data } = await apiClient.put('/cloud/config', newCfg)
      const saved: CloudConfig = {
        mode: data?.mode ?? newCfg.mode,
        gateways: data?.gateways ?? newCfg.gateways,
        databricks_connections: data?.databricks_connections ?? newCfg.databricks_connections,
        storage_connections: data?.storage_connections ?? newCfg.storage_connections,
      }
      setConfig(saved)
    } catch (e: any) {
      throw new Error(e?.response?.data?.detail || e?.message || 'Failed to save config')
    }
  }, [])

  // ── testConnection ──
  const testConnection = useCallback(async (
    type: 'databricks' | 'storage',
    name: string,
  ): Promise<TestConnectionResult> => {
    const { data } = await apiClient.post('/cloud/test-connection', {
      connection_type: type,
      connection_name: name,
    })
    return data
  }, [])

  const value = React.useMemo<CloudState>(
    () => ({
      // Original shape (CloudModeToggle, JobProgressPanel)
      mode: config.mode,
      loading,
      error,
      scheduler,
      switchMode,
      refreshScheduler,
      toggleScheduler,

      // Extended shape (SettingsPage, CloudBrowserPage)
      config,
      isCloudMode: config.mode === 'cloud',
      setMode: switchMode,
      updateConfig,
      testConnection,
    }),
    [config, loading, error, scheduler, switchMode, refreshScheduler, toggleScheduler, updateConfig, testConnection]
  )

  return <CloudContext.Provider value={value}>{children}</CloudContext.Provider>
}

export function useCloud() {
  const ctx = useContext(CloudContext)
  if (!ctx) throw new Error('useCloud must be used within CloudProvider')
  return ctx
}