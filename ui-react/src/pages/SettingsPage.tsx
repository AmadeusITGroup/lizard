// ui-react/src/pages/SettingsPage.tsx
import React from 'react'
import {
  Box,
  Typography,
  Paper,
  Stack,
  Switch,
  FormControlLabel,
  Tabs,
  Tab,
  TextField,
  Button,
  Alert,
  Chip,
  IconButton,
  Divider,
  Tooltip,
  CircularProgress,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  MenuItem,
} from '@mui/material'
import {
  Add as AddIcon,
  Delete as DeleteIcon,
  Edit as EditIcon,
  ExpandMore as ExpandMoreIcon,
  CheckCircle as CheckIcon,
  Error as ErrorIcon,
  PlayArrow as TestIcon,
  Cloud as CloudIcon,
  Computer as LocalIcon,
  Dns as GatewayIcon,
  Storage as StorageIcon,
  Hub as DatabricksIcon,
  Save as SaveIcon,
} from '@mui/icons-material'
import {
  useCloud,
  type GatewayConfig,
  type DatabricksConnectionConfig,
  type StorageConnectionConfig,
  type AuthConfig,
  type ClusterConfig,
  type TestConnectionResult,
} from '../context/CloudContext'

// ============================================================
// Sub-components
// ============================================================

// ---- Gateway Editor Dialog ----
function GatewayEditorDialog({
  open,
  gateway,
  onClose,
  onSave,
}: {
  open: boolean
  gateway: GatewayConfig | null
  onClose: () => void
  onSave: (gw: GatewayConfig) => void
}) {
  const [form, setForm] = React.useState<GatewayConfig>({
    name: '',
    fqdn: '',
    environment: '',
    exposed_workspaces: [],
    exposed_storage_accounts: [],
  })
  const [workspacesText, setWorkspacesText] = React.useState('')
  const [storageText, setStorageText] = React.useState('')

  React.useEffect(() => {
    if (gateway) {
      setForm(gateway)
      setWorkspacesText((gateway.exposed_workspaces || []).join(', '))
      setStorageText((gateway.exposed_storage_accounts || []).join(', '))
    } else {
      setForm({ name: '', fqdn: '', environment: '', exposed_workspaces: [], exposed_storage_accounts: [] })
      setWorkspacesText('')
      setStorageText('')
    }
  }, [gateway, open])

  const handleSave = () => {
    onSave({
      ...form,
      exposed_workspaces: workspacesText
        .split(',')
        .map(s => s.trim())
        .filter(Boolean),
      exposed_storage_accounts: storageText
        .split(',')
        .map(s => s.trim())
        .filter(Boolean),
    })
  }

  return (
    <Dialog open={open} onClose={onClose} maxWidth="sm" fullWidth>
      <DialogTitle>{gateway ? 'Edit Gateway' : 'Add Gateway'}</DialogTitle>
      <DialogContent dividers>
        <Stack spacing={2} sx={{ mt: 1 }}>
          <TextField
            label="Name"
            size="small"
            fullWidth
            value={form.name}
            onChange={e => setForm({ ...form, name: e.target.value })}
            disabled={!!gateway}
            helperText="Unique identifier (e.g. tst-gateway)"
          />
          <TextField
            label="FQDN"
            size="small"
            fullWidth
            value={form.fqdn}
            onChange={e => setForm({ ...form, fqdn: e.target.value })}
            helperText="Fully qualified domain name (e.g. gateway-tst.corp.com)"
          />
          <TextField
            label="Environment"
            size="small"
            fullWidth
            value={form.environment || ''}
            onChange={e => setForm({ ...form, environment: e.target.value })}
            helperText="e.g. TST, CCP, PRD"
          />
          <TextField
            label="Exposed Workspaces"
            size="small"
            fullWidth
            value={workspacesText}
            onChange={e => setWorkspacesText(e.target.value)}
            helperText="Comma-separated workspace IDs exposed through this gateway"
          />
          <TextField
            label="Exposed Storage Accounts"
            size="small"
            fullWidth
            value={storageText}
            onChange={e => setStorageText(e.target.value)}
            helperText="Comma-separated storage account names exposed through this gateway"
          />
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button variant="contained" onClick={handleSave} disabled={!form.name || !form.fqdn}>
          Save
        </Button>
      </DialogActions>
    </Dialog>
  )
}

// ---- Auth Fields ----
function AuthFields({
  auth,
  onChange,
}: {
  auth: AuthConfig
  onChange: (a: AuthConfig) => void
}) {
  return (
    <Stack spacing={1.5}>
      <TextField
        select
        label="Auth Type"
        size="small"
        value={auth.type}
        onChange={e => onChange({ ...auth, type: e.target.value as AuthConfig['type'] })}
        sx={{ maxWidth: 250 }}
      >
        <MenuItem value="service_principal">Service Principal (OAuth)</MenuItem>
        <MenuItem value="developer_token">Developer Token (PAT)</MenuItem>
        <MenuItem value="username_password">Username / Password</MenuItem>
      </TextField>

      {auth.type === 'service_principal' && (
        <Stack direction="row" spacing={1.5} flexWrap="wrap" useFlexGap>
          <TextField
            label="Tenant ID"
            size="small"
            value={auth.tenant_id || ''}
            onChange={e => onChange({ ...auth, tenant_id: e.target.value })}
            sx={{ minWidth: 280 }}
            helperText="Or set AZURE_TENANT_ID env var"
          />
          <TextField
            label="Client ID"
            size="small"
            value={auth.client_id || ''}
            onChange={e => onChange({ ...auth, client_id: e.target.value })}
            sx={{ minWidth: 280 }}
          />
          <TextField
            label="Client Secret"
            size="small"
            type="password"
            value={auth.client_secret || ''}
            onChange={e => onChange({ ...auth, client_secret: e.target.value })}
            sx={{ minWidth: 280 }}
          />
        </Stack>
      )}

      {auth.type === 'developer_token' && (
        <TextField
          label="Token (PAT)"
          size="small"
          type="password"
          value={auth.token || ''}
          onChange={e => onChange({ ...auth, token: e.target.value })}
          sx={{ maxWidth: 500 }}
          helperText="Personal Access Token — for local testing only"
        />
      )}

      {auth.type === 'username_password' && (
        <Stack direction="row" spacing={1.5} flexWrap="wrap" useFlexGap>
          <TextField
            label="Tenant ID"
            size="small"
            value={auth.tenant_id || ''}
            onChange={e => onChange({ ...auth, tenant_id: e.target.value })}
            sx={{ minWidth: 220 }}
          />
          <TextField
            label="Client ID"
            size="small"
            value={auth.client_id || ''}
            onChange={e => onChange({ ...auth, client_id: e.target.value })}
            sx={{ minWidth: 220 }}
          />
          <TextField
            label="Username"
            size="small"
            value={auth.username || ''}
            onChange={e => onChange({ ...auth, username: e.target.value })}
            sx={{ minWidth: 220 }}
          />
          <TextField
            label="Password"
            size="small"
            type="password"
            value={auth.password || ''}
            onChange={e => onChange({ ...auth, password: e.target.value })}
            sx={{ minWidth: 220 }}
          />
        </Stack>
      )}
    </Stack>
  )
}

// ============================================================
// Main Settings Page
// ============================================================

export default function SettingsPage() {
  const { config, loading, error, isCloudMode, updateConfig, setMode, testConnection } = useCloud()
  const [activeTab, setActiveTab] = React.useState(0)
  const [saving, setSaving] = React.useState(false)
  const [saveMsg, setSaveMsg] = React.useState<{ text: string; severity: 'success' | 'error' } | null>(null)

  // Gateway dialog
  const [gwDialogOpen, setGwDialogOpen] = React.useState(false)
  const [editingGw, setEditingGw] = React.useState<GatewayConfig | null>(null)

  // Local editable copy of config
  const [draft, setDraft] = React.useState(config)
  React.useEffect(() => {
    setDraft(config)
  }, [config])

  // Test results
  const [testResults, setTestResults] = React.useState<Record<string, TestConnectionResult>>({})
  const [testingKey, setTestingKey] = React.useState<string | null>(null)

  // ---- Persistence ----
  const handleSave = async () => {
    setSaving(true)
    setSaveMsg(null)
    try {
      await updateConfig(draft)
      setSaveMsg({ text: 'Configuration saved successfully.', severity: 'success' })
    } catch (err: any) {
      setSaveMsg({ text: err.message || 'Failed to save.', severity: 'error' })
    } finally {
      setSaving(false)
    }
  }

  const handleModeToggle = async () => {
    const next = draft.mode === 'cloud' ? 'local' : 'cloud'
    setDraft(prev => ({ ...prev, mode: next }))
    try {
      await setMode(next)
    } catch {
      // revert on failure
      setDraft(prev => ({ ...prev, mode: prev.mode === 'cloud' ? 'local' : 'cloud' }))
    }
  }

  // ---- Gateways ----
  const handleGatewaySave = (gw: GatewayConfig) => {
    setDraft(prev => {
      const existing = prev.gateways.findIndex(g => g.name.toLowerCase() === gw.name.toLowerCase())
      const next = [...prev.gateways]
      if (existing >= 0) {
        next[existing] = gw
      } else {
        next.push(gw)
      }
      return { ...prev, gateways: next }
    })
    setGwDialogOpen(false)
    setEditingGw(null)
  }

  const handleGatewayDelete = (name: string) => {
    setDraft(prev => ({
      ...prev,
      gateways: prev.gateways.filter(g => g.name !== name),
    }))
  }

  // ---- Databricks connections ----
  const addDatabricksConnection = () => {
    const newConn: DatabricksConnectionConfig = {
      name: `dbx-${Date.now().toString(36)}`,
      workspace_id: '',
      connectivity: 'direct',
      auth: { type: 'service_principal' },
      cluster: {},
    }
    setDraft(prev => ({
      ...prev,
      databricks_connections: [...prev.databricks_connections, newConn],
    }))
  }

  const updateDatabricksConnection = (idx: number, conn: DatabricksConnectionConfig) => {
    setDraft(prev => {
      const next = [...prev.databricks_connections]
      next[idx] = conn
      return { ...prev, databricks_connections: next }
    })
  }

  const deleteDatabricksConnection = (idx: number) => {
    setDraft(prev => ({
      ...prev,
      databricks_connections: prev.databricks_connections.filter((_, i) => i !== idx),
    }))
  }

  // ---- Storage connections ----
  const addStorageConnection = () => {
    const newConn: StorageConnectionConfig = {
      name: `stg-${Date.now().toString(36)}`,
      account_name: '',
      connectivity: 'direct',
      auth: { type: 'service_principal' },
    }
    setDraft(prev => ({
      ...prev,
      storage_connections: [...prev.storage_connections, newConn],
    }))
  }

  const updateStorageConnection = (idx: number, conn: StorageConnectionConfig) => {
    setDraft(prev => {
      const next = [...prev.storage_connections]
      next[idx] = conn
      return { ...prev, storage_connections: next }
    })
  }

  const deleteStorageConnection = (idx: number) => {
    setDraft(prev => ({
      ...prev,
      storage_connections: prev.storage_connections.filter((_, i) => i !== idx),
    }))
  }

  // ---- Test connection ----
  const handleTestConnection = async (type: 'databricks' | 'storage', name: string) => {
    const key = `${type}:${name}`
    setTestingKey(key)
    try {
      const result = await testConnection(type, name)
      setTestResults(prev => ({ ...prev, [key]: result }))
    } catch (err: any) {
      setTestResults(prev => ({
        ...prev,
        [key]: {
          connection_type: type,
          connection_name: name,
          overall: 'error',
          steps: [],
          error: { message: err.message || 'Test failed' },
        },
      }))
    } finally {
      setTestingKey(null)
    }
  }

  // ---- Detect unsaved changes ----
  const hasChanges = JSON.stringify(draft) !== JSON.stringify(config)

  if (loading) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: 'calc(100vh - 64px)' }}>
        <CircularProgress />
      </Box>
    )
  }

  return (
    <Box sx={{ p: 3, maxWidth: 1200, mx: 'auto' }}>
      {/* Header */}
      <Stack direction="row" justifyContent="space-between" alignItems="center" sx={{ mb: 3 }}>
        <Box>
          <Typography variant="h4" gutterBottom>
            ⚙️ Settings
          </Typography>
          <Typography variant="body2" color="text.secondary">
            Configure LIZARD mode, cloud connections, gateways, and authentication.
          </Typography>
        </Box>
        <Stack direction="row" spacing={1}>
          {hasChanges && (
            <Chip label="Unsaved changes" color="warning" size="small" />
          )}
          <Button
            variant="contained"
            startIcon={saving ? <CircularProgress size={16} color="inherit" /> : <SaveIcon />}
            onClick={handleSave}
            disabled={saving || !hasChanges}
          >
            Save Configuration
          </Button>
        </Stack>
      </Stack>

      {/* Feedback messages */}
      {saveMsg && (
        <Alert severity={saveMsg.severity} onClose={() => setSaveMsg(null)} sx={{ mb: 2 }}>
          {saveMsg.text}
        </Alert>
      )}
      {error && (
        <Alert severity="info" sx={{ mb: 2 }}>
          Cloud API not available — running in local mode. ({error})
        </Alert>
      )}

      {/* Mode Toggle */}
      <Paper variant="outlined" sx={{ p: 2.5, mb: 3 }}>
        <Stack direction="row" justifyContent="space-between" alignItems="center">
          <Stack direction="row" spacing={2} alignItems="center">
            {draft.mode === 'cloud' ? (
              <CloudIcon sx={{ fontSize: 40, color: 'primary.main' }} />
            ) : (
              <LocalIcon sx={{ fontSize: 40, color: 'text.secondary' }} />
            )}
            <Box>
              <Typography variant="h6">
                Execution Mode: <strong>{draft.mode === 'cloud' ? 'Cloud' : 'Local'}</strong>
              </Typography>
              <Typography variant="body2" color="text.secondary">
                {draft.mode === 'cloud'
                  ? 'Data is read from Azure / DBFS. Computations run on Databricks.'
                  : 'All data and computations are local. No cloud access.'}
              </Typography>
            </Box>
          </Stack>
          <FormControlLabel
            control={
              <Switch
                checked={draft.mode === 'cloud'}
                onChange={handleModeToggle}
                color="primary"
              />
            }
            label={draft.mode === 'cloud' ? 'Cloud' : 'Local'}
          />
        </Stack>
      </Paper>

      {/* Tabs */}
      <Tabs value={activeTab} onChange={(_, v) => setActiveTab(v)} sx={{ mb: 2 }}>
        <Tab icon={<GatewayIcon />} label="Gateways" iconPosition="start" />
        <Tab icon={<DatabricksIcon />} label="Databricks" iconPosition="start" />
        <Tab icon={<StorageIcon />} label="Storage" iconPosition="start" />
      </Tabs>

      {/* ============================================================ */}
      {/* TAB 0: Gateways */}
      {/* ============================================================ */}
      {activeTab === 0 && (
        <Paper variant="outlined" sx={{ p: 2 }}>
          <Stack direction="row" justifyContent="space-between" alignItems="center" sx={{ mb: 2 }}>
            <Box>
              <Typography variant="h6">Application Gateways</Typography>
              <Typography variant="body2" color="text.secondary">
                Configure gateways for routing traffic when direct network access is blocked.
              </Typography>
            </Box>
            <Button
              variant="outlined"
              startIcon={<AddIcon />}
              onClick={() => {
                setEditingGw(null)
                setGwDialogOpen(true)
              }}
            >
              Add Gateway
            </Button>
          </Stack>

          {draft.gateways.length === 0 ? (
            <Alert severity="info">
              No gateways configured. Add one if direct network access to Azure resources is blocked.
            </Alert>
          ) : (
            <Stack spacing={1.5}>
              {draft.gateways.map(gw => (
                <Paper key={gw.name} variant="outlined" sx={{ p: 2 }}>
                  <Stack direction="row" justifyContent="space-between" alignItems="flex-start">
                    <Box>
                      <Stack direction="row" spacing={1} alignItems="center">
                        <GatewayIcon color="action" />
                        <Typography variant="subtitle1" fontWeight={600}>
                          {gw.name}
                        </Typography>
                        {gw.environment && (
                          <Chip label={gw.environment} size="small" variant="outlined" />
                        )}
                      </Stack>
                      <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
                        {gw.fqdn}
                      </Typography>
                      <Stack direction="row" spacing={2} sx={{ mt: 1 }}>
                        <Typography variant="caption" color="text.secondary">
                          Workspaces: {(gw.exposed_workspaces || []).length}
                        </Typography>
                        <Typography variant="caption" color="text.secondary">
                          Storage accounts: {(gw.exposed_storage_accounts || []).length}
                        </Typography>
                      </Stack>
                    </Box>
                    <Stack direction="row" spacing={0.5}>
                      <Tooltip title="Edit">
                        <IconButton
                          size="small"
                          onClick={() => {
                            setEditingGw(gw)
                            setGwDialogOpen(true)
                          }}
                        >
                          <EditIcon fontSize="small" />
                        </IconButton>
                      </Tooltip>
                      <Tooltip title="Delete">
                        <IconButton size="small" color="error" onClick={() => handleGatewayDelete(gw.name)}>
                          <DeleteIcon fontSize="small" />
                        </IconButton>
                      </Tooltip>
                    </Stack>
                  </Stack>
                </Paper>
              ))}
            </Stack>
          )}

          <GatewayEditorDialog
            open={gwDialogOpen}
            gateway={editingGw}
            onClose={() => {
              setGwDialogOpen(false)
              setEditingGw(null)
            }}
            onSave={handleGatewaySave}
          />
        </Paper>
      )}

      {/* ============================================================ */}
      {/* TAB 1: Databricks Connections */}
      {/* ============================================================ */}
      {activeTab === 1 && (
        <Paper variant="outlined" sx={{ p: 2 }}>
          <Stack direction="row" justifyContent="space-between" alignItems="center" sx={{ mb: 2 }}>
            <Box>
              <Typography variant="h6">Databricks Connections</Typography>
              <Typography variant="body2" color="text.secondary">
                Configure Databricks workspace connections, clusters, and authentication.
              </Typography>
            </Box>
            <Button variant="outlined" startIcon={<AddIcon />} onClick={addDatabricksConnection}>
              Add Connection
            </Button>
          </Stack>

          {draft.databricks_connections.length === 0 ? (
            <Alert severity="info">No Databricks connections configured.</Alert>
          ) : (
            <Stack spacing={2}>
              {draft.databricks_connections.map((conn, idx) => {
                const testKey = `databricks:${conn.name}`
                const result = testResults[testKey]
                const isTesting = testingKey === testKey

                return (
                  <Accordion key={idx} defaultExpanded={draft.databricks_connections.length === 1}>
                    <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                      <Stack direction="row" spacing={1.5} alignItems="center" sx={{ width: '100%', pr: 2 }}>
                        <DatabricksIcon color="action" />
                        <Typography fontWeight={600}>{conn.name}</Typography>
                        <Chip
                          label={conn.connectivity}
                          size="small"
                          color={conn.connectivity === 'gateway' ? 'warning' : 'default'}
                          variant="outlined"
                        />
                        {conn.workspace_id && (
                          <Typography variant="caption" color="text.secondary">
                            WS: {conn.workspace_id}
                          </Typography>
                        )}
                        <Box sx={{ flex: 1 }} />
                        {result && (
                          <Chip
                            icon={result.overall === 'ok' ? <CheckIcon /> : <ErrorIcon />}
                            label={result.overall}
                            size="small"
                            color={result.overall === 'ok' ? 'success' : result.overall === 'partial' ? 'warning' : 'error'}
                          />
                        )}
                      </Stack>
                    </AccordionSummary>
                    <AccordionDetails>
                      <Stack spacing={2}>
                        {/* Basic fields */}
                        <Stack direction="row" spacing={1.5} flexWrap="wrap" useFlexGap>
                          <TextField
                            label="Connection Name"
                            size="small"
                            value={conn.name}
                            onChange={e => updateDatabricksConnection(idx, { ...conn, name: e.target.value })}
                            sx={{ minWidth: 220 }}
                          />
                          <TextField
                            label="Workspace ID"
                            size="small"
                            value={conn.workspace_id}
                            onChange={e => updateDatabricksConnection(idx, { ...conn, workspace_id: e.target.value })}
                            sx={{ minWidth: 220 }}
                          />
                          <TextField
                            select
                            label="Connectivity"
                            size="small"
                            value={conn.connectivity}
                            onChange={e =>
                              updateDatabricksConnection(idx, {
                                ...conn,
                                connectivity: e.target.value as 'direct' | 'gateway',
                              })
                            }
                            sx={{ minWidth: 160 }}
                          >
                            <MenuItem value="direct">Direct</MenuItem>
                            <MenuItem value="gateway">Gateway</MenuItem>
                          </TextField>
                          {conn.connectivity === 'gateway' && (
                            <TextField
                              select
                              label="Gateway"
                              size="small"
                              value={conn.gateway_name || ''}
                              onChange={e =>
                                updateDatabricksConnection(idx, { ...conn, gateway_name: e.target.value })
                              }
                              sx={{ minWidth: 200 }}
                            >
                              {draft.gateways.map(gw => (
                                <MenuItem key={gw.name} value={gw.name}>
                                  {gw.name} ({gw.environment || gw.fqdn})
                                </MenuItem>
                              ))}
                            </TextField>
                          )}
                        </Stack>

                        {/* Cluster config */}
                        <Divider />
                        <Typography variant="subtitle2">Cluster</Typography>
                        <Stack direction="row" spacing={1.5} flexWrap="wrap" useFlexGap>
                          <TextField
                            label="Cluster ID"
                            size="small"
                            value={conn.cluster?.cluster_id || ''}
                            onChange={e =>
                              updateDatabricksConnection(idx, {
                                ...conn,
                                cluster: { ...conn.cluster, cluster_id: e.target.value },
                              })
                            }
                            sx={{ minWidth: 280 }}
                            helperText="e.g. 0123-456789-abc"
                          />
                          <TextField
                            label="Cluster Name"
                            size="small"
                            value={conn.cluster?.cluster_name || ''}
                            onChange={e =>
                              updateDatabricksConnection(idx, {
                                ...conn,
                                cluster: { ...conn.cluster, cluster_name: e.target.value },
                              })
                            }
                            sx={{ minWidth: 280 }}
                          />
                        </Stack>

                        {/* Auth */}
                        <Divider />
                        <Typography variant="subtitle2">Authentication</Typography>
                        <AuthFields
                          auth={conn.auth}
                          onChange={auth => updateDatabricksConnection(idx, { ...conn, auth })}
                        />

                        {/* Test result detail */}
                        {result && (
                          <>
                            <Divider />
                            <Typography variant="subtitle2">Test Result</Typography>
                            {result.error && (
                              <Alert severity="error">
                                {result.error.message}
                                {result.error.action && (
                                  <Typography variant="body2" sx={{ mt: 0.5 }}>
                                    💡 {result.error.action}
                                  </Typography>
                                )}
                              </Alert>
                            )}
                            {result.steps.map((step, si) => (
                              <Alert key={si} severity={step.status === 'ok' ? 'success' : step.status === 'skipped' ? 'info' : 'warning'}>
                                <strong>{step.step}</strong>: {step.status}
                                {step.host && ` → ${step.host}`}
                                {step.url && ` → ${step.url}`}
                                {step.detail && typeof step.detail === 'object' && step.detail.message && (
                                  <Typography variant="body2" sx={{ mt: 0.5 }}>
                                    {step.detail.message}
                                    {step.detail.action && <> — 💡 {step.detail.action}</>}
                                  </Typography>
                                )}
                              </Alert>
                            ))}
                          </>
                        )}

                        {/* Actions */}
                        <Stack direction="row" spacing={1} justifyContent="flex-end">
                          <Button
                            size="small"
                            variant="outlined"
                            color="primary"
                            startIcon={isTesting ? <CircularProgress size={14} /> : <TestIcon />}
                            onClick={() => handleTestConnection('databricks', conn.name)}
                            disabled={isTesting || !conn.workspace_id}
                          >
                            Test Connection
                          </Button>
                          <Button
                            size="small"
                            variant="outlined"
                            color="error"
                            startIcon={<DeleteIcon />}
                            onClick={() => deleteDatabricksConnection(idx)}
                          >
                            Remove
                          </Button>
                        </Stack>
                      </Stack>
                    </AccordionDetails>
                  </Accordion>
                )
              })}
            </Stack>
          )}
        </Paper>
      )}

      {/* ============================================================ */}
      {/* TAB 2: Storage Connections */}
      {/* ============================================================ */}
      {activeTab === 2 && (
        <Paper variant="outlined" sx={{ p: 2 }}>
          <Stack direction="row" justifyContent="space-between" alignItems="center" sx={{ mb: 2 }}>
            <Box>
              <Typography variant="h6">Storage Connections</Typography>
              <Typography variant="body2" color="text.secondary">
                Configure Azure Blob Storage / ADLS Gen2 connections.
              </Typography>
            </Box>
            <Button variant="outlined" startIcon={<AddIcon />} onClick={addStorageConnection}>
              Add Connection
            </Button>
          </Stack>

          {draft.storage_connections.length === 0 ? (
            <Alert severity="info">No storage connections configured.</Alert>
          ) : (
            <Stack spacing={2}>
              {draft.storage_connections.map((conn, idx) => {
                const testKey = `storage:${conn.name}`
                const result = testResults[testKey]
                const isTesting = testingKey === testKey

                return (
                  <Accordion key={idx} defaultExpanded={draft.storage_connections.length === 1}>
                    <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                      <Stack direction="row" spacing={1.5} alignItems="center" sx={{ width: '100%', pr: 2 }}>
                        <StorageIcon color="action" />
                        <Typography fontWeight={600}>{conn.name}</Typography>
                        <Chip
                          label={conn.connectivity}
                          size="small"
                          color={conn.connectivity === 'gateway' ? 'warning' : 'default'}
                          variant="outlined"
                        />
                        {conn.account_name && (
                          <Typography variant="caption" color="text.secondary">
                            {conn.account_name}
                          </Typography>
                        )}
                        <Box sx={{ flex: 1 }} />
                        {result && (
                          <Chip
                            icon={result.overall === 'ok' ? <CheckIcon /> : <ErrorIcon />}
                            label={result.overall}
                            size="small"
                            color={result.overall === 'ok' ? 'success' : result.overall === 'partial' ? 'warning' : 'error'}
                          />
                        )}
                      </Stack>
                    </AccordionSummary>
                    <AccordionDetails>
                      <Stack spacing={2}>
                        <Stack direction="row" spacing={1.5} flexWrap="wrap" useFlexGap>
                          <TextField
                            label="Connection Name"
                            size="small"
                            value={conn.name}
                            onChange={e => updateStorageConnection(idx, { ...conn, name: e.target.value })}
                            sx={{ minWidth: 220 }}
                          />
                          <TextField
                            label="Account Name"
                            size="small"
                            value={conn.account_name}
                            onChange={e => updateStorageConnection(idx, { ...conn, account_name: e.target.value })}
                            sx={{ minWidth: 220 }}
                          />
                          <TextField
                            label="Container"
                            size="small"
                            value={conn.container || ''}
                            onChange={e => updateStorageConnection(idx, { ...conn, container: e.target.value })}
                            sx={{ minWidth: 220 }}
                          />
                          <TextField
                            select
                            label="Endpoint Type"
                            size="small"
                            value={conn.endpoint_type || 'blob'}
                            onChange={e =>
                              updateStorageConnection(idx, {
                                ...conn,
                                endpoint_type: e.target.value as 'blob' | 'dfs',
                              })
                            }
                            sx={{ minWidth: 140 }}
                          >
                            <MenuItem value="blob">Blob</MenuItem>
                            <MenuItem value="dfs">DFS (ADLS Gen2)</MenuItem>
                          </TextField>
                          <TextField
                            select
                            label="Connectivity"
                            size="small"
                            value={conn.connectivity}
                            onChange={e =>
                              updateStorageConnection(idx, {
                                ...conn,
                                connectivity: e.target.value as 'direct' | 'gateway',
                              })
                            }
                            sx={{ minWidth: 160 }}
                          >
                            <MenuItem value="direct">Direct</MenuItem>
                            <MenuItem value="gateway">Gateway</MenuItem>
                          </TextField>
                          {conn.connectivity === 'gateway' && (
                            <TextField
                              select
                              label="Gateway"
                              size="small"
                              value={conn.gateway_name || ''}
                              onChange={e =>
                                updateStorageConnection(idx, { ...conn, gateway_name: e.target.value })
                              }
                              sx={{ minWidth: 200 }}
                            >
                              {draft.gateways.map(gw => (
                                <MenuItem key={gw.name} value={gw.name}>
                                  {gw.name} ({gw.environment || gw.fqdn})
                                </MenuItem>
                              ))}
                            </TextField>
                          )}
                        </Stack>

                        {/* Auth */}
                        <Divider />
                        <Typography variant="subtitle2">Authentication</Typography>
                        <AuthFields
                          auth={conn.auth}
                          onChange={auth => updateStorageConnection(idx, { ...conn, auth })}
                        />

                        {/* Test result detail */}
                        {result && (
                          <>
                            <Divider />
                            <Typography variant="subtitle2">Test Result</Typography>
                            {result.error && (
                              <Alert severity="error">
                                {result.error.message}
                                {result.error.action && (
                                  <Typography variant="body2" sx={{ mt: 0.5 }}>
                                    💡 {result.error.action}
                                  </Typography>
                                )}
                              </Alert>
                            )}
                            {result.steps.map((step, si) => (
                              <Alert key={si} severity={step.status === 'ok' ? 'success' : step.status === 'skipped' ? 'info' : 'warning'}>
                                <strong>{step.step}</strong>: {step.status}
                                {step.host && ` → ${step.host}`}
                                {step.url && ` → ${step.url}`}
                                {step.detail && typeof step.detail === 'object' && step.detail.message && (
                                  <Typography variant="body2" sx={{ mt: 0.5 }}>
                                    {step.detail.message}
                                    {step.detail.action && <> — 💡 {step.detail.action}</>}
                                  </Typography>
                                )}
                              </Alert>
                            ))}
                          </>
                        )}

                        {/* Actions */}
                        <Stack direction="row" spacing={1} justifyContent="flex-end">
                          <Button
                            size="small"
                            variant="outlined"
                            color="primary"
                            startIcon={isTesting ? <CircularProgress size={14} /> : <TestIcon />}
                            onClick={() => handleTestConnection('storage', conn.name)}
                            disabled={isTesting || !conn.account_name}
                          >
                            Test Connection
                          </Button>
                          <Button
                            size="small"
                            variant="outlined"
                            color="error"
                            startIcon={<DeleteIcon />}
                            onClick={() => deleteStorageConnection(idx)}
                          >
                            Remove
                          </Button>
                        </Stack>
                      </Stack>
                    </AccordionDetails>
                  </Accordion>
                )
              })}
            </Stack>
          )}
        </Paper>
      )}
    </Box>
  )
}