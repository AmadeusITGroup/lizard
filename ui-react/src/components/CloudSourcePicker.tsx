// path: ui-react/src/components/CloudSourcePicker.tsx
/**
 * CloudSourcePicker — Inline cloud data source browser + manual path entry.
 * Used inside UploadDrawer to let users pick a cloud resource as simply as a local CSV.
 * Only rendered when cloud mode is active and at least one connection exists.
 */
import React from 'react'
import {
  Box,
  Stack,
  Typography,
  TextField,
  MenuItem,
  Button,
  Paper,
  Chip,
  Alert,
  CircularProgress,
  IconButton,
  Tooltip,
  Tabs,
  Tab,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Breadcrumbs,
  Link,
} from '@mui/material'
import CloudIcon from '@mui/icons-material/Cloud'
import FolderIcon from '@mui/icons-material/Folder'
import InsertDriveFileIcon from '@mui/icons-material/InsertDriveFile'
import ArrowBackIcon from '@mui/icons-material/ArrowBack'
import RefreshIcon from '@mui/icons-material/Refresh'
import CheckCircleIcon from '@mui/icons-material/CheckCircle'
import EditIcon from '@mui/icons-material/Edit'
import SearchIcon from '@mui/icons-material/Search'
import StorageIcon from '@mui/icons-material/Storage'
import axios from 'axios'
import { useCloud } from '../context/CloudContext'

const API = import.meta.env.VITE_API_BASE || 'http://localhost:8000'

// ============================================================
// Types
// ============================================================

interface BrowseItem {
  name: string
  path: string
  provider: string
  size_bytes?: number
  last_modified?: string
  content_type?: string
  container?: string
  account?: string
  extra?: Record<string, any>
}

interface ContainerInfo {
  name: string
  last_modified?: string
}

export interface CloudSourceSelection {
  connectionName: string
  connectionType: 'storage' | 'dbfs'
  path: string
  fileName: string
  container?: string
}

interface Props {
  onSelect: (selection: CloudSourceSelection) => void
  selection: CloudSourceSelection | null
}

// ============================================================
// Helpers
// ============================================================

function formatBytes(bytes?: number): string {
  if (bytes == null) return '—'
  if (bytes === 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return `${(bytes / Math.pow(k, i)).toFixed(1)} ${sizes[i]}`
}

function getFileName(path: string): string {
  const parts = path.split('/').filter(Boolean)
  return parts[parts.length - 1] || path
}

// ============================================================
// Main Component
// ============================================================

export default function CloudSourcePicker({ onSelect, selection }: Props) {
  const { config } = useCloud()

  // Mode: 'browse' or 'manual'
  const [mode, setMode] = React.useState<'browse' | 'manual'>('browse')

  // Connection selection
  const storageConns = config.storage_connections || []
  const dbxConns = config.databricks_connections || []

  const allConnections = React.useMemo(() => {
    const conns: Array<{ name: string; type: 'storage' | 'dbfs'; label: string }> = []
    storageConns.forEach(c => conns.push({
      name: c.name,
      type: 'storage',
      label: `${c.name} (Storage: ${c.account_name})`,
    }))
    dbxConns.forEach(c => conns.push({
      name: c.name,
      type: 'dbfs',
      label: `${c.name} (DBFS: ${c.workspace_id})`,
    }))
    return conns
  }, [storageConns, dbxConns])

  const [selectedConn, setSelectedConn] = React.useState(allConnections[0]?.name || '')
  const selectedConnType = allConnections.find(c => c.name === selectedConn)?.type || 'storage'

  // Browse state (storage)
  const [containers, setContainers] = React.useState<ContainerInfo[]>([])
  const [currentContainer, setCurrentContainer] = React.useState('')
  const [blobs, setBlobs] = React.useState<BrowseItem[]>([])
  const [blobSearch, setBlobSearch] = React.useState('')

  // Browse state (DBFS)
  const [dbfsPath, setDbfsPath] = React.useState('/')
  const [dbfsItems, setDbfsItems] = React.useState<BrowseItem[]>([])
  const [dbfsSearch, setDbfsSearch] = React.useState('')

  // Manual path entry
  const [manualPath, setManualPath] = React.useState('')

  // Loading / error
  const [loading, setLoading] = React.useState(false)
  const [error, setError] = React.useState<string | null>(null)

  // ---- Storage: list containers ----
  const loadContainers = React.useCallback(async () => {
    if (!selectedConn || selectedConnType !== 'storage') return
    setLoading(true)
    setError(null)
    try {
      const { data } = await axios.get(`${API}/cloud/browse/storage/${selectedConn}/containers`)
      setContainers(data.containers || [])
    } catch (e: any) {
      setError(e.response?.data?.detail || e.message)
    } finally {
      setLoading(false)
    }
  }, [selectedConn, selectedConnType])

  // ---- Storage: list blobs ----
  const loadBlobs = React.useCallback(async () => {
    if (!selectedConn || !currentContainer) return
    setLoading(true)
    setError(null)
    try {
      const { data } = await axios.get(`${API}/cloud/browse/storage/${selectedConn}/blobs`, {
        params: {
          container: currentContainer,
          search: blobSearch || undefined,
          limit: 100,
        },
      })
      setBlobs(data.items || [])
    } catch (e: any) {
      setError(e.response?.data?.detail || e.message)
    } finally {
      setLoading(false)
    }
  }, [selectedConn, currentContainer, blobSearch])

  // ---- DBFS: list files ----
  const loadDbfs = React.useCallback(async () => {
    if (!selectedConn || selectedConnType !== 'dbfs') return
    setLoading(true)
    setError(null)
    try {
      const { data } = await axios.get(`${API}/cloud/browse/dbfs/${selectedConn}`, {
        params: {
          path: dbfsPath,
          search: dbfsSearch || undefined,
          limit: 200,
        },
      })
      setDbfsItems(data.items || [])
    } catch (e: any) {
      setError(e.response?.data?.detail || e.message)
    } finally {
      setLoading(false)
    }
  }, [selectedConn, selectedConnType, dbfsPath, dbfsSearch])

  // Auto-load when connection changes
  React.useEffect(() => {
    if (mode !== 'browse' || !selectedConn) return
    if (selectedConnType === 'storage') {
      setCurrentContainer('')
      setBlobs([])
      loadContainers()
    } else {
      setDbfsPath('/')
      setDbfsItems([])
      loadDbfs()
    }
  }, [selectedConn, selectedConnType, mode])

  // Auto-load blobs when container changes
  React.useEffect(() => {
    if (currentContainer && selectedConnType === 'storage') {
      loadBlobs()
    }
  }, [currentContainer, blobSearch, loadBlobs])

  // Auto-load DBFS when path changes
  React.useEffect(() => {
    if (selectedConnType === 'dbfs' && mode === 'browse') {
      loadDbfs()
    }
  }, [dbfsPath, dbfsSearch, loadDbfs, mode])

  // ---- Select a file (from browse) ----
  const handleSelectItem = (item: BrowseItem) => {
    onSelect({
      connectionName: selectedConn,
      connectionType: selectedConnType,
      path: item.path,
      fileName: item.name || getFileName(item.path),
      container: currentContainer || undefined,
    })
  }

  // ---- Select from manual path ----
  const handleManualSelect = () => {
    if (!manualPath.trim()) return
    onSelect({
      connectionName: selectedConn,
      connectionType: selectedConnType,
      path: manualPath.trim(),
      fileName: getFileName(manualPath.trim()),
      container: undefined,
    })
  }

  // Reset connection-specific state on connection change
  const handleConnectionChange = (connName: string) => {
    setSelectedConn(connName)
    setCurrentContainer('')
    setBlobs([])
    setDbfsPath('/')
    setDbfsItems([])
    setBlobSearch('')
    setDbfsSearch('')
    setError(null)
  }

  // ---- Already selected ----
  if (selection) {
    return (
      <Paper
        variant="outlined"
        sx={{
          p: 3,
          textAlign: 'center',
          borderColor: 'success.main',
          bgcolor: 'success.50',
          borderWidth: 2,
        }}
      >
        <CheckCircleIcon sx={{ fontSize: 48, color: 'success.main', mb: 1 }} />
        <Typography variant="h6" gutterBottom>
          {selection.fileName}
        </Typography>
        <Typography variant="body2" color="text.secondary" gutterBottom>
          {selection.connectionName} ({selection.connectionType})
        </Typography>
        <Chip
          label={selection.path}
          size="small"
          variant="outlined"
          sx={{ mt: 0.5, maxWidth: '100%' }}
        />
        <Box sx={{ mt: 2 }}>
          <Button
            size="small"
            variant="outlined"
            onClick={() => onSelect(null as any)}
          >
            Change Source
          </Button>
        </Box>
      </Paper>
    )
  }

  // ---- No connections available ----
  if (allConnections.length === 0) {
    return (
      <Alert severity="warning" icon={<CloudIcon />}>
        No cloud connections configured. Add a Storage or Databricks connection in
        <strong> Settings → Cloud</strong> first.
      </Alert>
    )
  }

  return (
    <Box>
      {/* Connection Selector */}
      <Stack direction="row" spacing={2} alignItems="center" sx={{ mb: 2 }}>
        <TextField
          select
          label="Cloud Connection"
          size="small"
          value={selectedConn}
          onChange={e => handleConnectionChange(e.target.value)}
          sx={{ minWidth: 280 }}
        >
          {allConnections.map(c => (
            <MenuItem key={c.name} value={c.name}>
              <Stack direction="row" spacing={1} alignItems="center">
                <StorageIcon fontSize="small" />
                <span>{c.label}</span>
              </Stack>
            </MenuItem>
          ))}
        </TextField>

        <Chip
          label={selectedConnType === 'storage' ? 'Azure Storage' : 'DBFS'}
          size="small"
          color="info"
          variant="outlined"
        />
      </Stack>

      {/* Browse / Manual Toggle */}
      <Tabs
        value={mode === 'browse' ? 0 : 1}
        onChange={(_, v) => setMode(v === 0 ? 'browse' : 'manual')}
        sx={{ mb: 2 }}
      >
        <Tab label="Browse" icon={<FolderIcon />} iconPosition="start" sx={{ minHeight: 40 }} />
        <Tab label="Enter Path" icon={<EditIcon />} iconPosition="start" sx={{ minHeight: 40 }} />
      </Tabs>

      {error && (
        <Alert severity="error" onClose={() => setError(null)} sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}

      {/* ========================================== */}
      {/* BROWSE MODE                                */}
      {/* ========================================== */}
      {mode === 'browse' && (
        <Box>
          {loading && (
            <Box sx={{ display: 'flex', justifyContent: 'center', py: 3 }}>
              <CircularProgress size={28} />
            </Box>
          )}

          {/* --- Storage Browse --- */}
          {!loading && selectedConnType === 'storage' && (
            <>
              {!currentContainer ? (
                /* Container List */
                <Box>
                  <Stack direction="row" justifyContent="space-between" alignItems="center" sx={{ mb: 1 }}>
                    <Typography variant="subtitle2">Select Container</Typography>
                    <Tooltip title="Refresh">
                      <IconButton size="small" onClick={loadContainers}>
                        <RefreshIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                  </Stack>
                  {containers.length === 0 ? (
                    <Alert severity="info" variant="outlined">
                      No containers found. Check connection settings.
                    </Alert>
                  ) : (
                    <Stack spacing={0.5} sx={{ maxHeight: 250, overflow: 'auto' }}>
                      {containers.map(c => (
                        <Paper
                          key={c.name}
                          variant="outlined"
                          sx={{
                            p: 1.5,
                            cursor: 'pointer',
                            '&:hover': { bgcolor: 'action.hover', borderColor: 'primary.main' },
                            transition: 'all 0.15s',
                          }}
                          onClick={() => setCurrentContainer(c.name)}
                        >
                          <Stack direction="row" spacing={1} alignItems="center">
                            <FolderIcon color="primary" fontSize="small" />
                            <Typography variant="body2" fontWeight={600}>{c.name}</Typography>
                          </Stack>
                        </Paper>
                      ))}
                    </Stack>
                  )}
                </Box>
              ) : (
                /* Blob List inside a container */
                <Box>
                  <Stack direction="row" spacing={1} alignItems="center" sx={{ mb: 1 }}>
                    <IconButton size="small" onClick={() => { setCurrentContainer(''); setBlobs([]) }}>
                      <ArrowBackIcon fontSize="small" />
                    </IconButton>
                    <Breadcrumbs>
                      <Link component="button" underline="hover" variant="body2"
                        onClick={() => { setCurrentContainer(''); setBlobs([]) }}
                      >
                        Containers
                      </Link>
                      <Typography variant="body2" color="text.primary" fontWeight={600}>
                        {currentContainer}
                      </Typography>
                    </Breadcrumbs>
                    <Box flex={1} />
                    <Tooltip title="Refresh">
                      <IconButton size="small" onClick={loadBlobs}>
                        <RefreshIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                  </Stack>

                  <TextField
                    size="small"
                    placeholder="Search files..."
                    value={blobSearch}
                    onChange={e => setBlobSearch(e.target.value)}
                    InputProps={{
                      startAdornment: <SearchIcon sx={{ mr: 1, color: 'text.secondary', fontSize: 18 }} />,
                    }}
                    sx={{ mb: 1, maxWidth: 350 }}
                    fullWidth
                  />

                  <TableContainer sx={{ maxHeight: 250 }}>
                    <Table size="small" stickyHeader>
                      <TableHead>
                        <TableRow>
                          <TableCell>Name</TableCell>
                          <TableCell>Size</TableCell>
                          <TableCell align="right">Action</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {blobs.length === 0 ? (
                          <TableRow>
                            <TableCell colSpan={3} align="center">
                              <Typography variant="body2" color="text.secondary" sx={{ py: 2 }}>
                                No files found.
                              </Typography>
                            </TableCell>
                          </TableRow>
                        ) : (
                          blobs.map((item, idx) => (
                            <TableRow key={idx} hover>
                              <TableCell>
                                <Stack direction="row" spacing={1} alignItems="center">
                                  <InsertDriveFileIcon fontSize="small" color="action" />
                                  <Typography variant="body2" noWrap sx={{ maxWidth: 250 }}>
                                    {item.name}
                                  </Typography>
                                </Stack>
                              </TableCell>
                              <TableCell>
                                <Typography variant="caption">{formatBytes(item.size_bytes)}</Typography>
                              </TableCell>
                              <TableCell align="right">
                                <Button size="small" variant="outlined" onClick={() => handleSelectItem(item)}>
                                  Select
                                </Button>
                              </TableCell>
                            </TableRow>
                          ))
                        )}
                      </TableBody>
                    </Table>
                  </TableContainer>
                </Box>
              )}
            </>
          )}

          {/* --- DBFS Browse --- */}
          {!loading && selectedConnType === 'dbfs' && (
            <Box>
              <Stack direction="row" spacing={1} alignItems="center" sx={{ mb: 1 }}>
                {dbfsPath !== '/' && (
                  <IconButton
                    size="small"
                    onClick={() => {
                      const parts = dbfsPath.split('/').filter(Boolean)
                      parts.pop()
                      setDbfsPath('/' + parts.join('/') || '/')
                    }}
                  >
                    <ArrowBackIcon fontSize="small" />
                  </IconButton>
                )}
                <Breadcrumbs>
                  <Link component="button" underline="hover" variant="body2" onClick={() => setDbfsPath('/')}>
                    /
                  </Link>
                  {dbfsPath.split('/').filter(Boolean).map((part, idx, arr) => {
                    const fullPath = '/' + arr.slice(0, idx + 1).join('/')
                    return idx < arr.length - 1 ? (
                      <Link key={idx} component="button" underline="hover" variant="body2"
                        onClick={() => setDbfsPath(fullPath)}
                      >
                        {part}
                      </Link>
                    ) : (
                      <Typography key={idx} variant="body2" color="text.primary" fontWeight={600}>
                        {part}
                      </Typography>
                    )
                  })}
                </Breadcrumbs>
                <Box flex={1} />
                <Tooltip title="Refresh">
                  <IconButton size="small" onClick={loadDbfs}>
                    <RefreshIcon fontSize="small" />
                  </IconButton>
                </Tooltip>
              </Stack>

              <TextField
                size="small"
                placeholder="Search files..."
                value={dbfsSearch}
                onChange={e => setDbfsSearch(e.target.value)}
                InputProps={{
                  startAdornment: <SearchIcon sx={{ mr: 1, color: 'text.secondary', fontSize: 18 }} />,
                }}
                sx={{ mb: 1, maxWidth: 350 }}
                fullWidth
              />

              <TableContainer sx={{ maxHeight: 250 }}>
                <Table size="small" stickyHeader>
                  <TableHead>
                    <TableRow>
                      <TableCell>Name</TableCell>
                      <TableCell>Size</TableCell>
                      <TableCell align="right">Action</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {dbfsItems.length === 0 ? (
                      <TableRow>
                        <TableCell colSpan={3} align="center">
                          <Typography variant="body2" color="text.secondary" sx={{ py: 2 }}>
                            Empty directory.
                          </Typography>
                        </TableCell>
                      </TableRow>
                    ) : (
                      dbfsItems.map((item, idx) => {
                        const isDir = item.extra?.is_dir
                        return (
                          <TableRow key={idx} hover sx={{ cursor: isDir ? 'pointer' : 'default' }}>
                            <TableCell
                              onClick={() => {
                                if (isDir) setDbfsPath(item.path.replace('dbfs:', ''))
                              }}
                            >
                              <Stack direction="row" spacing={1} alignItems="center">
                                {isDir
                                  ? <FolderIcon fontSize="small" color="primary" />
                                  : <InsertDriveFileIcon fontSize="small" color="action" />
                                }
                                <Typography variant="body2" noWrap sx={{ maxWidth: 250 }}>
                                  {item.name}
                                </Typography>
                                {isDir && <Chip label="dir" size="small" variant="outlined" sx={{ height: 18, fontSize: 10 }} />}
                              </Stack>
                            </TableCell>
                            <TableCell>
                              <Typography variant="caption">{isDir ? '—' : formatBytes(item.size_bytes)}</Typography>
                            </TableCell>
                            <TableCell align="right">
                              {!isDir && (
                                <Button size="small" variant="outlined" onClick={() => handleSelectItem(item)}>
                                  Select
                                </Button>
                              )}
                            </TableCell>
                          </TableRow>
                        )
                      })
                    )}
                  </TableBody>
                </Table>
              </TableContainer>
            </Box>
          )}
        </Box>
      )}

      {/* ========================================== */}
      {/* MANUAL PATH MODE                           */}
      {/* ========================================== */}
      {mode === 'manual' && (
        <Stack spacing={2} sx={{ mt: 1 }}>
          <Alert severity="info" variant="outlined">
            Enter the full path to a CSV, JSON, or Parquet file in the selected cloud connection.
          </Alert>
          <TextField
            label="Cloud Resource Path"
            placeholder={selectedConnType === 'storage'
              ? 'container-name/path/to/data.csv'
              : '/mnt/data/events.parquet'}
            value={manualPath}
            onChange={e => setManualPath(e.target.value)}
            fullWidth
            helperText={selectedConnType === 'storage'
              ? 'Format: container/path/to/file.csv'
              : 'Format: /mnt/path/to/file.csv or dbfs:/path/to/file.csv'}
          />
          <Button
            variant="contained"
            onClick={handleManualSelect}
            disabled={!manualPath.trim()}
            startIcon={<CloudIcon />}
            sx={{ alignSelf: 'flex-start' }}
          >
            Use This Path
          </Button>
        </Stack>
      )}
    </Box>
  )
}