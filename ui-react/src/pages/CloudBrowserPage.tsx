// ui-react/src/pages/CloudBrowserPage.tsx
/**
 * Cloud Data Browser — Browse Azure Storage containers/blobs and DBFS.
 * Only active when cloud mode is enabled.
 * Does NOT replace the existing DataManager/DataManagerPage.
 */
import React from 'react'
import {
  Box,
  Typography,
  Paper,
  Stack,
  Button,
  Alert,
  Chip,
  CircularProgress,
  TextField,
  MenuItem,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TablePagination,
  IconButton,
  Tooltip,
  Breadcrumbs,
  Link,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Tabs,
  Tab,
} from '@mui/material'
import {
  Folder as FolderIcon,
  InsertDriveFile as FileIcon,
  ArrowBack as BackIcon,
  Search as SearchIcon,
  Visibility as PreviewIcon,
  Refresh as RefreshIcon,
  Cloud as CloudIcon,
  Computer as LocalIcon,
  Storage as StorageIcon,
  Hub as DbxIcon,
} from '@mui/icons-material'
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

interface PreviewData {
  columns: string[]
  row_count: number
  data: any[]
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

// ============================================================
// Main Component
// ============================================================

export default function CloudBrowserPage() {
  const { config, isCloudMode } = useCloud()
  const [activeTab, setActiveTab] = React.useState(0) // 0=storage, 1=dbfs

  // Connection selection
  const storageConns = config.storage_connections || []
  const dbxConns = config.databricks_connections || []
  const [selectedStorage, setSelectedStorage] = React.useState(storageConns[0]?.name || '')
  const [selectedDbx, setSelectedDbx] = React.useState(dbxConns[0]?.name || '')

  // Browse state (storage)
  const [containers, setContainers] = React.useState<ContainerInfo[]>([])
  const [currentContainer, setCurrentContainer] = React.useState('')
  const [blobs, setBlobs] = React.useState<BrowseItem[]>([])
  const [blobSearch, setBlobSearch] = React.useState('')
  const [blobPage, setBlobPage] = React.useState(0)
  const [blobsPerPage, setBlobsPerPage] = React.useState(50)

  // Browse state (DBFS)
  const [dbfsPath, setDbfsPath] = React.useState('/')
  const [dbfsItems, setDbfsItems] = React.useState<BrowseItem[]>([])
  const [dbfsSearch, setDbfsSearch] = React.useState('')

  // Loading / error
  const [loading, setLoading] = React.useState(false)
  const [error, setError] = React.useState<string | null>(null)

  // Preview dialog
  const [previewOpen, setPreviewOpen] = React.useState(false)
  const [previewData, setPreviewData] = React.useState<PreviewData | null>(null)
  const [previewPath, setPreviewPath] = React.useState('')
  const [previewLoading, setPreviewLoading] = React.useState(false)

  // ---- Storage: list containers ----
  const loadContainers = React.useCallback(async () => {
    if (!selectedStorage) return
    setLoading(true)
    setError(null)
    try {
      const { data } = await axios.get(`${API}/cloud/browse/storage/${selectedStorage}/containers`)
      setContainers(data.containers || [])
    } catch (e: any) {
      setError(e.response?.data?.detail || e.message)
    } finally {
      setLoading(false)
    }
  }, [selectedStorage])

  // ---- Storage: list blobs ----
  const loadBlobs = React.useCallback(async () => {
    if (!selectedStorage || !currentContainer) return
    setLoading(true)
    setError(null)
    try {
      const { data } = await axios.get(`${API}/cloud/browse/storage/${selectedStorage}/blobs`, {
        params: {
          container: currentContainer,
          search: blobSearch || undefined,
          limit: blobsPerPage,
          offset: blobPage * blobsPerPage,
        },
      })
      setBlobs(data.items || [])
    } catch (e: any) {
      setError(e.response?.data?.detail || e.message)
    } finally {
      setLoading(false)
    }
  }, [selectedStorage, currentContainer, blobSearch, blobPage, blobsPerPage])

  // ---- DBFS: list files ----
  const loadDbfs = React.useCallback(async () => {
    if (!selectedDbx) return
    setLoading(true)
    setError(null)
    try {
      const { data } = await axios.get(`${API}/cloud/browse/dbfs/${selectedDbx}`, {
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
  }, [selectedDbx, dbfsPath, dbfsSearch])

  // ---- Preview a dataset ----
  const handlePreview = async (item: BrowseItem, connType: 'storage' | 'dbfs') => {
    setPreviewPath(item.path)
    setPreviewOpen(true)
    setPreviewLoading(true)
    setPreviewData(null)
    try {
      const connName = connType === 'storage' ? selectedStorage : selectedDbx
      const { data } = await axios.post(`${API}/cloud/browse/preview`, null, {
        params: {
          connection_name: connName,
          connection_type: connType,
          path: item.path,
          limit: 100,
        },
      })
      setPreviewData({ columns: data.columns, row_count: data.row_count, data: data.data })
    } catch (e: any) {
      setError(e.response?.data?.detail || e.message)
      setPreviewOpen(false)
    } finally {
      setPreviewLoading(false)
    }
  }

  // Auto-load containers when storage connection changes
  React.useEffect(() => {
    if (activeTab === 0 && selectedStorage && isCloudMode) {
      loadContainers()
    }
  }, [selectedStorage, activeTab, isCloudMode, loadContainers])

  // Auto-load blobs when container changes
  React.useEffect(() => {
    if (currentContainer) {
      loadBlobs()
    }
  }, [currentContainer, blobSearch, blobPage, blobsPerPage, loadBlobs])

  // Auto-load DBFS when path changes
  React.useEffect(() => {
    if (activeTab === 1 && selectedDbx && isCloudMode) {
      loadDbfs()
    }
  }, [selectedDbx, dbfsPath, dbfsSearch, activeTab, isCloudMode, loadDbfs])

  // ---- Not in cloud mode ----
  if (!isCloudMode) {
    return (
      <Box sx={{ p: 3, maxWidth: 800, mx: 'auto' }}>
        <Typography variant="h4" gutterBottom>☁️ Cloud Browser</Typography>
        <Alert severity="info" icon={<LocalIcon />}>
          Cloud data browsing is only available in <strong>Cloud mode</strong>.
          Switch to Cloud mode in <strong>Settings</strong> to browse Azure Storage and DBFS.
        </Alert>
      </Box>
    )
  }

  return (
    <Box sx={{ p: 3, maxWidth: 1400, mx: 'auto' }}>
      <Stack direction="row" justifyContent="space-between" alignItems="center" sx={{ mb: 2 }}>
        <Box>
          <Typography variant="h4" gutterBottom>☁️ Cloud Data Browser</Typography>
          <Typography variant="body2" color="text.secondary">
            Browse and preview datasets from Azure Storage and Databricks DBFS.
          </Typography>
        </Box>
      </Stack>

      {error && (
        <Alert severity="error" onClose={() => setError(null)} sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}

      <Tabs value={activeTab} onChange={(_, v) => setActiveTab(v)} sx={{ mb: 2 }}>
        <Tab icon={<StorageIcon />} label="Azure Storage" iconPosition="start" disabled={storageConns.length === 0} />
        <Tab icon={<DbxIcon />} label="DBFS" iconPosition="start" disabled={dbxConns.length === 0} />
      </Tabs>

      {/* ============================== */}
      {/* TAB 0: Azure Storage Browser   */}
      {/* ============================== */}
      {activeTab === 0 && (
        <Paper variant="outlined" sx={{ p: 2 }}>
          {/* Connection selector */}
          <Stack direction="row" spacing={2} alignItems="center" sx={{ mb: 2 }}>
            <TextField
              select
              label="Storage Connection"
              size="small"
              value={selectedStorage}
              onChange={e => {
                setSelectedStorage(e.target.value)
                setCurrentContainer('')
                setBlobs([])
              }}
              sx={{ minWidth: 250 }}
            >
              {storageConns.map((c: any) => (
                <MenuItem key={c.name} value={c.name}>
                  {c.name} ({c.account_name})
                </MenuItem>
              ))}
            </TextField>
            <Tooltip title="Refresh">
              <IconButton onClick={currentContainer ? loadBlobs : loadContainers}>
                <RefreshIcon />
              </IconButton>
            </Tooltip>
          </Stack>

          {/* Container list OR blob list */}
          {!currentContainer ? (
            <>
              <Typography variant="subtitle1" sx={{ mb: 1 }}>Containers</Typography>
              {loading ? (
                <Box sx={{ display: 'flex', justifyContent: 'center', py: 4 }}>
                  <CircularProgress />
                </Box>
              ) : containers.length === 0 ? (
                <Alert severity="info">No containers found. Check connection settings.</Alert>
              ) : (
                <Stack spacing={0.5}>
                  {containers.map(c => (
                    <Paper
                      key={c.name}
                      variant="outlined"
                      sx={{ p: 1.5, cursor: 'pointer', '&:hover': { bgcolor: 'action.hover' } }}
                      onClick={() => setCurrentContainer(c.name)}
                    >
                      <Stack direction="row" spacing={1.5} alignItems="center">
                        <FolderIcon color="primary" />
                        <Typography fontWeight={600}>{c.name}</Typography>
                        {c.last_modified && (
                          <Typography variant="caption" color="text.secondary">
                            Modified: {c.last_modified}
                          </Typography>
                        )}
                      </Stack>
                    </Paper>
                  ))}
                </Stack>
              )}
            </>
          ) : (
            <>
              {/* Breadcrumb + back */}
              <Stack direction="row" spacing={1} alignItems="center" sx={{ mb: 1.5 }}>
                <IconButton size="small" onClick={() => { setCurrentContainer(''); setBlobs([]) }}>
                  <BackIcon />
                </IconButton>
                <Breadcrumbs>
                  <Link
                    component="button"
                    underline="hover"
                    onClick={() => { setCurrentContainer(''); setBlobs([]) }}
                  >
                    Containers
                  </Link>
                  <Typography color="text.primary" fontWeight={600}>
                    {currentContainer}
                  </Typography>
                </Breadcrumbs>
              </Stack>

              {/* Search */}
              <TextField
                size="small"
                placeholder="Search blobs..."
                value={blobSearch}
                onChange={e => { setBlobSearch(e.target.value); setBlobPage(0) }}
                InputProps={{ startAdornment: <SearchIcon sx={{ mr: 1, color: 'text.secondary' }} /> }}
                sx={{ mb: 1.5, maxWidth: 400 }}
                fullWidth
              />

              {loading ? (
                <Box sx={{ display: 'flex', justifyContent: 'center', py: 4 }}>
                  <CircularProgress />
                </Box>
              ) : (
                <>
                  <TableContainer>
                    <Table size="small">
                      <TableHead>
                        <TableRow>
                          <TableCell>Name</TableCell>
                          <TableCell>Size</TableCell>
                          <TableCell>Modified</TableCell>
                          <TableCell align="right">Actions</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {blobs.map((item, idx) => (
                          <TableRow key={idx} hover>
                            <TableCell>
                              <Stack direction="row" spacing={1} alignItems="center">
                                <FileIcon fontSize="small" color="action" />
                                <Typography variant="body2">{item.name}</Typography>
                              </Stack>
                            </TableCell>
                            <TableCell>{formatBytes(item.size_bytes)}</TableCell>
                            <TableCell>
                              <Typography variant="caption">{item.last_modified || '—'}</Typography>
                            </TableCell>
                            <TableCell align="right">
                              <Tooltip title="Preview">
                                <IconButton size="small" onClick={() => handlePreview(item, 'storage')}>
                                  <PreviewIcon fontSize="small" />
                                </IconButton>
                              </Tooltip>
                            </TableCell>
                          </TableRow>
                        ))}
                        {blobs.length === 0 && (
                          <TableRow>
                            <TableCell colSpan={4} align="center">
                              <Typography variant="body2" color="text.secondary" sx={{ py: 2 }}>
                                No blobs found.
                              </Typography>
                            </TableCell>
                          </TableRow>
                        )}
                      </TableBody>
                    </Table>
                  </TableContainer>
                  <TablePagination
                    component="div"
                    count={-1}
                    page={blobPage}
                    onPageChange={(_, p) => setBlobPage(p)}
                    rowsPerPage={blobsPerPage}
                    onRowsPerPageChange={e => { setBlobsPerPage(parseInt(e.target.value, 10)); setBlobPage(0) }}
                    rowsPerPageOptions={[25, 50, 100]}
                    labelDisplayedRows={() => `Page ${blobPage + 1}`}
                  />
                </>
              )}
            </>
          )}
        </Paper>
      )}

      {/* ============================== */}
      {/* TAB 1: DBFS Browser            */}
      {/* ============================== */}
      {activeTab === 1 && (
        <Paper variant="outlined" sx={{ p: 2 }}>
          <Stack direction="row" spacing={2} alignItems="center" sx={{ mb: 2 }}>
            <TextField
              select
              label="Databricks Connection"
              size="small"
              value={selectedDbx}
              onChange={e => { setSelectedDbx(e.target.value); setDbfsPath('/') }}
              sx={{ minWidth: 250 }}
            >
              {dbxConns.map((c: any) => (
                <MenuItem key={c.name} value={c.name}>
                  {c.name} (WS: {c.workspace_id})
                </MenuItem>
              ))}
            </TextField>
            <Tooltip title="Refresh">
              <IconButton onClick={loadDbfs}>
                <RefreshIcon />
              </IconButton>
            </Tooltip>
          </Stack>

          {/* Path breadcrumb */}
          <Stack direction="row" spacing={1} alignItems="center" sx={{ mb: 1.5 }}>
            {dbfsPath !== '/' && (
              <IconButton
                size="small"
                onClick={() => {
                  const parts = dbfsPath.split('/').filter(Boolean)
                  parts.pop()
                  setDbfsPath('/' + parts.join('/') || '/')
                }}
              >
                <BackIcon />
              </IconButton>
            )}
            <Breadcrumbs>
              <Link component="button" underline="hover" onClick={() => setDbfsPath('/')}>
                /
              </Link>
              {dbfsPath.split('/').filter(Boolean).map((part, idx, arr) => {
                const fullPath = '/' + arr.slice(0, idx + 1).join('/')
                return idx < arr.length - 1 ? (
                  <Link key={idx} component="button" underline="hover" onClick={() => setDbfsPath(fullPath)}>
                    {part}
                  </Link>
                ) : (
                  <Typography key={idx} color="text.primary" fontWeight={600}>{part}</Typography>
                )
              })}
            </Breadcrumbs>
          </Stack>

          {/* Search */}
          <TextField
            size="small"
            placeholder="Search files..."
            value={dbfsSearch}
            onChange={e => setDbfsSearch(e.target.value)}
            InputProps={{ startAdornment: <SearchIcon sx={{ mr: 1, color: 'text.secondary' }} /> }}
            sx={{ mb: 1.5, maxWidth: 400 }}
            fullWidth
          />

          {loading ? (
            <Box sx={{ display: 'flex', justifyContent: 'center', py: 4 }}>
              <CircularProgress />
            </Box>
          ) : (
            <TableContainer>
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>Name</TableCell>
                    <TableCell>Size</TableCell>
                    <TableCell align="right">Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {dbfsItems.map((item, idx) => {
                    const isDir = item.extra?.is_dir
                    return (
                      <TableRow
                        key={idx}
                        hover
                        sx={{ cursor: isDir ? 'pointer' : 'default' }}
                        onClick={() => {
                          if (isDir) {
                            const rawPath = item.path.replace('dbfs:', '')
                            setDbfsPath(rawPath)
                          }
                        }}
                      >
                        <TableCell>
                          <Stack direction="row" spacing={1} alignItems="center">
                            {isDir ? <FolderIcon color="primary" fontSize="small" /> : <FileIcon color="action" fontSize="small" />}
                            <Typography variant="body2">{item.name}</Typography>
                            {isDir && <Chip label="dir" size="small" variant="outlined" />}
                          </Stack>
                        </TableCell>
                        <TableCell>{isDir ? '—' : formatBytes(item.size_bytes)}</TableCell>
                        <TableCell align="right">
                          {!isDir && (
                            <Tooltip title="Preview">
                              <IconButton size="small" onClick={e => { e.stopPropagation(); handlePreview(item, 'dbfs') }}>
                                <PreviewIcon fontSize="small" />
                              </IconButton>
                            </Tooltip>
                          )}
                        </TableCell>
                      </TableRow>
                    )
                  })}
                  {dbfsItems.length === 0 && (
                    <TableRow>
                      <TableCell colSpan={3} align="center">
                        <Typography variant="body2" color="text.secondary" sx={{ py: 2 }}>
                          Empty directory.
                        </Typography>
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
            </TableContainer>
          )}
        </Paper>
      )}

      {/* ============================== */}
      {/* Preview Dialog                 */}
      {/* ============================== */}
      <Dialog open={previewOpen} onClose={() => setPreviewOpen(false)} maxWidth="lg" fullWidth>
        <DialogTitle>
          Dataset Preview
          <Typography variant="body2" color="text.secondary">{previewPath}</Typography>
        </DialogTitle>
        <DialogContent dividers>
          {previewLoading ? (
            <Box sx={{ display: 'flex', justifyContent: 'center', py: 6 }}>
              <CircularProgress />
            </Box>
          ) : previewData ? (
            <>
              <Typography variant="body2" sx={{ mb: 1 }}>
                {previewData.row_count} rows × {previewData.columns.length} columns
              </Typography>
              <TableContainer sx={{ maxHeight: 500 }}>
                <Table size="small" stickyHeader>
                  <TableHead>
                    <TableRow>
                      {previewData.columns.map(col => (
                        <TableCell key={col} sx={{ fontWeight: 700 }}>{col}</TableCell>
                      ))}
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {previewData.data.map((row, ri) => (
                      <TableRow key={ri}>
                        {previewData.columns.map(col => (
                          <TableCell key={col}>
                            <Typography variant="body2" noWrap sx={{ maxWidth: 250 }}>
                              {row[col] != null ? String(row[col]) : '—'}
                            </Typography>
                          </TableCell>
                        ))}
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            </>
          ) : (
            <Alert severity="info">No data to display.</Alert>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setPreviewOpen(false)}>Close</Button>
        </DialogActions>
      </Dialog>
    </Box>
  )
}