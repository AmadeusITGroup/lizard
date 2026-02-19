// path: ui-react/src/components/DataWorkbench/SourceSelector.tsx
/**
 * SourceSelector - Left panel for browsing and selecting data sources
 */
import React from 'react'
import {
  Box,
  Typography,
  Stack,
  List,
  ListItem,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  ListItemSecondaryAction,
  IconButton,
  Chip,
  Divider,
  TextField,
  InputAdornment,
  Collapse,
  Tooltip,
  CircularProgress,
  Menu,
  MenuItem,
  Alert,
} from '@mui/material'
import {
  Storage as TableIcon,
  ViewModule as ViewIcon,
  Search as SearchIcon,
  ExpandMore as ExpandMoreIcon,
  ExpandLess as ExpandLessIcon,
  MoreVert as MoreIcon,
  Refresh as RefreshIcon,
  Delete as DeleteIcon,
  PlayArrow as LoadIcon,
  Save as MaterializeIcon,
} from '@mui/icons-material'
import { DataSource, DerivedView } from './DataWorkbench'

interface SourceSelectorProps {
  sources: DataSource[]
  views: DerivedView[]
  selectedSource: string | null
  onSelectSource: (sourceName: string) => void
  onLoadView: (view: DerivedView) => void
  onDeleteView: (viewId: string) => Promise<void>
  onMaterializeView: (viewId: string) => Promise<void>
  isLoading: boolean
}

export default function SourceSelector({
  sources,
  views,
  selectedSource,
  onSelectSource,
  onLoadView,
  onDeleteView,
  onMaterializeView,
  isLoading,
}: SourceSelectorProps) {
  const [searchQuery, setSearchQuery] = React.useState('')
  const [tablesExpanded, setTablesExpanded] = React.useState(true)
  const [viewsExpanded, setViewsExpanded] = React.useState(true)
  const [menuAnchor, setMenuAnchor] = React.useState<null | HTMLElement>(null)
  const [menuView, setMenuView] = React.useState<DerivedView | null>(null)

  // Filter sources by search
  const filteredSources = React.useMemo(() => {
    if (!searchQuery) return sources
    const q = searchQuery.toLowerCase()
    return sources.filter(s => 
      s.name.toLowerCase().includes(q) || 
      s.description?.toLowerCase().includes(q)
    )
  }, [sources, searchQuery])

  const filteredViews = React.useMemo(() => {
    if (!searchQuery) return views
    const q = searchQuery.toLowerCase()
    return views.filter(v => 
      v.name.toLowerCase().includes(q) || 
      v.description?.toLowerCase().includes(q)
    )
  }, [views, searchQuery])

  // Separate tables and views from sources
  const tables = filteredSources.filter(s => s.type === 'table')
  const sourceViews = filteredSources.filter(s => s.type === 'view')

  const handleOpenMenu = (event: React.MouseEvent<HTMLElement>, view: DerivedView) => {
    event.stopPropagation()
    setMenuAnchor(event.currentTarget)
    setMenuView(view)
  }

  const handleCloseMenu = () => {
    setMenuAnchor(null)
    setMenuView(null)
  }

  const handleDeleteView = async () => {
    if (menuView) {
      await onDeleteView(menuView.id)
      handleCloseMenu()
    }
  }

  const handleMaterializeView = async () => {
    if (menuView) {
      await onMaterializeView(menuView.id)
      handleCloseMenu()
    }
  }

  const formatRowCount = (count?:  number) => {
    if (count === undefined || count === null) return '?'
    if (count >= 1000000) return `${(count / 1000000).toFixed(1)}M`
    if (count >= 1000) return `${(count / 1000).toFixed(1)}K`
    return count.toString()
  }

  return (
    <Box sx={{ height: '100%', display: 'flex', flexDirection:  'column' }}>
      {/* Header */}
      <Typography variant="subtitle1" fontWeight={600} gutterBottom>
        Data Sources
      </Typography>

      {/* Search */}
      <TextField
        size="small"
        placeholder="Search sources..."
        value={searchQuery}
        onChange={(e) => setSearchQuery(e.target.value)}
        InputProps={{
          startAdornment: (
            <InputAdornment position="start">
              <SearchIcon fontSize="small" sx={{ opacity: 0.5 }} />
            </InputAdornment>
          ),
        }}
        sx={{ mb: 2 }}
        fullWidth
      />

      {/* Loading */}
      {isLoading && (
        <Box sx={{ display: 'flex', justifyContent: 'center', py: 2 }}>
          <CircularProgress size={24} />
        </Box>
      )}

      {/* Sources List */}
      <Box sx={{ flex: 1, overflow: 'auto' }}>
        {/* Tables Section */}
        <Box>
          <ListItemButton 
            onClick={() => setTablesExpanded(!tablesExpanded)}
            sx={{ py: 0.5, px: 1, borderRadius: 1 }}
          >
            {tablesExpanded ? <ExpandLessIcon fontSize="small" /> : <ExpandMoreIcon fontSize="small" />}
            <Typography variant="body2" fontWeight={600} sx={{ ml: 1, flex: 1 }}>
              Tables ({tables.length})
            </Typography>
          </ListItemButton>
          
          <Collapse in={tablesExpanded}>
            <List dense disablePadding>
              {tables.length === 0 ? (
                <ListItem>
                  <ListItemText 
                    secondary="No tables found. Import some data first."
                    secondaryTypographyProps={{ variant: 'caption' }}
                  />
                </ListItem>
              ) : (
                tables.map(source => (
                  <ListItemButton
                    key={source.name}
                    selected={selectedSource === source.name}
                    onClick={() => onSelectSource(source.name)}
                    sx={{ pl: 3, py: 0.5, borderRadius: 1 }}
                  >
                    <ListItemIcon sx={{ minWidth: 32 }}>
                      <TableIcon fontSize="small" color="primary" />
                    </ListItemIcon>
                    <ListItemText
                      primary={source.name}
                      secondary={`${formatRowCount(source.row_count)} rows`}
                      primaryTypographyProps={{ variant: 'body2', noWrap: true }}
                      secondaryTypographyProps={{ variant: 'caption' }}
                    />
                  </ListItemButton>
                ))
              )}
            </List>
          </Collapse>
        </Box>

        <Divider sx={{ my: 1 }} />

        {/* Views Section */}
        <Box>
          <ListItemButton 
            onClick={() => setViewsExpanded(!viewsExpanded)}
            sx={{ py: 0.5, px: 1, borderRadius: 1 }}
          >
            {viewsExpanded ? <ExpandLessIcon fontSize="small" /> : <ExpandMoreIcon fontSize="small" />}
            <Typography variant="body2" fontWeight={600} sx={{ ml: 1, flex: 1 }}>
              Saved Views ({views.length})
            </Typography>
          </ListItemButton>
          
          <Collapse in={viewsExpanded}>
            <List dense disablePadding>
              {views.length === 0 ? (
                <ListItem>
                  <ListItemText 
                    secondary="No saved views yet. Build a query and save it."
                    secondaryTypographyProps={{ variant: 'caption' }}
                  />
                </ListItem>
              ) : (
                views.map(view => (
                  <ListItemButton
                    key={view.id}
                    onClick={() => onLoadView(view)}
                    sx={{ pl: 3, py: 0.5, borderRadius: 1 }}
                  >
                    <ListItemIcon sx={{ minWidth:  32 }}>
                      <ViewIcon fontSize="small" color={view.is_materialized ? 'success' : 'secondary'} />
                    </ListItemIcon>
                    <ListItemText
                      primary={
                        <Stack direction="row" spacing={0.5} alignItems="center">
                          <Typography variant="body2" noWrap sx={{ flex: 1 }}>
                            {view.name}
                          </Typography>
                          {view.is_materialized && (
                            <Chip label="M" size="small" color="success" sx={{ height: 16, fontSize: 10 }} />
                          )}
                        </Stack>
                      }
                      secondary={view.row_count ? `${formatRowCount(view.row_count)} rows` : 'Virtual'}
                      secondaryTypographyProps={{ variant:  'caption' }}
                    />
                    <ListItemSecondaryAction>
                      <IconButton 
                        size="small" 
                        onClick={(e) => handleOpenMenu(e, view)}
                      >
                        <MoreIcon fontSize="small" />
                      </IconButton>
                    </ListItemSecondaryAction>
                  </ListItemButton>
                ))
              )}
            </List>
          </Collapse>
        </Box>
      </Box>

      {/* Stats */}
      <Divider sx={{ my: 1 }} />
      <Box sx={{ opacity: 0.7 }}>
        <Typography variant="caption" display="block">
          {tables.length} table(s), {views.length} view(s)
        </Typography>
      </Box>

      {/* View Context Menu */}
      <Menu
        anchorEl={menuAnchor}
        open={Boolean(menuAnchor)}
        onClose={handleCloseMenu}
      >
        <MenuItem onClick={() => { if (menuView) onLoadView(menuView); handleCloseMenu() }}>
          <LoadIcon fontSize="small" sx={{ mr: 1 }} />
          Load Pipeline
        </MenuItem>
        {menuView && ! menuView.is_materialized && (
          <MenuItem onClick={handleMaterializeView}>
            <MaterializeIcon fontSize="small" sx={{ mr: 1 }} />
            Materialize
          </MenuItem>
        )}
        {menuView && menuView.is_materialized && (
          <MenuItem onClick={handleMaterializeView}>
            <RefreshIcon fontSize="small" sx={{ mr: 1 }} />
            Refresh
          </MenuItem>
        )}
        <Divider />
        <MenuItem onClick={handleDeleteView} sx={{ color: 'error.main' }}>
          <DeleteIcon fontSize="small" sx={{ mr:  1 }} />
          Delete
        </MenuItem>
      </Menu>
    </Box>
  )
}