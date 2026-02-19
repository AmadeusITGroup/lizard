// ui-react/src/pages/Dashboard.tsx
// Flexible Dashboard with drag-and-drop, resize, and dynamic layout
// FIX: Ensure panels render correctly and maintain proper height

import React, { useState, useCallback, useEffect, useRef } from 'react'
import {
  Box,
  Paper,
  Stack,
  IconButton,
  Tooltip,
  Button,
  Typography,
  Menu,
  MenuItem,
  ListItemIcon,
  ListItemText,
  Divider,
  Chip,
  alpha,
} from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import CloseIcon from '@mui/icons-material/Close'
import ContentCopyIcon from '@mui/icons-material/ContentCopy'
import DragIndicatorIcon from '@mui/icons-material/DragIndicator'
import SettingsIcon from '@mui/icons-material/Settings'
import FullscreenIcon from '@mui/icons-material/Fullscreen'
import FullscreenExitIcon from '@mui/icons-material/FullscreenExit'
import TimelinePanel, { TimelineConfig } from '../sections/TimelinePanel'
import MapPanel from '../sections/MapPanel'
import DataPanel from '../sections/DataPanel'
import GraphPanel from '../sections/GraphPanel'
import GlobalFiltersBar from '../components/GlobalFiltersBar'
import FlowSankeyPanel from '../sections/FlowSankeyPanel'
import AddVisualizationDialog, { VisualizationType } from '../components/AddVisualizationDialog'
import {
  DashboardLayoutEngine,
  LayoutItem,
  LayoutPreset,
  LAYOUT_PRESETS
} from '../components/DashboardLayoutEngine'
import PieChartPanel from '../sections/PieChartPanel'
import BarChartPanel from '../sections/BarChartPanel'
import ScatterPlotPanel from '../sections/ScatterPlotPanel'
import JobProgressPanel from '../components/JobProgressPanel'


// ============================================================
// Types
// ============================================================

interface VisualizationPanel {
  id: string
  type: VisualizationType
  title: string
  config?: any
  layout: LayoutItem
}

interface DashboardState {
  panels: VisualizationPanel[]
  columnCount: number
  rowHeight: number
  gap: number
}

// ============================================================
// Constants
// ============================================================

const STORAGE_KEY = 'lizard.dashboard.state'

const VIZ_TITLES: Record<VisualizationType, string> = {
  timeline: 'Timeline',
  map: 'Map / Globe',
  grid: 'Data Grid',
  graph: 'Network Graph',
  sankey: 'Flow Diagram',
  pie: 'Pie Chart',
  bar: 'Bar Chart',
  scatter: 'Scatter Plot',
  jobs: 'Job Progress',
}

const VIZ_COLORS: Record<VisualizationType, string> = {
  timeline: '#2196F3',
  map: '#4CAF50',
  grid: '#FF9800',
  graph: '#9C27B0',
  sankey: '#00BCD4',
  pie: '#E91E63',
  bar: '#673AB7',
  scatter: '#FF5722',
  jobs: '#009688',
}

const DEFAULT_STATE: DashboardState = {
  panels: [],
  columnCount: 12,
  rowHeight: 60,
  gap: 12,
}

// ============================================================
// Utility Functions
// ============================================================

function generatePanelId(type: VisualizationType): string {
  return `${type}_${Date.now()}_${Math.random().toString(36).substring(2, 8)}`
}

function computeNextPosition(panels: VisualizationPanel[], columns: number): { x: number; y: number } {
  if (panels.length === 0) {
    return { x: 0, y: 0 }
  }

  // Find the lowest occupied row
  let maxY = 0
  panels.forEach(p => {
    const bottomEdge = p.layout.y + p.layout.h
    if (bottomEdge > maxY) maxY = bottomEdge
  })

  // Try to find space in existing rows first
  for (let testY = 0; testY <= maxY; testY++) {
    for (let testX = 0; testX <= columns - 4; testX++) {
      const testRect = { x: testX, y: testY, w: 6, h: 5 }
      const hasCollision = panels.some(p => {
        return !(
          testRect.x + testRect.w <= p.layout.x ||
          testRect.x >= p.layout.x + p.layout.w ||
          testRect.y + testRect.h <= p.layout.y ||
          testRect.y >= p.layout.y + p.layout.h
        )
      })
      if (!hasCollision) {
        return { x: testX, y: testY }
      }
    }
  }

  // Place at bottom
  return { x: 0, y: maxY }
}

function getDefaultLayoutForType(type: VisualizationType): Omit<LayoutItem, 'x' | 'y'> {
  switch (type) {
    case 'timeline':
      return { w: 12, h: 6, minW: 6, minH: 5 }
    case 'map':
      return { w: 6, h: 6, minW: 4, minH: 4 }
    case 'grid':
      return { w: 6, h: 6, minW: 4, minH: 4 }
    case 'graph':
      return { w: 8, h: 8, minW: 5, minH: 5 }
    case 'sankey':
      return { w: 6, h: 6, minW: 4, minH: 4 }
    case 'pie':
      return { w: 5, h: 6, minW: 4, minH: 5 }
    case 'bar':
      return { w: 6, h: 5, minW: 4, minH: 4 }
    case 'scatter':
      return { w: 6, h: 6, minW: 5, minH: 5 }
    case 'jobs':
      return { w: 4, h: 6, minW: 3, minH: 4 }
    default:
      return { w: 6, h: 5, minW: 3, minH: 3 }
  }
}

// ============================================================
// Panel Component
// ============================================================

interface PanelWrapperProps {
  panel: VisualizationPanel
  isSelected: boolean
  isDragging: boolean
  isResizing: boolean
  onSelect: () => void
  onRemove: () => void
  onDuplicate: () => void
  onToggleFullscreen: () => void
  isFullscreen: boolean
  onDragStart: (e: React.MouseEvent) => void
  onResizeStart: (e: React.MouseEvent, direction: string) => void
  children: React.ReactNode
}

function PanelWrapper({
  panel,
  isSelected,
  isDragging,
  isResizing,
  onSelect,
  onRemove,
  onDuplicate,
  onToggleFullscreen,
  isFullscreen,
  onDragStart,
  onResizeStart,
  children,
}: PanelWrapperProps) {
  const color = VIZ_COLORS[panel.type] || '#666'

  return (
    <Paper
      variant="outlined"
      onClick={(e) => {
        e.stopPropagation()
        onSelect()
      }}
      sx={{
        height: '100%',
        display: 'flex',
        flexDirection: 'column',
        overflow: 'hidden',
        position: 'relative',
        borderColor: isSelected ? color : 'divider',
        borderWidth: isSelected ? 2 : 1,
        transition: isDragging || isResizing ? 'none' : 'border-color 0.2s, box-shadow 0.2s',
        boxShadow: isSelected ? `0 0 0 1px ${alpha(color, 0.2)}` : 'none',
        '&:hover': {
          borderColor: alpha(color, 0.5),
        },
      }}
    >
      {/* Panel Header */}
      <Box
        sx={{
          display: 'flex',
          alignItems: 'center',
          px: 1,
          py: 0.5,
          borderBottom: '1px solid',
          borderColor: 'divider',
          bgcolor: alpha(color, 0.05),
          cursor: 'move',
          userSelect: 'none',
          flexShrink: 0,
        }}
        onMouseDown={(e) => {
          e.stopPropagation()
          onDragStart(e)
        }}
      >
        <DragIndicatorIcon
          sx={{
            fontSize: 18,
            color: 'text.secondary',
            mr: 0.5,
          }}
        />
        <Chip
          label={panel.title}
          size="small"
          sx={{
            bgcolor: alpha(color, 0.1),
            color: color,
            fontWeight: 600,
            fontSize: 11,
            height: 22,
          }}
        />
        <Box sx={{ flex: 1 }} />

        {/* Panel Actions */}
        <Stack direction="row" spacing={0.25}>
          <Tooltip title="Duplicate">
            <IconButton size="small" onClick={(e) => { e.stopPropagation(); onDuplicate(); }}>
              <ContentCopyIcon sx={{ fontSize: 16 }} />
            </IconButton>
          </Tooltip>
          <Tooltip title={isFullscreen ? "Exit Fullscreen" : "Fullscreen"}>
            <IconButton size="small" onClick={(e) => { e.stopPropagation(); onToggleFullscreen(); }}>
              {isFullscreen ? (
                <FullscreenExitIcon sx={{ fontSize: 16 }} />
              ) : (
                <FullscreenIcon sx={{ fontSize: 16 }} />
              )}
            </IconButton>
          </Tooltip>
          <Tooltip title="Remove">
            <IconButton
              size="small"
              onClick={(e) => { e.stopPropagation(); onRemove(); }}
              sx={{ color: 'error.main' }}
            >
              <CloseIcon sx={{ fontSize: 16 }} />
            </IconButton>
          </Tooltip>
        </Stack>
      </Box>

      {/* Panel Content - KEY FIX: enable scrolling */}
        <Box
          sx={{
            flex: 1,
            minHeight: 0,  // Important for flex children
            overflow: 'hidden',  // Changed from 'auto' to 'hidden'
            position: 'relative',
            display: 'flex',
            flexDirection: 'column',
          }}
        >
          {children}
        </Box>

      {/* Resize Handles */}
      {isSelected && !isFullscreen && (
        <>
          {/* Right edge */}
          <Box
            sx={{
              position: 'absolute',
              right: 0,
              top: 30,
              bottom: 0,
              width: 8,
              cursor: 'ew-resize',
              '&:hover': { bgcolor: alpha(color, 0.2) },
            }}
            onMouseDown={(e) => {
              e.stopPropagation()
              onResizeStart(e, 'e')
            }}
          />
          {/* Bottom edge */}
          <Box
            sx={{
              position: 'absolute',
              bottom: 0,
              left: 0,
              right: 0,
              height: 8,
              cursor: 'ns-resize',
              '&:hover': { bgcolor: alpha(color, 0.2) },
            }}
            onMouseDown={(e) => {
              e.stopPropagation()
              onResizeStart(e, 's')
            }}
          />
          {/* Bottom-right corner */}
          <Box
            sx={{
              position: 'absolute',
              right: 0,
              bottom: 0,
              width: 14,
              height: 14,
              cursor: 'nwse-resize',
              '&:hover': { bgcolor: alpha(color, 0.3) },
              '&::after': {
                content: '""',
                position: 'absolute',
                right: 2,
                bottom: 2,
                width: 8,
                height: 8,
                borderRight: `2px solid ${color}`,
                borderBottom: `2px solid ${color}`,
              },
            }}
            onMouseDown={(e) => {
              e.stopPropagation()
              onResizeStart(e, 'se')
            }}
          />
        </>
      )}
    </Paper>
  )
}

// ============================================================
// Main Dashboard Component
// ============================================================

export default function Dashboard() {
  // State
  const [state, setState] = useState<DashboardState>(() => {
    try {
      const stored = localStorage.getItem(STORAGE_KEY)
      if (stored) {
        const parsed = JSON.parse(stored)
        return { ...DEFAULT_STATE, ...parsed }
      }
    } catch (err) {
      console.warn('Failed to load dashboard state:', err)
    }
    return DEFAULT_STATE
  })

  const [selectedPanelId, setSelectedPanelId] = useState<string | null>(null)
  const [fullscreenPanelId, setFullscreenPanelId] = useState<string | null>(null)
  const [addDialogOpen, setAddDialogOpen] = useState(false)
  const [layoutMenuAnchor, setLayoutMenuAnchor] = useState<null | HTMLElement>(null)

  // Drag and resize state
  const [dragState, setDragState] = useState<{
    panelId: string
    startX: number
    startY: number
    startLayout: LayoutItem
  } | null>(null)

  const [resizeState, setResizeState] = useState<{
    panelId: string
    direction: string
    startX: number
    startY: number
    startLayout: LayoutItem
  } | null>(null)

  const containerRef = useRef<HTMLDivElement>(null)

  // Persist state
  useEffect(() => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(state))
  }, [state])

  // ============================================================
  // Panel Management
  // ============================================================

  const addPanel = useCallback((type: VisualizationType) => {
    const newId = generatePanelId(type)
    const position = computeNextPosition(state.panels, state.columnCount)
    const layoutDefaults = getDefaultLayoutForType(type)

    const newPanel: VisualizationPanel = {
      id: newId,
      type,
      title: VIZ_TITLES[type] || type,
      layout: {
        ...layoutDefaults,
        x: position.x,
        y: position.y,
      },
    }

    setState(prev => ({
      ...prev,
      panels: [...prev.panels, newPanel],
    }))

    setSelectedPanelId(newId)
  }, [state.panels, state.columnCount])

  const removePanel = useCallback((panelId: string) => {
    setState(prev => ({
      ...prev,
      panels: prev.panels.filter(p => p.id !== panelId),
    }))
    if (selectedPanelId === panelId) {
      setSelectedPanelId(null)
    }
    if (fullscreenPanelId === panelId) {
      setFullscreenPanelId(null)
    }
  }, [selectedPanelId, fullscreenPanelId])

  const duplicatePanel = useCallback((panel: VisualizationPanel) => {
    const newId = generatePanelId(panel.type)
    const position = computeNextPosition(state.panels, state.columnCount)

    const newPanel: VisualizationPanel = {
      ...panel,
      id: newId,
      title: `${panel.title} (Copy)`,
      layout: {
        ...panel.layout,
        x: position.x,
        y: position.y,
      },
      config: panel.config ? { ...panel.config } : undefined,
    }

    setState(prev => ({
      ...prev,
      panels: [...prev.panels, newPanel],
    }))

    setSelectedPanelId(newId)
  }, [state.panels, state.columnCount])

  const updatePanelLayout = useCallback((panelId: string, newLayout: Partial<LayoutItem>) => {
    setState(prev => ({
      ...prev,
      panels: prev.panels.map(p =>
        p.id === panelId
          ? { ...p, layout: { ...p.layout, ...newLayout } }
          : p
      ),
    }))
  }, [])

  const updatePanelConfig = useCallback((panelId: string, config: any) => {
    setState(prev => ({
      ...prev,
      panels: prev.panels.map(p =>
        p.id === panelId ? { ...p, config } : p
      ),
    }))
  }, [])

  // ============================================================
  // Drag and Resize Handlers
  // ============================================================

  const handleDragStart = useCallback((e: React.MouseEvent, panel: VisualizationPanel) => {
    e.preventDefault()
    e.stopPropagation()
    setDragState({
      panelId: panel.id,
      startX: e.clientX,
      startY: e.clientY,
      startLayout: { ...panel.layout },
    })
    setSelectedPanelId(panel.id)
  }, [])

  const handleResizeStart = useCallback((e: React.MouseEvent, panel: VisualizationPanel, direction: string) => {
    e.preventDefault()
    e.stopPropagation()
    setResizeState({
      panelId: panel.id,
      direction,
      startX: e.clientX,
      startY: e.clientY,
      startLayout: { ...panel.layout },
    })
  }, [])

  useEffect(() => {
    if (!dragState && !resizeState) return

    const handleMouseMove = (e: MouseEvent) => {
      if (!containerRef.current) return

      const containerRect = containerRef.current.getBoundingClientRect()
      const cellWidth = (containerRect.width - (state.columnCount - 1) * state.gap) / state.columnCount
      const cellHeight = state.rowHeight

      if (dragState) {
        const deltaX = e.clientX - dragState.startX
        const deltaY = e.clientY - dragState.startY

        const deltaColsRaw = deltaX / (cellWidth + state.gap)
        const deltaRowsRaw = deltaY / (cellHeight + state.gap)

        const deltaCols = Math.round(deltaColsRaw)
        const deltaRows = Math.round(deltaRowsRaw)

        const panel = state.panels.find(p => p.id === dragState.panelId)
        if (!panel) return

        let newX = dragState.startLayout.x + deltaCols
        let newY = dragState.startLayout.y + deltaRows

        // Clamp to bounds
        newX = Math.max(0, Math.min(newX, state.columnCount - panel.layout.w))
        newY = Math.max(0, newY)

        updatePanelLayout(dragState.panelId, { x: newX, y: newY })
      }

      if (resizeState) {
        const panel = state.panels.find(p => p.id === resizeState.panelId)
        if (!panel) return

        const deltaX = e.clientX - resizeState.startX
        const deltaY = e.clientY - resizeState.startY

        const deltaColsRaw = deltaX / (cellWidth + state.gap)
        const deltaRowsRaw = deltaY / (cellHeight + state.gap)

        let newW = resizeState.startLayout.w
        let newH = resizeState.startLayout.h

        if (resizeState.direction.includes('e')) {
          newW = resizeState.startLayout.w + Math.round(deltaColsRaw)
        }
        if (resizeState.direction.includes('s')) {
          newH = resizeState.startLayout.h + Math.round(deltaRowsRaw)
        }

        // Apply constraints
        const minW = panel.layout.minW || 2
        const minH = panel.layout.minH || 2
        newW = Math.max(minW, Math.min(newW, state.columnCount - panel.layout.x))
        newH = Math.max(minH, newH)

        updatePanelLayout(resizeState.panelId, { w: newW, h: newH })
      }
    }

    const handleMouseUp = () => {
      setDragState(null)
      setResizeState(null)
    }

    document.addEventListener('mousemove', handleMouseMove)
    document.addEventListener('mouseup', handleMouseUp)

    return () => {
      document.removeEventListener('mousemove', handleMouseMove)
      document.removeEventListener('mouseup', handleMouseUp)
    }
  }, [dragState, resizeState, state.panels, state.columnCount, state.rowHeight, state.gap, updatePanelLayout])

  // ============================================================
  // Layout Presets
  // ============================================================

  const applyPreset = useCallback((preset: LayoutPreset) => {
    const newPanels: VisualizationPanel[] = preset.panels.map(item => ({
      id: generatePanelId(item.type),
      type: item.type,
      title: VIZ_TITLES[item.type] || item.type,
      layout: item.layout,
    }))

    setState(prev => ({
      ...prev,
      panels: newPanels,
    }))

    setLayoutMenuAnchor(null)
  }, [])

  const clearDashboard = useCallback(() => {
    setState(prev => ({ ...prev, panels: [] }))
    setSelectedPanelId(null)
    setFullscreenPanelId(null)
    setLayoutMenuAnchor(null)
  }, [])

  // ============================================================
  // Render Visualization Content
  // ============================================================

const renderPanelContent = (panel: VisualizationPanel) => {
  switch (panel.type) {
    case 'timeline':
      return (
        <Box sx={{ height: '100%', overflow: 'auto' }}>
          <TimelinePanel
            instanceId={panel.id}
            initialConfig={panel.config}
            onConfigChange={(cfg) => updatePanelConfig(panel.id, cfg)}
          />
        </Box>
      )
    case 'map':
      return (
        <Box sx={{ height: '100%', overflow: 'auto' }}>
          <MapPanel />
        </Box>
      )
    case 'grid':
      return (
        <Box sx={{ height: '100%', overflow: 'auto' }}>
          <DataPanel />
        </Box>
      )
    case 'graph':
      return (
        <Box sx={{ height: '100%', overflow: 'auto' }}>
          <GraphPanel />
        </Box>
      )
    case 'sankey':
      return (
        <Box sx={{ height: '100%', overflow: 'auto' }}>
          <FlowSankeyPanel />
        </Box>
      )
    case 'pie':
      return (
        <Box sx={{ height: '100%', overflow: 'auto' }}>
          <PieChartPanel
            instanceId={panel.id}
            initialConfig={panel.config}
            onConfigChange={(cfg) => updatePanelConfig(panel.id, cfg)}
          />
        </Box>
      )
    case 'bar':
      return (
        <Box sx={{ height: '100%', overflow: 'auto' }}>
          <BarChartPanel
            instanceId={panel.id}
            initialConfig={panel.config}
            onConfigChange={(cfg) => updatePanelConfig(panel.id, cfg)}
          />
        </Box>
      )
    case 'scatter':
      return (
        <Box sx={{ height: '100%', overflow: 'auto' }}>
          <ScatterPlotPanel
            instanceId={panel.id}
            initialConfig={panel.config}
            onConfigChange={(cfg) => updatePanelConfig(panel.id, cfg)}
          />
        </Box>
      )
    case 'jobs':
      return (
        <Box sx={{ height: '100%', overflow: 'auto' }}>
          <JobProgressPanel />
        </Box>
      )
    default:
      return (
        <Box
          sx={{
            height: '100%',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            color: 'text.secondary',
          }}
        >
          <Typography>Coming soon: {panel.type}</Typography>
        </Box>
      )
  }
}

  // ============================================================
  // Compute Grid Styles
  // ============================================================

  const computePanelStyle = (layout: LayoutItem): React.CSSProperties => {
    const { x, y, w, h } = layout
    return {
      gridColumn: `${x + 1} / span ${w}`,
      gridRow: `${y + 1} / span ${h}`,
    }
  }

  const computeGridRows = (): number => {
    if (state.panels.length === 0) return 8
    let maxRow = 0
    state.panels.forEach(p => {
      const bottom = p.layout.y + p.layout.h
      if (bottom > maxRow) maxRow = bottom
    })
    return Math.max(maxRow + 2, 8) // Extra space for new panels
  }

  // ============================================================
  // Fullscreen Panel
  // ============================================================

  const fullscreenPanel = fullscreenPanelId
    ? state.panels.find(p => p.id === fullscreenPanelId)
    : null

  if (fullscreenPanel) {
    return (
      <Box sx={{ height: 'calc(100vh - 64px)', display: 'flex', flexDirection: 'column' }}>
        <GlobalFiltersBar />
        <Box sx={{ flex: 1, p: 1, display: 'flex', flexDirection: 'column' }}>
          <PanelWrapper
            panel={fullscreenPanel}
            isSelected={true}
            isDragging={false}
            isResizing={false}
            onSelect={() => {}}
            onRemove={() => removePanel(fullscreenPanel.id)}
            onDuplicate={() => duplicatePanel(fullscreenPanel)}
            onToggleFullscreen={() => setFullscreenPanelId(null)}
            isFullscreen={true}
            onDragStart={() => {}}
            onResizeStart={() => {}}
          >
            {renderPanelContent(fullscreenPanel)}
          </PanelWrapper>
        </Box>
      </Box>
    )
  }

  // ============================================================
  // Main Render
  // ============================================================

  return (
    <Box
      sx={{
        height: 'calc(100vh - 64px)',
        display: 'flex',
        flexDirection: 'column',
        bgcolor: 'background.default',
      }}
    >
      <GlobalFiltersBar />

      {/* Toolbar */}
      <Stack
        direction="row"
        spacing={1}
        alignItems="center"
        sx={{ px: 2, py: 1, borderBottom: '1px solid', borderColor: 'divider' }}
      >
        <Button
          variant="contained"
          startIcon={<AddIcon />}
          onClick={() => setAddDialogOpen(true)}
          size="small"
        >
          Add Panel
        </Button>

        <Button
          variant="outlined"
          startIcon={<SettingsIcon />}
          onClick={(e) => setLayoutMenuAnchor(e.currentTarget)}
          size="small"
        >
          Layouts
        </Button>

        <Menu
          anchorEl={layoutMenuAnchor}
          open={Boolean(layoutMenuAnchor)}
          onClose={() => setLayoutMenuAnchor(null)}
        >
          <MenuItem disabled>
            <Typography variant="caption" color="text.secondary">
              Quick Layouts
            </Typography>
          </MenuItem>
          {LAYOUT_PRESETS.map(preset => (
            <MenuItem key={preset.id} onClick={() => applyPreset(preset)}>
              <ListItemText primary={preset.name} secondary={preset.description} />
            </MenuItem>
          ))}
          <Divider />
          <MenuItem onClick={clearDashboard}>
            <ListItemIcon>
              <CloseIcon fontSize="small" color="error" />
            </ListItemIcon>
            <ListItemText primary="Clear All" />
          </MenuItem>
        </Menu>

        <Box sx={{ flex: 1 }} />

        <Typography variant="body2" color="text.secondary">
          {state.panels.length} panel{state.panels.length !== 1 ? 's' : ''}
        </Typography>
      </Stack>

      {/* Dashboard Grid */}
      <Box
        ref={containerRef}
        onClick={() => setSelectedPanelId(null)}
        sx={{
          flex: 1,
          overflow: 'auto',
          p: 2,
        }}
      >
        {state.panels.length === 0 ? (
          <Box
            sx={{
              height: '100%',
              display: 'flex',
              flexDirection: 'column',
              alignItems: 'center',
              justifyContent: 'center',
              color: 'text.secondary',
            }}
          >
            <Typography variant="h6" gutterBottom>
              No visualizations yet
            </Typography>
            <Typography variant="body2" sx={{ mb: 2 }}>
              Click "Add Panel" to start building your dashboard
            </Typography>
            <Button
              variant="outlined"
              startIcon={<AddIcon />}
              onClick={() => setAddDialogOpen(true)}
            >
              Add Your First Panel
            </Button>
          </Box>
        ) : (
          <Box
            sx={{
              display: 'grid',
              gridTemplateColumns: `repeat(${state.columnCount}, 1fr)`,
              gridTemplateRows: `repeat(${computeGridRows()}, ${state.rowHeight}px)`,
              gap: `${state.gap}px`,
              minHeight: '100%',
            }}
          >
            {state.panels.map(panel => (
              <Box
                key={panel.id}
                sx={{
                  ...computePanelStyle(panel.layout),
                  transition: (dragState?.panelId === panel.id || resizeState?.panelId === panel.id)
                    ? 'none'
                    : 'all 0.15s ease-out',
                }}
              >
                <PanelWrapper
                  panel={panel}
                  isSelected={selectedPanelId === panel.id}
                  isDragging={dragState?.panelId === panel.id}
                  isResizing={resizeState?.panelId === panel.id}
                  onSelect={() => setSelectedPanelId(panel.id)}
                  onRemove={() => removePanel(panel.id)}
                  onDuplicate={() => duplicatePanel(panel)}
                  onToggleFullscreen={() => setFullscreenPanelId(panel.id)}
                  isFullscreen={false}
                  onDragStart={(e) => handleDragStart(e, panel)}
                  onResizeStart={(e, dir) => handleResizeStart(e, panel, dir)}
                >
                  {renderPanelContent(panel)}
                </PanelWrapper>
              </Box>
            ))}
          </Box>
        )}
      </Box>

      {/* Add Dialog */}
      <AddVisualizationDialog
        open={addDialogOpen}
        onClose={() => setAddDialogOpen(false)}
        onAdd={(type) => {
          addPanel(type)
          setAddDialogOpen(false)
        }}
      />
    </Box>
  )
}