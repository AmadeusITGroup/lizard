// ui-react/src/components/AddVisualizationDialog.tsx
// Enhanced dialog for adding visualizations with improved UX

import React, { useState } from 'react'
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Card,
  CardContent,
  CardActionArea,
  Typography,
  Box,
  Chip,
  Stack,
  alpha,
  TextField,
  InputAdornment,
  Tabs,
  Tab,
} from '@mui/material'
import TimelineIcon from '@mui/icons-material/Timeline'
import MapIcon from '@mui/icons-material/Map'
import TableChartIcon from '@mui/icons-material/TableChart'
import AccountTreeIcon from '@mui/icons-material/AccountTree'
import SsidChartIcon from '@mui/icons-material/SsidChart'
import PieChartIcon from '@mui/icons-material/PieChart'
import BarChartIcon from '@mui/icons-material/BarChart'
import BubbleChartIcon from '@mui/icons-material/BubbleChart'
import WorkIcon from '@mui/icons-material/Work'
import SearchIcon from '@mui/icons-material/Search'

export type VisualizationType =
  | 'timeline'
  | 'map'
  | 'grid'
  | 'graph'
  | 'sankey'
  | 'pie'
  | 'bar'
  | 'scatter'
  | 'jobs'

interface VisualizationOption {
  type: VisualizationType
  label: string
  description: string
  icon: React.ReactNode
  available: boolean
  tags: string[]
  color: string
  category: 'core' | 'analysis' | 'chart' | 'cloud'
}

const VISUALIZATION_OPTIONS: VisualizationOption[] = [
  {
    type: 'timeline',
    label: 'Timeline',
    description: 'Time series with anomaly detection. Track events and metrics over time.',
    icon: <TimelineIcon sx={{ fontSize: 36 }} />,
    available: true,
    tags: ['time-series', 'anomaly', 'trends'],
    color: '#2196F3',
    category: 'core',
  },
  {
    type: 'map',
    label: 'Map / Globe',
    description: 'Geographic visualization with heatmaps and route analysis.',
    icon: <MapIcon sx={{ fontSize: 36 }} />,
    available: true,
    tags: ['geo', 'heatmap', 'routes'],
    color: '#4CAF50',
    category: 'core',
  },
  {
    type: 'grid',
    label: 'Data Grid',
    description: 'Tabular view with sorting, filtering, and export capabilities.',
    icon: <TableChartIcon sx={{ fontSize: 36 }} />,
    available: true,
    tags: ['table', 'explore', 'export'],
    color: '#FF9800',
    category: 'core',
  },
  {
    type: 'graph',
    label: 'Network Graph',
    description: 'Entity relationships showing connections between users, devices, IPs.',
    icon: <AccountTreeIcon sx={{ fontSize: 36 }} />,
    available: true,
    tags: ['relationships', 'network', 'entities'],
    color: '#9C27B0',
    category: 'analysis',
  },
  {
    type: 'sankey',
    label: 'Flow / Sankey',
    description: 'Flow diagram showing paths and transitions between states.',
    icon: <SsidChartIcon sx={{ fontSize: 36 }} />,
    available: true,
    tags: ['flow', 'funnel', 'paths'],
    color: '#00BCD4',
    category: 'analysis',
  },
  {
    type: 'pie',
    label: 'Pie / Donut',
    description: 'Distribution visualization for categorical breakdown.',
    icon: <PieChartIcon sx={{ fontSize: 36 }} />,
    available: true,
    tags: ['distribution', 'categories'],
    color: '#E91E63',
    category: 'chart',
  },
  {
    type: 'bar',
    label: 'Bar Chart',
    description: 'Compare values across categories or time periods.',
    icon: <BarChartIcon sx={{ fontSize: 36 }} />,
    available: true,
    tags: ['comparison', 'ranking'],
    color: '#673AB7',
    category: 'chart',
  },
  {
    type: 'scatter',
    label: 'Scatter Plot',
    description: 'Correlation analysis between two numeric variables.',
    icon: <BubbleChartIcon sx={{ fontSize: 36 }} />,
    available: true,
    tags: ['correlation', 'outliers'],
    color: '#FF5722',
    category: 'chart',
  },
  {
    type: 'jobs',
    label: 'Job Progress',
    description: 'Live scheduler job status with run/enable/disable controls. Requires cloud mode.',
    icon: <WorkIcon sx={{ fontSize: 36 }} />,
    available: true,
    tags: ['scheduler', 'cloud', 'monitoring'],
    color: '#009688',
    category: 'cloud',
  },
]

const CATEGORIES = [
  { id: 'all', label: 'All' },
  { id: 'core', label: 'Core' },
  { id: 'analysis', label: 'Analysis' },
  { id: 'chart', label: 'Charts' },
  { id: 'cloud', label: 'Cloud' },
]

interface AddVisualizationDialogProps {
  open: boolean
  onClose: () => void
  onAdd: (type: VisualizationType) => void
}

export default function AddVisualizationDialog({
  open,
  onClose,
  onAdd,
}: AddVisualizationDialogProps) {
  const [searchQuery, setSearchQuery] = useState('')
  const [selectedCategory, setSelectedCategory] = useState('all')
  const [hoveredType, setHoveredType] = useState<VisualizationType | null>(null)

  const handleAdd = (type: VisualizationType) => {
    onAdd(type)
    setSearchQuery('')
    setSelectedCategory('all')
    onClose()
  }

  const handleClose = () => {
    setSearchQuery('')
    setSelectedCategory('all')
    onClose()
  }

  // Filter options based on search and category
  const filteredOptions = VISUALIZATION_OPTIONS.filter(option => {
    const matchesSearch = searchQuery === '' ||
      option.label.toLowerCase().includes(searchQuery.toLowerCase()) ||
      option.description.toLowerCase().includes(searchQuery.toLowerCase()) ||
      option.tags.some(tag => tag.toLowerCase().includes(searchQuery.toLowerCase()))

    const matchesCategory = selectedCategory === 'all' || option.category === selectedCategory

    return matchesSearch && matchesCategory
  })

  return (
    <Dialog
      open={open}
      onClose={handleClose}
      maxWidth="md"
      fullWidth
      PaperProps={{
        sx: { borderRadius: 2, maxHeight: '85vh' },
      }}
    >
      <DialogTitle sx={{ pb: 1 }}>
        <Typography variant="h6" fontWeight={600}>
          Add Visualization Panel
        </Typography>
        <Typography variant="body2" color="text.secondary">
          Select a visualization type to add to your dashboard
        </Typography>
      </DialogTitle>

      <Box sx={{ px: 3, pb: 2 }}>
        <TextField
          fullWidth
          size="small"
          placeholder="Search visualizations..."
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          InputProps={{
            startAdornment: (
              <InputAdornment position="start">
                <SearchIcon color="action" />
              </InputAdornment>
            ),
          }}
          sx={{ mb: 2 }}
        />

        <Tabs
          value={selectedCategory}
          onChange={(_, val) => setSelectedCategory(val)}
          variant="scrollable"
          scrollButtons="auto"
        >
          {CATEGORIES.map(cat => (
            <Tab key={cat.id} value={cat.id} label={cat.label} />
          ))}
        </Tabs>
      </Box>

      <DialogContent sx={{ pt: 1 }}>
        <Box
          sx={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))',
            gap: 2,
          }}
        >
          {filteredOptions.map((option) => (
            <Card
              key={option.type}
              variant="outlined"
              onMouseEnter={() => setHoveredType(option.type)}
              onMouseLeave={() => setHoveredType(null)}
              sx={{
                opacity: option.available ? 1 : 0.55,
                borderColor: hoveredType === option.type && option.available
                  ? option.color
                  : 'divider',
                bgcolor: hoveredType === option.type && option.available
                  ? alpha(option.color, 0.04)
                  : 'background.paper',
                transition: 'all 0.15s ease',
                transform: hoveredType === option.type && option.available
                  ? 'translateY(-2px)'
                  : 'none',
              }}
            >
              <CardActionArea
                onClick={() => option.available && handleAdd(option.type)}
                disabled={!option.available}
                sx={{ height: '100%' }}
              >
                <CardContent sx={{ textAlign: 'center' }}>
                  <Box
                    sx={{
                      color: option.color,
                      mb: 1,
                    }}
                  >
                    {option.icon}
                  </Box>

                  <Typography variant="subtitle2" fontWeight={600} gutterBottom>
                    {option.label}
                  </Typography>

                  {!option.available && (
                    <Chip
                      label="Coming Soon"
                      size="small"
                      sx={{ mb: 1, fontSize: 10, height: 18 }}
                    />
                  )}

                  <Typography
                    variant="caption"
                    color="text.secondary"
                    sx={{
                      display: 'block',
                      lineHeight: 1.4,
                      minHeight: 40,
                    }}
                  >
                    {option.description}
                  </Typography>

                  <Stack
                    direction="row"
                    spacing={0.5}
                    sx={{ mt: 1.5 }}
                    justifyContent="center"
                    flexWrap="wrap"
                  >
                    {option.tags.slice(0, 3).map((tag) => (
                      <Chip
                        key={tag}
                        label={tag}
                        size="small"
                        variant="outlined"
                        sx={{
                          fontSize: 9,
                          height: 18,
                          borderColor: alpha(option.color, 0.3),
                          color: option.color,
                        }}
                      />
                    ))}
                  </Stack>
                </CardContent>
              </CardActionArea>
            </Card>
          ))}
        </Box>

        {filteredOptions.length === 0 && (
          <Box sx={{ textAlign: 'center', py: 4 }}>
            <Typography color="text.secondary">
              No visualizations match your search
            </Typography>
          </Box>
        )}
      </DialogContent>

      <DialogActions sx={{ px: 3, pb: 2 }}>
        <Button onClick={handleClose} color="inherit">
          Cancel
        </Button>
      </DialogActions>
    </Dialog>
  )
}