// path: ui-react/src/components/AppLayout.tsx
import React from 'react'
import TableChartIcon from '@mui/icons-material/TableChart'
import { Outlet, useNavigate, useLocation } from 'react-router-dom'
import {
  AppBar,
  Box,
  Toolbar,
  Typography,
  Button,
  Stack,
  Chip,
  IconButton,
  Drawer,
  List,
  ListItem,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  Divider,
  useMediaQuery,
  useTheme,
} from '@mui/material'
import {
  Menu as MenuIcon,
  Dashboard as DashboardIcon,
  Security as RulesIcon,
  Build as BuildIcon,
} from '@mui/icons-material'
import StorageIcon from '@mui/icons-material/Storage'
import CloudIcon from '@mui/icons-material/Cloud'
import SettingsIcon from '@mui/icons-material/Settings'
import CloudModeToggle from './CloudModeToggle'

const NAV_ITEMS = [
  { path: '/dashboard', label: 'Dashboard', icon: <DashboardIcon /> },
  { path: '/rules', label: 'Rules Engine', icon: <RulesIcon /> },
  { path: '/mapping', label: 'Data Ingestion & Mapping', icon: <TableChartIcon /> },
  { label: 'Data Workbench', path: '/workbench', icon: <BuildIcon /> },
  { label: 'Data', path: '/data', icon: <StorageIcon /> },
  { label: 'Cloud Browser', path: '/cloud-browser', icon: <CloudIcon /> },
  { label: 'Cloud Settings', path: '/cloud-settings', icon: <SettingsIcon /> },
]

export default function AppLayout() {
  const navigate = useNavigate()
  const location = useLocation()
  const theme = useTheme()
  const isMobile = useMediaQuery(theme.breakpoints.down('md'))
  const [drawerOpen, setDrawerOpen] = React.useState(false)

  const currentPath = location.pathname

  const handleNavigate = (path: string) => {
    navigate(path)
    setDrawerOpen(false)
  }

  const NavContent = () => (
    <List>
      {NAV_ITEMS.map((item) => (
        <ListItem key={item.path} disablePadding>
          <ListItemButton
            selected={currentPath === item.path}
            onClick={() => handleNavigate(item.path)}
          >
            <ListItemIcon>{item.icon}</ListItemIcon>
            <ListItemText primary={item.label} />
          </ListItemButton>
        </ListItem>
      ))}
    </List>
  )

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', minHeight: '100vh' }}>
      {/* App Bar */}
      <AppBar position="fixed" elevation={1}>
        <Toolbar>
          {isMobile && (
            <IconButton
              color="inherit"
              edge="start"
              onClick={() => setDrawerOpen(true)}
              sx={{ mr: 2 }}
            >
              <MenuIcon />
            </IconButton>
          )}

          <Typography
            variant="h6"
            component="div"
            sx={{ cursor: 'pointer' }}
            onClick={() => navigate('/dashboard')}
          >
            🦎 LIZARD
          </Typography>

          <Chip
            label="Fraud Pattern Visualizer"
            size="small"
            sx={{ ml: 2, bgcolor: 'rgba(255,255,255,0.15)', color: 'inherit' }}
          />

          {/* Desktop Navigation */}
          {!isMobile && (
            <Stack direction="row" spacing={1} sx={{ ml: 4 }}>
              {NAV_ITEMS.map((item) => (
                <Button
                  key={item.path}
                  color="inherit"
                  startIcon={item.icon}
                  onClick={() => handleNavigate(item.path)}
                  sx={{
                    bgcolor: currentPath === item.path ? 'rgba(255,255,255,0.15)' : 'transparent',
                  }}
                >
                  {item.label}
                </Button>
              ))}
            </Stack>
          )}

          <Box sx={{ flex: 1 }} />

          {/* Cloud Mode Toggle */}
          <CloudModeToggle />

          <Typography variant="caption" sx={{ opacity: 0.7, ml: 1 }}>
            v0.1.0
          </Typography>
        </Toolbar>
      </AppBar>

      {/* Mobile Drawer */}
      <Drawer
        anchor="left"
        open={drawerOpen}
        onClose={() => setDrawerOpen(false)}
      >
        <Box sx={{ width: 250 }}>
          <Toolbar>
            <Typography variant="h6">🧙 LIZARD</Typography>
          </Toolbar>
          <Divider />
          <NavContent />
        </Box>
      </Drawer>

      {/* Main Content */}
      <Box component="main" sx={{ flexGrow: 1, mt: '64px' }}>
        <Outlet />
      </Box>
    </Box>
  )
}