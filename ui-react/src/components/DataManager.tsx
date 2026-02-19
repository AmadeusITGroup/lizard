// ui-react/src/components/DataManager.tsx
/**
 * Data Manager - Manage uploaded data sources
 * Features:
 * - View all data sources with counts
 * - Delete individual sources or all data
 * - Clear ingestion history
 * - Reset workbench state
 */

import React, { useState, useCallback } from 'react';
import {
  Box,
  Paper,
  Typography,
  Button,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  IconButton,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogContentText,
  DialogActions,
  Alert,
  Chip,
  CircularProgress,
  Tooltip,
  Divider,
} from '@mui/material';
import DeleteIcon from '@mui/icons-material/Delete';
import DeleteSweepIcon from '@mui/icons-material/DeleteSweep';
import RefreshIcon from '@mui/icons-material/Refresh';
import StorageIcon from '@mui/icons-material/Storage';
import WarningAmberIcon from '@mui/icons-material/WarningAmber';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  listEventSources,
  deleteEventsBySource,
  deleteAllEvents,
  clearIngestionLogs,
  resetWorkbench,
  getWorkbenchStatus,
} from '../api';

interface DataSource {
  source: string;
  count: number;
}

export default function DataManager() {
  const queryClient = useQueryClient();
  
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [deleteTarget, setDeleteTarget] = useState<string | null>(null); // null = all
  const [successMessage, setSuccessMessage] = useState<string | null>(null);

  // Fetch data sources
  const sourcesQuery = useQuery({
    queryKey: ['eventSources'],
    queryFn: listEventSources,
    refetchInterval: 10000, // Refresh every 10s
  });

  // Fetch workbench status
  const statusQuery = useQuery({
    queryKey: ['workbenchStatus'],
    queryFn: getWorkbenchStatus,
  });

  // Delete mutations
  const deleteSourceMutation = useMutation({
    mutationFn: (sourceName: string) => deleteEventsBySource(sourceName),
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: ['eventSources'] });
      queryClient.invalidateQueries({ queryKey: ['workbenchStatus'] });
      setSuccessMessage(`Deleted ${data.deleted} events from source`);
      setDeleteDialogOpen(false);
      setTimeout(() => setSuccessMessage(null), 5000);
    },
  });

  const deleteAllMutation = useMutation({
    mutationFn: deleteAllEvents,
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: ['eventSources'] });
      queryClient.invalidateQueries({ queryKey: ['workbenchStatus'] });
      setSuccessMessage(`Deleted all ${data.deleted} events`);
      setDeleteDialogOpen(false);
      setTimeout(() => setSuccessMessage(null), 5000);
    },
  });

  const clearLogsMutation = useMutation({
    mutationFn: clearIngestionLogs,
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: ['ingestionLogs'] });
      setSuccessMessage(`Cleared ${data.deleted} ingestion logs`);
      setTimeout(() => setSuccessMessage(null), 5000);
    },
  });

  const resetWorkbenchMutation = useMutation({
    mutationFn:  resetWorkbench,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['workbenchStatus'] });
      queryClient.invalidateQueries({ queryKey: ['workbenchViews'] });
      setSuccessMessage('Workbench state reset successfully');
      setTimeout(() => setSuccessMessage(null), 5000);
    },
  });

  const handleDeleteClick = useCallback((sourceName: string | null) => {
    setDeleteTarget(sourceName);
    setDeleteDialogOpen(true);
  }, []);

  const handleConfirmDelete = useCallback(() => {
    if (deleteTarget === null) {
      deleteAllMutation.mutate();
    } else {
      deleteSourceMutation.mutate(deleteTarget);
    }
  }, [deleteTarget, deleteAllMutation, deleteSourceMutation]);

  const handleRefresh = useCallback(() => {
    queryClient.invalidateQueries({ queryKey: ['eventSources'] });
    queryClient.invalidateQueries({ queryKey: ['workbenchStatus'] });
  }, [queryClient]);

  const sources = sourcesQuery.data || [];
  const totalEvents = sources.reduce((sum, s) => sum + s.count, 0);
  const status = statusQuery.data;

  const isLoading = 
    deleteSourceMutation.isPending || 
    deleteAllMutation.isPending ||
    clearLogsMutation.isPending ||
    resetWorkbenchMutation.isPending;

  return (
    <Box sx={{ p: 2 }}>
      <Stack spacing={2}>
        {/* Header */}
        <Paper sx={{ p: 2 }}>
          <Stack direction="row" justifyContent="space-between" alignItems="center">
            <Stack direction="row" spacing={2} alignItems="center">
              <StorageIcon color="primary" sx={{ fontSize: 32 }} />
              <Box>
                <Typography variant="h5" fontWeight={600}>
                  Data Manager
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Manage uploaded data sources and clear cached data
                </Typography>
              </Box>
            </Stack>
            <Stack direction="row" spacing={1}>
              <Tooltip title="Refresh">
                <IconButton onClick={handleRefresh} disabled={sourcesQuery.isFetching}>
                  <RefreshIcon />
                </IconButton>
              </Tooltip>
            </Stack>
          </Stack>
        </Paper>

        {/* Success Message */}
        {successMessage && (
          <Alert severity="success" icon={<CheckCircleIcon />} onClose={() => setSuccessMessage(null)}>
            {successMessage}
          </Alert>
        )}

        {/* Stats Cards */}
        <Stack direction="row" spacing={2}>
          <Paper sx={{ p:  2, flex: 1 }}>
            <Typography variant="body2" color="text.secondary">Total Events</Typography>
            <Typography variant="h4" fontWeight={700}>{totalEvents.toLocaleString()}</Typography>
          </Paper>
          <Paper sx={{ p: 2, flex: 1 }}>
            <Typography variant="body2" color="text.secondary">Data Sources</Typography>
            <Typography variant="h4" fontWeight={700}>{sources.length}</Typography>
          </Paper>
          <Paper sx={{ p: 2, flex: 1 }}>
            <Typography variant="body2" color="text.secondary">Workbench Views</Typography>
            <Typography variant="h4" fontWeight={700}>{status?.views_count ??0}</Typography>
          </Paper>
        </Stack>

        {/* Data Sources Table */}
        <Paper sx={{ p: 2 }}>
          <Stack direction="row" justifyContent="space-between" alignItems="center" mb={2}>
            <Typography variant="h6" fontWeight={600}>
              Data Sources
            </Typography>
            <Button
              variant="outlined"
              color="error"
              startIcon={<DeleteSweepIcon />}
              onClick={() => handleDeleteClick(null)}
              disabled={sources.length === 0 || isLoading}
            >
              Delete All Data
            </Button>
          </Stack>

          {sourcesQuery.isLoading ? (
            <Box sx={{ display:  'flex', justifyContent:  'center', py: 4 }}>
              <CircularProgress />
            </Box>
          ) : sources.length === 0 ? (
            <Alert severity="info">
              No data sources found.Upload data from the Mapping page.
            </Alert>
          ) : (
            <TableContainer>
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell sx={{ fontWeight: 600 }}>Source Name</TableCell>
                    <TableCell align="right" sx={{ fontWeight: 600 }}>Event Count</TableCell>
                    <TableCell align="right" sx={{ fontWeight: 600 }}>Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {sources.map((source) => (
                    <TableRow key={source.source} hover>
                      <TableCell>
                        <Chip label={source.source} size="small" variant="outlined" />
                      </TableCell>
                      <TableCell align="right">
                        {source.count.toLocaleString()}
                      </TableCell>
                      <TableCell align="right">
                        <Tooltip title="Delete this source">
                          <IconButton
                            size="small"
                            color="error"
                            onClick={() => handleDeleteClick(source.source)}
                            disabled={isLoading}
                          >
                            <DeleteIcon fontSize="small" />
                          </IconButton>
                        </Tooltip>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          )}
        </Paper>

        {/* Maintenance Actions */}
        <Paper sx={{ p: 2 }}>
          <Typography variant="h6" fontWeight={600} gutterBottom>
            Maintenance
          </Typography>
          <Divider sx={{ mb: 2 }} />
          <Stack direction="row" spacing={2}>
            <Button
              variant="outlined"
              onClick={() => clearLogsMutation.mutate()}
              disabled={isLoading}
            >
              Clear Ingestion History
            </Button>
            <Button
              variant="outlined"
              onClick={() => resetWorkbenchMutation.mutate()}
              disabled={isLoading}
            >
              Reset Workbench Cache
            </Button>
          </Stack>
          <Typography variant="caption" color="text.secondary" sx={{ mt: 1, display: 'block' }}>
            These actions clear in-memory caches. They do not affect the database.
          </Typography>
        </Paper>
      </Stack>

      {/* Delete Confirmation Dialog */}
      <Dialog open={deleteDialogOpen} onClose={() => setDeleteDialogOpen(false)}>
        <DialogTitle sx={{ display: 'flex', alignItems: 'center', gap:  1 }}>
          <WarningAmberIcon color="error" />
          Confirm Delete
        </DialogTitle>
        <DialogContent>
          <DialogContentText>
            {deleteTarget === null ? (
              <>
                Are you sure you want to delete <strong>ALL events</strong> from the database?
                <br /><br />
                This will remove <strong>{totalEvents.toLocaleString()}</strong> events across{' '}
                <strong>{sources.length}</strong> sources.
              </>
            ) : (
              <>
                Are you sure you want to delete all events from source{' '}
                <strong>"{deleteTarget}"</strong>? 
                <br /><br />
                This will remove{' '}
                <strong>
                  {sources.find(s => s.source === deleteTarget)?.count.toLocaleString() || 0}
                </strong>{' '}
                events.
              </>
            )}
            <br /><br />
            <strong>This action cannot be undone.</strong>
          </DialogContentText>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDeleteDialogOpen(false)} disabled={isLoading}>
            Cancel
          </Button>
          <Button
            onClick={handleConfirmDelete}
            color="error"
            variant="contained"
            disabled={isLoading}
            startIcon={isLoading ? <CircularProgress size={16} /> : <DeleteIcon />}
          >
            {isLoading ? 'Deleting...' : 'Delete'}
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
}