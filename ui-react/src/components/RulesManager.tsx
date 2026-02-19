// path: ui-react/src/components/RulesManager.tsx
import React from 'react';
import {
  Box,
  Paper,
  Typography,
  Button,
  IconButton,
  Switch,
  Chip,
  Stack,
  TextField,
  MenuItem,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Tooltip,
  Alert,
  Divider,
  FormControlLabel,
  Tabs,
  Tab,
  CircularProgress,
} from '@mui/material';
import {
  Add as AddIcon,
  Edit as EditIcon,
  Delete as DeleteIcon,
  ContentCopy as CopyIcon,
  PlayArrow as TestIcon,
  Refresh as RefreshIcon,
  Download as ExportIcon,
  Upload as ImportIcon,
  Check as CheckIcon,
  Close as CloseIcon,
} from '@mui/icons-material';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import axios from 'axios';

const API = import.meta.env.VITE_API_BASE || 'http://localhost:8000';

// Types
interface RuleCondition {
  field: string;
  op: string;
  value?:  any;
  value2?: any;
}

interface RuleConditionGroup {
  operator: 'AND' | 'OR' | 'NOT';
  rules: (RuleCondition | RuleConditionGroup)[];
}

interface Rule {
  id: string;
  name: string;
  description: string;
  severity:  'low' | 'medium' | 'high' | 'critical';
  enabled:  boolean;
  conditions: RuleConditionGroup;
  tags: string[];
  actions: string[];
  score_contribution:  number;
  metadata?:  Record<string, any>;
  created_at?:  string;
  updated_at?: string;
}

// Severity colors
const SEVERITY_COLORS:  Record<string, 'default' | 'info' | 'warning' | 'error'> = {
  low: 'default',
  medium:  'info',
  high: 'warning',
  critical: 'error',
};

// Available operators
const OPERATORS = [
  { value:  'eq', label: '= (equals)' },
  { value:  'ne', label: '≠ (not equals)' },
  { value: 'gt', label: '> (greater than)' },
  { value: 'gte', label: '≥ (greater or equal)' },
  { value: 'lt', label:  '< (less than)' },
  { value: 'lte', label:  '≤ (less or equal)' },
  { value: 'contains', label: 'contains' },
  { value: 'not_contains', label:  'not contains' },
  { value: 'startswith', label: 'starts with' },
  { value: 'endswith', label:  'ends with' },
  { value: 'matches', label: 'regex matches' },
  { value: 'in', label: 'in list' },
  { value: 'not_in', label:  'not in list' },
  { value: 'is_null', label: 'is null' },
  { value: 'is_not_null', label: 'is not null' },
  { value: 'between', label: 'between' },
];

// Common fields for fraud detection
const COMMON_FIELDS = [
  { value: 'speed_kmh', label: 'Travel Speed (km/h)', type: 'number' },
  { value: 'dist_prev_km', label:  'Distance from Previous (km)', type: 'number' },
  { value: 'dist_home_km', label:  'Distance from Home (km)', type: 'number' },
  { value:  'is_new_device', label: 'Is New Device (0/1)', type: 'number' },
  { value: 'is_new_ip', label: 'Is New IP (0/1)', type: 'number' },
  { value:  'is_new_device_rolling', label: 'Is New Device (Rolling)', type: 'number' },
  { value: 'is_new_ip_rolling', label:  'Is New IP (Rolling)', type: 'number' },
  { value:  'anom_score', label: 'Anomaly Score (0-1)', type: 'number' },
  { value: 'z_fail', label: 'Failure Z-Score', type:  'number' },
  { value:  'hour_rarity', label: 'Hour Rarity', type: 'number' },
  { value: 'amount', label: 'Transaction Amount', type: 'number' },
  { value: 'event_type', label:  'Event Type', type: 'string' },
  { value: 'country', label: 'Country', type: 'string' },
  { value: 'city', label:  'City', type: 'string' },
  { value: 'user_id', label: 'User ID', type:  'string' },
  { value:  'account_id', label:  'Account ID', type: 'string' },
  { value: 'device_id', label:  'Device ID', type: 'string' },
  { value: 'ip', label: 'IP Address', type: 'string' },
  { value: 'card_hash', label: 'Card Hash', type:  'string' },
  { value:  'carrier', label: 'Carrier', type:  'string' },
  { value:  'origin', label: 'Origin', type: 'string' },
  { value: 'dest', label: 'Destination', type: 'string' },
];

// API functions
async function fetchRules(): Promise<Rule[]> {
  const { data } = await axios.get(`${API}/rules/`);
  return data;
}

async function fetchBuiltinRules(): Promise<Rule[]> {
  const { data } = await axios.get(`${API}/rules/builtins`);
  return data;
}

async function createRule(rule: Partial<Rule>): Promise<Rule> {
  const { data } = await axios.post(`${API}/rules/`, rule);
  return data;
}

async function updateRule(id: string, updates: Partial<Rule>): Promise<Rule> {
  const { data } = await axios.put(`${API}/rules/${id}`, updates);
  return data;
}

async function deleteRule(id: string): Promise<void> {
  await axios.delete(`${API}/rules/${id}`);
}

async function toggleRule(id: string, enabled: boolean): Promise<void> {
  await axios.post(`${API}/rules/${id}/${enabled ? 'enable' : 'disable'}`);
}

async function initBuiltins(): Promise<{ imported: number; skipped: number }> {
  const { data } = await axios.post(`${API}/rules/init-builtins`);
  return data;
}

async function resetBuiltins(): Promise<{ rules_count: number }> {
  const { data } = await axios.post(`${API}/rules/reset-builtins`);
  return data;
}

async function testRule(rule:  Partial<Rule>, testData: any[]): Promise<any> {
  const { data } = await axios.post(`${API}/rules/test`, { rule, test_data: testData });
  return data;
}

// Condition Editor Component
function ConditionEditor({
  condition,
  onChange,
  onRemove,
  index,
}: {
  condition: RuleCondition;
  onChange: (c: RuleCondition) => void;
  onRemove:  () => void;
  index: number;
}) {
  const needsValue = ! ['is_null', 'is_not_null'].includes(condition.op);
  const needsValue2 = condition.op === 'between';

  return (
    <Paper variant="outlined" sx={{ p: 1.5, mb: 1 }}>
      <Stack direction="row" spacing={1} alignItems="center" flexWrap="wrap" useFlexGap>
        <Chip label={`#${index + 1}`} size="small" variant="outlined" />

        <TextField
          select
          size="small"
          label="Field"
          value={condition.field || ''}
          onChange={(e) => onChange({ ...condition, field: e.target.value })}
          sx={{ minWidth: 200 }}
        >
          {COMMON_FIELDS.map((f) => (
            <MenuItem key={f.value} value={f.value}>
              {f.label}
            </MenuItem>
          ))}
        </TextField>

        <TextField
          select
          size="small"
          label="Operator"
          value={condition.op || 'eq'}
          onChange={(e) => onChange({ ...condition, op: e.target.value })}
          sx={{ minWidth: 160 }}
        >
          {OPERATORS.map((op) => (
            <MenuItem key={op.value} value={op.value}>
              {op.label}
            </MenuItem>
          ))}
        </TextField>

        {needsValue && (
          <TextField
            size="small"
            label="Value"
            value={condition.value ??  ''}
            onChange={(e) => {
              const val = e.target.value;
              // Try to parse as number if it looks like one
              const parsed = ! isNaN(Number(val)) && val !== '' ? Number(val) : val;
              onChange({ ...condition, value: parsed });
            }}
            sx={{ minWidth: 120, maxWidth: 200 }}
            placeholder="value"
          />
        )}

        {needsValue2 && (
          <TextField
            size="small"
            label="Value 2"
            value={condition.value2 ?? ''}
            onChange={(e) => {
              const val = e.target.value;
              const parsed = !isNaN(Number(val)) && val !== '' ? Number(val) : val;
              onChange({ ...condition, value2: parsed });
            }}
            sx={{ minWidth:  120, maxWidth: 200 }}
            placeholder="upper bound"
          />
        )}

        <IconButton size="small" color="error" onClick={onRemove}>
          <DeleteIcon fontSize="small" />
        </IconButton>
      </Stack>
    </Paper>
  );
}

// Rule Editor Dialog
function RuleEditorDialog({
  open,
  rule,
  onClose,
  onSave,
  isSaving,
}:  {
  open:  boolean;
  rule:  Partial<Rule> | null;
  onClose: () => void;
  onSave: (rule: Partial<Rule>) => void;
  isSaving:  boolean;
}) {
  const [formData, setFormData] = React.useState<Partial<Rule>>({
    name: '',
    description: '',
    severity: 'medium',
    enabled: true,
    conditions: { operator: 'AND', rules: [] },
    tags: [],
    actions: ['flag'],
    score_contribution: 0,
  });
  const [tagInput, setTagInput] = React.useState('');

  React.useEffect(() => {
    if (open) {
      if (rule) {
        setFormData({
          ...rule,
          conditions: rule.conditions || { operator: 'AND', rules: [] },
        });
        setTagInput(rule.tags?.join(', ') || '');
      } else {
        setFormData({
          name: '',
          description: '',
          severity: 'medium',
          enabled:  true,
          conditions: { operator: 'AND', rules:  [] },
          tags: [],
          actions: ['flag'],
          score_contribution:  0,
        });
        setTagInput('');
      }
    }
  }, [rule, open]);

  const addCondition = () => {
    const newCondition: RuleCondition = { field: 'anom_score', op: 'gte', value: 0.5 };
    setFormData((prev) => ({
      ...prev,
      conditions: {
        ...prev.conditions! ,
        rules:  [...(prev.conditions?.rules || []), newCondition],
      },
    }));
  };

  const updateCondition = (index: number, condition: RuleCondition) => {
    setFormData((prev) => ({
      ...prev,
      conditions: {
        ...prev.conditions!,
        rules: prev.conditions! .rules.map((c, i) => (i === index ? condition : c)),
      },
    }));
  };

  const removeCondition = (index: number) => {
    setFormData((prev) => ({
      ...prev,
      conditions: {
        ...prev.conditions!,
        rules: prev.conditions!.rules.filter((_, i) => i !== index),
      },
    }));
  };

  const handleTagsChange = (value: string) => {
    setTagInput(value);
    const tags = value
      .split(',')
      .map((t) => t.trim().toLowerCase())
      .filter(Boolean);
    setFormData((prev) => ({ ...prev, tags }));
  };

  const handleSave = () => {
    onSave(formData);
  };

  const isValid = formData.name && formData.conditions?.rules?.length;

  return (
    <Dialog open={open} onClose={onClose} maxWidth="md" fullWidth>
      <DialogTitle>
        {rule?.id ? 'Edit Rule' : 'Create New Rule'}
      </DialogTitle>
      <DialogContent dividers>
        <Stack spacing={2.5}>
          {/* Basic Info */}
          <TextField
            label="Rule Name"
            value={formData.name || ''}
            onChange={(e) => setFormData((p) => ({ ...p, name: e.target.value }))}
            fullWidth
            required
            placeholder="e.g., Impossible Travel Detection"
          />

          <TextField
            label="Description"
            value={formData.description || ''}
            onChange={(e) => setFormData((p) => ({ ...p, description: e.target.value }))}
            fullWidth
            multiline
            rows={2}
            placeholder="Describe what this rule detects..."
          />

          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
            <TextField
              select
              label="Severity"
              value={formData.severity || 'medium'}
              onChange={(e) => setFormData((p) => ({ ...p, severity: e.target.value as any }))}
              sx={{ minWidth: 140 }}
            >
              <MenuItem value="low">Low</MenuItem>
              <MenuItem value="medium">Medium</MenuItem>
              <MenuItem value="high">High</MenuItem>
              <MenuItem value="critical">Critical</MenuItem>
            </TextField>

            <TextField
              type="number"
              label="Score Contribution"
              value={formData.score_contribution || 0}
              onChange={(e) =>
                setFormData((p) => ({ ...p, score_contribution: parseFloat(e.target.value) || 0 }))
              }
              inputProps={{ step: 0.05, min: 0, max: 1 }}
              sx={{ width: 180 }}
              helperText="Added to anomaly score (0-1)"
            />

            <TextField
              label="Tags"
              value={tagInput}
              onChange={(e) => handleTagsChange(e.target.value)}
              sx={{ flex: 1 }}
              placeholder="velocity, geo, ato"
              helperText="Comma-separated tags"
            />
          </Stack>

          <FormControlLabel
            control={
              <Switch
                checked={formData.enabled !== false}
                onChange={(e) => setFormData((p) => ({ ...p, enabled: e.target.checked }))}
              />
            }
            label="Rule Enabled"
          />

          <Divider />

          {/* Conditions */}
          <Box>
            <Stack direction="row" justifyContent="space-between" alignItems="center" sx={{ mb: 2 }}>
              <Typography variant="h6">Conditions</Typography>
              <Stack direction="row" spacing={1} alignItems="center">
                <Typography variant="body2" color="text.secondary">
                  Match when
                </Typography>
                <TextField
                  select
                  size="small"
                  value={formData.conditions?.operator || 'AND'}
                  onChange={(e) =>
                    setFormData((p) => ({
                      ...p,
                      conditions: { ...p.conditions!, operator: e.target.value as any },
                    }))
                  }
                  sx={{ width: 100 }}
                >
                  <MenuItem value="AND">ALL</MenuItem>
                  <MenuItem value="OR">ANY</MenuItem>
                </TextField>
                <Typography variant="body2" color="text.secondary">
                  conditions are true
                </Typography>
              </Stack>
            </Stack>

            {formData.conditions?.rules?.map((cond, idx) => (
              <ConditionEditor
                key={idx}
                index={idx}
                condition={cond as RuleCondition}
                onChange={(c) => updateCondition(idx, c)}
                onRemove={() => removeCondition(idx)}
              />
            ))}

            <Button
              variant="outlined"
              startIcon={<AddIcon />}
              onClick={addCondition}
              sx={{ mt: 1 }}
            >
              Add Condition
            </Button>

            {(! formData.conditions?.rules || formData.conditions.rules.length === 0) && (
              <Alert severity="info" sx={{ mt: 2 }}>
                No conditions defined.Add at least one condition for the rule to work.
              </Alert>
            )}
          </Box>
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose} disabled={isSaving}>
          Cancel
        </Button>
        <Button
          variant="contained"
          onClick={handleSave}
          disabled={! isValid || isSaving}
          startIcon={isSaving ? <CircularProgress size={16} /> : <CheckIcon />}
        >
          {rule?.id ? 'Update Rule' : 'Create Rule'}
        </Button>
      </DialogActions>
    </Dialog>
  );
}

// Main Component
export default function RulesManager() {
  const queryClient = useQueryClient();
  const [editorOpen, setEditorOpen] = React.useState(false);
  const [editingRule, setEditingRule] = React.useState<Partial<Rule> | null>(null);
  const [activeTab, setActiveTab] = React.useState(0);

  // Queries
  const rulesQuery = useQuery({
    queryKey: ['rules'],
    queryFn: fetchRules,
    refetchInterval: 30000, // Refresh every 30s
  });

  const builtinsQuery = useQuery({
    queryKey:  ['rules-builtins'],
    queryFn:  fetchBuiltinRules,
  });

  // Mutations
  const createMutation = useMutation({
    mutationFn: createRule,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['rules'] });
      setEditorOpen(false);
      setEditingRule(null);
    },
  });

  const updateMutation = useMutation({
    mutationFn: ({ id, updates }: { id:  string; updates:  Partial<Rule> }) => updateRule(id, updates),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['rules'] });
      setEditorOpen(false);
      setEditingRule(null);
    },
  });

  const deleteMutation = useMutation({
    mutationFn:  deleteRule,
    onSuccess: () => queryClient.invalidateQueries({ queryKey:  ['rules'] }),
  });

  const toggleMutation = useMutation({
    mutationFn: ({ id, enabled }: { id: string; enabled:  boolean }) => toggleRule(id, enabled),
    onSuccess:  () => queryClient.invalidateQueries({ queryKey:  ['rules'] }),
  });

  const initBuiltinsMutation = useMutation({
    mutationFn: initBuiltins,
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: ['rules'] });
      alert(`Imported ${data.imported} rules, skipped ${data.skipped} (already exist)`);
    },
  });

  const resetBuiltinsMutation = useMutation({
    mutationFn: resetBuiltins,
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: ['rules'] });
      alert(`Reset complete.Now have ${data.rules_count} built-in rules.`);
    },
  });

  const handleSave = (rule:  Partial<Rule>) => {
    if (editingRule?.id) {
      updateMutation.mutate({ id: editingRule.id, updates: rule });
    } else {
      createMutation.mutate(rule);
    }
  };

  const handleEdit = (rule: Rule) => {
    setEditingRule(rule);
    setEditorOpen(true);
  };

  const handleCreate = () => {
    setEditingRule(null);
    setEditorOpen(true);
  };

  const handleDuplicate = (rule: Rule) => {
    setEditingRule({
      ...rule,
      id: undefined,
      name: `${rule.name} (Copy)`,
    });
    setEditorOpen(true);
  };

  const handleDelete = (rule: Rule) => {
    if (window.confirm(`Delete rule "${rule.name}"?  This cannot be undone.`)) {
      deleteMutation.mutate(rule.id);
    }
  };

  const rules = rulesQuery.data || [];
  const enabledRules = rules.filter((r) => r.enabled);
  const disabledRules = rules.filter((r) => !r.enabled);

  // Stats
  const stats = {
    total: rules.length,
    enabled: enabledRules.length,
    disabled:  disabledRules.length,
    bySeverity: {
      critical: rules.filter((r) => r.severity === 'critical').length,
      high: rules.filter((r) => r.severity === 'high').length,
      medium: rules.filter((r) => r.severity === 'medium').length,
      low: rules.filter((r) => r.severity === 'low').length,
    },
  };

  return (
    <Box sx={{ height: '100%', display: 'flex', flexDirection:  'column' }}>
      {/* Header */}
      <Paper sx={{ p: 2, mb: 2 }}>
        <Stack direction={{ xs: 'column', md: 'row' }} justifyContent="space-between" alignItems={{ md: 'center' }} spacing={2}>
          <Box>
            <Typography variant="h5" gutterBottom>
              🛡️ Rules Engine
            </Typography>
            <Typography variant="body2" color="text.secondary">
              Define custom fraud detection rules to complement ML-based anomaly detection
            </Typography>
          </Box>
          <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
            <Button
              variant="outlined"
              size="small"
              startIcon={<RefreshIcon />}
              onClick={() => rulesQuery.refetch()}
              disabled={rulesQuery.isFetching}
            >
              Refresh
            </Button>
            <Button
              variant="outlined"
              size="small"
              startIcon={<ImportIcon />}
              onClick={() => initBuiltinsMutation.mutate()}
              disabled={initBuiltinsMutation.isPending}
            >
              Load Built-ins
            </Button>
            <Button
              variant="contained"
              startIcon={<AddIcon />}
              onClick={handleCreate}
            >
              Create Rule
            </Button>
          </Stack>
        </Stack>

        {/* Stats */}
        <Stack direction="row" spacing={2} sx={{ mt:  2 }} flexWrap="wrap" useFlexGap>
          <Chip label={`Total: ${stats.total}`} />
          <Chip label={`Enabled: ${stats.enabled}`} color="success" variant="outlined" />
          <Chip label={`Disabled: ${stats.disabled}`} variant="outlined" />
          <Divider orientation="vertical" flexItem />
          <Chip label={`Critical: ${stats.bySeverity.critical}`} color="error" size="small" />
          <Chip label={`High: ${stats.bySeverity.high}`} color="warning" size="small" />
          <Chip label={`Medium: ${stats.bySeverity.medium}`} color="info" size="small" />
          <Chip label={`Low: ${stats.bySeverity.low}`} size="small" />
        </Stack>
      </Paper>

      {/* Loading/Error States */}
      {rulesQuery.isLoading && (
        <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
          <CircularProgress />
        </Box>
      )}
      {rulesQuery.error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Failed to load rules.Make sure the API is running.
        </Alert>
      )}

      {/* Rules Table */}
      {! rulesQuery.isLoading && (
        <TableContainer component={Paper} sx={{ flex: 1, overflow: 'auto' }}>
          <Table size="small" stickyHeader>
            <TableHead>
              <TableRow>
                <TableCell width={60}>Status</TableCell>
                <TableCell>Rule</TableCell>
                <TableCell width={100}>Severity</TableCell>
                <TableCell width={200}>Tags</TableCell>
                <TableCell width={80}>Score</TableCell>
                <TableCell width={80}>Conditions</TableCell>
                <TableCell width={140} align="right">Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {rules.length === 0 ?  (
                <TableRow>
                  <TableCell colSpan={7} align="center" sx={{ py: 8 }}>
                    <Typography color="text.secondary" gutterBottom>
                      No rules defined yet
                    </Typography>
                    <Button
                      variant="outlined"
                      startIcon={<ImportIcon />}
                      onClick={() => initBuiltinsMutation.mutate()}
                      sx={{ mr: 1 }}
                    >
                      Load Built-in Rules
                    </Button>
                    <Button
                      variant="contained"
                      startIcon={<AddIcon />}
                      onClick={handleCreate}
                    >
                      Create First Rule
                    </Button>
                  </TableCell>
                </TableRow>
              ) : (
                rules.map((rule) => (
                  <TableRow
                    key={rule.id}
                    hover
                    sx={{ opacity: rule.enabled ?  1 : 0.6 }}
                  >
                    <TableCell>
                      <Switch
                        checked={rule.enabled}
                        onChange={(e) =>
                          toggleMutation.mutate({ id: rule.id, enabled: e.target.checked })
                        }
                        size="small"
                        disabled={toggleMutation.isPending}
                      />
                    </TableCell>
                    <TableCell>
                      <Typography variant="body2" fontWeight={500}>
                        {rule.name}
                      </Typography>
                      {rule.description && (
                        <Typography variant="caption" color="text.secondary" sx={{ display: 'block' }}>
                          {rule.description.length > 80
                            ? `${rule.description.slice(0, 80)}...`
                            : rule.description}
                        </Typography>
                      )}
                    </TableCell>
                    <TableCell>
                      <Chip
                        label={rule.severity}
                        size="small"
                        color={SEVERITY_COLORS[rule.severity]}
                      />
                    </TableCell>
                    <TableCell>
                      <Stack direction="row" spacing={0.5} flexWrap="wrap" useFlexGap>
                        {rule.tags?.slice(0, 3).map((tag) => (
                          <Chip key={tag} label={tag} size="small" variant="outlined" />
                        ))}
                        {(rule.tags?.length || 0) > 3 && (
                          <Chip label={`+${rule.tags! .length - 3}`} size="small" />
                        )}
                      </Stack>
                    </TableCell>
                    <TableCell>
                      <Typography variant="body2">
                        +{(rule.score_contribution || 0).toFixed(2)}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Chip
                        label={`${rule.conditions?.rules?.length || 0} conditions`}
                        size="small"
                        variant="outlined"
                      />
                    </TableCell>
                    <TableCell align="right">
                      <Tooltip title="Edit">
                        <IconButton size="small" onClick={() => handleEdit(rule)}>
                          <EditIcon fontSize="small" />
                        </IconButton>
                      </Tooltip>
                      <Tooltip title="Duplicate">
                        <IconButton size="small" onClick={() => handleDuplicate(rule)}>
                          <CopyIcon fontSize="small" />
                        </IconButton>
                      </Tooltip>
                      <Tooltip title="Delete">
                        <IconButton
                          size="small"
                          color="error"
                          onClick={() => handleDelete(rule)}
                          disabled={deleteMutation.isPending}
                        >
                          <DeleteIcon fontSize="small" />
                        </IconButton>
                      </Tooltip>
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </TableContainer>
      )}

      {/* Rule Editor Dialog */}
      <RuleEditorDialog
        open={editorOpen}
        rule={editingRule}
        onClose={() => {
          setEditorOpen(false);
          setEditingRule(null);
        }}
        onSave={handleSave}
        isSaving={createMutation.isPending || updateMutation.isPending}
      />
    </Box>
  );
}