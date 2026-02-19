// path: ui-react/src/components/DataWorkbench/PipelineBuilder.tsx
/**
 * PipelineBuilder - Visual builder for data transformation pipelines
 */
import React from 'react'
import {
  Box,
  Typography,
  Stack,
  Button,
  IconButton,
  Chip,
  Menu,
  MenuItem,
  ListItemIcon,
  ListItemText,
  Divider,
  Alert,
  Tooltip,
} from '@mui/material'
import {
  Add as AddIcon,
  Delete as DeleteIcon,
  Clear as ClearIcon,
  FilterAlt as FilterIcon,
  ViewColumn as SelectIcon,
  MergeType as JoinIcon,
  Functions as AggregateIcon,
  Transform as TransformIcon,
  Sort as SortIcon,
  Storage as SourceIcon,
  CallSplit as UnionIcon,
  FilterNone as DistinctIcon,
  Edit as RenameIcon,
  RemoveCircleOutline as DropIcon,
  DragIndicator as DragIcon,
} from '@mui/icons-material'
import { PipelineStep, DataSource } from './DataWorkbench'
import PipelineStepCard from './PipelineStepCard'

interface PipelineBuilderProps {
  pipeline: PipelineStep[]
  availableColumns: string[]
  sources: DataSource[]
  sourceColumns: string[]
  onAddStep: (type: string) => void
  onUpdateStep: (stepId: string, config: Record<string, any>) => void
  onRemoveStep: (stepId: string) => void
  onReorderSteps: (fromIndex: number, toIndex: number) => void
  onClear: () => void
}

const STEP_TYPES = [
  { type: 'filter', label: 'Filter', icon: FilterIcon, description: 'Filter rows by conditions' },
  { type: 'select', label: 'Select Columns', icon: SelectIcon, description: 'Choose which columns to keep' },
  { type: 'join', label: 'Join', icon: JoinIcon, description: 'Join with another table' },
  { type: 'aggregate', label: 'Aggregate', icon: AggregateIcon, description: 'Group by and aggregate' },
  { type: 'transform', label: 'Transform', icon: TransformIcon, description:  'Add computed columns' },
  { type:  'sort', label: 'Sort', icon: SortIcon, description: 'Order results' },
  { type: 'distinct', label: 'Distinct', icon: DistinctIcon, description: 'Remove duplicates' },
  { type: 'rename', label: 'Rename', icon: RenameIcon, description: 'Rename columns' },
  { type: 'drop', label: 'Drop Columns', icon: DropIcon, description: 'Remove columns' },
  { type: 'union', label: 'Union', icon: UnionIcon, description: 'Combine with another table' },
]

export default function PipelineBuilder({
  pipeline,
  availableColumns,
  sourceColumns,
  sources,
  onAddStep,
  onUpdateStep,
  onRemoveStep,
  onReorderSteps,
  onClear,
}: PipelineBuilderProps) {
  const [addMenuAnchor, setAddMenuAnchor] = React.useState<null | HTMLElement>(null)

  const handleOpenAddMenu = (event: React.MouseEvent<HTMLElement>) => {
    setAddMenuAnchor(event.currentTarget)
  }

  const handleCloseAddMenu = () => {
    setAddMenuAnchor(null)
  }

  const handleAddStep = (type: string) => {
    onAddStep(type)
    handleCloseAddMenu()
  }

  const getStepIcon = (type: string) => {
    const stepType = STEP_TYPES.find(s => s.type === type)
    if (stepType) {
      const Icon = stepType.icon
      return <Icon fontSize="small" />
    }
    if (type === 'source') return <SourceIcon fontSize="small" />
    return null
  }

  const getStepLabel = (type: string) => {
    if (type === 'source') return 'Source'
    return STEP_TYPES.find(s => s.type === type)?.label || type
  }

  // Check if pipeline has a source
  const hasSource = pipeline.some(s => s.type === 'source')

  return (
    <Box>
      {/* Header */}
      <Stack direction="row" justifyContent="space-between" alignItems="center" mb={2}>
        <Stack direction="row" spacing={1} alignItems="center">
          <Typography variant="subtitle1" fontWeight={600}>
            Pipeline
          </Typography>
          <Chip 
            label={`${pipeline.length} step${pipeline.length !== 1 ? 's' :  ''}`} 
            size="small" 
            variant="outlined"
          />
        </Stack>
        <Stack direction="row" spacing={1}>
          {pipeline.length > 0 && (
            <Button
              size="small"
              startIcon={<ClearIcon />}
              onClick={onClear}
              color="inherit"
            >
              Clear
            </Button>
          )}
          <Button
            size="small"
            variant="contained"
            startIcon={<AddIcon />}
            onClick={handleOpenAddMenu}
            disabled={!hasSource}
          >
            Add Step
          </Button>
        </Stack>
      </Stack>

      {/* Empty State */}
      {pipeline.length === 0 && (
        <Alert severity="info" sx={{ mb: 2 }}>
          Select a data source from the left panel to start building your pipeline.
        </Alert>
      )}

      {/* Pipeline Steps */}
      <Stack spacing={1}>
        {pipeline.map((step, index) => (
          <PipelineStepCard
            key={step.id}
            step={step}
            index={index}
            totalSteps={pipeline.length}
            availableColumns={availableColumns}
            sourceColumns={sourceColumns}
            sources={sources}
            icon={getStepIcon(step.type)}
            label={getStepLabel(step.type)}
            onUpdate={(config) => onUpdateStep(step.id, config)}
            onRemove={() => onRemoveStep(step.id)}
            onMoveUp={index > 1 ? () => onReorderSteps(index, index - 1) : undefined}
            onMoveDown={index < pipeline.length - 1 && index > 0 ? () => onReorderSteps(index, index + 1) : undefined}
            isSource={step.type === 'source'}
          />
        ))}
      </Stack>

      {/* Add Step Menu */}
      <Menu
        anchorEl={addMenuAnchor}
        open={Boolean(addMenuAnchor)}
        onClose={handleCloseAddMenu}
        PaperProps={{ sx: { minWidth: 250 } }}
      >
        <Typography variant="caption" sx={{ px: 2, py: 1, display: 'block', opacity: 0.7 }}>
          Add Transformation Step
        </Typography>
        <Divider />
        {STEP_TYPES.map(({ type, label, icon: Icon, description }) => (
          <MenuItem key={type} onClick={() => handleAddStep(type)}>
            <ListItemIcon>
              <Icon fontSize="small" />
            </ListItemIcon>
            <ListItemText 
              primary={label} 
              secondary={description}
              secondaryTypographyProps={{ variant:  'caption' }}
            />
          </MenuItem>
        ))}
      </Menu>
    </Box>
  )
}