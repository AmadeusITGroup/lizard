// path: ui-react/src/components/DataWorkbench/SaveViewDialog.tsx
/**
 * SaveViewDialog - Dialog for saving pipeline as a derived view
 */
import React from 'react'
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  TextField,
  Stack,
  FormControlLabel,
  Switch,
  Chip,
  Typography,
  Alert,
} from '@mui/material'
import { PipelineStep } from './DataWorkbench'

interface SaveViewDialogProps {
  open: boolean
  onClose: () => void
  onSave:  (data: {
    name: string
    description: string
    is_materialized: boolean
    tags: string[]
  }) => void
  pipeline: PipelineStep[]
}

export default function SaveViewDialog({
  open,
  onClose,
  onSave,
  pipeline,
}: SaveViewDialogProps) {
  const [name, setName] = React.useState('')
  const [description, setDescription] = React.useState('')
  const [isMaterialized, setIsMaterialized] = React.useState(false)
  const [tags, setTags] = React.useState<string[]>([])
  const [tagInput, setTagInput] = React.useState('')

  // Reset form when dialog opens
  React.useEffect(() => {
    if (open) {
      setName('')
      setDescription('')
      setIsMaterialized(false)
      setTags([])
      setTagInput('')
    }
  }, [open])

  const handleAddTag = () => {
    if (tagInput.trim() && !tags.includes(tagInput.trim())) {
      setTags([...tags, tagInput.trim()])
      setTagInput('')
    }
  }

  const handleRemoveTag = (tag: string) => {
    setTags(tags.filter(t => t !== tag))
  }

  const handleSave = () => {
    if (! name.trim()) return
    onSave({
      name:  name.trim(),
      description: description.trim(),
      is_materialized: isMaterialized,
      tags,
    })
  }

  // Extract source tables from pipeline
  const sourceTables = pipeline
    .filter(s => s.type === 'source' || s.type === 'join')
    .map(s => s.config.table)
    .filter(Boolean)

  return (
    <Dialog open={open} onClose={onClose} maxWidth="sm" fullWidth>
      <DialogTitle>Save as Derived View</DialogTitle>
      <DialogContent>
        <Stack spacing={3} sx={{ mt: 1 }}>
          <TextField
            label="View Name"
            value={name}
            onChange={(e) => setName(e.target.value.toLowerCase().replace(/[^a-z0-9_]/g, '_'))}
            fullWidth
            required
            helperText="Use lowercase letters, numbers, and underscores only"
            placeholder="e.g., user_activity_summary"
          />

          <TextField
            label="Description"
            value={description}
            onChange={(e) => setDescription(e.target.value)}
            fullWidth
            multiline
            rows={2}
            placeholder="Describe what this view contains..."
          />

          <FormControlLabel
            control={
              <Switch
                checked={isMaterialized}
                onChange={(e) => setIsMaterialized(e.target.checked)}
              />
            }
            label="Materialize (persist results)"
          />

          {isMaterialized ? (
            <Alert severity="info">
              <Typography variant="body2">
                <strong>Materialized view:</strong> Results will be computed and stored.
                Faster to query but needs manual refresh when source data changes.
              </Typography>
            </Alert>
          ) : (
            <Alert severity="info">
              <Typography variant="body2">
                <strong>Virtual view:</strong> Pipeline is executed on-demand each time.
                Always up-to-date but may be slower for complex transformations.
              </Typography>
            </Alert>
          )}

          <Stack spacing={1}>
            <Typography variant="body2" color="text.secondary">Tags</Typography>
            <Stack direction="row" spacing={1}>
              <TextField
                size="small"
                placeholder="Add tag..."
                value={tagInput}
                onChange={(e) => setTagInput(e.target.value)}
                onKeyPress={(e) => {
                  if (e.key === 'Enter') {
                    e.preventDefault()
                    handleAddTag()
                  }
                }}
                sx={{ flex: 1 }}
              />
              <Button onClick={handleAddTag}>Add</Button>
            </Stack>
            {tags.length > 0 && (
              <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
                {tags.map(tag => (
                  <Chip
                    key={tag}
                    label={tag}
                    size="small"
                    onDelete={() => handleRemoveTag(tag)}
                  />
                ))}
              </Stack>
            )}
          </Stack>

          {/* Pipeline Summary */}
          <Stack spacing={1}>
            <Typography variant="body2" color="text.secondary">Pipeline Summary</Typography>
            <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
              <Chip label={`${pipeline.length} step(s)`} size="small" variant="outlined" />
              {sourceTables.length > 0 && (
                <Chip label={`Sources: ${sourceTables.join(', ')}`} size="small" variant="outlined" />
              )}
            </Stack>
          </Stack>
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button
          variant="contained"
          onClick={handleSave}
          disabled={! name.trim()}
        >
          Save View
        </Button>
      </DialogActions>
    </Dialog>
  )
}