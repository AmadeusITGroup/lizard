// ui-react/src/components/DashboardLayoutEngine.tsx
// Layout utilities and presets for the flexible dashboard

import { VisualizationType } from './AddVisualizationDialog'

// ============================================================
// Types
// ============================================================

export interface LayoutItem {
  x: number
  y: number
  w: number
  h:  number
  minW?: number
  minH?:  number
  maxW?: number
  maxH?: number
}

export interface LayoutPresetPanel {
  type: VisualizationType
  layout: LayoutItem
}

export interface LayoutPreset {
  id: string
  name: string
  description: string
  panels: LayoutPresetPanel[]
}

// ============================================================
// Preset Layouts
// ============================================================

export const LAYOUT_PRESETS: LayoutPreset[] = [
  {
    id: 'fraud-overview',
    name: 'Fraud Overview',
    description: 'Timeline + Map + Grid for investigation',
    panels: [
      {
        type: 'timeline',
        layout: { x: 0, y: 0, w: 12, h: 5, minW: 6, minH: 4 },
      },
      {
        type: 'map',
        layout: { x: 0, y: 5, w:  6, h: 6, minW: 4, minH: 4 },
      },
      {
        type: 'grid',
        layout: { x: 6, y: 5, w: 6, h:  6, minW: 4, minH: 4 },
      },
    ],
  },
  {
    id:  'entity-analysis',
    name: 'Entity Analysis',
    description: 'Graph + Grid for relationship analysis',
    panels: [
      {
        type: 'graph',
        layout: { x:  0, y: 0, w: 8, h: 8, minW: 5, minH: 5 },
      },
      {
        type: 'grid',
        layout: { x: 8, y: 0, w: 4, h: 8, minW: 3, minH: 4 },
      },
    ],
  },
  {
    id: 'full-investigation',
    name: 'Full Investigation',
    description: 'All panels for comprehensive analysis',
    panels: [
      {
        type: 'timeline',
        layout: { x: 0, y: 0, w: 12, h: 4, minW: 6, minH: 3 },
      },
      {
        type:  'map',
        layout:  { x: 0, y:  4, w: 4, h: 5, minW: 3, minH: 3 },
      },
      {
        type:  'grid',
        layout:  { x: 4, y:  4, w: 4, h: 5, minW:  3, minH: 3 },
      },
      {
        type: 'graph',
        layout: { x:  8, y: 4, w: 4, h: 5, minW: 3, minH: 3 },
      },
      {
        type: 'sankey',
        layout: { x: 0, y: 9, w: 12, h: 5, minW: 6, minH: 4 },
      },
    ],
  },
  {
    id: 'timeline-focus',
    name: 'Timeline Focus',
    description: 'Large timeline with supporting data',
    panels: [
      {
        type: 'timeline',
        layout: { x: 0, y: 0, w: 12, h:  7, minW: 8, minH: 5 },
      },
      {
        type: 'grid',
        layout: { x:  0, y: 7, w: 12, h: 5, minW: 6, minH: 4 },
      },
    ],
  },
  {
    id: 'geo-analysis',
    name: 'Geographic Analysis',
    description: 'Map-focused layout with timeline',
    panels: [
      {
        type: 'map',
        layout: { x:  0, y: 0, w: 8, h: 8, minW: 5, minH: 5 },
      },
      {
        type: 'timeline',
        layout: { x: 8, y: 0, w:  4, h: 4, minW: 3, minH: 3 },
      },
      {
        type: 'grid',
        layout: { x:  8, y: 4, w:  4, h: 4, minW: 3, minH: 3 },
      },
    ],
  },
  {
    id: 'comparison',
    name: 'Side-by-Side Comparison',
    description: 'Two timelines for comparison',
    panels: [
      {
        type: 'timeline',
        layout: { x: 0, y: 0, w: 6, h: 6, minW: 4, minH: 4 },
      },
      {
        type: 'timeline',
        layout: { x:  6, y: 0, w: 6, h: 6, minW: 4, minH: 4 },
      },
      {
        type: 'grid',
        layout: { x:  0, y: 6, w:  12, h: 5, minW: 6, minH: 4 },
      },
    ],
  },
]

// ============================================================
// Layout Utilities
// ============================================================

export class DashboardLayoutEngine {
  private columnCount: number
  private rowHeight: number
  private gap: number
  
  constructor(columnCount: number = 12, rowHeight: number = 60, gap: number = 12) {
    this.columnCount = columnCount
    this.rowHeight = rowHeight
    this.gap = gap
  }
  
  // Check if two layout items overlap
  checkCollision(itemA: LayoutItem, itemB: LayoutItem): boolean {
    const aRight = itemA.x + itemA.w
    const aBottom = itemA.y + itemA.h
    const bRight = itemB.x + itemB.w
    const bBottom = itemB.y + itemB.h
    
    return !(aRight <= itemB.x || itemA.x >= bRight || aBottom <= itemB.y || itemA.y >= bBottom)
  }
  
  // Find all items that collide with the given item
  findCollisions(item: LayoutItem, items: LayoutItem[], excludeIndex?:  number): number[] {
    const collisions:  number[] = []
    items.forEach((other, index) => {
      if (index !== excludeIndex && this.checkCollision(item, other)) {
        collisions.push(index)
      }
    })
    return collisions
  }
  
  // Resolve collisions by pushing items down
  resolveCollisions(items: LayoutItem[], movedIndex: number): LayoutItem[] {
    const result = [...items]
    const moved = result[movedIndex]
    
    let hasChanges = true
    let iterations = 0
    const maxIterations = 100
    
    while (hasChanges && iterations < maxIterations) {
      hasChanges = false
      iterations++
      
      for (let i = 0; i < result.length; i++) {
        if (i === movedIndex) continue
        
        if (this.checkCollision(moved, result[i])) {
          // Push the colliding item down
          result[i] = {
            ...result[i],
            y: moved.y + moved.h,
          }
          hasChanges = true
        }
      }
    }
    
    return result
  }
  
  // Compact the layout by moving items up where possible
  compactLayout(items: LayoutItem[]): LayoutItem[] {
    const sorted = [...items].sort((a, b) => a.y - b.y || a.x - b.x)
    const result: LayoutItem[] = []
    
    for (const item of sorted) {
      let newY = 0
      let placed = false
      
      while (!placed) {
        const testItem = { ...item, y: newY }
        const hasCollision = result.some(other => this.checkCollision(testItem, other))
        
        if (! hasCollision) {
          result.push(testItem)
          placed = true
        } else {
          newY++
        }
        
        // Safety check
        if (newY > 1000) {
          result.push(item)
          break
        }
      }
    }
    
    return result
  }
  
  // Find the first available position for a new item
  findAvailablePosition(items: LayoutItem[], width: number, height: number): { x: number; y: number } {
    const maxY = items.reduce((max, item) => Math.max(max, item.y + item.h), 0)
    
    for (let y = 0; y <= maxY + 1; y++) {
      for (let x = 0; x <= this.columnCount - width; x++) {
        const testItem = { x, y, w: width, h: height }
        const hasCollision = items.some(item => this.checkCollision(testItem, item))
        
        if (!hasCollision) {
          return { x, y }
        }
      }
    }
    
    return { x:  0, y: maxY }
  }
  
  // Constrain an item to grid bounds
  constrainToBounds(item: LayoutItem): LayoutItem {
    return {
      ...item,
      x: Math.max(0, Math.min(item.x, this.columnCount - item.w)),
      y: Math.max(0, item.y),
      w: Math.max(item.minW || 1, Math.min(item.w, item.maxW || this.columnCount)),
      h: Math.max(item.minH || 1, Math.min(item.h, item.maxH || 100)),
    }
  }
  
  // Calculate pixel position from grid position
  gridToPixels(item: LayoutItem, containerWidth: number): { left: number; top: number; width: number; height: number } {
    const cellWidth = (containerWidth - (this.columnCount - 1) * this.gap) / this.columnCount
    
    return {
      left: item.x * (cellWidth + this.gap),
      top: item.y * (this.rowHeight + this.gap),
      width: item.w * cellWidth + (item.w - 1) * this.gap,
      height: item.h * this.rowHeight + (item.h - 1) * this.gap,
    }
  }
  
  // Calculate grid position from pixel position
  pixelsToGrid(left: number, top: number, containerWidth: number): { x: number; y: number } {
    const cellWidth = (containerWidth - (this.columnCount - 1) * this.gap) / this.columnCount
    
    return {
      x: Math.round(left / (cellWidth + this.gap)),
      y: Math.round(top / (this.rowHeight + this.gap)),
    }
  }
}

// Export singleton for convenience
export const layoutEngine = new DashboardLayoutEngine()