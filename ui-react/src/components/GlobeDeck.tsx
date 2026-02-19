// path: src/components/GlobeDeck.tsx
import React, { useMemo } from 'react'
import DeckGL from '@deck.gl/react'
// GlobeView is experimental -> import as _GlobeView; fallback to MapView if missing
import { _GlobeView as GlobeView, MapView, MapViewState } from '@deck.gl/core'
import { ScatterplotLayer } from '@deck.gl/layers'
import { HexagonLayer } from '@deck.gl/aggregation-layers'
import Legend from './Legend'

type Ev = {
  ts: string
  user_id?: string
  event_type?: string
  ip?: string
  country?: string
  city?: string
  geo_lat?: number
  geo_lon?: number
  anom_score?: number
  anomaly?: boolean
  is_unusual?: boolean
  explain?: string
  cluster?: number
}

function turboRGBA(t: number) {
  const clamp = (x: number) => Math.max(0, Math.min(1, x))
  const r = Math.floor(255 * clamp(1.5 - 4 * Math.abs(t - 0.5)))
  const g = Math.floor(255 * clamp(1.5 - 4 * Math.abs(t - 0.25)))
  const b = Math.floor(255 * clamp(1.5 - 4 * Math.abs(t - 0.75)))
  return [r, g, b, 220] as [number, number, number, number]
}

const catColor = (i: number) => {
  const palette = [
    [41, 98, 255, 220], [0, 200, 83, 220], [170, 0, 255, 220], [255, 109, 0, 220],
    [197, 17, 98, 220], [0, 145, 234, 220], [255, 214, 0, 220], [0, 184, 212, 220]
  ]
  return palette[Math.abs(i) % palette.length]
}

export type GlobeParams = {
  enabled: boolean
  colorBy: 'anom_score' | 'cluster' | 'is_unusual'
  hexEnabled: boolean
  hexRadiusKm: number
  opacity: number
  zThr?: number // NEW: z-threshold (maps to score threshold via logistic)
}

export default function GlobeDeck({
  data,
  params,
  zThr,
  thresholdPct,      // advanced score_quantile (0..1)
  thresholdLabel,    // advanced label text
  colorByFields,
  topK = 6
}: {
  data: Ev[]
  params: GlobeParams
  /** simple-only: show legend tick at sigmoid(zThr) */
  zThr?: number
  /** advanced-only: pass quantile in 0..1 for the legend tick */
  thresholdPct?: number
  /** optional legend caption, e.g., "contamination=5.0% (score≥0.82)" */
  thresholdLabel?: string
  /** NEW: split into categories by a composite key of these fields */
  colorByFields?: string[]
  /** NEW: limit the number of distinct categories drawn as separate hex layers */
  topK?: number
}) {
  const events = useMemo(
    () => data.filter(d => Number.isFinite(d.geo_lon) && Number.isFinite(d.geo_lat)),
    [data]
  )

  const initialViewState: Partial<MapViewState> = {
    latitude: 20,
    longitude: 10,
    zoom: 0,
    rotationX: 25,
    rotationOrbit: 0
  }

  const colorBy = params.colorBy

  // Simple mode: compute score threshold (0..1) from zThr using a logistic mapping
  const simpleScoreThr = (typeof zThr === 'number') ? (1 / (1 + Math.exp(-zThr))) : undefined

  const scatter = new ScatterplotLayer<Ev>({
    id: 'events-scatter',
    data: events,
    visible: params.enabled,
    pickable: true,
    parameters: { depthTest: true },
    getPosition: d => [d.geo_lon!, d.geo_lat!],
    radiusUnits: 'meters',
    getRadius: d => 60000 + (d.anom_score ?? 0) * 300000, // size with severity
    getFillColor: d => {
      if (colorBy === 'is_unusual') return d.is_unusual ? [255, 80, 80, 220] : [80, 160, 255, 220]
      if (colorBy === 'cluster' && Number.isFinite(d.cluster)) return catColor(Number(d.cluster))
      const s = Math.max(0, Math.min(1, d.anom_score ?? 0))
      return turboRGBA(s)
    },
    opacity: params.opacity,
    getTooltip: ({ object }) =>
      object &&
      `
<b>${(object as any).user_id ?? 'user ?'}</b> — ${(object as any).event_type ?? 'event'}
${(object as any).city ?? '?'} / ${(object as any).country ?? '?'}
ip=${(object as any).ip ?? '?'}
unusual=${(object as any).is_unusual ? 'yes' : 'no'} | anomaly=${(object as any).anomaly ? 'yes' : 'no'}
score=${((object as any).anom_score ?? 0).toFixed(2)}
<i>${(object as any).explain ?? ''}</i>
`.trim()
  })

  const hex = params.hexEnabled
    ? new HexagonLayer<Ev>({
        id: 'hex-agg',
        data: events,
        visible: params.enabled,
        getPosition: d => [d.geo_lon!, d.geo_lat!],
        radius: params.hexRadiusKm * 1000,
        elevationScale: 30,
        extruded: true,
        colorAggregation: colorBy === 'is_unusual' ? 'MEAN' : 'MEAN',
        getColorWeight: d => {
          if (colorBy === 'is_unusual') return d.is_unusual ? 1 : 0
          if (colorBy === 'cluster') return (d.cluster ?? 0) % 8 / 8
          return d.anom_score ?? 0
        },
        getElevationWeight: () => 1,
        elevationAggregation: 'SUM',
        getFillColor: (cell: any) => {
          const v = (cell?.colorValue as number) || 0
          if (colorBy === 'cluster') return catColor(Math.round(v * 8))
          return turboRGBA(v)
        },
        material: true,
        pickable: true,
        opacity: 0.5,
        coverage: 0.85,
        upperPercentile: 100
      })
    : null

  // NEW: Build layers, optionally splitting hex aggregation by composite category
  const layers: any[] = []
  if (params.hexEnabled && Array.isArray(colorByFields) && colorByFields.length > 0) {
    const keyOf = (e: any) => colorByFields.map(f => String(e?.[f] ?? '∅')).join(' · ')
    const counts = new Map<string, number>()
    for (const e of events) {
      const k = keyOf(e)
      counts.set(k, (counts.get(k) || 0) + 1)
    }
    const cats = Array.from(counts.entries()).sort((a, b) => b[1] - a[1]).slice(0, topK).map(([k]) => k)

    cats.forEach((cat, idx) => {
      const subset = events.filter(e => keyOf(e) === cat)
      layers.push(new HexagonLayer<Ev>({
        id: `hex-cat-${idx}`,
        data: subset,
        visible: params.enabled,
        getPosition: d => [d.geo_lon!, d.geo_lat!],
        radius: params.hexRadiusKm * 1000,
        extruded: true,
        elevationScale: 30,
        colorAggregation: 'MEAN',
        getColorWeight: _ => 1,
        getElevationWeight: () => 1,
        elevationAggregation: 'SUM',
        getFillColor: catColor(idx),
        pickable: true,
        opacity: 0.5,
        coverage: 0.85,
        upperPercentile: 100
      }))
    })

    const others = events.filter(e => !cats.includes(keyOf(e)))
    if (others.length) {
      layers.push(new HexagonLayer<Ev>({
        id: 'hex-cat-others',
        data: others,
        visible: params.enabled,
        getPosition: d => [d.geo_lon!, d.geo_lat!],
        radius: params.hexRadiusKm * 1000,
        extruded: true,
        elevationScale: 20,
        colorAggregation: 'MEAN',
        getColorWeight: _ => 1,
        getElevationWeight: () => 1,
        elevationAggregation: 'SUM',
        getFillColor: [158, 158, 158, 160],
        pickable: true,
        opacity: 0.35,
        coverage: 0.85,
        upperPercentile: 100
      }))
    }
    // keep scatter for tooltips on individual points
    layers.push(scatter)
  } else {
    // existing behavior
    if (hex) layers.push(hex)
    layers.push(scatter)
  }

  const viewInstance = GlobeView ? new (GlobeView as any)() : new MapView()

  return (
    <div style={{ position: 'relative', height: 560 }}>
      <DeckGL
        views={viewInstance}
        controller
        initialViewState={initialViewState}
        layers={layers}
        getTooltip={({ object, layer }) => {
          if (!object) return null
          const lid = (layer?.id as string) || ''
          if (lid === 'hex-agg' || lid.startsWith('hex-cat')) {
            const count = (object as any).elevationValue as number
            const val = (object as any).colorValue as number
            return {
              html:
                `<div class="deck-tooltip"><b>Cluster cell</b><br/>events=${count}<br/>` +
                `value=${Number.isFinite(val) ? val.toFixed(2) : 'n/a'}</div>`
            }
          }
          const o: any = object
          return {
            html: `<div class="deck-tooltip"><b>${o.user_id ?? 'user ?'}</b> — ${
              o.event_type ?? 'event'
            }<br/>
${o.city ?? '?'} / ${o.country ?? '?'}<br/>
ip=${o.ip ?? '?'}<br/>
unusual=${o.is_unusual ? 'yes' : 'no'} | anomaly=${o.anomaly ? 'yes' : 'no'}<br/>
score=${(o.anom_score ?? 0).toFixed(2)}<br/>
<i>${o.explain ?? ''}</i></div>`
          }
        }}
      />
      <Legend
        title={
          Array.isArray(colorByFields) && colorByFields.length > 0
            ? `Top ${topK} of ${colorByFields.join(' · ')}`
            : (params.colorBy === 'anom_score' ? 'Anomaly score' :
               params.colorBy === 'is_unusual' ? 'Unusual ratio' : 'Cluster index')
        }
        thresholdPct={
          typeof thresholdPct === 'number' ? thresholdPct :
          (typeof simpleScoreThr === 'number' ? simpleScoreThr : undefined)
        }
        thresholdLabel={
          thresholdLabel ??
          (typeof simpleScoreThr === 'number'
            ? `z_thr=${zThr?.toFixed(1)} (score≥${simpleScoreThr.toFixed(2)})`
            : undefined)
        }
      />
    </div>
  )
}