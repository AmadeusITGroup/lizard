import React, {useMemo} from 'react'
import Plot from 'react-plotly.js'
import { Box, FormControlLabel, Switch, Slider, Stack, Typography } from '@mui/material'

type Ev = { ts: string; user_id?: string; anom_score?: number; anomaly?: boolean }

function minuteKey(ts: string) { return ts.slice(0,16) }

export type TimelineParams = {
  enabled: boolean
  showAnomalies: boolean
  topN: number
}

export default function TimelineChart({
  data,
  params,
  onParams
}:{
  data: Ev[],
  params: TimelineParams,
  onParams: (p:TimelineParams)=>void
}) {
  const byUser = useMemo(()=>{
    const m = new Map<string, number>()
    for (const e of data) if (e.user_id) m.set(e.user_id, (m.get(e.user_id)||0)+1)
    return Array.from(m.entries()).sort((a,b)=>b[1]-a[1]).map(([u])=>u)
  },[data])

  const tops = new Set(byUser.slice(0, params.topN))
  const grouped: Record<string, Record<string, number>> = {}
  const anomByMinute: Record<string, number> = {}

  for (const e of data) {
    const mk = minuteKey(e.ts)
    const bucket = e.user_id && tops.has(e.user_id) ? e.user_id : 'Others'
    grouped[mk] ??= {}
    grouped[mk][bucket] = (grouped[mk][bucket]||0) + 1
    if (params.showAnomalies && e.anomaly) {
      anomByMinute[mk] = Math.max(anomByMinute[mk]||0, e.anom_score||0)
    }
  }

  const minutes = Object.keys(grouped).sort()
  const users = Array.from(new Set(Object.values(grouped).flatMap(x=>Object.keys(x)) ))
  const palette = ['#005EB8','#00C853','#AA00FF','#FF6D00','#C51162','#0091EA','#FFD600','#00B8D4']
  const colors = new Map<string,string>()
  users.forEach((u,i)=>colors.set(u, u==='Others' ? '#9E9E9E' : palette[i % palette.length]))

  const traces:any[] = users.map(u => ({
    x: minutes,
    y: minutes.map(m => grouped[m]?.[u] || 0),
    type: 'scatter',
    mode: 'lines+markers',
    name: u,
    marker: { color: colors.get(u) }
  }))

  if (params.enabled && params.showAnomalies) {
    traces.push({
      x: Object.keys(anomByMinute),
      y: Object.keys(anomByMinute).map(_=>0),
      mode: 'markers',
      name: 'Anomaly spikes',
      marker: { color: '#FF1744', symbol: 'x', size: 9 },
      text: Object.values(anomByMinute).map(s=>`max score=${s.toFixed(2)}`),
      hovertemplate: '%{x}<br>%{text}<extra></extra>'
    })
  }

  return (
    <Box>
      <Stack direction="row" spacing={3} alignItems="center" sx={{mb:1}}>
        <FormControlLabel
          control={<Switch checked={params.enabled} onChange={(_,v)=>onParams({...params, enabled:v})} />}
          label="Apply analytics on timeline"
        />
        <FormControlLabel
          control={<Switch checked={params.showAnomalies} disabled={!params.enabled} onChange={(_,v)=>onParams({...params, showAnomalies:v})} />}
          label="Show anomaly markers"
        />
        <Box sx={{width:240}}>
          <Typography variant="caption">Top‑N users {params.topN}</Typography>
          <Slider min={3} max={20} step={1} value={params.topN} onChange={(_,v)=>onParams({...params, topN:v as number})} />
        </Box>
      </Stack>
      <Plot
        data={traces}
        layout={{
          title: `Timeline (Top‑${params.topN} users, Others grouped)`,
          xaxis: { title: 'Time (minute)' },
          yaxis: { title: 'Events' },
          legend: { orientation: 'h' },
          hovermode: 'x unified',
          paper_bgcolor: 'rgba(0,0,0,0)',
          plot_bgcolor: 'rgba(0,0,0,0)'
        }}
        style={{width:'100%', height: 420}}
        config={{displaylogo:false, responsive:true}}
      />
    </Box>
  )
}