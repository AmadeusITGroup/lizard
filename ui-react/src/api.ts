// path: ui-react/src/api.ts
/**
 * API client for Lizard backend
 * Includes all endpoints for visualization, upload, mapping, and rules
 */
import axios from 'axios';

// -------------------------------
// API base configuration
// -------------------------------
const API = import.meta.env.VITE_API_BASE || 'http://localhost:8000';

// Create axios instance with defaults
const apiClient = axios.create({
  baseURL: API,
  timeout: 60000,
  headers: {
    'Content-Type': 'application/json',
  },
});

// -------------------------------
// Types
// -------------------------------
export type FieldType = 'string' | 'number' | 'datetime' | 'boolean' | 'json';
export type AnalyticsMode = 'none' | 'simple' | 'advanced';
export type Bucket = '30s' | '1m' | '2m' | '5m' | '10m' | '15m' | '30m' | '1h' | '3h' | '6h' | '12h' | '1d';
export type Metric = 'count' | 'avg' | 'max' | 'sum' | 'min';

export type FilterCond = {
  field: string;
  op: 
    | 'eq' | 'ne' | 'lt' | 'lte' | 'gt' | 'gte'
    | 'in' | 'nin'
    | 'contains' | 'icontains' | 'startswith' | 'endswith';
  value: any;
};

// Mapping types
export interface MappingTemplate {
  id: string;
  name: string;
  description: string;
  mapping: Record<string, string>;
  expressions: Record<string, any>;
  source_type: string;
  category: string;
  tags: string[];
  sample_columns: string[];
  validation_rules: any[];
  use_count: number;
  last_used_at: string | null;
  created_at:  string;
  updated_at:  string;
  is_builtin: boolean;
  is_active: boolean;
  created_by: string;
}

export interface MatchResult {
  template_id: string;
  template_name: string;
  category: string;
  description: string;
  score: number;
  columns_matched: string[];
  columns_missing: string[];
}

export interface SuggestionResult {
  filename: string;
  total_rows: number;
  columns:  string[];
  suggested_mapping: Record<string, string>;
  suggested_expressions: Record<string, any>;
  candidates: Record<string, Array<{ column: string; score: number }>>;
  column_analysis: Record<string, {
    detected_type: string;
    null_count: number;
    null_percent: number;
    unique_count: number;
    sample_values: string[];
  }>;
  engine_used: string;
}

export interface PreviewResult {
  filename: string;
  total_rows: number;
  sample_rows: number;
  columns_source: string[];
  columns_mapped: string[];
  mapping:  Record<string, string>;
  expressions: Record<string, any>;
  template_used: string | null;
  template_id: string | null;
  sample:  any[];
  validation:  {
    is_valid: boolean;
    total_rows: number;
    valid_rows: number;
    rejected_rows: number;
    warning_rows: number;
    issues: Array<{
      row: number;
      field: string;
      rule: string;
      type: string;
      severity: string;
      message: string;
      original: string | null;
      fixed: string | null;
      action: string;
    }>;
  } | null;
}

export interface IngestionLog {
  id: string;
  filename: string;
  source_name: string;
  template_id: string | null;
  template_name: string | null;
  mapping_used: Record<string, any>;
  status: string;
  rows_total: number;
  rows_ingested: number;
  rows_rejected: number;
  started_at: string;
  completed_at: string | null;
}

export interface FieldInfo {
  type: string;
  required: boolean;
  description: string;
}

// Timeline specific types
export interface TimelineAnomalyEvent {
  ts: string | null;
  user_id?:  string;
  event_type?:  string;
  anom_score:  number;
  reasons?:  Array<{ code: string; desc?:  string; weight?: number }>;
  explain?:  string;
  ip?: string;
  device_id?: string;
  country?: string;
  city?: string;
  account_id?: string;
  [key: string]: any;
}

export interface TimelineResponse {
  minutes: string[];
  series: Array<{ name: string; values: number[] }>;
  anom_by_minute: Record<string, number>;
  anom_by_minute_score: Record<string, number>;
  reasons_by_minute: Record<string, string[]>;
  explain_by_minute: Record<string, string>;
  anomaly_events: Record<string, TimelineAnomalyEvent[]>;
  thresholds: {
    mode?:  'simple' | 'advanced';
    z_thr?:  number;
    score_thr?: number;
    contamination?: number;
    score_quantile?: number;
  };
  top_users:  string[];
  rows: any[];
  metric: string;
  value_field?:  string;
  group_field:  string;
  groups: string[];
  normalize: boolean;
  smooth: number;
}

export interface TopUsersResponse {
  users:  string[];
  counts: Record<string, number>;
  group_field: string;
  total_unique: number;
}

// Data source types for workbench integration
export interface DataSource {
  name: string;
  type: 'table' | 'view';
  row_count?:  number;
  description?: string;
  columns?:  Record<string, any>;
  view_id?: string;
  is_materialized?: boolean;
  min_ts?: string;
  max_ts?: string;
}

// -------------------------------
// Schema Endpoints (REQUIRED BY EXISTING COMPONENTS)
// -------------------------------

export async function fetchSchemaFields(): Promise<{
  fields: string[];
  types: Record<string, FieldType>;
}> {
  const { data } = await apiClient.get('/schema/events/fields');
  const fields = data.fields?.map((f: any) => f.name || f) || [];
  const types:  Record<string, FieldType> = {};
  if (data.fields) {
    data.fields.forEach((f: any) => {
      if (typeof f === 'object') {
        types[f.name] = f.type || 'string';
      } else {
        types[f] = 'string';
      }
    });
  }
  return { fields, types };
}

export async function fetchSchemaOperators(): Promise<Record<FieldType, string[]>> {
  return {
    string: ['eq', 'ne', 'contains', 'icontains', 'startswith', 'endswith', 'in', 'nin'],
    number: ['eq', 'ne', 'lt', 'lte', 'gt', 'gte', 'in', 'nin'],
    datetime: ['eq', 'ne', 'lt', 'lte', 'gt', 'gte'],
    boolean: ['eq', 'ne'],
    json: ['eq', 'ne', 'contains'],
  };
}

// -------------------------------
// Top Users Endpoint (REQUIRED BY TimelinePanel)
// -------------------------------

export async function fetchTopUsers(params:  {
  start:  string;
  end: string;
  n?:  number;
  group_field?: string;
  where?: FilterCond[];
}): Promise<TopUsersResponse> {
  try {
    const { data } = await apiClient.post('/analytics/top-users', {
      start: params.start,
      end: params.end,
      n: params.n || 50,
      group_field: params.group_field || 'user_id',
      where: params.where || [],
    });
    return {
      users: data.users || [],
      counts: data.counts || {},
      group_field:  data.group_field || 'user_id',
      total_unique: data.total_unique || 0,
    };
  } catch (e) {
    // Fallback:  try GET method
    try {
      const { data } = await apiClient.get('/analytics/top-users', {
        params: { 
          start: params.start, 
          end: params.end, 
          n: params.n || 50,
          group_field: params.group_field || 'user_id',
        },
      });
      return {
        users: data.users || data || [],
        counts: data.counts || {},
        group_field:  data.group_field || 'user_id',
        total_unique: data.total_unique || 0,
      };
    } catch {
      console.warn('fetchTopUsers failed, returning empty');
      return { users: [], counts: {}, group_field: 'user_id', total_unique: 0 };
    }
  }
}

// -------------------------------
// Data Sources (Workbench Integration)
// -------------------------------

export async function fetchDataSources(): Promise<DataSource[]> {
  try {
    const { data } = await apiClient.get('/workbench/sources');
    return data;
  } catch (e) {
    console.warn('fetchDataSources failed:', e);
    return [];
  }
}

export async function fetchDataSourceInfo(sourceName: string): Promise<DataSource & { columns: Record<string, any> }> {
  const { data } = await apiClient.get(`/workbench/sources/${sourceName}`);
  return data;
}

// -------------------------------
// Upload / Ingest Endpoints
// -------------------------------

export async function uploadPreview(
  file: File,
  engine: string = 'heuristic',
  sampleRows: number = 25
) {
  const formData = new FormData();
  formData.append('file', file);
  const { data } = await apiClient.post('/upload/preview', formData, {
    params: { engine_name: engine, sample_rows: sampleRows },
    headers: { 'Content-Type': 'multipart/form-data' },
  });
  return data as {
    mapping: Record<string, any>;
    sample:  any[];
    columns?:  string[];
    suggested_expressions?: Record<string, any>;
  };
}

export async function uploadCommit(opts: {
  file: File;
  engine?:  string;
  sourceName?: string;
  mappingJson?: Record<string, any>;
}) {
  const formData = new FormData();
  formData.append('file', opts.file);
  if (opts.engine) formData.append('engine_name', opts.engine);
  if (opts.sourceName) formData.append('source_name', opts.sourceName);
  if (opts.mappingJson) {
    formData.append('mapping_json', JSON.stringify(opts.mappingJson));
  }
  const { data } = await apiClient.post('/upload/events', formData, {
    headers: { 'Content-Type':  'multipart/form-data' },
  });
  return data as { ingested: number; source: string };
}

// -------------------------------
// Mapping Template Endpoints
// -------------------------------

export async function fetchMappingTemplates(params?:  {
  category?: string;
  tag?: string;
  search?: string;
  active_only?: boolean;
}): Promise<MappingTemplate[]> {
  const { data } = await apiClient.get('/mapping/templates', { params });
  return data;
}

export async function fetchMappingTemplate(id: string): Promise<MappingTemplate> {
  const { data } = await apiClient.get(`/mapping/templates/${id}`);
  return data;
}

export async function createMappingTemplate(
  template:  Partial<MappingTemplate>
): Promise<MappingTemplate> {
  const { data } = await apiClient.post('/mapping/templates', template);
  return data;
}

export async function updateMappingTemplate(
  id:  string,
  updates: Partial<MappingTemplate>
): Promise<MappingTemplate> {
  const { data } = await apiClient.put(`/mapping/templates/${id}`, updates);
  return data;
}

export async function deleteMappingTemplate(id: string): Promise<void> {
  await apiClient.delete(`/mapping/templates/${id}`);
}

export async function cloneMappingTemplate(
  id: string,
  newName?:  string
): Promise<MappingTemplate> {
  const { data } = await apiClient.post(`/mapping/templates/${id}/clone`, null, {
    params: { new_name: newName },
  });
  return data;
}

export async function recordMappingTemplateUse(
  id: string
): Promise<{ template_id: string; use_count: number }> {
  const { data } = await apiClient.post(`/mapping/templates/${id}/use`);
  return data;
}

export async function matchMappingTemplates(
  file: File,
  threshold:  number = 0.3
): Promise<MatchResult[]> {
  const formData = new FormData();
  formData.append('file', file);
  const { data } = await apiClient.post('/mapping/templates/match', formData, {
    params: { threshold },
    headers:  { 'Content-Type': 'multipart/form-data' },
  });
  return data;
}

export async function suggestMapping(
  file: File,
  engine: string = 'heuristic'
): Promise<SuggestionResult> {
  const formData = new FormData();
  formData.append('file', file);
  const { data } = await apiClient.post('/mapping/templates/suggest', formData, {
    params: { engine },
    headers: { 'Content-Type': 'multipart/form-data' },
  });
  return data;
}

export async function previewMapping(
  file: File,
  options?:  {
    templateId?: string;
    mapping?: Record<string, string>;
    expressions?: Record<string, any>;
    sampleRows?: number;
  }
): Promise<PreviewResult> {
  const formData = new FormData();
  formData.append('file', file);
  if (options?.templateId) formData.append('template_id', options.templateId);
  if (options?.mapping) formData.append('mapping_json', JSON.stringify(options.mapping));
  if (options?.expressions) formData.append('expressions_json', JSON.stringify(options.expressions));
  if (options?.sampleRows) formData.append('sample_rows', String(options.sampleRows));

  const { data } = await apiClient.post('/mapping/preview', formData, {
    headers:  { 'Content-Type': 'multipart/form-data' },
  });
  return data;
}

export async function validateMapping(
  file: File,
  options?: {
    templateId?: string;
    rulesJson?: any[];
    applyMapping?: boolean;
  }
): Promise<any> {
  const formData = new FormData();
  formData.append('file', file);
  if (options?.templateId) formData.append('template_id', options.templateId);
  if (options?.rulesJson) formData.append('rules_json', JSON.stringify(options.rulesJson));
  if (options?.applyMapping !== undefined) formData.append('apply_mapping', String(options.applyMapping));

  const { data } = await apiClient.post('/mapping/validate', formData, {
    headers: { 'Content-Type': 'multipart/form-data' },
  });
  return data;
}

// -------------------------------
// Mapping Fields & Schema Endpoints
// -------------------------------

export async function fetchMappingFields(): Promise<{
  fields: string[];
  field_info: Record<string, FieldInfo>;
}> {
  const { data } = await apiClient.get('/mapping/fields');
  return data;
}

export async function fetchMappingFieldInfo(fieldName: string): Promise<FieldInfo & { name: string }> {
  const { data } = await apiClient.get(`/mapping/fields/${fieldName}`);
  return data;
}

export async function fetchExpressionDocs(): Promise<Record<string, any>> {
  const { data } = await apiClient.get('/mapping/expressions');
  return data;
}

export async function fetchBuiltinValidationRules(): Promise<any[]> {
  const { data } = await apiClient.get('/mapping/validation-rules/builtins');
  return data;
}

export async function fetchValidationRuleTypes(): Promise<Record<string, any>> {
  const { data } = await apiClient.get('/mapping/validation-rules/types');
  return data;
}

// -------------------------------
// Ingestion Log Endpoints
// -------------------------------

export async function fetchIngestionLogs(params?:  {
  limit?: number;
  status?: string;
}): Promise<IngestionLog[]> {
  const { data } = await apiClient.get('/mapping/ingestion-logs', { params });
  return data;
}

export async function fetchIngestionLog(id: string): Promise<IngestionLog> {
  const { data } = await apiClient.get(`/mapping/ingestion-logs/${id}`);
  return data;
}

export async function createIngestionLog(log: {
  filename: string;
  source_name: string;
  template_id?: string;
  template_name?: string;
  mapping_used?: Record<string, any>;
}): Promise<IngestionLog> {
  const { data } = await apiClient.post('/mapping/ingestion-logs', log);
  return data;
}

export async function updateIngestionLog(
  id: string,
  updates: Partial<IngestionLog>
): Promise<IngestionLog> {
  const { data } = await apiClient.put(`/mapping/ingestion-logs/${id}`, updates);
  return data;
}

// -------------------------------
// Query Endpoints
// -------------------------------

export async function queryRawAdvanced(opts: {
  start: string;
  end: string;
  where?: FilterCond[];
  select?: string[];
  limit?: number;
}) {
  const { data } = await apiClient.post('/query/raw/advanced', opts);
  return data as any[];
}

export async function fetchDistinct(
  field: string,
  start: string,
  end: string,
  limit:  number = 100
) {
  const { data } = await apiClient.get('/schema/events/distinct', {
    params:  { field, start, end, limit },
  });
  return data as { field: string; values: string[] };
}

export async function fetchEventFields() {
  const { data } = await apiClient.get('/schema/events/fields');
  return data as { fields: Array<{ name: string; type:  FieldType }> };
}

export async function fetchOperators(fieldType: FieldType) {
  const { data } = await apiClient.get('/schema/events/operators', {
    params:  { field_type: fieldType },
  });
  return data as { operators: string[] };
}

// -------------------------------
// Visualization Endpoints
// -------------------------------

export async function vizTimeline(body: {
  start: string;
  end: string;
  analytics?:  AnalyticsMode;
  z_thr?: number;
  contamination?: number;
  speed_thr?: number;
  dist_thr?: number;
  bucket?:  Bucket;
  metric?:  Metric;
  value_field?:  string;
  group_field?:  string;
  top_n?: number;
  normalize?: boolean;
  smooth?: number;
  tz?: string;
  where?: FilterCond[];
  source?: string; // NEW: data source name
}): Promise<TimelineResponse> {
  const { data } = await apiClient.post('/viz/timeline', body);
  return data;
}

export async function vizGlobe(body: {
  start:  string;
  end: string;
  analytics?: AnalyticsMode;
  z_thr?: number;
  contamination?:  number;
  speed_thr?: number;
  dist_thr?: number;
  cluster?: boolean;
  bucket?: Bucket;
  metric?:  Metric;
  value_field?: string;
  tz?: string;
  where?:  FilterCond[];
  route_mode?: boolean;
  route_metric?:  Metric;
  carrier?: string;
  source?:  string;
}) {
  const { data } = await apiClient.post('/viz/globe', body);
  return data as { events: any[]; routes: any[] };
}

export async function vizGrid(body: {
  start:  string;
  end: string;
  analytics?: AnalyticsMode;
  z_thr?: number;
  contamination?:  number;
  speed_thr?: number;
  dist_thr?: number;
  where?: FilterCond[];
  limit?: number;
  offset?: number;
  sort_by?: string;
  sort_dir?: 'asc' | 'desc';
  aggregate?: boolean;
  group_by?: string[];
  metric?: Metric;
  value_field?: string;
  source?:  string;
}) {
  const { data } = await apiClient.post('/viz/grid', body);
  return data as {
    rows: any[];
    total: number;
    limit: number;
    offset: number;
  };
}

export async function vizGraph(body: {
  start:  string;
  end: string;
  analytics?: AnalyticsMode;
  contamination?: number;
  speed_thr?: number;
  dist_thr?: number;
  edges?: string[];
  metric?: Metric;
  value_field?: string;
  min_link_value?: number;
  max_nodes?: number;
  max_links?: number;
  z_thr?: number;
  where?: FilterCond[];
  source?:  string;
}) {
  const { data } = await apiClient.post('/viz/graph', body);
  return data as {
    nodes: Array<{
      id: string;
      type: string;
      label: string;
      value: number;
      degree: number;
      community:  number;
      anom_max?:  number;
      reasons_top?: string[];
    }>;
    links: Array<{
      source: string;
      target: string;
      etype: string;
      value: number;
    }>;
  };
}

// -------------------------------
// Rules Engine Endpoints
// -------------------------------

export async function fetchRules(params?: {
  enabled_only?: boolean;
  tag?: string;
  severity?: string;
}): Promise<any[]> {
  const { data } = await apiClient.get('/rules/', { params });
  return data;
}

export async function fetchBuiltinRules(): Promise<any[]> {
  const { data } = await apiClient.get('/rules/builtins');
  return data;
}

export async function fetchRule(id: string): Promise<any> {
  const { data } = await apiClient.get(`/rules/${id}`);
  return data;
}

export async function createRule(rule: any): Promise<any> {
  const { data } = await apiClient.post('/rules/', rule);
  return data;
}

export async function updateRule(id: string, updates: any): Promise<any> {
  const { data } = await apiClient.put(`/rules/${id}`, updates);
  return data;
}

export async function deleteRule(id: string): Promise<void> {
  await apiClient.delete(`/rules/${id}`);
}

export async function enableRule(id: string): Promise<any> {
  const { data } = await apiClient.post(`/rules/${id}/enable`);
  return data;
}

export async function disableRule(id:  string): Promise<any> {
  const { data } = await apiClient.post(`/rules/${id}/disable`);
  return data;
}

export async function testRule(rule: any, testData: any[]): Promise<any> {
  const { data } = await apiClient.post('/rules/test', { rule, test_data: testData });
  return data;
}

export async function importRules(rules: any[], replace: boolean = false): Promise<any> {
  const { data } = await apiClient.post('/rules/import', rules, {
    params: { replace },
  });
  return data;
}

export async function exportRules(): Promise<any[]> {
  const { data } = await apiClient.get('/rules/export');
  return data;
}

// -------------------------------
// Export / Bundle Endpoints
// -------------------------------

export async function exportBundle(selection: Record<string, any>): Promise<Blob> {
  const { data } = await apiClient.post('/export/bundle', { selection }, {
    responseType: 'blob',
  });
  return data;
}

// -------------------------------
// Health & Metrics
// -------------------------------

export async function healthCheck(): Promise<{ status: string }> {
  const { data } = await apiClient.get('/health');
  return data;
}

// Export the API base URL for components that need it
export { API };
export default apiClient;

export interface CustomField {
  name: string;
  type: string;
  description: string;
  required: boolean;
  is_custom: boolean;
  created_at: string;
  updated_at?:  string;
}

export async function fetchCustomFields(): Promise<CustomField[]> {
  const { data } = await apiClient.get('/mapping/custom-fields');
  return data;
}

export async function createCustomField(field: {
  name: string;
  type: string;
  description:  string;
  required: boolean;
}): Promise<CustomField> {
  const { data } = await apiClient.post('/mapping/custom-fields', field);
  return data;
}

export async function updateCustomField(
  fieldName: string,
  updates:  Partial<CustomField>
): Promise<CustomField> {
  const { data } = await apiClient.put(`/mapping/custom-fields/${fieldName}`, updates);
  return data;
}

export async function deleteCustomField(fieldName: string): Promise<void> {
  await apiClient.delete(`/mapping/custom-fields/${fieldName}`);
}

// ============================================================
// Re-mapping API
// ============================================================

export interface RemapRequest {
  source_name: string;
  mapping:  Record<string, any>;
  expressions: Record<string, any>;
  start?:  string;
  end?: string;
  dry_run:  boolean;
}

export interface RemapResult {
  status: string;
  source_name: string;
  mapping:  Record<string, any>;
  expressions: Record<string, any>;
  message: string;
  affected_rows_estimate?:  number;
  preview_rows?: any[];
}

export async function remapData(request: RemapRequest): Promise<RemapResult> {
  const { data } = await apiClient.post('/mapping/remap', request);
  return data;
}

export async function previewRemap(
  sourceName: string,
  mapping:  Record<string, any>,
  limit: number = 10
): Promise<RemapResult> {
  const formData = new FormData();
  formData.append('source_name', sourceName);
  formData.append('mapping_json', JSON.stringify(mapping));
  formData.append('limit', String(limit));
  
  const { data } = await apiClient.post('/mapping/remap/preview', formData);
  return data;
}

// ============================================================
// Data Management Endpoints
// ============================================================

/**
 * Delete all events from the database
 */
export async function deleteAllEvents(): Promise<{ deleted: number; message: string }> {
  const res = await apiClient.delete('/events');
  return res.data;
}

/**
 * Delete events by source name
 */
export async function deleteEventsBySource(sourceName: string): Promise<{ deleted: number; message: string }> {
  const res = await apiClient.delete(`/events/source/${encodeURIComponent(sourceName)}`);
  return res.data;
}

/**
 * List all event sources with counts
 */
export async function listEventSources(): Promise<Array<{ source: string; count:  number }>> {
  const res = await apiClient.get('/events/sources');
  return res.data;
}

/**
 * Clear all ingestion logs
 */
export async function clearIngestionLogs(): Promise<{ deleted:  number; message: string }> {
  const res = await apiClient.delete('/mapping/ingestion-logs');
  return res.data;
}

/**
 * Delete all workbench views
 */
export async function deleteAllViews(): Promise<{ deleted: number; message:  string }> {
  const res = await apiClient.delete('/workbench/views');
  return res.data;
}

/**
 * Reset workbench state
 */
export async function resetWorkbench(): Promise<{ message: string }> {
  const res = await apiClient.post('/workbench/reset');
  return res.data;
}

/**
 * Get workbench status
 */
export async function getWorkbenchStatus(): Promise<{
  views_count: number;
  materialized_count: number;
  database_events: number;
  status: string;
}> {
  const res = await apiClient.get('/workbench/status');
  return res.data;
}

// ============================================================
// Cloud Mode API
// ============================================================

export interface CloudConfig {
  mode: 'local' | 'cloud';
  gateways: any[];
  databricks_connections: any[];
  storage_connections: any[];
}

export interface SchedulerJob {
  name: string;
  interval_seconds: number;
  category: string;
  enabled: boolean;
  is_async: boolean;
  last_run: string | null;
  last_status: string | null;
  last_duration_ms: number | null;
  last_error: string | null;
  run_count: number;
  error_count: number;
}

export interface SchedulerStatus {
  running: boolean;
  jobs: SchedulerJob[];
  job_count: number;
}

export interface CloudHealthStatus {
  mode: string;
  config_loaded: boolean;
  databricks_connections: number;
  storage_connections: number;
  gateways: number;
  scheduler_running: boolean;
  recent_audit: any[];
}

// ── Cloud Config ──────────────────────────────────────

export async function fetchCloudConfig(): Promise<CloudConfig> {
  const { data } = await apiClient.get('/cloud/config');
  return data;
}

export async function updateCloudConfig(config: Partial<CloudConfig>): Promise<CloudConfig> {
  const { data } = await apiClient.put('/cloud/config', config);
  return data;
}

export async function setCloudMode(mode: 'local' | 'cloud'): Promise<{ mode: string }> {
  const { data } = await apiClient.post('/cloud/mode', { mode });
  return data;
}

// ── Scheduler ─────────────────────────────────────────

export async function fetchSchedulerStatus(): Promise<SchedulerStatus> {
  try {
    const { data } = await apiClient.get('/cloud/scheduler');
    return data;
  } catch {
    return { running: false, jobs: [], job_count: 0 };
  }
}

export async function startScheduler(): Promise<{ status: string }> {
  const { data } = await apiClient.post('/cloud/scheduler/start');
  return data;
}

export async function stopScheduler(): Promise<{ status: string }> {
  const { data } = await apiClient.post('/cloud/scheduler/stop');
  return data;
}

export async function runJobNow(jobName: string): Promise<SchedulerJob> {
  const { data } = await apiClient.post(`/cloud/scheduler/jobs/${jobName}/run`);
  return data;
}

export async function enableJob(jobName: string): Promise<{ status: string }> {
  const { data } = await apiClient.post(`/cloud/scheduler/jobs/${jobName}/enable`);
  return data;
}

export async function disableJob(jobName: string): Promise<{ status: string }> {
  const { data } = await apiClient.post(`/cloud/scheduler/jobs/${jobName}/disable`);
  return data;
}

// ── Cloud Health ──────────────────────────────────────

export async function fetchCloudHealth(): Promise<CloudHealthStatus> {
  try {
    const { data } = await apiClient.get('/cloud/health');
    return data;
  } catch {
    return {
      mode: 'local',
      config_loaded: false,
      databricks_connections: 0,
      storage_connections: 0,
      gateways: 0,
      scheduler_running: false,
      recent_audit: [],
    };
  }
}

// ── Cloud Providers ───────────────────────────────────

export async function fetchCloudProviders(): Promise<{ mode: string; providers: any[] }> {
  try {
    const { data } = await apiClient.get('/cloud/providers');
    return data;
  } catch {
    return { mode: 'local', providers: [] };
  }
}

// ── Test Connection ───────────────────────────────────

export async function testCloudConnection(
  connectionType: 'databricks' | 'storage',
  connectionName: string
): Promise<any> {
  const { data } = await apiClient.post('/cloud/test-connection', {
    connection_type: connectionType,
    connection_name: connectionName,
  });
  return data;
}

