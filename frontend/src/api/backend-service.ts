import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

/** 跨模块数据流：获取全平台领域快照 */
export async function loadNodeStats(module?: string): Promise<Record<string, unknown>> {
  const query = module ? `?module=${module}` : ''
  const response = await request(`/api/nodestats${query}`)
  if (!response.ok) throw new Error('节点统计读取失败')
  return await response.json()
}

/** 跨模块数据流：获取指定 topic 的通道消息 */
export async function loadChannelSnapshot(topic: string): Promise<Record<string, unknown>> {
  const response = await request(`/api/channel/${topic}`)
  if (!response.ok) throw new Error('通道快照读取失败')
  return await response.json()
}

/** 跨模块数据流：查询脏数据隔离区 */
export async function loadDirtyVault(source?: string): Promise<Record<string, unknown>> {
  const query = source ? `?source=${source}` : ''
  const response = await request(`/api/dirty-vault${query}`)
  if (!response.ok) throw new Error('脏数据查询失败')
  return await response.json()
}

/** 跨模块数据流：查询采集记录 */
export async function loadCollectRecords(batchRef?: string): Promise<Record<string, unknown>> {
  const query = batchRef ? `?batch_ref=${batchRef}` : ''
  const response = await request(`/api/collect-records${query}`)
  if (!response.ok) throw new Error('采集记录查询失败')
  return await response.json()
}

/** 跨模块数据流：执行RPA全生命周期管道 */
export async function triggerPipeline(lifecycle: string): Promise<Record<string, unknown>> {
  const response = await request(`/api/pipeline/${lifecycle}`, {
    method: 'POST',
  })
  if (!response.ok) throw new Error('管道执行失败')
  return await response.json()
}

/** 跨模块数据流：获取失败恢复状态 */
export async function loadRecoveryStats(): Promise<Record<string, unknown>> {
  const response = await request('/api/recovery')
  if (!response.ok) throw new Error('恢复状态查询失败')
  return await response.json()
}

// ---- 页面建模：分页续做工作台 ----

export type PaginationVersion = {
  version: number
  page_no: number
  fingerprint: string
  row_count: number
  captured_at: string
}

export type PaginationModelView = {
  model_id: number
  model_name: string
  status: string
  site: string
  pagination_mode: string
  boundary_policy: string
  page_size: number
  revision: number
  pagination_version: string
  covered_pages: number[]
  next_page: number
  seen_count: number
  versions: Record<string, PaginationVersion[]>
  uncovered: boolean
  interrupted: { reason: string; page_no: number | null; at: string } | null
  deferred_pages: number[]
  updated_at: string
}

export type ResumeOverview = {
  sites: {
    site: string
    model_count: number
    covered_pages: number[]
    seen_rows: number
    next_page: number
  }[]
  models: PaginationModelView[]
  summary: { model_count: number; covered_pages: number; seen_rows: number }
}

export type PaginationConfig = {
  pagination_mode: string
  page_size: number
  total_pages: number | null
  boundary_policy: string
  revision?: number
}

export type FieldMapping = {
  source_field: string
  target_field: string
  data_type?: string
  format?: string
}

export type FieldMappingEntry = {
  id: number
  model_id: number
  model_name: string
  site: string
  revision: number
  note: string
  mappings: FieldMapping[]
  created_at: string
}

/** 按站点查看分页状态、已覆盖页和已见内容数量 */
export async function loadPaginationOverview(site?: string): Promise<ResumeOverview> {
  const query = site ? `?site=${encodeURIComponent(site)}` : ''
  const response = await request(`/api/page_model/resume${query}`)
  if (!response.ok) throw new Error('分页状态读取失败')
  return await response.json()
}

/** 保存页面模型的分页配置（不改动既有续做边界） */
export async function savePaginationConfig(
  modelId: number,
  config: PaginationConfig,
): Promise<ActionResult & { entry?: unknown }> {
  const response = await request(`/api/page_model/${modelId}/pagination`, {
    method: 'POST',
    body: JSON.stringify(config),
  })
  return await response.json()
}

/** 继续未覆盖页：重开页面不会重复累计同一行 */
export async function resumePagination(
  modelId: number,
  payload: { handle_ref?: string; max_pages?: number; observed_last_page?: number | null } = {},
): Promise<Record<string, unknown>> {
  const response = await request(`/api/page_model/${modelId}/resume`, {
    method: 'POST',
    body: JSON.stringify(payload),
  })
  if (!response.ok) throw new Error('续做未覆盖页失败')
  return await response.json()
}

/** 保存增量字段映射明细 */
export async function saveFieldMappings(
  modelId: number,
  mappings: FieldMapping[],
  options: { revision?: number; note?: string } = {},
): Promise<ActionResult> {
  const response = await request('/api/page_model/mappings', {
    method: 'POST',
    body: JSON.stringify({
      model_id: modelId,
      revision: options.revision ?? null,
      note: options.note ?? '',
      mappings,
    }),
  })
  return await response.json()
}

/** 读取增量字段映射采集明细 */
export async function loadFieldMappings(
  modelId?: number,
): Promise<{ count: number; fields: number; entries: FieldMappingEntry[] }> {
  const query = modelId ? `?model_id=${modelId}` : ''
  const response = await request(`/api/page_model/mappings${query}`)
  if (!response.ok) throw new Error('字段映射明细读取失败')
  return await response.json()
}

/** 核对重开页面不会重复累计同一行数据 */
export async function checkDuplicateAccumulation(): Promise<Record<string, unknown>> {
  const response = await request('/api/data_extract/duplicate-check')
  if (!response.ok) throw new Error('重复累计核对失败')
  return await response.json()
}
