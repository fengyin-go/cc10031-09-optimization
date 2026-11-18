<template>
  <section class="pagination-panel" data-testid="pagination-workbench">
    <header class="panel-head">
      <div>
        <h3>分页续做工作台</h3>
        <p class="page-desc">
          已见内容指纹与分页游标一起保存；续做只扩展未覆盖页，重开页面不会重复累计同一行。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" type="button" data-testid="pagination-refresh" @click="reload">
          刷新分页状态
        </button>
        <button class="btn" type="button" data-testid="pagination-duplicate-check" @click="verifyDuplicate">
          核对重复累计
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">页面模型数</span>
        <strong class="stat-value">{{ overview?.summary.model_count ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">已覆盖页</span>
        <strong class="stat-value">{{ overview?.summary.covered_pages ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">已见内容</span>
        <strong class="stat-value">{{ overview?.summary.seen_rows ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">重复入库</span>
        <strong class="stat-value">{{ duplicateCheck?.duplicated_rows ?? 0 }}</strong>
      </article>
    </div>

    <section v-if="overview?.sites.length" class="site-strip">
      <button
        v-for="bucket in overview.sites"
        :key="bucket.site"
        type="button"
        class="site-chip"
        :class="{ active: activeSite === bucket.site }"
        :data-testid="`pagination-site-${bucket.site}`"
        @click="selectSite(bucket.site)"
      >
        <strong>{{ bucket.site }}</strong>
        <span>覆盖 {{ bucket.covered_pages.join('、') || '—' }}</span>
        <span>待续第 {{ bucket.next_page }} 页 · 已见 {{ bucket.seen_rows }}</span>
      </button>
    </section>

    <table class="data-table">
      <thead>
        <tr>
          <th>页面模型</th>
          <th>站点</th>
          <th>分页模式</th>
          <th>边界策略</th>
          <th>分页版本</th>
          <th>已覆盖页</th>
          <th>下一页</th>
          <th>已见内容</th>
          <th>状态</th>
          <th>续做</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="model in visibleModels"
          :key="model.model_id"
          :data-testid="`pagination-row-${model.model_id}`"
        >
          <td>{{ model.model_name }}</td>
          <td>{{ model.site }}</td>
          <td>{{ model.pagination_mode }} · {{ model.page_size }} 条/页</td>
          <td>{{ boundaryLabel(model.boundary_policy) }}</td>
          <td class="mono">{{ model.pagination_version }}</td>
          <td>{{ model.covered_pages.join('、') || '—' }}</td>
          <td>{{ model.next_page }}</td>
          <td>{{ model.seen_count }}</td>
          <td>
            <span v-if="model.interrupted" class="tag warn">已中断：{{ model.interrupted.reason }}</span>
            <span v-else-if="model.uncovered" class="tag">待续做</span>
            <span v-else class="tag ok">已续做完成</span>
            <span v-if="model.deferred_pages.length" class="tag warn">
              挂起页 {{ model.deferred_pages.join('、') }}
            </span>
          </td>
          <td class="row-actions">
            <button
              class="link"
              type="button"
              :data-testid="`pagination-resume-${model.model_id}`"
              :disabled="busy"
              @click="resume(model)"
            >
              继续未覆盖页
            </button>
            <button
              class="link"
              type="button"
              :data-testid="`pagination-detail-${model.model_id}`"
              @click="openDetail(model)"
            >
              版本明细
            </button>
          </td>
        </tr>
        <tr v-if="!visibleModels.length">
          <td colspan="10" class="empty-state">暂无页面模型分页数据</td>
        </tr>
      </tbody>
    </table>

    <p v-if="panelMessage" class="panel-message" data-testid="pagination-message">{{ panelMessage }}</p>
    <p v-if="panelError" class="error-text" data-testid="pagination-error">{{ panelError }}</p>

    <details v-if="detailModel" class="detail-block" open>
      <summary>{{ detailModel.model_name }} · 分页版本明细</summary>
      <div class="detail-grid">
        <div>
          <h4>页码版本</h4>
          <ul class="version-list">
            <li v-for="(versions, page) in detailModel.versions" :key="page">
              第 {{ page }} 页：
              <span v-for="version in versions" :key="version.version" class="tag">
                v{{ version.version }} · {{ version.fingerprint }} · {{ version.row_count }} 行
              </span>
              <span v-if="!versions.length" class="muted">尚未采集</span>
            </li>
            <li v-if="!Object.keys(detailModel.versions).length" class="muted">尚未采集任何页面</li>
          </ul>
        </div>
        <div>
          <h4>增量字段映射明细</h4>
          <form class="mapping-form" @submit.prevent="addMapping">
            <label class="mapping-field">
              <span>源字段</span>
              <input v-model="mappingDraft.source_field" :data-testid="'mapping-source'" placeholder="如 标题" />
            </label>
            <label class="mapping-field">
              <span>目标字段</span>
              <input v-model="mappingDraft.target_field" :data-testid="'mapping-target'" placeholder="如 title" />
            </label>
            <label class="mapping-field">
              <span>数据类型</span>
              <input v-model="mappingDraft.data_type" placeholder="string / number" />
            </label>
            <button class="btn" type="submit" data-testid="mapping-add">加入明细</button>
          </form>
          <table class="data-table compact">
            <thead>
              <tr><th>源字段</th><th>目标字段</th><th>类型</th><th></th></tr>
            </thead>
            <tbody>
              <tr v-for="(item, index) in mappingDraftList" :key="`${item.source_field}-${index}`">
                <td>{{ item.source_field }}</td>
                <td>{{ item.target_field }}</td>
                <td>{{ item.data_type || 'string' }}</td>
                <td><button class="link" type="button" @click="mappingDraftList.splice(index, 1)">移除</button></td>
              </tr>
              <tr v-if="!mappingDraftList.length">
                <td colspan="4" class="empty-state">暂无待保存映射</td>
              </tr>
            </tbody>
          </table>
          <div class="page-actions">
            <button class="btn primary" type="button" data-testid="mapping-save" :disabled="busy" @click="saveMappings">
              保存增量字段映射明细
            </button>
          </div>
          <ul class="mapping-history">
            <li v-for="entry in mappingEntries" :key="entry.id">
              #{{ entry.id }} · 修订 {{ entry.revision }} · {{ entry.mappings.length }} 个字段
              <span class="muted">{{ entry.created_at }}</span>
            </li>
            <li v-if="!mappingEntries.length" class="muted">还没有保存过映射明细</li>
          </ul>
        </div>
      </div>
    </details>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  checkDuplicateAccumulation,
  loadFieldMappings,
  loadPaginationOverview,
  resumePagination,
  saveFieldMappings,
  type FieldMapping,
  type FieldMappingEntry,
  type PaginationModelView,
  type ResumeOverview,
} from '@/api/backend-service'

const overview = ref<ResumeOverview | null>(null)
const duplicateCheck = ref<Record<string, unknown> | null>(null)
const activeSite = ref('')
const busy = ref(false)
const panelMessage = ref('')
const panelError = ref('')
const detailModel = ref<PaginationModelView | null>(null)
const mappingEntries = ref<FieldMappingEntry[]>([])
const mappingDraft = ref<FieldMapping>({ source_field: '', target_field: '', data_type: 'string' })
const mappingDraftList = ref<FieldMapping[]>([])

const visibleModels = computed(() => {
  const models = overview.value?.models ?? []
  if (!activeSite.value) return models
  return models.filter((model) => model.site === activeSite.value)
})

const BOUNDARY_LABELS: Record<string, string> = {
  content_first: '按内容指纹追加版本',
  page_first: '页码推进后整页重抓',
  page_then_content: '页码优先，同页补差',
}

function boundaryLabel(policy: string): string {
  return BOUNDARY_LABELS[policy] ?? policy
}

function selectSite(site: string) {
  activeSite.value = activeSite.value === site ? '' : site
}

async function reload() {
  panelError.value = ''
  try {
    overview.value = await loadPaginationOverview()
  } catch (error) {
    panelError.value = error instanceof Error ? error.message : '分页状态读取失败'
  }
}

async function verifyDuplicate() {
  panelError.value = ''
  try {
    duplicateCheck.value = await checkDuplicateAccumulation()
    panelMessage.value = duplicateCheck.value?.duplicate_accumulation
      ? '发现重复累计，请检查游标与已见内容指纹'
      : '核对通过：重开页面没有重复累计同一行数据'
  } catch (error) {
    panelError.value = error instanceof Error ? error.message : '重复累计核对失败'
  }
}

async function resume(model: PaginationModelView) {
  busy.value = true
  panelError.value = ''
  panelMessage.value = ''
  try {
    const payload = await resumePagination(model.model_id, { max_pages: 5 })
    const extract = payload.extract as Record<string, unknown> | null
    const pages = (extract?.pages_covered as number[] | undefined) ?? []
    const extracted = Number(extract?.extracted ?? 0)
    const remainingRaw = (extract?.remaining_pages as number[] | undefined) ?? []
    const remaining = remainingRaw.length ? remainingRaw.join('、') : '无'
    panelMessage.value = `${String(payload.message ?? '续做完成')}：本次覆盖页 ${
      pages.join('、') || '无新增'
    }，新增 ${extracted} 条记录，剩余页 ${remaining}`
    await reload()
    if (detailModel.value?.model_id === model.model_id) {
      detailModel.value = overview.value?.models.find((item) => item.model_id === model.model_id) ?? null
    }
  } catch (error) {
    panelError.value = error instanceof Error ? error.message : '续做未覆盖页失败'
  } finally {
    busy.value = false
  }
}

async function openDetail(model: PaginationModelView) {
  detailModel.value = model
  mappingDraftList.value = []
  panelError.value = ''
  try {
    const detail = await loadFieldMappings(model.model_id)
    mappingEntries.value = detail.entries
  } catch (error) {
    panelError.value = error instanceof Error ? error.message : '字段映射明细读取失败'
  }
}

function addMapping() {
  const source = mappingDraft.value.source_field.trim()
  const target = mappingDraft.value.target_field.trim()
  if (!source || !target) {
    panelError.value = '源字段和目标字段都不能为空'
    return
  }
  panelError.value = ''
  mappingDraftList.value.push({
    source_field: source,
    target_field: target,
    data_type: mappingDraft.value.data_type || 'string',
  })
  mappingDraft.value = { source_field: '', target_field: '', data_type: 'string' }
}

async function saveMappings() {
  if (!detailModel.value) return
  if (!mappingDraftList.value.length) {
    panelError.value = '请先加入至少一条字段映射'
    return
  }
  busy.value = true
  panelError.value = ''
  try {
    const result = await saveFieldMappings(detailModel.value.model_id, mappingDraftList.value, {
      revision: detailModel.value.revision,
    })
    if (!result.ok) throw new Error(result.message)
    panelMessage.value = result.message
    mappingDraftList.value = []
    const detail = await loadFieldMappings(detailModel.value.model_id)
    mappingEntries.value = detail.entries
  } catch (error) {
    panelError.value = error instanceof Error ? error.message : '字段映射明细保存失败'
  } finally {
    busy.value = false
  }
}

onMounted(() => {
  void reload()
  void verifyDuplicate()
})
</script>
