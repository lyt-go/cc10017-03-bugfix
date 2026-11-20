<template>
  <section class="page" data-module="topic_registry">
    <header class="page-head">
      <div>
        <h2>主题注册管理</h2>
        <p class="page-desc">维护实时数据主题的消息结构、分区与保留策略注册，为主题注册提供可验证的数据工程流程。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记数据主题</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出主题注册清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无主题注册数据，可先登记数据主题</td>
        </tr>
      </tbody>
    </table>

    <div v-if="feedback" class="snapshot-panel" data-testid="feedback-panel">
      <div class="snapshot-head">
        <div>
          <h3 class="snapshot-title">快照回查：{{ feedback.topic_code }}（上线版本 v{{ feedback.online_version ?? '-' }}）</h3>
          <p class="snapshot-desc">主题动作已写入接收就绪数据快照，以下为消息接入写入的配置版本回查结果。</p>
        </div>
        <button class="btn ghost" type="button" @click="feedback = null">收起</button>
      </div>
      <div class="snapshot-list">
        <div v-for="snap in feedback.snapshots" :key="String(snap.config_code)" class="snapshot-item">
          <span><span class="k">配置编号</span>{{ snap.config_code }}</span>
          <span><span class="k">消费组</span>{{ snap.consumer_group ?? '—' }}</span>
          <span><span class="k">配置版本</span>v{{ snap.config_version }}</span>
          <span><span class="k">主题上线版本</span>{{ snap.topic_online_version ? `v${snap.topic_online_version}` : '—' }}</span>
          <span>
            <span class="k">生效判定</span>
            <em class="verdict" :class="`verdict-${snap.verdict}`">{{ snap.verdict }}</em>
          </span>
          <span><span class="k">首条消费位置</span>{{ snap.first_offset ?? '—' }}</span>
          <span><span class="k">数据版本</span>v{{ snap.data_version }}</span>
          <span class="blocked-text"><span class="k">阻塞原因</span>{{ snap.blocked_reason ?? '—' }}</span>
        </div>
        <p v-if="!feedback.snapshots.length" class="snapshot-empty">该主题暂无关联的消费组接收快照</p>
      </div>
    </div>

    <div class="snapshot-panel" data-testid="snapshot-sync-panel">
      <div class="snapshot-head">
        <div>
          <h3 class="snapshot-title">接收就绪数据快照同步</h3>
          <p class="snapshot-desc">与消息接入工作台读取同一份快照；刷新页面后数据保持一致。</p>
        </div>
      </div>
      <div class="snapshot-list">
        <div v-for="snap in boundSnapshots" :key="String(snap.id)" class="snapshot-item" data-testid="snapshot-sync-item">
          <span><span class="k">主题编号</span>{{ snap.topic_code }}</span>
          <span><span class="k">配置编号</span>{{ snap.config_code }}</span>
          <span><span class="k">配置版本</span>v{{ snap.config_version }}</span>
          <span><span class="k">主题上线版本</span>{{ snap.topic_online_version ? `v${snap.topic_online_version}` : '—' }}</span>
          <span>
            <span class="k">生效判定</span>
            <em class="verdict" :class="`verdict-${snap.verdict}`">{{ snap.verdict }}</em>
          </span>
          <span><span class="k">数据版本</span>v{{ snap.data_version }}</span>
          <span class="blocked-text" style="grid-column: span 2;">
            <span class="k">阻塞原因</span>{{ snap.blocked_reason ?? '—' }}
          </span>
        </div>
        <p v-if="!boundSnapshots.length" class="snapshot-empty">暂无绑定主题的接收就绪数据快照</p>
      </div>
    </div>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记数据主题</h3>
        <label v-for="field in columns" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条主题注册记录</span>
      <span v-if="noticeMessage" class="notice-text">{{ noticeMessage }}</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>
type Snapshot = Record<string, string | number | boolean | null>
type Feedback = { topic_code: string; online_version: number | null; snapshots: Snapshot[] }

const ENDPOINT = '/api/topic_registry'
const columns = ["主题编号", "主题名称", "消息格式", "序列化协议", "分区键列", "保留时长", "压缩类型", "Schema版本"]
const actions = ["提交校验", "确认上线", "标记下线"]
const statuses = ["草稿", "待校验", "已上线", "已下线"]
const metricNames = ["数据主题总数", "待校验主题", "已上线主题"]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const noticeMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const boundSnapshots = ref<Snapshot[]>([])
const feedback = ref<Feedback | null>(null)
const stats = computed(() => [
  { label: metricNames[0], value: total.value },
  { label: metricNames[1], value: rows.value.filter((row) => row.status === statuses[1]).length },
  { label: metricNames[2], value: rows.value.filter((row) => row.status === statuses[statuses.length - 1]).length },
])

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value).toString()
  try {
    const response = await request(`${ENDPOINT}/export?${query}`)
    if (!response.ok) throw new Error('导出失败，请稍后重试')
    const url = URL.createObjectURL(await response.blob())
    const link = document.createElement('a')
    link.href = url
    link.download = 'topic_registry.csv'
    link.click()
    URL.revokeObjectURL(url)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(columns.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const response = await request(ENDPOINT, {
      method: 'POST', body: JSON.stringify(createValues.value),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '登记失败')
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  noticeMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) {
      throw new Error(payload.message || '主题注册动作未生效，请稍后重试')
    }
    noticeMessage.value = payload.message || ''
    // 下线与重新上线各回查一次接收就绪数据快照
    if (payload.extra && Array.isArray(payload.extra.snapshots)) {
      feedback.value = payload.extra as Feedback
    }
    await reload()
    await reloadSnapshots()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '主题注册操作失败'
  }
}

async function reloadSnapshots() {
  try {
    const response = await request('/api/kafka_connector/snapshots')
    if (!response.ok) throw new Error('接收就绪数据快照读取失败')
    const payload = await response.json()
    boundSnapshots.value = (payload.items ?? []).filter((item: Snapshot) => item.topic_code)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '接收就绪数据快照读取失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('数据主题列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '主题注册列表读取失败'
  }
}

onMounted(() => {
  void reload()
  void reloadSnapshots()
})
</script>
