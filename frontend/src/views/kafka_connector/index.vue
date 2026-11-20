<template>
  <section class="page" data-module="kafka_connector">
    <header class="page-head">
      <div>
        <h2>消息接入管理</h2>
        <p class="page-desc">维护Kafka消费端的连接、偏移与反序列化配置，为消息接入提供可验证的数据工程流程。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记接入配置</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出消息接入清单</button>
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
            <button class="link" type="button" :data-testid="`threshold-${row.id}`" @click="openThreshold(row)">修改阈值</button>
            <button class="link" type="button" :data-testid="`snapshot-${row.id}`" @click="openSnapshot(row)">查看快照</button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无消息接入数据，可先登记接入配置</td>
        </tr>
      </tbody>
    </table>

    <div class="snapshot-panel" data-testid="snapshot-panel">
      <div class="snapshot-head">
        <div>
          <h3 class="snapshot-title">接收就绪数据快照</h3>
          <p class="snapshot-desc">生效判断与阻塞原因落在同一份快照里；主题上线版本须先于消费组生效版本被确认。</p>
        </div>
        <button class="btn" data-testid="resume-snapshots" type="button" @click="resumeSnapshots">恢复未完成快照</button>
      </div>
      <div class="snapshot-list">
        <div v-for="snap in snapshots" :key="String(snap.id)" class="snapshot-item" data-testid="snapshot-item">
          <span><span class="k">配置编号</span>{{ snap.config_code }}</span>
          <span><span class="k">消费组</span>{{ snap.consumer_group ?? '—' }}</span>
          <span><span class="k">配置版本</span>v{{ snap.config_version }}</span>
          <span><span class="k">关联主题</span>{{ snap.topic_code ?? '未绑定' }}</span>
          <span><span class="k">主题上线版本</span>{{ snap.topic_online_version ? `v${snap.topic_online_version}` : '—' }}</span>
          <span><span class="k">偏移阈值</span>{{ snap.offset_threshold ?? '—' }}</span>
          <span>
            <span class="k">生效判定</span>
            <em class="verdict" :class="`verdict-${snap.verdict}`">{{ snap.verdict }}</em>
          </span>
          <span><span class="k">首条消费位置</span>{{ snap.first_offset ?? '—' }}</span>
          <span><span class="k">数据版本</span>v{{ snap.data_version }}</span>
          <span><span class="k">已完成</span>{{ snap.completed ? '是' : '否' }}</span>
          <span class="blocked-text" style="grid-column: span 2;">
            <span class="k">阻塞原因</span>{{ snap.blocked_reason ?? '—' }}
          </span>
        </div>
        <p v-if="!snapshots.length" class="snapshot-empty">暂无接收就绪数据快照，登记接入配置后自动生成</p>
      </div>
    </div>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记接入配置</h3>
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

    <div v-if="showThreshold" class="modal-backdrop" data-testid="threshold-modal">
      <form class="modal" @submit.prevent="submitThreshold">
        <h3>修改偏移阈值（{{ thresholdRow?.['配置编号'] }}）</h3>
        <label class="create-field">
          <span>批量大小（偏移阈值）</span>
          <input v-model="thresholdValues['批量大小']" data-testid="threshold-批量大小" required />
        </label>
        <label class="create-field">
          <span>超时时间</span>
          <input v-model="thresholdValues['超时时间']" data-testid="threshold-超时时间" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showThreshold = false">取消</button>
          <button class="btn primary" data-testid="submit-threshold" type="submit" :disabled="savingThreshold">
            {{ savingThreshold ? '保存中…' : '保存并重核对' }}
          </button>
        </div>
      </form>
    </div>

    <div v-if="showSnapshot" class="modal-backdrop" data-testid="snapshot-modal">
      <div class="modal">
        <h3>接收就绪数据快照（{{ currentSnapshot?.config_code }}）</h3>
        <div v-if="currentSnapshot" class="detail-grid">
          <span><span class="k">消费组</span>{{ currentSnapshot.consumer_group ?? '—' }}</span>
          <span><span class="k">配置版本</span>v{{ currentSnapshot.config_version }}</span>
          <span><span class="k">关联主题</span>{{ currentSnapshot.topic_code ?? '未绑定' }}</span>
          <span><span class="k">主题范围</span>{{ currentSnapshot.topic_scope ?? '—' }}</span>
          <span><span class="k">主题上线版本</span>{{ currentSnapshot.topic_online_version ? `v${currentSnapshot.topic_online_version}` : '—' }}</span>
          <span><span class="k">偏移阈值</span>{{ currentSnapshot.offset_threshold ?? '—' }}（{{ currentSnapshot.threshold_check }}）</span>
          <span>
            <span class="k">生效判定</span>
            <em class="verdict" :class="`verdict-${currentSnapshot.verdict}`">{{ currentSnapshot.verdict }}</em>
          </span>
          <span><span class="k">首条消费位置</span>{{ currentSnapshot.first_offset ?? '—' }}</span>
          <span><span class="k">数据版本</span>v{{ currentSnapshot.data_version }}</span>
          <span><span class="k">最近触发</span>{{ currentSnapshot.last_trigger ?? '—' }}</span>
          <span class="blocked-text" style="grid-column: span 2;">
            <span class="k">阻塞原因</span>{{ currentSnapshot.blocked_reason ?? '—' }}
          </span>
        </div>
        <div class="page-actions" style="margin-top: 12px;">
          <button class="btn" type="button" @click="showSnapshot = false">关闭</button>
        </div>
      </div>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条消息接入记录</span>
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

const ENDPOINT = '/api/kafka_connector'
const columns = ["配置编号", "集群地址", "消费组名", "偏移策略", "反序列化器", "批量大小", "超时时间", "死信队列"]
const actions = ["提交校验", "标记生效", "停用配置"]
const statuses = ["草稿", "待校验", "已生效", "已停用"]
const metricNames = ["接入配置总数", "待校验配置", "已生效配置"]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const noticeMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const snapshots = ref<Snapshot[]>([])
const showThreshold = ref(false)
const savingThreshold = ref(false)
const thresholdRow = ref<Row | null>(null)
const thresholdValues = ref<Record<string, string>>({ 批量大小: '', 超时时间: '' })
const showSnapshot = ref(false)
const currentSnapshot = ref<Snapshot | null>(null)
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
    link.download = 'kafka_connector.csv'
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
    await reloadSnapshots()
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
      throw new Error(payload.message || '消息接入动作未生效，请稍后重试')
    }
    noticeMessage.value = payload.message || ''
    await reload()
    await reloadSnapshots()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '消息接入操作失败'
    await reloadSnapshots()
  }
}

function openThreshold(row: Row) {
  thresholdRow.value = row
  thresholdValues.value = {
    批量大小: String(row['批量大小'] ?? ''),
    超时时间: String(row['超时时间'] ?? ''),
  }
  errorMessage.value = ''
  showThreshold.value = true
}

async function submitThreshold() {
  if (!thresholdRow.value) return
  savingThreshold.value = true
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${thresholdRow.value.id}/threshold`, {
      method: 'PUT',
      body: JSON.stringify(thresholdValues.value),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '阈值修改失败')
    noticeMessage.value = payload.message || ''
    showThreshold.value = false
    await reload()
    await reloadSnapshots()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '阈值修改失败'
  } finally {
    savingThreshold.value = false
  }
}

async function openSnapshot(row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/snapshot`)
    if (!response.ok) throw new Error('接收就绪数据快照读取失败')
    currentSnapshot.value = await response.json()
    showSnapshot.value = true
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '接收就绪数据快照读取失败'
  }
}

async function resumeSnapshots() {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/snapshots/resume`, { method: 'POST' })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '快照恢复失败')
    noticeMessage.value = payload.message || ''
    await reload()
    await reloadSnapshots()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '快照恢复失败'
  }
}

async function reloadSnapshots() {
  try {
    const response = await request(`${ENDPOINT}/snapshots`)
    if (!response.ok) throw new Error('接收就绪数据快照读取失败')
    const payload = await response.json()
    snapshots.value = payload.items ?? []
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
      throw new Error('接入配置列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '消息接入列表读取失败'
  }
}

onMounted(() => {
  void reload()
  void reloadSnapshots()
})
</script>
