<template>
  <div>
    <div class="toolbar">
      <select v-model="tab" @change="onTab" style="width:200px">
        <option value="machine">支付状态机视图（按订单）</option>
        <option value="unlinked">待关联队列</option>
        <option value="conflicts">冲突待裁决</option>
        <option value="replays">隔离重放记录</option>
      </select>
      <div class="spacer"></div>
      <span class="muted">重放仅生成影子状态，write_back 恒为 false，不回写生产账本</span>
    </div>

    <!-- ================= 订单状态机视图 ================= -->
    <div v-if="tab === 'machine'">
      <div class="toolbar">
        <select v-model="orderRef" @change="loadOrder" style="width:220px">
          <option value="" disabled>选择订单…</option>
          <option v-for="r in orderRefs" :key="r" :value="r">{{ r }}</option>
        </select>
        <input v-model="orderInput" placeholder="或输入订单号 ORD-005" style="width:200px"
               @keyup.enter="loadManual" />
        <button class="primary" @click="loadManual">查询</button>
        <button v-if="orderRef" @click="replayOrder">隔离影子重放当前订单</button>
      </div>

      <div v-if="!orderView" class="card empty">请选择或输入订单号查看支付状态机</div>
      <div v-for="pay in orderView?.payments || []" :key="pay.payment_ref" class="card section-gap">
        <div class="card-title" style="display:flex;align-items:center;gap:10px">
          <span class="mono" style="font-size:14px;color:var(--text)">{{ pay.payment_ref }}</span>
          <span class="badge CONFIRMED">{{ pay.production_state.state }}</span>
          <span v-if="pay.pending_conflict" class="badge OPEN">存在待裁决冲突</span>
          <span class="muted mono" style="font-weight:400">渠道参考 {{ pay.channel_ref || '—' }}</span>
        </div>

        <div class="grid cols-4" style="margin-bottom:14px">
          <div><div class="muted" style="font-size:12px">授权</div>
            <div class="num">{{ fmt(pay.production_state.authorized_amount) }}</div></div>
          <div><div class="muted" style="font-size:12px">捕获</div>
            <div class="num">{{ fmt(pay.production_state.captured_amount) }}</div></div>
          <div><div class="muted" style="font-size:12px">退款</div>
            <div class="num">{{ fmt(pay.production_state.refunded_amount) }}</div></div>
          <div><div class="muted" style="font-size:12px">拒付</div>
            <div class="num">{{ fmt(pay.production_state.chargeback_amount) }}</div></div>
        </div>

        <div class="card-title">事件接受 / 拒绝时间线（按业务时间推导，非到达顺序）</div>
        <ul class="timeline">
          <li v-for="d in pay.decisions" :key="d.id">
            <div>
              <span class="badge" :class="decisionClass(d.decision)">{{ decisionLabel(d.decision) }}</span>
              <b style="margin-left:6px">{{ d.event_type }}</b>
              <span class="badge PROPOSED" style="margin-left:6px">{{ d.source }}</span>
              <span v-if="d.to_state" class="muted mono" style="margin-left:6px;font-size:11.5px">
                {{ d.from_state || '∅' }} → {{ d.to_state }}
              </span>
            </div>
            <div class="t-meta mono">{{ fmtTime(d.event_time) }} · 业务事件#{{ d.business_event_id || '—' }}</div>
            <div v-if="d.reason_detail" class="soft" style="font-size:12px;margin-top:2px">
              <span class="mono">{{ d.reason_code }}</span>：{{ d.reason_detail }}
            </div>
          </li>
          <li v-if="!pay.decisions.length" class="muted">暂无状态机决策</li>
        </ul>
      </div>
    </div>

    <!-- ================= 待关联队列 ================= -->
    <div v-if="tab === 'unlinked'" class="card">
      <div class="table-wrap">
        <table class="data">
          <thead><tr><th>#</th><th>来源</th><th>事件</th><th>关联线索</th>
            <th>业务时间</th><th>无法关联原因</th><th>状态</th><th></th></tr></thead>
          <tbody>
            <tr v-for="u in unlinked" :key="u.id">
              <td class="mono muted">{{ u.id }}</td>
              <td><span class="badge PROPOSED">{{ u.source }}</span></td>
              <td><b>{{ u.event_type }}</b></td>
              <td class="mono" style="font-size:11.5px">
                order={{ u.order_ref_hint || '—' }}<br/>
                ref={{ u.payment_ref_hint || '—' }}<br/>
                ch={{ u.channel_ref_hint || '—' }}
              </td>
              <td class="mono">{{ fmtTime(u.event_time) }}</td>
              <td class="neg" style="font-size:11.5px">{{ reasonLabel(u.reason_code) }}</td>
              <td><span class="badge" :class="u.status === 'PENDING' ? 'OPEN' : 'CLOSED'">{{ u.status }}</span></td>
              <td style="white-space:nowrap">
                <button class="sm" @click="replayUnlinked(u)">隔离重放</button>
                <button class="sm primary" :disabled="u.status !== 'PENDING'" @click="openLink(u)">人工关联</button>
                <button class="sm danger" :disabled="u.status !== 'PENDING'" @click="discardUnlinked(u)">丢弃</button>
              </td>
            </tr>
          </tbody>
        </table>
        <div v-if="!unlinked.length" class="empty">没有待关联事件</div>
      </div>
    </div>

    <!-- ================= 冲突裁决 ================= -->
    <div v-if="tab === 'conflicts'" class="card">
      <div class="table-wrap">
        <table class="data">
          <thead><tr><th>#</th><th>支付</th><th>类型</th><th>业务时间</th><th>互斥事件</th>
            <th>状态</th><th></th></tr></thead>
          <tbody>
            <tr v-for="c in conflicts" :key="c.id">
              <td class="mono muted">{{ c.id }}</td>
              <td class="mono">{{ c.payment_ref || c.order_ref }}</td>
              <td><span class="badge UNKNOWN">{{ c.conflict_type }}</span></td>
              <td class="mono">{{ fmtTime(c.event_time) }}</td>
              <td class="mono" style="font-size:11.5px">
                BE#{{ c.detail.existing_event_id }} {{ c.detail.existing_type }}
                <b> ⟷ </b>
                BE#{{ c.detail.incoming_event_id }} {{ c.detail.incoming_type }}
              </td>
              <td><span class="badge" :class="c.status === 'PENDING' ? 'OPEN' : 'CLOSED'">{{ c.status }}</span></td>
              <td style="white-space:nowrap">
                <button class="sm primary" :disabled="c.status !== 'PENDING'" @click="openResolve(c)">裁决</button>
                <button class="sm danger" :disabled="c.status !== 'PENDING'" @click="discardConflict(c)">作废</button>
              </td>
            </tr>
          </tbody>
        </table>
        <div v-if="!conflicts.length" class="empty">没有待裁决冲突</div>
      </div>
    </div>

    <!-- ================= 重放记录 ================= -->
    <div v-if="tab === 'replays'" class="card">
      <div class="table-wrap">
        <table class="data">
          <thead><tr><th>#</th><th>运行号</th><th>范围</th><th>结论</th><th>步骤</th>
            <th>回写生产</th><th>操作人</th><th>时间</th><th></th></tr></thead>
          <tbody>
            <tr v-for="r in replays" :key="r.id">
              <td class="mono muted">{{ r.id }}</td>
              <td class="mono">{{ r.run_ref }}</td>
              <td>{{ r.scope_type }} {{ r.scope_ref }}</td>
              <td><span class="badge" :class="outcomeClass(r.outcome)">{{ outcomeLabel(r.outcome) }}</span></td>
              <td class="num">{{ r.step_count }}</td>
              <td><span class="badge CLOSED">{{ r.write_back ? '是' : '否（影子）' }}</span></td>
              <td>{{ r.operator }}</td>
              <td class="mono">{{ fmtTime(r.created_at) }}</td>
              <td><button class="sm" @click="openReplay(r)">详情对比</button></td>
            </tr>
          </tbody>
        </table>
        <div v-if="!replays.length" class="empty">暂无重放记录</div>
      </div>
    </div>

    <!-- ========== 弹窗：影子重放对比 ========== -->
    <div v-if="replayResult" class="modal-mask" @click.self="replayResult = null">
      <div class="modal" style="width:720px">
        <h3>隔离影子重放结果
          <span class="badge" :class="outcomeClass(replayResult.outcome)" style="margin-left:8px">
            {{ outcomeLabel(replayResult.outcome) }}
          </span>
        </h3>
        <p class="muted" style="font-size:12.5px">
          {{ replayResult.run_ref }} · 模式 {{ replayResult.mode }} ·
          write_back = <b class="pos">{{ replayResult.write_back }}</b>（false 表示生产账本零改动）
        </p>
        <div class="card-title">生产态 vs 影子态</div>
        <pre class="json">{{ JSON.stringify({
          production: replayResult.production_state,
          shadow: replayResult.shadow_state,
          divergence: replayResult.divergence_detail,
        }, null, 2) }}</pre>
        <div v-if="replaySteps.length" class="section-gap">
          <div class="card-title">影子步骤（SHADOW 决策，不进生产）</div>
          <ul class="timeline">
            <li v-for="s in replaySteps" :key="s.id">
              <span class="badge PROPOSED">{{ s.decision }}</span>
              <b style="margin-left:6px">{{ s.event_type }}</b>
              <span v-if="s.reason_code" class="soft mono" style="font-size:11.5px"> · {{ s.reason_code }}</span>
              <div class="t-meta mono">{{ fmtTime(s.event_time) }} · {{ s.from_state || '∅' }} → {{ s.to_state || '∅' }}</div>
            </li>
          </ul>
        </div>
        <div class="modal-actions"><button @click="replayResult = null">关闭</button></div>
      </div>
    </div>

    <!-- ========== 弹窗：人工关联 ========== -->
    <div v-if="linkTarget" class="modal-mask" @click.self="linkTarget = null">
      <div class="modal">
        <h3>人工关联待关联事件 #{{ linkTarget.id }}</h3>
        <pre class="json">{{ JSON.stringify(linkTarget.payload, null, 2) }}</pre>
        <label class="field"><span>归属订单号 *</span><input v-model="linkForm.order_ref" /></label>
        <label class="field"><span>支付/退款实体号（可选）</span><input v-model="linkForm.payment_ref" /></label>
        <label class="field"><span>处理人</span><input v-model="linkForm.operator" /></label>
        <label class="field"><span>说明</span><input v-model="linkForm.comment" /></label>
        <div class="modal-actions">
          <button @click="linkTarget = null">取消</button>
          <button class="primary" @click="submitLink">重新提交状态机并入账</button>
        </div>
      </div>
    </div>

    <!-- ========== 弹窗：冲突裁决 ========== -->
    <div v-if="resolveTarget" class="modal-mask" @click.self="resolveTarget = null">
      <div class="modal">
        <h3>冲突裁决 #{{ resolveTarget.id }}</h3>
        <p class="soft" style="font-size:12.5px">{{ resolveTarget.title }}</p>
        <label class="field"><span>选择胜出事件（落败者业务记录保留但不生效）</span>
          <select v-model="resolveForm.winning_event_id">
            <option :value="resolveTarget.detail.existing_event_id">
              BE#{{ resolveTarget.detail.existing_event_id }} {{ resolveTarget.detail.existing_type }}（已先到）
            </option>
            <option :value="resolveTarget.detail.incoming_event_id">
              BE#{{ resolveTarget.detail.incoming_event_id }} {{ resolveTarget.detail.incoming_type }}（后到）
            </option>
          </select>
        </label>
        <label class="field"><span>处理人</span><input v-model="resolveForm.operator" /></label>
        <label class="field"><span>裁决依据</span><input v-model="resolveForm.comment" /></label>
        <div class="modal-actions">
          <button @click="resolveTarget = null">取消</button>
          <button class="primary danger" @click="submitResolve">确认裁决</button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { onMounted, reactive, ref } from 'vue'
import api from '../api'

const tab = ref('machine')
const orderRefs = ref([])
const orderRef = ref('')
const orderInput = ref('')
const orderView = ref(null)
const unlinked = ref([])
const conflicts = ref([])
const replays = ref([])
const replayResult = ref(null)
const replaySteps = ref([])
const linkTarget = ref(null)
const resolveTarget = ref(null)
const linkForm = reactive({ operator: 'finance.chen', order_ref: '', payment_ref: '', comment: '' })
const resolveForm = reactive({ operator: 'finance.chen', winning_event_id: null, comment: '' })

const fmt = (v) => Number(v || 0).toLocaleString('zh-CN', { minimumFractionDigits: 2 })
const fmtTime = (t) => (t ? new Date(t).toLocaleString('zh-CN') : '')

const decisionLabel = (d) => ({
  ACCEPTED: '接受', REJECTED: '拒绝', DUPLICATE: '重复',
  UNLINKED: '待关联', CONFLICT: '冲突', SHADOW: '影子',
}[d] || d)
const decisionClass = (d) => ({
  ACCEPTED: 'CLOSED', REJECTED: 'OPEN', DUPLICATE: 'SUPERSEDED',
  UNLINKED: 'CLAIMED', CONFLICT: 'UNKNOWN', SHADOW: 'PROPOSED',
}[d] || 'PROPOSED')
const reasonLabel = (r) => ({
  UNLINKED_NO_ORDER_OR_PAYMENT: '缺少订单号/支付号/渠道参考，无法关联',
  UNLINKED_UNKNOWN_ORDER: '引用的订单在系统中不存在',
}[r] || r)
const outcomeLabel = (o) => ({ MATCHED: '一致', DIVERGED: '有分歧', BLOCKED: '受阻' }[o] || o)
const outcomeClass = (o) => ({ MATCHED: 'CLOSED', DIVERGED: 'CLAIMED', BLOCKED: 'OPEN' }[o] || 'PROPOSED')

function toast(msg, ok) {
  const el = document.createElement('div')
  el.className = `toast ${ok ? 'ok' : 'err'}`
  el.textContent = msg
  document.body.appendChild(el)
  setTimeout(() => el.remove(), 3200)
}

async function onTab() {
  if (tab.value === 'unlinked') await loadUnlinked()
  else if (tab.value === 'conflicts') await loadConflicts()
  else if (tab.value === 'replays') await loadReplays()
  else await loadOrders()
}

async function loadOrders() {
  const data = await api.smOrders()
  orderRefs.value = data.order_refs
  if (!orderRef.value && orderRefs.value.length) {
    orderRef.value = orderRefs.value[0]
    await loadOrder()
  }
}
async function loadOrder() {
  if (!orderRef.value) return
  orderView.value = await api.smOrder(orderRef.value)
}
async function loadManual() {
  const ref = (orderInput.value || orderRef.value || '').trim()
  if (!ref) return
  orderRef.value = ref
  await loadOrder()
}

async function replayOrder() {
  try {
    const run = await api.smReplayOrder(orderRef.value, 'finance.chen')
    await showReplay(run.id)
    await loadReplays()
  } catch (e) { toast(e.message, false) }
}
async function replayUnlinked(u) {
  try {
    const run = await api.smReplayUnlinked(u.id, 'finance.chen')
    await showReplay(run.id)
    await loadReplays()
  } catch (e) { toast(e.message, false) }
}
async function showReplay(runId) {
  const detail = await api.smReplay(runId)
  replayResult.value = detail
  replaySteps.value = detail.steps || []
}
async function openReplay(r) { await showReplay(r.id) }

async function loadUnlinked() { unlinked.value = await api.smUnlinked('PENDING') }
function openLink(u) {
  linkTarget.value = u
  Object.assign(linkForm, {
    operator: 'finance.chen', order_ref: u.order_ref_hint || '',
    payment_ref: u.payment_ref_hint || '', comment: '',
  })
}
async function submitLink() {
  if (!linkForm.order_ref) return toast('请填写归属订单号', false)
  try {
    await api.smLink(linkTarget.value.id, linkForm.operator,
      linkForm.order_ref, linkForm.payment_ref || null, linkForm.comment)
    toast('已重新提交状态机并入账', true)
    linkTarget.value = null
    await loadUnlinked()
  } catch (e) { toast(e.message, false) }
}
async function discardUnlinked(u) {
  try {
    await api.smDiscardUnlinked(u.id, 'finance.chen', '人工判定为无效事件')
    toast('已丢弃（原始证据保留）', true)
    await loadUnlinked()
  } catch (e) { toast(e.message, false) }
}

async function loadConflicts() { conflicts.value = await api.smConflicts('PENDING') }
function openResolve(c) {
  resolveTarget.value = c
  Object.assign(resolveForm, {
    operator: 'finance.chen',
    winning_event_id: c.detail.existing_event_id,
    comment: '',
  })
}
async function submitResolve() {
  try {
    await api.smResolveConflict(resolveTarget.value.id, resolveForm.operator,
      resolveForm.winning_event_id, resolveForm.comment)
    toast('裁决完成，两条业务记录均已保留', true)
    resolveTarget.value = null
    await loadConflicts()
    await loadOrder()
  } catch (e) { toast(e.message, false) }
}
async function discardConflict(c) {
  try {
    await api.smDiscardConflict(c.id, 'finance.chen', '人工作废该冲突组')
    toast('冲突组已作废', true)
    await loadConflicts()
  } catch (e) { toast(e.message, false) }
}

async function loadReplays() { replays.value = await api.smReplays() }

onMounted(loadOrders)
</script>
