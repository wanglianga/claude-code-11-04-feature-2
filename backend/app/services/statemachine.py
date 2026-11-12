"""支付状态机：接受/拒绝判定、待关联队列、冲突待裁决与隔离影子重放。

设计要点（对应需求）：
  1. 按订单展示授权/捕获/退款/拒付/反转及 webhook/轮询来源事件的「接受或拒绝原因」——
     每条进入状态机的事件都在 event_decisions 留下 ACCEPTED/REJECTED/UNLINKED/CONFLICT 结论。
  2. 无法关联到任何订单/支付的事件不进账本，进入 unlinked_events 待关联队列，
     可用原始报文在隔离（SHADOW）模式重放当前状态。
  3. 乱序到达不按到达顺序覆盖状态：评估时把「已存事件 + 候选事件」合并后按业务时间
     event_time 整体重放，迟到的前置事件自动归位，绝不按到达顺序覆盖；只拒绝真正的业务
     不变量违规（如已捕获后再来更晚的授权/失败、退款超过捕获）。同业务时间的互斥事件无法
     自动定序，构成冲突——两条业务记录都保留并标记 PENDING 待人工裁决，互不覆盖状态。
  4. 重放只能生成影子状态（replay_runs + decision=SHADOW）供对比，write_back 恒为 False，
     不写入 business_events / ledger_entries / 任何生产投影。

状态推导核心 run_replay 是纯内存、无副作用的，生产摄入闸门与隔离影子重放共用同一套规则，
保证「同一套事件 → 同一个结论」，可对比、可复算。
"""
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..enums import (
    ConflictStatus, EventDecision as VD, EventType, PaymentState as PS,
    PAYMENT_LIFECYCLE_EVENTS, PAYMENT_STATE_DRIVING_EVENTS, RejectReason as RR,
    ReplayMode, ReplayOutcome, UnlinkedStatus,
)
from ..models import (
    BusinessEvent, EventDecision, OrderState, PaymentState, RawEvent,
    ReplayRun, StateConflict, UnlinkedEvent,
)
from .money import quantize


class StateMachineError(Exception):
    pass


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _amt(v) -> Decimal:
    return Decimal(str(v or "0"))


def _iso(t: datetime | None) -> str | None:
    return t.isoformat() if t else None


# ================================================================
# 纯内存重放引擎：无 DB、无副作用，生产评估与隔离重放共用
# ================================================================
@dataclass
class ShadowStep:
    event_type: str
    source: str
    event_time: datetime
    amount: Decimal
    verdict: str
    reason_code: str | None
    reason_detail: str | None
    from_state: str | None
    to_state: str | None
    out_of_order: bool = False
    business_event_id: int | None = None
    raw_event_id: int | None = None


@dataclass
class ReplayEvent:
    event_type: str
    amount: Decimal
    event_time: datetime
    source: str = ""
    business_event_id: int | None = None
    raw_event_id: int | None = None
    is_candidate: bool = False


@dataclass
class PaymentShadowMachine:
    """按业务时间重放单个支付尝试的生命周期，累计金额推导状态，绝不按到达顺序覆盖。"""
    payment_ref: str | None = None
    order_ref: str | None = None
    channel_ref: str | None = None
    currency: str = "CNY"
    created: bool = False
    state: str = PS.CREATED
    authorized: Decimal = Decimal("0")
    captured: Decimal = Decimal("0")
    refunded: Decimal = Decimal("0")
    chargeback: Decimal = Decimal("0")
    driving_event_time: datetime | None = None
    events_at_time: dict[str, set[str]] = field(default_factory=dict)
    history: list[ShadowStep] = field(default_factory=list)
    has_conflict: bool = False

    def snapshot(self) -> dict:
        return {
            "payment_ref": self.payment_ref,
            "order_ref": self.order_ref,
            "channel_ref": self.channel_ref,
            "state": self.state,
            "currency": self.currency,
            "authorized_amount": str(self.authorized),
            "captured_amount": str(self.captured),
            "refunded_amount": str(self.refunded),
            "chargeback_amount": str(self.chargeback),
            "driving_event_time": _iso(self.driving_event_time),
            "has_conflict": self.has_conflict,
        }

    def _derive_state(self) -> str:
        # 与生产投影 ledger._project_payment 完全一致的推导规则
        if self.chargeback > 0:
            return PS.CHARGEBACK
        if self.captured > 0 and self.refunded >= self.captured:
            return PS.REFUNDED
        if self.refunded > 0:
            return PS.PARTIALLY_REFUNDED
        if self.captured > 0:
            return PS.CAPTURED
        if self.authorized > 0:
            return PS.AUTHORIZED
        return PS.CREATED

    def apply(self, e: ReplayEvent) -> ShadowStep:
        """容忍式重放单条事件（事件已按业务时间排序）。只拒绝真正的不变量违规。"""
        et, amt, t = e.event_type, e.amount, e.event_time
        from_state = self.state

        def step(verdict, code=None, detail=None, to=None, ooo=False):
            s = ShadowStep(et, e.source, t, amt, verdict, code, detail,
                           from_state, self.state if to is None else to, ooo,
                           e.business_event_id, e.raw_event_id)
            self.history.append(s)
            return s

        if et not in PAYMENT_LIFECYCLE_EVENTS:
            return step(VD.REJECTED, RR.UNKNOWN_EVENT_TYPE, f"状态机不识别的事件类型 {et}")

        # PAYMENT_CREATED：幂等建态
        if et == EventType.PAYMENT_CREATED:
            if self.created:
                return step(VD.REJECTED, RR.DUP_BUSINESS_KEY, "支付已创建，重复创建事件不生效")
            self.created = True
            self.state = PS.CREATED
            self.driving_event_time = t
            self.events_at_time.setdefault(_iso(t), set()).add(et)
            return step(VD.ACCEPTED, to=self.state)

        # 容忍乱序前置：生命周期事件先于 CREATED 到达时，隐式建态（事实在业务时间点成立）
        if not self.created:
            self.created = True

        is_driving = et in PAYMENT_STATE_DRIVING_EVENTS

        # 同业务时间互斥的驱动事件 -> 冲突（两条都保留，待裁决，互不覆盖）
        same_time = self.events_at_time.get(_iso(t), set())
        if is_driving and same_time and et not in same_time:
            self.has_conflict = True
            return step(VD.CONFLICT, RR.CONFLICT_SAME_TIMESTAMP,
                        f"业务时间 {t} 已存在 {sorted(same_time)}，与 {et} 互斥，待人工裁决")

        # 真正的业务不变量违规（重放按时间排序，能走到这里说明是更晚时间的回退/越界，而非乱序）
        if et == EventType.PAYMENT_AUTHORIZED and self.captured > 0:
            return step(VD.REJECTED, RR.ILLEGAL_AUTH_AFTER_CAPTURE, "已捕获后不允许再次授权")
        if et == EventType.PAYMENT_AUTH_EXPIRED and self.captured > 0:
            return step(VD.REJECTED, RR.ILLEGAL_AUTH_EXPIRED_AFTER_CAPTURE,
                        "已捕获后授权过期通知无效")
        if et == EventType.PAYMENT_FAILED and self.captured > 0:
            return step(VD.REJECTED, RR.ILLEGAL_FAILED_AFTER_CAPTURE,
                        "已捕获后支付失败通知无效")
        if et == EventType.PAYMENT_CAPTURED and self.captured > 0 and self.refunded >= self.captured:
            return step(VD.REJECTED, RR.ILLEGAL_CAPTURE_AFTER_REFUND,
                        "已全额退款后不允许再捕获")
        # 退款累计不得超过捕获额；零捕获（含隔离空态）下任何退款都属越界
        if et == EventType.REFUND_SUCCEEDED and self.refunded + amt > self.captured:
            return step(
                VD.REJECTED, RR.ILLEGAL_REFUND_EXCEEDS_CAPTURE,
                f"退款累计 {self.refunded + amt} 超过捕获 {self.captured}",
            )

        # ---- 接受：累计金额推导状态 ----
        if et == EventType.PAYMENT_AUTHORIZED:
            self.authorized = max(self.authorized, amt)
        elif et == EventType.PAYMENT_CAPTURED:
            self.captured += amt
        elif et == EventType.REFUND_SUCCEEDED:
            self.refunded += amt
        elif et == EventType.CHARGEBACK_OPENED:
            self.chargeback += amt
        elif et == EventType.CHARGEBACK_REVERSED:
            self.chargeback = max(Decimal("0"), self.chargeback - amt)
        elif et in (EventType.PAYMENT_FAILED, EventType.PAYMENT_AUTH_EXPIRED):
            self.state = PS.FAILED
            self.driving_event_time = t
            self.events_at_time.setdefault(_iso(t), set()).add(et)
            return step(VD.ACCEPTED, to=self.state)
        # REFUND_REQUESTED 非驱动：登记但不改累计金额/状态

        new_state = self._derive_state()
        self.state = new_state
        if is_driving:
            self.driving_event_time = t
            self.events_at_time.setdefault(_iso(t), set()).add(et)
        return step(VD.ACCEPTED, to=self.state)


def run_replay(events: list[ReplayEvent], machine: "PaymentShadowMachine | None" = None
               ) -> tuple[PaymentShadowMachine, dict[str, ShadowStep]]:
    """把一组事件按（业务时间, 事件序号）排序后从空态重放，返回机器与每个事件的结论。"""
    m = machine or PaymentShadowMachine()
    ordered = sorted(
        enumerate(events),
        key=lambda x: (x[1].event_time, x[1].business_event_id or 0, x[0]),
    )
    steps: dict[str, ShadowStep] = {}
    for idx, e in ordered:
        s = m.apply(e)
        steps[f"{e.business_event_id or 'CAND'}-{idx}-{e.event_type}"] = s
    return m, steps


# ================================================================
# 关联解析
# ================================================================
@dataclass
class LinkTarget:
    linked: bool
    payment_ref: str | None = None
    order_ref: str | None = None
    channel_ref: str | None = None
    reason_code: str | None = None
    reason_detail: str | None = None


async def resolve_link(db: AsyncSession, p: dict) -> LinkTarget:
    """判断事件能否关联到已知订单/支付。

    关联优先级（与生产投影 ledger._project_payment 的回找顺序一致）：
      已有支付尝试（支付号/渠道参考号/订单号）> 凭支付令牌自建 > 凭已知订单自建 > 无法关联。
    携带 channel_ref/entity_ref 的事件可自建支付态（兼容捕获先行等乱序场景）；
    只有完全缺少关联键、或仅指向不存在订单且无支付令牌时，才进入待关联队列。
    """
    order_ref = p.get("order_ref")
    channel_ref = p.get("channel_ref")
    entity_ref = p.get("entity_ref")
    et = p["event_type"]

    pay = None
    if et.startswith("PAYMENT_") and entity_ref:
        pay = (await db.execute(
            select(PaymentState).where(PaymentState.payment_ref == entity_ref)
        )).scalar_one_or_none()
    if pay is None and channel_ref:
        pay = (await db.execute(
            select(PaymentState).where(PaymentState.channel_ref == channel_ref)
        )).scalar_one_or_none()
    if pay is None and order_ref:
        pay = (await db.execute(
            select(PaymentState).where(PaymentState.order_ref == order_ref)
        )).scalar_one_or_none()
    if pay is not None:
        return LinkTarget(True, pay.payment_ref, pay.order_ref, pay.channel_ref)

    # 自带支付令牌：可自建支付态
    if channel_ref or (et.startswith("PAYMENT_") and entity_ref):
        return LinkTarget(True, entity_ref, order_ref, channel_ref)

    if order_ref:
        order = (await db.execute(
            select(OrderState).where(OrderState.order_ref == order_ref)
        )).scalar_one_or_none()
        if order is not None:
            return LinkTarget(True, entity_ref, order_ref, channel_ref)
        return LinkTarget(
            False, order_ref=order_ref,
            reason_code=RR.UNLINKED_UNKNOWN_ORDER,
            reason_detail=f"订单 {order_ref} 在系统中不存在，且报文无渠道参考号可自建支付态",
        )

    return LinkTarget(
        False,
        reason_code=RR.UNLINKED_NO_ORDER_OR_PAYMENT,
        reason_detail="报文缺少订单号/支付号/渠道参考，无法关联任何实体",
    )


def _or(*conditions):
    from sqlalchemy import or_
    return or_(*conditions)


async def _ineffective_event_ids(db: AsyncSession) -> set[int]:
    """当前不生效的业务事件 ID：被拒绝者、未裁决/已作废冲突的当事方、裁决落败者。

    裁决胜出者即使留有历史 CONFLICT 决策也视为生效（不跳过），保证裁决后影子重放能复算胜者。
    """
    ids: set[int] = set()
    winner_ids: set[int] = set()
    # 冲突当事方按冲突当前状态决定是否生效
    conflicts = (await db.execute(select(StateConflict))).scalars().all()
    for c in conflicts:
        incoming = c.detail.get("incoming_event_id")
        existing = c.detail.get("existing_event_id")
        if c.status == ConflictStatus.RESOLVED:
            winner_ids.add(c.winning_event_id)
            loser = c.losing_event_id or next(
                (x for x in (incoming, existing) if x != c.winning_event_id), None
            )
            if loser is not None:
                ids.add(loser)
        else:
            # PENDING / DISCARDED：两条事实都暂不生效
            for x in (incoming, existing):
                if x is not None:
                    ids.add(x)
    # 显式 REJECTED（裁决胜出者除外）
    rejected_rows = (await db.execute(
        select(EventDecision.business_event_id).where(
            EventDecision.business_event_id.is_not(None),
            EventDecision.decision == VD.REJECTED,
            EventDecision.replay_run_id.is_(None),
        )
    )).all()
    ids |= {r[0] for r in rejected_rows}
    return ids - winner_ids


# 向后兼容的别名（内部统一以“失效事件”语义使用）
_rejected_or_conflict_ids = _ineffective_event_ids


async def _target_conditions(target: LinkTarget):
    conds = []
    if target.payment_ref:
        conds.append(BusinessEvent.entity_ref == target.payment_ref)
    if target.order_ref:
        conds.append(BusinessEvent.order_ref == target.order_ref)
    if target.channel_ref:
        conds.append(BusinessEvent.channel_ref == target.channel_ref)
    return conds


async def _load_lifecycle_bes(db: AsyncSession, target: LinkTarget,
                              include_rejected: bool = False) -> list[BusinessEvent]:
    conds = await _target_conditions(target)
    if not conds:
        return []
    skip = set() if include_rejected else await _rejected_or_conflict_ids(db)
    bes = (await db.execute(
        select(BusinessEvent).where(_or(*conds))
        .order_by(BusinessEvent.event_time, BusinessEvent.id)
    )).scalars().all()
    return [b for b in bes
            if b.event_type in PAYMENT_LIFECYCLE_EVENTS and b.id not in skip]


def _be_to_replay(be: BusinessEvent) -> ReplayEvent:
    return ReplayEvent(
        event_type=be.event_type, amount=_amt(be.payload.get("amount", "0")),
        event_time=be.event_time, source=be.source,
        business_event_id=be.id, raw_event_id=be.source_raw_event_id,
    )


async def reconstruct_machine(db: AsyncSession, target: LinkTarget
                              ) -> tuple[PaymentShadowMachine, list[BusinessEvent]]:
    """从生产已接受事件按业务时间重放，重建影子机（与生产投影同规则）。"""
    m = PaymentShadowMachine(
        payment_ref=target.payment_ref, order_ref=target.order_ref,
        channel_ref=target.channel_ref,
    )
    bes = await _load_lifecycle_bes(db, target)
    if bes:
        m.currency = bes[0].payload.get("txn_currency", m.currency)
    run_replay([_be_to_replay(b) for b in bes], m)
    return m, bes


# ================================================================
# 生产摄入闸门：合并已存事件 + 候选事件整体重放，得出候选结论（不写库）
# ================================================================
@dataclass
class GateResult:
    verdict: str
    reason_code: str | None
    reason_detail: str | None
    target: LinkTarget
    from_state: str | None
    to_state: str | None
    out_of_order: bool = False


async def assess_payment_event(db: AsyncSession, p: dict) -> GateResult:
    target = await resolve_link(db, p)
    if not target.linked:
        return GateResult(VD.UNLINKED, target.reason_code, target.reason_detail,
                          target, None, None)

    existing_bes = await _load_lifecycle_bes(db, target)
    t = p.get("event_time")
    t = datetime.fromisoformat(t) if isinstance(t, str) else t
    et = p["event_type"]

    # 显式同业务时间冲突预检（与重放排序无关、确定性）：
    # 只要已接受事件中存在同一业务时间的驱动事件，两条状态事实无法自动定序 -> 冲突待裁决。
    if et in PAYMENT_STATE_DRIVING_EVENTS:
        partner = next(
            (b for b in existing_bes
             if b.event_time == t and b.event_type in PAYMENT_STATE_DRIVING_EVENTS),
            None,
        )
        if partner is not None:
            same_type = partner.event_type == et
            code = RR.CONFLICT_DUPLICATED_FACT if same_type else RR.CONFLICT_SAME_TIMESTAMP
            detail = (
                f"业务时间 {t} 已存在同类型事件 {partner.event_type}（BE{partner.id}），"
                "同一生命周期事实出现两条不同记录，待人工裁决"
                if same_type else
                f"业务时间 {t} 已存在互斥事件 {partner.event_type}（BE{partner.id}），"
                f"与 {et} 无法自动定序，待人工裁决"
            )
            return GateResult(VD.CONFLICT, code, detail, target, None, None)

    # 生产当前驱动时间（仅由已接受事件决定），用于识别迟到事件
    prod_machine, _ = run_replay([_be_to_replay(b) for b in existing_bes],
                                 PaymentShadowMachine(
                                     payment_ref=target.payment_ref,
                                     order_ref=target.order_ref,
                                     channel_ref=target.channel_ref))
    candidate = ReplayEvent(
        event_type=et, amount=_amt(p.get("amount", "0")),
        event_time=t, source=p.get("source", ""), is_candidate=True,
    )
    combined = PaymentShadowMachine(
        payment_ref=target.payment_ref, order_ref=target.order_ref,
        channel_ref=target.channel_ref,
    )
    _, steps = run_replay([_be_to_replay(b) for b in existing_bes] + [candidate], combined)
    cand_step = next(s for s in steps.values()
                     if s.event_type == candidate.event_type and s.event_time == t
                     and s.business_event_id is None)

    out_of_order = (
        prod_machine.driving_event_time is not None
        and t < prod_machine.driving_event_time
        and cand_step.verdict == VD.ACCEPTED
    )
    detail = cand_step.reason_detail
    if out_of_order:
        detail = (detail + "；" if detail else "") + \
            f"迟到事件（业务时间 {t} 早于当前驱动时间 {prod_machine.driving_event_time}）已按业务时间归位，不回退当前状态"
    return GateResult(cand_step.verdict, cand_step.reason_code, detail, target,
                      cand_step.from_state, cand_step.to_state, out_of_order)


# ================================================================
# 决策/队列/冲突落库
# ================================================================
async def add_decision(db: AsyncSession, *, raw_event_id, business_event_id,
                       replay_run_id, order_ref, payment_ref, channel_ref,
                       event_type, source, event_time, decision, reason_code=None,
                       reason_detail=None, from_state=None, to_state=None,
                       conflict_id=None) -> EventDecision:
    if isinstance(event_time, str):
        event_time = datetime.fromisoformat(event_time)
    d = EventDecision(
        raw_event_id=raw_event_id, business_event_id=business_event_id,
        replay_run_id=replay_run_id, order_ref=order_ref, payment_ref=payment_ref,
        channel_ref=channel_ref, event_type=event_type, source=source,
        event_time=event_time, decision=decision, reason_code=reason_code,
        reason_detail=reason_detail, from_state=from_state, to_state=to_state,
        conflict_id=conflict_id,
    )
    db.add(d)
    await db.flush()
    return d


async def queue_unlinked(db: AsyncSession, raw: RawEvent, p: dict,
                         reason_code: str, reason_detail: str) -> UnlinkedEvent:
    row = UnlinkedEvent(
        raw_event_id=raw.id, idempotency_key=raw.idempotency_key, source=raw.source,
        event_type=p["event_type"], topic=raw.topic,
        order_ref_hint=p.get("order_ref"), payment_ref_hint=p.get("entity_ref"),
        channel_ref_hint=p.get("channel_ref"),
        event_time=raw.event_occurred_at, received_at=raw.received_at,
        payload=raw.payload, reason_code=reason_code, reason_detail=reason_detail,
        status=UnlinkedStatus.PENDING,
    )
    db.add(row)
    await db.flush()
    return row


async def find_existing_same_time_event(db: AsyncSession, target: LinkTarget,
                                        event_time, event_type: str):
    bes = await _load_lifecycle_bes(db, target)
    for be in bes:
        if be.event_time == event_time and be.event_type != event_type \
                and be.event_type in PAYMENT_STATE_DRIVING_EVENTS:
            return be
    return None


async def register_conflict(db: AsyncSession, target: LinkTarget, event_time,
                            incoming_be: BusinessEvent, existing_be: BusinessEvent | None,
                            reason_code: str, reason_detail: str) -> StateConflict:
    """同业务时间互斥：创建 PENDING 冲突，两条业务事件都保留，互不覆盖状态。"""
    conflict = StateConflict(
        order_ref=target.order_ref, payment_ref=target.payment_ref,
        conflict_type="SAME_TIMESTAMP", status=ConflictStatus.PENDING,
        event_time=event_time,
        title=f"{target.payment_ref or target.order_ref} 在 {event_time:%Y-%m-%d %H:%M} 出现互斥事件",
        detail={
            "reason_code": reason_code,
            "incoming_event_id": incoming_be.id,
            "existing_event_id": existing_be.id if existing_be else None,
            "incoming_type": incoming_be.event_type,
            "existing_type": existing_be.event_type if existing_be else None,
            "incoming_amount": str(incoming_be.payload.get("amount")),
            "existing_amount": str(existing_be.payload.get("amount")) if existing_be else None,
        },
    )
    db.add(conflict)
    await db.flush()
    for be in (existing_be, incoming_be):
        if be is None:
            continue
        await add_decision(
            db, raw_event_id=be.source_raw_event_id, business_event_id=be.id,
            replay_run_id=None, order_ref=target.order_ref,
            payment_ref=target.payment_ref, channel_ref=target.channel_ref,
            event_type=be.event_type, source=be.source, event_time=be.event_time,
            decision=VD.CONFLICT, reason_code=reason_code,
            reason_detail=reason_detail, conflict_id=conflict.id,
        )
    return conflict


# ================================================================
# 订单状态机视图
# ================================================================
def _decision_dict(d: EventDecision) -> dict:
    return {
        "id": d.id, "raw_event_id": d.raw_event_id,
        "business_event_id": d.business_event_id, "replay_run_id": d.replay_run_id,
        "order_ref": d.order_ref, "payment_ref": d.payment_ref,
        "channel_ref": d.channel_ref, "event_type": d.event_type, "source": d.source,
        "event_time": d.event_time, "decision": d.decision,
        "reason_code": d.reason_code, "reason_detail": d.reason_detail,
        "from_state": d.from_state, "to_state": d.to_state,
        "conflict_id": d.conflict_id,
    }


def _conflict_dict(c: StateConflict) -> dict:
    return {
        "id": c.id, "order_ref": c.order_ref, "payment_ref": c.payment_ref,
        "conflict_type": c.conflict_type, "status": c.status,
        "event_time": c.event_time, "title": c.title, "detail": c.detail,
        "winning_event_id": c.winning_event_id, "losing_event_id": c.losing_event_id,
        "resolved_by": c.resolved_by, "resolution_comment": c.resolution_comment,
        "resolved_at": c.resolved_at,
    }


def _unlinked_dict(u: UnlinkedEvent) -> dict:
    return {
        "id": u.id, "raw_event_id": u.raw_event_id,
        "idempotency_key": u.idempotency_key, "source": u.source,
        "event_type": u.event_type, "topic": u.topic,
        "order_ref_hint": u.order_ref_hint, "payment_ref_hint": u.payment_ref_hint,
        "channel_ref_hint": u.channel_ref_hint, "event_time": u.event_time,
        "received_at": u.received_at, "payload": u.payload,
        "reason_code": u.reason_code, "reason_detail": u.reason_detail,
        "status": u.status, "linked_order_ref": u.linked_order_ref,
        "linked_payment_ref": u.linked_payment_ref, "resolved_by": u.resolved_by,
        "resolution_comment": u.resolution_comment, "resolved_at": u.resolved_at,
    }


async def build_order_machine_view(db: AsyncSession, order_ref: str) -> dict:
    order = (await db.execute(
        select(OrderState).where(OrderState.order_ref == order_ref)
    )).scalar_one_or_none()
    pays = (await db.execute(
        select(PaymentState).where(PaymentState.order_ref == order_ref)
    )).scalars().all()
    payments_out = []
    for pay in pays:
        decisions = (await db.execute(
            select(EventDecision)
            .where(EventDecision.payment_ref == pay.payment_ref,
                   EventDecision.replay_run_id.is_(None))
            .order_by(EventDecision.event_time, EventDecision.id)
        )).scalars().all()
        conflicts = (await db.execute(
            select(StateConflict)
            .where(StateConflict.payment_ref == pay.payment_ref)
            .order_by(StateConflict.id)
        )).scalars().all()
        payments_out.append({
            "payment_ref": pay.payment_ref,
            "channel_ref": pay.channel_ref,
            "channel": pay.channel,
            "production_state": {
                "state": pay.state, "currency": pay.currency,
                "authorized_amount": str(pay.authorized_amount),
                "captured_amount": str(pay.captured_amount),
                "refunded_amount": str(pay.refunded_amount),
                "chargeback_amount": str(pay.chargeback_amount),
                "last_event_id": pay.last_event_id,
            },
            "decisions": [_decision_dict(d) for d in decisions],
            "conflicts": [_conflict_dict(c) for c in conflicts],
            "pending_conflict": any(c.status == ConflictStatus.PENDING for c in conflicts),
        })
    return {
        "order_ref": order_ref,
        "order": None if order is None else {
            "status": order.status, "amount": str(order.amount),
            "currency": order.currency, "channel": order.channel,
        },
        "payments": payments_out,
    }


async def list_order_refs(db: AsyncSession) -> list[str]:
    """订单线索 = 订单投影 ∪ 支付投影上的订单号（部分订单只有支付捕获、无订单 CDC 事件）。"""
    o_refs = {r[0] for r in (await db.execute(
        select(OrderState.order_ref)
    )).all() if r[0]}
    p_refs = {r[0] for r in (await db.execute(
        select(PaymentState.order_ref).where(PaymentState.order_ref.is_not(None))
    )).all() if r[0]}
    return sorted(o_refs | p_refs)


# ================================================================
# 待关联队列：查询 / 人工关联重放并入账 / 丢弃
# ================================================================
async def list_unlinked(db: AsyncSession, status: str | None = None):
    stmt = select(UnlinkedEvent).order_by(UnlinkedEvent.id)
    if status:
        stmt = stmt.where(UnlinkedEvent.status == status)
    return (await db.execute(stmt)).scalars().all()


async def discard_unlinked(db: AsyncSession, unlinked_id: int, operator: str,
                           comment: str | None) -> dict:
    u = await db.get(UnlinkedEvent, unlinked_id)
    if u is None:
        raise StateMachineError("待关联事件不存在")
    if u.status != UnlinkedStatus.PENDING:
        raise StateMachineError(f"队列项已是 {u.status}，不可重复处置")
    u.status = UnlinkedStatus.DISCARDED
    u.resolved_by = operator
    u.resolution_comment = comment
    u.resolved_at = _now()
    raw = await db.get(RawEvent, u.raw_event_id)
    if raw:
        raw.status = "REJECTED"
        raw.reject_reason = f"人工判定无效丢弃：{comment or ''}"
        raw.processed_at = _now()
    await add_decision(
        db, raw_event_id=u.raw_event_id, business_event_id=None, replay_run_id=None,
        order_ref=u.order_ref_hint, payment_ref=u.payment_ref_hint,
        channel_ref=u.channel_ref_hint, event_type=u.event_type, source=u.source,
        event_time=u.event_time, decision=VD.REJECTED,
        reason_code="DISCARDED_MANUAL", reason_detail=comment,
    )
    await db.flush()
    return _unlinked_dict(u)


class RawEventInLike:
    """轻量容器，link 时复用 ledger.ingest_raw_event（它只取属性），避免依赖 Pydantic。"""
    def __init__(self, idempotency_key, source, event_occurred_at, topic, payload):
        self.idempotency_key = idempotency_key
        self.source = source
        self.event_occurred_at = event_occurred_at
        self.topic = topic
        self.payload = payload


async def link_unlinked(db: AsyncSession, unlinked_id: int, operator: str,
                        order_ref: str, payment_ref: str | None,
                        comment: str | None) -> dict:
    """人工补全关联后，用原始报文重新投递进入标准两层幂等 + 状态机闸门并正常入账。"""
    from .ledger import ingest_raw_event, process_event
    u = await db.get(UnlinkedEvent, unlinked_id)
    if u is None:
        raise StateMachineError("待关联事件不存在")
    if u.status != UnlinkedStatus.PENDING:
        raise StateMachineError(f"队列项已是 {u.status}，不可重复关联")

    payload = dict(u.payload)
    payload["order_ref"] = order_ref
    if payment_ref:
        payload["entity_ref"] = payment_ref
    new_key = f"{u.idempotency_key}:LINKED-{u.id}"
    raw_in = RawEventInLike(
        idempotency_key=new_key, source=u.source,
        event_occurred_at=u.event_time, topic=u.topic, payload=payload,
    )
    raw, duplicate = await ingest_raw_event(db, raw_in)
    be = None
    if not duplicate:
        be = await process_event(db, raw)

    u.status = UnlinkedStatus.LINKED
    u.linked_order_ref = order_ref
    u.linked_payment_ref = payment_ref
    u.resolved_by = operator
    u.resolution_comment = comment
    u.resolved_at = _now()
    await db.flush()
    out = _unlinked_dict(u)
    out["reprocess"] = {
        "raw_event_id": raw.id, "duplicate": duplicate,
        "business_event_id": be.id if be else None,
    }
    return out


# ================================================================
# 冲突裁决
# ================================================================
async def list_conflicts(db: AsyncSession, status: str | None = None):
    stmt = select(StateConflict).order_by(StateConflict.id)
    if status:
        stmt = stmt.where(StateConflict.status == status)
    return (await db.execute(stmt)).scalars().all()


async def resolve_conflict(db: AsyncSession, conflict_id: int, winning_event_id: int,
                           operator: str, comment: str | None) -> dict:
    """裁决：选定胜者驱动状态，败者追加 REJECTED 决策；两条业务记录都保留不删。"""
    from sqlalchemy import func
    from ..models import LedgerEntry
    from .ledger import _HANDLERS
    c = await db.get(StateConflict, conflict_id)
    if c is None:
        raise StateMachineError("冲突不存在")
    if c.status != ConflictStatus.PENDING:
        raise StateMachineError(f"冲突已 {c.status}，不可重复裁决")
    candidate_ids = {c.detail.get("incoming_event_id"), c.detail.get("existing_event_id")}
    if winning_event_id not in candidate_ids:
        raise StateMachineError("胜出事件必须属于该冲突组")
    losing_event_id = next(x for x in candidate_ids if x != winning_event_id)

    winner = await db.get(BusinessEvent, winning_event_id)
    loser = await db.get(BusinessEvent, losing_event_id)
    if winner is None or loser is None:
        raise StateMachineError("冲突关联的业务事件缺失")

    # 胜者若到达时挂起、尚未过账，补跑其标准处理器（追加，不改历史）
    winner_posted = (await db.execute(
        select(LedgerEntry.id).where(LedgerEntry.business_event_id == winner.id).limit(1)
    )).first()
    handler = _HANDLERS.get(winner.event_type)
    if handler is not None and winner_posted is None and winner.event_type in (
        EventType.PAYMENT_CAPTURED, EventType.REFUND_SUCCEEDED,
        EventType.CHARGEBACK_OPENED, EventType.CHARGEBACK_REVERSED,
    ):
        await handler(db, winner, winner.payload)

    # 败者若已过账，追加红冲调账（只追加更正，不删除原分录），保持借贷守恒
    loser_posted = (await db.execute(
        select(LedgerEntry.id).where(LedgerEntry.business_event_id == loser.id).limit(1)
    )).first()
    if loser_posted is not None:
        await _reverse_loser_effect(db, c, loser, operator, comment)

    await add_decision(
        db, raw_event_id=winner.source_raw_event_id, business_event_id=winner.id,
        replay_run_id=None, order_ref=c.order_ref, payment_ref=c.payment_ref,
        channel_ref=winner.channel_ref, event_type=winner.event_type,
        source=winner.source, event_time=winner.event_time, decision=VD.ACCEPTED,
        reason_code="ADJUDICATED_WINNER",
        reason_detail=f"人工裁决胜出：{comment or ''}",
    )
    await add_decision(
        db, raw_event_id=loser.source_raw_event_id, business_event_id=loser.id,
        replay_run_id=None, order_ref=c.order_ref, payment_ref=c.payment_ref,
        channel_ref=loser.channel_ref, event_type=loser.event_type,
        source=loser.source, event_time=loser.event_time, decision=VD.REJECTED,
        reason_code="ADJUDICATED_LOSER",
        reason_detail=f"冲突裁决落败，业务记录保留但不生效：{comment or ''}",
        conflict_id=c.id,
    )

    c.status = ConflictStatus.RESOLVED
    c.winning_event_id = winning_event_id
    c.losing_event_id = losing_event_id
    c.resolved_by = operator
    c.resolution_comment = comment
    c.resolved_at = _now()
    await db.flush()
    return _conflict_dict(c)


async def _reverse_loser_effect(db: AsyncSession, c: StateConflict,
                                loser: BusinessEvent, operator: str,
                                comment: str | None) -> None:
    """对落败且已过账事件逐科目追加反向分录（只追加红冲，不删原分录）。"""
    from sqlalchemy import func
    from ..models import LedgerEntry
    rows = (await db.execute(
        select(LedgerEntry.account, LedgerEntry.direction,
               LedgerEntry.book_currency, func.sum(LedgerEntry.book_amount))
        .where(LedgerEntry.business_event_id == loser.id)
        .group_by(LedgerEntry.account, LedgerEntry.direction, LedgerEntry.book_currency)
    )).all()
    reversal_be = BusinessEvent(
        business_key=f"MANUAL_ADJUSTMENT:CONFLICT-{c.id}:REVERSE-BE{loser.id}",
        entity_type="MANUAL_ADJ", event_type=EventType.MANUAL_ADJUSTMENT,
        event_version=1, entity_ref=f"ADJ-CONFLICT-{c.id}",
        channel=loser.channel, merchant_id=loser.merchant_id,
        order_ref=loser.order_ref, source="MANUAL",
        source_raw_event_id=None, event_time=_now(),
        payload={
            "event_type": EventType.MANUAL_ADJUSTMENT,
            "entity_type": "MANUAL_ADJ",
            "entity_ref": f"ADJ-CONFLICT-{c.id}",
            "book_amount": "0", "txn_currency": loser.payload.get("txn_currency", "CNY"),
            "book_currency": "CNY", "decimal_places": 2,
            "reason": f"冲突 {c.id} 落败事件 BE{loser.id} 红冲",
            "operator": operator, "comment": comment,
        },
    )
    db.add(reversal_be)
    await db.flush()
    for account, direction, ccy, amount in rows:
        rev_direction = "CR" if direction == "DR" else "DR"
        db.add(LedgerEntry(
            business_event_id=reversal_be.id, account=account,
            direction=rev_direction, txn_amount=amount, txn_currency=ccy,
            settle_amount=amount, settle_currency=ccy,
            book_amount=quantize(amount), book_currency=ccy,
            decimal_places=2, raw_amount=amount,
            effective_month=_now().strftime("%Y-%m"),
            channel=loser.channel, merchant_id=loser.merchant_id,
            order_ref=loser.order_ref,
        ))
    await db.flush()


async def discard_conflict(db: AsyncSession, conflict_id: int, operator: str,
                           comment: str | None) -> dict:
    c = await db.get(StateConflict, conflict_id)
    if c is None:
        raise StateMachineError("冲突不存在")
    if c.status != ConflictStatus.PENDING:
        raise StateMachineError(f"冲突已 {c.status}，不可重复处置")
    c.status = ConflictStatus.DISCARDED
    c.resolved_by = operator
    c.resolution_comment = comment
    c.resolved_at = _now()
    await db.flush()
    return _conflict_dict(c)


# ================================================================
# 隔离影子重放（SHADOW）：只读生产、只写 replay_runs + SHADOW 决策，绝不回写生产
# ================================================================
async def _next_run_ref(db: AsyncSession) -> str:
    max_id = (await db.execute(
        select(ReplayRun.id).order_by(ReplayRun.id.desc()).limit(1)
    )).scalar_one_or_none() or 0
    return f"RP-{_now():%Y%m%d%H%M%S}-{max_id + 1}"


async def _persist_shadow_decisions(db: AsyncSession, run_id: int,
                                    machine: PaymentShadowMachine) -> None:
    for step in machine.history:
        await add_decision(
            db, raw_event_id=step.raw_event_id,
            business_event_id=step.business_event_id, replay_run_id=run_id,
            order_ref=machine.order_ref, payment_ref=machine.payment_ref,
            channel_ref=machine.channel_ref, event_type=step.event_type,
            source=step.source, event_time=step.event_time,
            decision=VD.SHADOW, reason_code=step.reason_code,
            reason_detail=step.reason_detail,
            from_state=step.from_state, to_state=step.to_state,
        )


def _diff_states(prod: dict, shadow_snap: dict) -> dict:
    state_keys = ("state",)
    amount_keys = ("authorized_amount", "captured_amount",
                   "refunded_amount", "chargeback_amount")
    diff: dict[str, dict] = {}
    for k in state_keys:
        if prod.get(k) != shadow_snap.get(k):
            diff[k] = {"production": prod.get(k), "shadow": shadow_snap.get(k)}
    # 金额按数值比较，避免 120 与 120.000000 的格式差异造成假分歧
    for k in amount_keys:
        if _amt(prod.get(k)) != _amt(shadow_snap.get(k)):
            diff[k] = {"production": prod.get(k), "shadow": shadow_snap.get(k)}
    return diff


def _replay_dict(r: ReplayRun) -> dict:
    return {
        "id": r.id, "run_ref": r.run_ref, "mode": r.mode,
        "scope_type": r.scope_type, "scope_ref": r.scope_ref,
        "trigger_raw_event_id": r.trigger_raw_event_id, "operator": r.operator,
        "production_state": r.production_state, "shadow_state": r.shadow_state,
        "outcome": r.outcome, "divergence_detail": r.divergence_detail,
        "step_count": r.step_count, "write_back": r.write_back,
        "created_at": r.created_at,
    }


async def replay_order(db: AsyncSession, order_ref: str, operator: str) -> dict:
    """对订单下每个支付尝试，从空态按业务时间重放全部已接受事件，生成影子态与生产态对比。"""
    pays = (await db.execute(
        select(PaymentState).where(PaymentState.order_ref == order_ref)
    )).scalars().all()
    if not pays:
        raise StateMachineError(f"订单 {order_ref} 没有可重放的支付尝试")

    # 1) 内存中完成全部重放计算（只读生产，不写任何表）
    divergence: dict[str, dict] = {}
    prod_snap: dict[str, dict] = {}
    shadow_snap: dict[str, dict] = {}
    machines: list[PaymentShadowMachine] = []
    total_steps = 0
    any_blocked = any_diverged = False
    for pay in pays:
        target = LinkTarget(True, pay.payment_ref, pay.order_ref, pay.channel_ref)
        bes = await _load_lifecycle_bes(db, target)
        machine = PaymentShadowMachine(
            payment_ref=pay.payment_ref, order_ref=pay.order_ref,
            channel_ref=pay.channel_ref, currency=pay.currency,
        )
        run_replay([_be_to_replay(b) for b in bes], machine)
        machines.append(machine)
        total_steps += len(machine.history)

        prod = {
            "state": pay.state,
            "authorized_amount": str(pay.authorized_amount),
            "captured_amount": str(pay.captured_amount),
            "refunded_amount": str(pay.refunded_amount),
            "chargeback_amount": str(pay.chargeback_amount),
        }
        shadow = machine.snapshot()
        prod_snap[pay.payment_ref] = prod
        shadow_snap[pay.payment_ref] = shadow
        diffs = _diff_states(prod, shadow)
        if any(s.verdict in (VD.REJECTED, VD.CONFLICT) for s in machine.history):
            any_blocked = True
        if diffs:
            any_diverged = True
            divergence[pay.payment_ref] = diffs

    outcome = (ReplayOutcome.BLOCKED if any_blocked
               else ReplayOutcome.DIVERGED if any_diverged
               else ReplayOutcome.MATCHED)

    # 2) 一次性插入完整 ReplayRun（只追加，禁止后续 UPDATE），再写 SHADOW 决策
    run_ref = await _next_run_ref(db)
    run = ReplayRun(
        run_ref=run_ref, mode=ReplayMode.SHADOW, scope_type="ORDER",
        scope_ref=order_ref, trigger_raw_event_id=None, operator=operator,
        production_state=prod_snap, shadow_state=shadow_snap, outcome=outcome,
        divergence_detail=divergence, step_count=total_steps, write_back=False,
    )
    db.add(run)
    await db.flush()
    for machine in machines:
        await _persist_shadow_decisions(db, run.id, machine)
    await db.flush()
    return _replay_dict(run)


async def replay_unlinked(db: AsyncSession, unlinked_id: int, operator: str) -> dict:
    """用待关联队列中的原始报文，在隔离空态下重放：观察它在当前规则下会被如何处置。"""
    u = await db.get(UnlinkedEvent, unlinked_id)
    if u is None:
        raise StateMachineError("待关联事件不存在")

    # 1) 内存隔离空态重放
    machine = PaymentShadowMachine(
        payment_ref=u.payment_ref_hint, order_ref=u.order_ref_hint,
        channel_ref=u.channel_ref_hint,
    )
    candidate = ReplayEvent(
        event_type=u.event_type, amount=_amt(u.payload.get("amount", "0")),
        event_time=u.event_time, source=u.source, raw_event_id=u.raw_event_id,
    )
    run_replay([candidate], machine)
    machine.history[0].raw_event_id = u.raw_event_id
    step = machine.history[0]
    outcome = (ReplayOutcome.MATCHED if step.verdict == VD.ACCEPTED
               else ReplayOutcome.BLOCKED)

    # 2) 单次插入完整运行记录，再写影子步骤
    run_ref = await _next_run_ref(db)
    run = ReplayRun(
        run_ref=run_ref, mode=ReplayMode.SHADOW, scope_type="UNLINKED",
        scope_ref=u.idempotency_key, trigger_raw_event_id=u.raw_event_id,
        operator=operator,
        production_state={"note": "该事件未关联任何生产实体，生产侧无对应状态"},
        shadow_state=machine.snapshot(),
        divergence_detail={
            "verdict": step.verdict, "reason_code": step.reason_code,
            "reason_detail": step.reason_detail,
        },
        outcome=outcome, step_count=len(machine.history), write_back=False,
    )
    db.add(run)
    await db.flush()
    await _persist_shadow_decisions(db, run.id, machine)
    await db.flush()
    return _replay_dict(run)


async def replay_raw_event(db: AsyncSession, raw_event_id: int, operator: str) -> dict:
    """对任意原始事件按其线索做隔离重放（能关联则在该支付影子态上叠加该报文）。"""
    raw = await db.get(RawEvent, raw_event_id)
    if raw is None:
        raise StateMachineError("原始事件不存在")
    p = raw.payload
    target = await resolve_link(db, p)
    candidate = ReplayEvent(
        event_type=p["event_type"], amount=_amt(p.get("amount", "0")),
        event_time=raw.event_occurred_at, source=raw.source, raw_event_id=raw.id,
    )

    # 1) 内存重放
    if target.linked:
        existing_bes = await _load_lifecycle_bes(db, target)
        before_machine = PaymentShadowMachine(
            payment_ref=target.payment_ref, order_ref=target.order_ref,
            channel_ref=target.channel_ref,
        )
        run_replay([_be_to_replay(b) for b in existing_bes], before_machine)
        prod_state = {"before": before_machine.snapshot()}

        machine = PaymentShadowMachine(
            payment_ref=target.payment_ref, order_ref=target.order_ref,
            channel_ref=target.channel_ref,
        )
        run_replay([_be_to_replay(b) for b in existing_bes] + [candidate], machine)
        step = next(s for s in machine.history if s.raw_event_id == raw.id)
        divergence_detail = {
            "incoming_verdict": step.verdict, "reason_code": step.reason_code,
            "reason_detail": step.reason_detail,
            "from_state": step.from_state, "to_state": step.to_state,
        }
        outcome = (ReplayOutcome.BLOCKED if step.verdict in (VD.REJECTED, VD.CONFLICT)
                   else ReplayOutcome.MATCHED)
        scope_type, scope_ref = "RAW_EVENT", raw.idempotency_key
    else:
        machine = PaymentShadowMachine(
            payment_ref=p.get("entity_ref"), order_ref=p.get("order_ref"),
            channel_ref=p.get("channel_ref"),
        )
        run_replay([candidate], machine)
        step = machine.history[0]
        prod_state = {"note": "无法关联，生产侧无对应状态"}
        divergence_detail = {
            "incoming_verdict": step.verdict, "reason_code": step.reason_code,
            "reason_detail": step.reason_detail,
        }
        outcome = ReplayOutcome.BLOCKED
        scope_type, scope_ref = "RAW_EVENT", raw.idempotency_key

    # 2) 单次插入完整运行记录，再写影子步骤
    run_ref = await _next_run_ref(db)
    run = ReplayRun(
        run_ref=run_ref, mode=ReplayMode.SHADOW, scope_type=scope_type,
        scope_ref=scope_ref, trigger_raw_event_id=raw.id, operator=operator,
        production_state=prod_state, shadow_state=machine.snapshot(),
        divergence_detail=divergence_detail, outcome=outcome,
        step_count=len(machine.history), write_back=False,
    )
    db.add(run)
    await db.flush()
    await _persist_shadow_decisions(db, run.id, machine)
    await db.flush()
    return _replay_dict(run)


async def list_replays(db: AsyncSession, limit: int = 100):
    rows = (await db.execute(
        select(ReplayRun).order_by(ReplayRun.id.desc()).limit(limit)
    )).scalars().all()
    return [_replay_dict(r) for r in rows]


async def get_replay(db: AsyncSession, run_id: int) -> dict:
    r = await db.get(ReplayRun, run_id)
    if r is None:
        raise StateMachineError("重放记录不存在")
    out = _replay_dict(r)
    steps = (await db.execute(
        select(EventDecision).where(EventDecision.replay_run_id == run_id)
        .order_by(EventDecision.event_time, EventDecision.id)
    )).scalars().all()
    out["steps"] = [_decision_dict(s) for s in steps]
    return out
