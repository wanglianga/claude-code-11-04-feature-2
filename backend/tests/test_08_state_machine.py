"""特性 04 支付状态机验收。

覆盖：
  - 接受/拒绝原因留痕（类型不变量、金额不变量）
  - 无法关联事件进入待关联队列（原始证据保留、不进账本）
  - 同业务时间互斥冲突：两条业务记录都保留并 PENDING 待裁决，互不覆盖状态
  - 乱序到达按业务时间推导，不按到达顺序覆盖
  - 隔离影子重放只生成影子状态，write_back 恒 False，不回写生产账本
  - 待关联人工关联后重入、冲突人工裁决
  - 状态机新证据表的只追加保护
"""
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.demo import events as demo
from app.demo.loader import ingest
from app.enums import EventDecision as VD, RejectReason as RR, UnlinkedStatus
from app.models import (
    BusinessEvent, EventDecision, LedgerEntry, PaymentState, ReplayRun,
    StateConflict, UnlinkedEvent,
)
from app.services import statemachine as sm

OPERATOR = "finance.chen"


async def _load(db):
    for ev in [*demo.fx_events(), *demo.order_payment_events(),
               *demo.state_machine_events()]:
        await ingest(db, ev)
    await db.commit()


async def _decisions(db, **filters):
    stmt = select(EventDecision).where(EventDecision.replay_run_id.is_(None))
    for col, val in filters.items():
        stmt = stmt.where(getattr(EventDecision, col) == val)
    return (await db.execute(stmt.order_by(EventDecision.id))).scalars().all()


# ---------------------------------------------------------------- 接受/拒绝原因
async def test_accepted_decisions_record_transition(db):
    await _load(db)
    # P-001 授权接受，且记录转移前后状态
    auth = await _decisions(db, payment_ref="P-001", event_type="PAYMENT_AUTHORIZED")
    assert auth and auth[0].decision == VD.ACCEPTED
    assert auth[0].to_state == "AUTHORIZED"
    # 多来源（POLL 创建 + WEBHOOK 授权）均接受
    p11 = await _decisions(db, payment_ref="P-011")
    verdicts = {d.event_type: d.decision for d in p11}
    assert verdicts["PAYMENT_CREATED"] == VD.ACCEPTED
    assert verdicts["PAYMENT_AUTHORIZED"] == VD.ACCEPTED
    pay11 = (await db.execute(
        select(PaymentState).where(PaymentState.payment_ref == "P-011")
    )).scalar_one()
    assert pay11.state == "AUTHORIZED"


async def test_illegal_transitions_rejected_with_reason(db):
    await _load(db)
    # R1：已捕获后更晚时间又授权 -> 拒绝，且不产生业务事件/分录
    late_auth = await _decisions(
        db, payment_ref="P-003", event_type="PAYMENT_AUTHORIZED", decision=VD.REJECTED
    )
    assert len(late_auth) == 1
    assert late_auth[0].reason_code == RR.ILLEGAL_AUTH_AFTER_CAPTURE
    assert late_auth[0].business_event_id is None  # 拒绝不生成业务事件

    # R2：退款超过捕获 -> 拒绝
    over_refund = await _decisions(
        db, payment_ref="P-004", event_type="REFUND_SUCCEEDED", decision=VD.REJECTED
    )
    assert len(over_refund) == 1
    assert over_refund[0].reason_code == RR.ILLEGAL_REFUND_EXCEEDS_CAPTURE

    # 生产状态/金额未被拒绝事件改变
    p3 = (await db.execute(
        select(PaymentState).where(PaymentState.payment_ref == "P-003")
    )).scalar_one()
    assert p3.state == "CAPTURED"
    p4 = (await db.execute(
        select(PaymentState).where(PaymentState.payment_ref == "P-004")
    )).scalar_one()
    assert p4.refunded_amount == Decimal("0")


async def test_duplicate_business_fact_recorded(db):
    await _load(db)
    dups = await _decisions(db, decision=VD.DUPLICATE)
    assert dups, "CDC 重放/轮询补拉的同业务事实应留 DUPLICATE 决策"
    assert all(d.reason_code == RR.DUP_BUSINESS_KEY for d in dups)


# ---------------------------------------------------------------- 待关联队列
async def test_unlinkable_events_enter_queue_not_ledger(db):
    await _load(db)
    queued = await sm.list_unlinked(db, UnlinkedStatus.PENDING)
    assert len(queued) == 2
    codes = {u.reason_code for u in queued}
    assert codes == {
        RR.UNLINKED_NO_ORDER_OR_PAYMENT,
        RR.UNLINKED_UNKNOWN_ORDER,
    }
    # 原始 raw 证据保留，但没有对应 business_event
    for u in queued:
        assert u.payload  # 原始报文留存
        dec = await _decisions(db, decision=VD.UNLINKED, raw_event_id=u.raw_event_id)
        assert len(dec) == 1
    # 待关联事件的 business_key 未进入业务账本
    be_keys = (await db.execute(
        select(BusinessEvent.business_key).where(
            BusinessEvent.business_key.in_([
                "REFUND_SUCCEEDED:R-UNKNOWN-1:v1",
                "REFUND_SUCCEEDED:R-999:v1",
            ])
        )
    )).all()
    assert be_keys == []


async def test_unlinked_shadow_replay_is_blocked(db):
    await _load(db)
    queued = await sm.list_unlinked(db, UnlinkedStatus.PENDING)
    run = await sm.replay_unlinked(db, queued[0].id, OPERATOR)
    await db.commit()
    # 隔离空态下无捕获资金即退款 -> 拒绝，重放 BLOCKED
    assert run["outcome"] == "BLOCKED"
    assert run["write_back"] is False
    assert run["divergence_detail"]["reason_code"] == RR.ILLEGAL_REFUND_EXCEEDS_CAPTURE


async def test_manual_link_reprocesses_event(db):
    await _load(db)
    queued = await sm.list_unlinked(db, UnlinkedStatus.PENDING)
    target = next(u for u in queued if u.reason_code == RR.UNLINKED_NO_ORDER_OR_PAYMENT)
    out = await sm.link_unlinked(
        db, target.id, OPERATOR, order_ref="ORD-003",
        payment_ref="R-UNKNOWN-1", comment="人工核实归属 ORD-003",
    )
    await db.commit()
    assert out["status"] == UnlinkedStatus.LINKED
    assert out["linked_order_ref"] == "ORD-003"
    assert out["reprocess"]["duplicate"] is False
    # 重入后生成业务事件并在状态机留痕
    refreshed = await db.get(UnlinkedEvent, target.id)
    assert refreshed.status == UnlinkedStatus.LINKED


# ---------------------------------------------------------------- 冲突待裁决
async def test_same_timestamp_conflict_keeps_two_records(db):
    await _load(db)
    conflicts = await sm.list_conflicts(db, "PENDING")
    assert len(conflicts) == 1
    c = conflicts[0]
    assert c.payment_ref == "P-010"
    # 两条互斥业务事件都只追加保留
    assert c.detail["incoming_event_id"] and c.detail["existing_event_id"]
    pair = (await db.execute(
        select(BusinessEvent).where(BusinessEvent.id.in_(
            [c.detail["incoming_event_id"], c.detail["existing_event_id"]]
        ))
    )).scalars().all()
    assert {b.event_type for b in pair} == {"PAYMENT_CAPTURED", "PAYMENT_FAILED"}
    # 两条都标记 CONFLICT
    conflict_dec = await _decisions(db, payment_ref="P-010", decision=VD.CONFLICT)
    assert len(conflict_dec) == 2
    # 生产状态不被互斥事件覆盖：捕获已生效，失败事件挂起
    p10 = (await db.execute(
        select(PaymentState).where(PaymentState.payment_ref == "P-010")
    )).scalar_one()
    assert p10.state == "CAPTURED"
    assert p10.captured_amount == Decimal("40")


async def test_conflict_adjudication_picks_winner(db):
    await _load(db)
    c = (await sm.list_conflicts(db, "PENDING"))[0]
    capture_be_id = c.detail["existing_event_id"]   # 捕获为胜
    failed_be_id = c.detail["incoming_event_id"]    # 失败为败
    out = await sm.resolve_conflict(
        db, c.id, capture_be_id, OPERATOR, "渠道对账确认捕获成功，失败通知为误报",
    )
    await db.commit()
    assert out["status"] == "RESOLVED"
    assert out["winning_event_id"] == capture_be_id
    assert out["losing_event_id"] == failed_be_id
    # 败者留 REJECTED 裁决决策，业务记录仍保留
    loser_dec = await _decisions(
        db, business_event_id=failed_be_id, decision=VD.REJECTED
    )
    assert loser_dec and loser_dec[0].reason_code == "ADJUDICATED_LOSER"
    loser_be = await db.get(BusinessEvent, failed_be_id)
    assert loser_be is not None  # 只追加，不删除
    p10 = (await db.execute(
        select(PaymentState).where(PaymentState.payment_ref == "P-010")
    )).scalar_one()
    assert p10.state == "CAPTURED"


async def test_shadow_replay_after_adjudication_includes_winner(db):
    """裁决胜出者即使留有历史 CONFLICT 决策，裁决后影子重放也必须复算到它。"""
    await _load(db)
    c = (await sm.list_conflicts(db, "PENDING"))[0]
    await sm.resolve_conflict(
        db, c.id, c.detail["existing_event_id"], OPERATOR, "确认捕获胜出",
    )
    await db.commit()
    run = await sm.replay_order(db, "ORD-010", OPERATOR)
    await db.commit()
    # 影子终态应复算出 CAPTURED（捕获为胜），且与生产一致
    assert run["outcome"] == "MATCHED"
    shadow_p10 = run["shadow_state"]["P-010"]
    assert shadow_p10["state"] == "CAPTURED"
    assert Decimal(shadow_p10["captured_amount"]) == Decimal("40")
    # 影子步骤包含捕获事件、不含落败的失败事件
    types = {s.event_type for s in (await db.execute(
        select(EventDecision).where(EventDecision.replay_run_id == run["id"])
    )).scalars().all()}
    assert "PAYMENT_CAPTURED" in types
    assert "PAYMENT_FAILED" not in types


# ---------------------------------------------------------------- 乱序安全
async def test_out_of_order_arrival_uses_business_time(db):
    await _load(db)
    # P-002：捕获（10-04）先到、授权（10-03）晚到，最终仍 CAPTURED，金额不翻倍
    p2 = (await db.execute(
        select(PaymentState).where(PaymentState.payment_ref == "P-002")
    )).scalar_one()
    assert p2.state == "CAPTURED"
    assert p2.captured_amount == Decimal("120")
    assert p2.authorized_amount == Decimal("120")
    # 迟到授权被接受但带「按业务时间归位、不回退状态」说明
    late_auth = await _decisions(
        db, payment_ref="P-002", event_type="PAYMENT_AUTHORIZED"
    )
    assert late_auth[0].decision == VD.ACCEPTED
    assert "迟到" in (late_auth[0].reason_detail or "")


# ---------------------------------------------------------------- 影子重放隔离
async def test_shadow_replay_never_writes_back(db):
    await _load(db)
    be_before = len((await db.execute(select(BusinessEvent.id))).all())
    le_before = len((await db.execute(select(LedgerEntry.id))).all())

    run = await sm.replay_order(db, "ORD-002", OPERATOR)
    await db.commit()

    be_after = len((await db.execute(select(BusinessEvent.id))).all())
    le_after = len((await db.execute(select(LedgerEntry.id))).all())
    assert be_after == be_before       # 重放不新增业务事件
    assert le_after == le_before       # 重放不新增账本分录
    assert run["write_back"] is False
    assert run["mode"] == "SHADOW"

    # 影子步骤带 replay_run_id，且决策类型为 SHADOW
    shadow_steps = (await db.execute(
        select(EventDecision).where(EventDecision.replay_run_id == run["id"])
    )).scalars().all()
    assert len(shadow_steps) == run["step_count"]
    assert all(s.decision == VD.SHADOW for s in shadow_steps)

    # 重放记录本身只追加：篡改必须被触发器拒绝
    from sqlalchemy import text
    with pytest.raises(Exception) as exc:
        await db.execute(
            text("UPDATE replay_runs SET write_back = true WHERE id = :rid"),
            {"rid": run["id"]},
        )
    assert "append-only" in str(exc.value)
    await db.rollback()


async def test_shadow_replay_matches_production_when_clean(db):
    await _load(db)
    run = await sm.replay_order(db, "ORD-001", OPERATOR)
    await db.commit()
    # 干净订单：影子终态与生产态一致
    assert run["outcome"] == "MATCHED"
    assert run["divergence_detail"] == {}


async def test_all_replay_runs_marked_no_write_back(db):
    await _load(db)
    await sm.replay_order(db, "ORD-001", OPERATOR)
    queued = await sm.list_unlinked(db, UnlinkedStatus.PENDING)
    await sm.replay_unlinked(db, queued[0].id, OPERATOR)
    await db.commit()
    runs = (await db.execute(select(ReplayRun))).scalars().all()
    assert runs and all(r.write_back is False for r in runs)


# ---------------------------------------------------------------- 证据表只追加
async def test_event_decisions_append_only(db):
    await _load(db)
    from sqlalchemy import text
    d_id = (await db.execute(select(EventDecision.id).limit(1))).scalar_one()
    with pytest.raises(Exception) as exc:
        await db.execute(
            text("UPDATE event_decisions SET decision = 'REJECTED' WHERE id = :did"),
            {"did": d_id},
        )
    assert "append-only" in str(exc.value)
    await db.rollback()
