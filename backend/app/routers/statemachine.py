"""支付状态机视图 API。

提供：
  - 按订单的状态机视图（授权/捕获/退款/拒付/反转、webhook/轮询事件的接受/拒绝原因时间线）
  - 待关联队列（无法关联事件）及其人工关联 / 丢弃 / 隔离影子重放
  - 状态冲突的待裁决队列与人工裁决
  - 隔离（SHADOW）重放运行记录与影子步骤

所有重放接口只读生产数据、只写 replay_runs/SHADOW 决策，write_back 恒为 False。
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_db
from ..models import EventDecision
from ..schemas import (
    DiscardConflictIn, DiscardUnlinkedIn, LinkUnlinkedIn, ReplayIn, ResolveConflictIn,
)
from ..services import statemachine as sm

router = APIRouter(prefix="/api/state-machine", tags=["state-machine"])


# ---------------------------------------------------------------- 订单状态机视图
@router.get("/orders")
async def orders(db: AsyncSession = Depends(get_db)):
    refs = await sm.list_order_refs(db)
    return {"order_refs": refs}


@router.get("/orders/{order_ref}")
async def order_machine(order_ref: str, db: AsyncSession = Depends(get_db)):
    try:
        return await sm.build_order_machine_view(db, order_ref)
    except Exception as exc:
        raise HTTPException(400, str(exc))


@router.get("/decisions")
async def decisions(order_ref: str | None = None, payment_ref: str | None = None,
                    decision: str | None = None,
                    limit: int = Query(500, le=2000),
                    db: AsyncSession = Depends(get_db)):
    stmt = (
        select(EventDecision)
        .where(EventDecision.replay_run_id.is_(None))
        .order_by(EventDecision.event_time, EventDecision.id)
    )
    if order_ref:
        stmt = stmt.where(EventDecision.order_ref == order_ref)
    if payment_ref:
        stmt = stmt.where(EventDecision.payment_ref == payment_ref)
    if decision:
        stmt = stmt.where(EventDecision.decision == decision)
    rows = (await db.execute(stmt.limit(limit))).scalars().all()
    return [sm._decision_dict(d) for d in rows]


# ---------------------------------------------------------------- 待关联队列
@router.get("/unlinked")
async def list_unlinked(status: str | None = "PENDING",
                        db: AsyncSession = Depends(get_db)):
    rows = await sm.list_unlinked(db, status)
    return [sm._unlinked_dict(u) for u in rows]


@router.post("/unlinked/{unlinked_id}/link")
async def link_unlinked(unlinked_id: int, data: LinkUnlinkedIn,
                        db: AsyncSession = Depends(get_db)):
    try:
        out = await sm.link_unlinked(
            db, unlinked_id, data.operator, data.order_ref,
            data.payment_ref, data.comment,
        )
        await db.commit()
        return out
    except Exception as exc:
        await db.rollback()
        raise HTTPException(400, str(exc))


@router.post("/unlinked/{unlinked_id}/discard")
async def discard_unlinked(unlinked_id: int, data: DiscardUnlinkedIn,
                           db: AsyncSession = Depends(get_db)):
    try:
        out = await sm.discard_unlinked(db, unlinked_id, data.operator, data.comment)
        await db.commit()
        return out
    except Exception as exc:
        await db.rollback()
        raise HTTPException(400, str(exc))


@router.post("/unlinked/{unlinked_id}/replay")
async def replay_unlinked(unlinked_id: int, data: ReplayIn,
                          db: AsyncSession = Depends(get_db)):
    try:
        out = await sm.replay_unlinked(db, unlinked_id, data.operator)
        await db.commit()
        return out
    except Exception as exc:
        await db.rollback()
        raise HTTPException(400, str(exc))


# ---------------------------------------------------------------- 冲突裁决
@router.get("/conflicts")
async def list_conflicts(status: str | None = "PENDING",
                         db: AsyncSession = Depends(get_db)):
    rows = await sm.list_conflicts(db, status)
    return [sm._conflict_dict(c) for c in rows]


@router.post("/conflicts/{conflict_id}/resolve")
async def resolve_conflict(conflict_id: int, data: ResolveConflictIn,
                           db: AsyncSession = Depends(get_db)):
    try:
        out = await sm.resolve_conflict(
            db, conflict_id, data.winning_event_id, data.operator, data.comment,
        )
        await db.commit()
        return out
    except Exception as exc:
        await db.rollback()
        raise HTTPException(400, str(exc))


@router.post("/conflicts/{conflict_id}/discard")
async def discard_conflict(conflict_id: int, data: DiscardConflictIn,
                           db: AsyncSession = Depends(get_db)):
    try:
        out = await sm.discard_conflict(db, conflict_id, data.operator, data.comment)
        await db.commit()
        return out
    except Exception as exc:
        await db.rollback()
        raise HTTPException(400, str(exc))


# ---------------------------------------------------------------- 隔离影子重放
@router.post("/replay/order/{order_ref}")
async def replay_order(order_ref: str, data: ReplayIn,
                       db: AsyncSession = Depends(get_db)):
    try:
        out = await sm.replay_order(db, order_ref, data.operator)
        await db.commit()
        return out
    except Exception as exc:
        await db.rollback()
        raise HTTPException(400, str(exc))


@router.post("/replay/raw/{raw_event_id}")
async def replay_raw(raw_event_id: int, data: ReplayIn,
                     db: AsyncSession = Depends(get_db)):
    try:
        out = await sm.replay_raw_event(db, raw_event_id, data.operator)
        await db.commit()
        return out
    except Exception as exc:
        await db.rollback()
        raise HTTPException(400, str(exc))


@router.get("/replays")
async def list_replays(limit: int = Query(100, le=500),
                       db: AsyncSession = Depends(get_db)):
    return await sm.list_replays(db, limit)


@router.get("/replays/{run_id}")
async def get_replay(run_id: int, db: AsyncSession = Depends(get_db)):
    try:
        return await sm.get_replay(db, run_id)
    except Exception as exc:
        raise HTTPException(400, str(exc))
