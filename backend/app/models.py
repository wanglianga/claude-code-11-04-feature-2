"""四层数据模型：

L1 原始事件层   raw_events
L2 标准业务账本层 business_events / fx_rates / payment_states / ledger_entries
L3 结算匹配层   settlement_batches / settlement_lines / bank_transactions /
                settlement_matches / discrepancies / discrepancy_actions
L4 月度汇总层   monthly_reports / monthly_report_versions

L1/L2 以及处置轨迹均为只追加（append-only）：PostgreSQL 触发器拒绝 UPDATE/DELETE。
"""
from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger, Boolean, DateTime, ForeignKey, Integer, Numeric, String, Text,
    UniqueConstraint, Index, func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .database import Base

# 金额统一 Numeric(20, 6)：记账币 2 位精度，外币与汇率保留 6 位原始信息
AMT = Numeric(20, 6)
RATE = Numeric(20, 10)


# ---------------------------------------------------------------- L1 原始事件层
class RawEvent(Base):
    __tablename__ = "raw_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    # 摄入幂等键：来源系统 + 来源消息唯一 ID（CDC LSN/消息 ID、webhook event_id、轮询游标+渠道ref）
    idempotency_key: Mapped[str] = mapped_column(String(200), nullable=False)
    source: Mapped[str] = mapped_column(String(20), nullable=False)
    # 来源系统中的发生时间（乱序到达时以它为准，而非摄入时间）
    event_occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.clock_timestamp()
    )
    topic: Mapped[str] = mapped_column(String(100), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    # 处理状态：RECEIVED -> PROCESSED / REJECTED
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="RECEIVED")
    reject_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        UniqueConstraint("source", "idempotency_key", name="uq_raw_event_source_key"),
        Index("ix_raw_events_occurred", "event_occurred_at"),
    )


# ---------------------------------------------------------------- L2 标准业务账本层
class BusinessEvent(Base):
    """归一化后的标准业务事件，只追加。同一业务事件可由多条 raw event 重放产生，仅入账一次。"""
    __tablename__ = "business_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    # 业务幂等键：同一业务事实（渠道参考号+事件类型+版本）永远只产生一条 business_event
    business_key: Mapped[str] = mapped_column(String(200), nullable=False, unique=True)
    entity_type: Mapped[str] = mapped_column(String(30), nullable=False)
    event_type: Mapped[str] = mapped_column(String(50), nullable=False)
    event_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    # 稳定业务键：订单号 / 支付尝试号 / 退款号 / 结算单号 ...
    entity_ref: Mapped[str] = mapped_column(String(100), nullable=False)
    # 渠道参考号（跨系统关联用）
    channel_ref: Mapped[str | None] = mapped_column(String(100), nullable=True)
    channel: Mapped[str | None] = mapped_column(String(30), nullable=True)
    merchant_id: Mapped[str | None] = mapped_column(String(50), nullable=True)
    order_ref: Mapped[str | None] = mapped_column(String(100), nullable=True)
    source: Mapped[str] = mapped_column(String(20), nullable=False)
    source_raw_event_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("raw_events.id"), nullable=True
    )
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.clock_timestamp()
    )
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    # 替代/修订链：结算单修订、汇率更正指向被替代事件
    supersedes_event_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("business_events.id"), nullable=True
    )

    __table_args__ = (
        Index("ix_be_entity_ref", "entity_ref"),
        Index("ix_be_order_ref", "order_ref"),
        Index("ix_be_channel_ref", "channel_ref"),
        Index("ix_be_event_time", "event_time"),
    )


class FxRate(Base):
    """历史汇率事实表，只追加；更正 = 新版本，永不覆盖旧事实。"""
    __tablename__ = "fx_rates"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    from_currency: Mapped[str] = mapped_column(String(8), nullable=False)
    to_currency: Mapped[str] = mapped_column(String(8), nullable=False)
    rate: Mapped[Decimal] = mapped_column(RATE, nullable=False)
    rate_source: Mapped[str] = mapped_column(String(40), nullable=False)
    # 适用时点：事件按 event_time 落到对应有效区间
    effective_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    effective_to: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    is_correction: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    business_event_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("business_events.id"), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.clock_timestamp()
    )

    __table_args__ = (
        UniqueConstraint(
            "from_currency", "to_currency", "effective_from", "version",
            name="uq_fx_rate_version",
        ),
    )


class OrderState(Base):
    """订单当前状态投影：按 event_time 重放，迟到的旧状态事件不能把订单打回旧状态。"""
    __tablename__ = "order_states"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    order_ref: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    channel: Mapped[str | None] = mapped_column(String(30), nullable=True)
    merchant_id: Mapped[str | None] = mapped_column(String(50), nullable=True)
    status: Mapped[str] = mapped_column(String(40), nullable=False)
    amount: Mapped[Decimal] = mapped_column(AMT, nullable=False, default=Decimal("0"))
    currency: Mapped[str] = mapped_column(String(8), nullable=False)
    last_event_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("business_events.id"), nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.clock_timestamp()
    )


class PaymentState(Base):
    """支付尝试当前状态投影（按 event_time 重放，不按到达顺序）。仅投影可更新，事件轨迹不变。"""
    __tablename__ = "payment_states"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    payment_ref: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    order_ref: Mapped[str | None] = mapped_column(String(100), nullable=True)
    channel: Mapped[str | None] = mapped_column(String(30), nullable=True)
    channel_ref: Mapped[str | None] = mapped_column(String(100), nullable=True)
    state: Mapped[str] = mapped_column(String(40), nullable=False)
    authorized_amount: Mapped[Decimal] = mapped_column(AMT, nullable=False, default=Decimal("0"))
    captured_amount: Mapped[Decimal] = mapped_column(AMT, nullable=False, default=Decimal("0"))
    refunded_amount: Mapped[Decimal] = mapped_column(AMT, nullable=False, default=Decimal("0"))
    chargeback_amount: Mapped[Decimal] = mapped_column(AMT, nullable=False, default=Decimal("0"))
    currency: Mapped[str] = mapped_column(String(8), nullable=False)
    # 驱动当前状态的最后一条业务事件（审计用）
    last_event_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("business_events.id"), nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.clock_timestamp()
    )


class LedgerEntry(Base):
    """标准账本分录，只追加。每个业务事件产生一组借贷平衡的分录。"""
    __tablename__ = "ledger_entries"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    business_event_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("business_events.id"), nullable=False
    )
    account: Mapped[str] = mapped_column(String(40), nullable=False)
    direction: Mapped[str] = mapped_column(String(2), nullable=False)  # DR / CR
    # 三种币种金额同时保留
    txn_amount: Mapped[Decimal] = mapped_column(AMT, nullable=False)
    txn_currency: Mapped[str] = mapped_column(String(8), nullable=False)
    settle_amount: Mapped[Decimal] = mapped_column(AMT, nullable=False)
    settle_currency: Mapped[str] = mapped_column(String(8), nullable=False)
    book_amount: Mapped[Decimal] = mapped_column(AMT, nullable=False)
    book_currency: Mapped[str] = mapped_column(String(8), nullable=False)
    # 汇率溯源
    txn_to_settle_rate: Mapped[Decimal | None] = mapped_column(RATE, nullable=True)
    settle_to_book_rate: Mapped[Decimal | None] = mapped_column(RATE, nullable=True)
    rate_source: Mapped[str | None] = mapped_column(String(60), nullable=True)
    rate_effective_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # 精度规则与原始金额
    decimal_places: Mapped[int] = mapped_column(Integer, nullable=False, default=2)
    raw_amount: Mapped[Decimal] = mapped_column(AMT, nullable=False)
    # 生效期间（YYYY-MM）：业务归属期，可能早于过账期（更正账/补单场景）
    effective_month: Mapped[str] = mapped_column(String(7), nullable=False)
    channel: Mapped[str | None] = mapped_column(String(30), nullable=True)
    merchant_id: Mapped[str | None] = mapped_column(String(50), nullable=True)
    order_ref: Mapped[str | None] = mapped_column(String(100), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.clock_timestamp()
    )

    __table_args__ = (
        Index("ix_le_event", "business_event_id"),
        Index("ix_le_order", "order_ref"),
        Index("ix_le_account", "account"),
    )


# ---------------------------------------------------------------- L3 结算匹配层
class SettlementBatch(Base):
    """渠道结算单（按渠道/商户/周期/币种）。修订生成新版本行，旧版本保留。"""
    __tablename__ = "settlement_batches"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    batch_ref: Mapped[str] = mapped_column(String(100), nullable=False)
    channel: Mapped[str] = mapped_column(String(30), nullable=False)
    merchant_id: Mapped[str] = mapped_column(String(50), nullable=False)
    period_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    period_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    currency: Mapped[str] = mapped_column(String(8), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    status: Mapped[str] = mapped_column(String(10), nullable=False, default="OPEN")
    gross_amount: Mapped[Decimal] = mapped_column(AMT, nullable=False, default=Decimal("0"))
    fee_amount: Mapped[Decimal] = mapped_column(AMT, nullable=False, default=Decimal("0"))
    net_amount: Mapped[Decimal] = mapped_column(AMT, nullable=False, default=Decimal("0"))
    business_event_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("business_events.id"), nullable=False
    )
    supersedes_batch_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("settlement_batches.id"), nullable=True
    )
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    closed_by: Mapped[str | None] = mapped_column(String(100), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.clock_timestamp()
    )

    __table_args__ = (
        UniqueConstraint("batch_ref", "version", name="uq_batch_ref_version"),
        Index("ix_batch_channel_period", "channel", "period_start", "period_end"),
    )

    lines: Mapped[list["SettlementLine"]] = relationship(back_populates="batch")


class SettlementLine(Base):
    """结算单行：一笔订单/退款/拒付在结算单中的体现。"""
    __tablename__ = "settlement_lines"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    batch_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("settlement_batches.id"), nullable=False
    )
    line_ref: Mapped[str] = mapped_column(String(100), nullable=False)
    order_ref: Mapped[str | None] = mapped_column(String(100), nullable=True)
    channel_ref: Mapped[str | None] = mapped_column(String(100), nullable=True)
    line_type: Mapped[str] = mapped_column(String(30), nullable=False)  # CAPTURE/REFUND/CHARGEBACK/FEE...
    gross_amount: Mapped[Decimal] = mapped_column(AMT, nullable=False)
    fee_amount: Mapped[Decimal] = mapped_column(AMT, nullable=False, default=Decimal("0"))
    net_amount: Mapped[Decimal] = mapped_column(AMT, nullable=False)
    currency: Mapped[str] = mapped_column(String(8), nullable=False)
    matched: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    __table_args__ = (
        UniqueConstraint("batch_id", "line_ref", name="uq_batch_line"),
        Index("ix_sl_order", "order_ref"),
        Index("ix_sl_channel_ref", "channel_ref"),
    )

    batch: Mapped[SettlementBatch] = relationship(back_populates="lines")


class BankTransaction(Base):
    """银行入账流水，一笔流水可合并多笔结算行（多对多通过 settlement_matches）。"""
    __tablename__ = "bank_transactions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    bank_ref: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    channel: Mapped[str] = mapped_column(String(30), nullable=False)
    currency: Mapped[str] = mapped_column(String(8), nullable=False)
    amount: Mapped[Decimal] = mapped_column(AMT, nullable=False)
    booked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    memo: Mapped[str | None] = mapped_column(Text, nullable=True)
    business_event_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("business_events.id"), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.clock_timestamp()
    )


class SettlementMatch(Base):
    """两层匹配：订单业务事件 <-> 结算行；结算行 <-> 银行流水（支持合并/拆分，金额可部分匹配）。"""
    __tablename__ = "settlement_matches"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    # LEVEL1: ORDER_TO_SETTLEMENT  LEVEL2: SETTLEMENT_TO_BANK
    match_level: Mapped[str] = mapped_column(String(30), nullable=False)
    # LEVEL2 时记录所属结算批次（一笔银行流水可跨批次合并分配）
    settlement_batch_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("settlement_batches.id"), nullable=True
    )
    business_event_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("business_events.id"), nullable=True
    )
    settlement_line_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("settlement_lines.id"), nullable=True
    )
    bank_transaction_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("bank_transactions.id"), nullable=True
    )
    # 本匹配承担的金额（结算币/银行币），支持一行拆多笔、一笔流水合多行
    allocated_amount: Mapped[Decimal] = mapped_column(AMT, nullable=False)
    currency: Mapped[str] = mapped_column(String(8), nullable=False)
    status: Mapped[str] = mapped_column(String(10), nullable=False, default="PROPOSED")
    # AUTO = 引擎建议；MANUAL = 财务确认/手工建立
    match_method: Mapped[str] = mapped_column(String(10), nullable=False, default="AUTO")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.clock_timestamp()
    )
    confirmed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("ix_match_line", "settlement_line_id"),
        Index("ix_match_bank", "bank_transaction_id"),
        Index("ix_match_event", "business_event_id"),
    )


class Discrepancy(Base):
    """可关闭差异清单中的一条差异。"""
    __tablename__ = "discrepancies"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    batch_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("settlement_batches.id"), nullable=False
    )
    kind: Mapped[str] = mapped_column(String(10), nullable=False)  # AMOUNT/FEE/FX/TIMING/UNKNOWN
    status: Mapped[str] = mapped_column(String(10), nullable=False, default="OPEN")
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    order_ref: Mapped[str | None] = mapped_column(String(100), nullable=True)
    channel_ref: Mapped[str | None] = mapped_column(String(100), nullable=True)
    settlement_line_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("settlement_lines.id"), nullable=True
    )
    bank_transaction_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("bank_transactions.id"), nullable=True
    )
    # 差异金额（记账币，带方向：正=渠道多给/我方少收 视角统一为账面-实际）
    expected_amount: Mapped[Decimal] = mapped_column(AMT, nullable=False, default=Decimal("0"))
    actual_amount: Mapped[Decimal] = mapped_column(AMT, nullable=False, default=Decimal("0"))
    diff_amount: Mapped[Decimal] = mapped_column(AMT, nullable=False, default=Decimal("0"))
    currency: Mapped[str] = mapped_column(String(8), nullable=False)
    detail: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    assignee: Mapped[str | None] = mapped_column(String(100), nullable=True)
    # 关联的调账建议（人工创建，过账后生成 MANUAL_ADJUSTMENT 业务事件）
    adjustment_event_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("business_events.id"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.clock_timestamp()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.clock_timestamp()
    )

    __table_args__ = (
        Index("ix_disc_batch_status", "batch_id", "status"),
        Index("ix_disc_kind", "kind"),
    )


class DiscrepancyAction(Base):
    """差异处置轨迹，只追加：认领/附件/调账建议/调账过账/关闭/重开。"""
    __tablename__ = "discrepancy_actions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    discrepancy_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("discrepancies.id"), nullable=False
    )
    action_type: Mapped[str] = mapped_column(String(20), nullable=False)
    operator: Mapped[str] = mapped_column(String(100), nullable=False)
    comment: Mapped[str | None] = mapped_column(Text, nullable=True)
    attachment: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.clock_timestamp()
    )


# ---------------------------------------------------------------- L4 月度汇总层
class MonthlyReport(Base):
    """月度报表头：一个渠道+商户+币种+月份一条，关闭后不可改，更正产生新版本。"""
    __tablename__ = "monthly_reports"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    channel: Mapped[str] = mapped_column(String(30), nullable=False)
    merchant_id: Mapped[str] = mapped_column(String(50), nullable=False)
    currency: Mapped[str] = mapped_column(String(8), nullable=False)
    report_month: Mapped[str] = mapped_column(String(7), nullable=False)  # YYYY-MM
    status: Mapped[str] = mapped_column(String(10), nullable=False, default="DRAFT")
    current_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.clock_timestamp()
    )

    __table_args__ = (
        UniqueConstraint("channel", "merchant_id", "currency", "report_month",
                         name="uq_monthly_report"),
    )


class MonthlyReportVersion(Base):
    """报表版本快照（不可变）。关闭后任何更正只追加新版本，旧版本标 SUPERSEDED 并保留。"""
    __tablename__ = "monthly_report_versions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    report_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("monthly_reports.id"), nullable=False
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    version_status: Mapped[str] = mapped_column(String(12), nullable=False, default="CURRENT")
    snapshot: Mapped[dict] = mapped_column(JSONB, nullable=False)
    # 触发本版本的更正事件（FX_RATE_CORRECTED / SETTLEMENT_BATCH_REVISED / 补单 ...）
    reason: Mapped[str | None] = mapped_column(String(300), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.clock_timestamp()
    )

    __table_args__ = (
        UniqueConstraint("report_id", "version", name="uq_report_version"),
    )


APPEND_ONLY_TABLES = [
    "raw_events",
    "business_events",
    "fx_rates",
    "ledger_entries",
    "discrepancy_actions",
    "monthly_report_versions",
    # 特性 04：状态机决策留痕与影子重放记录均为证据，只追加
    "event_decisions",
    "replay_runs",
]


# ================================================================
# 特性 04：支付状态机 —— 决策留痕 / 待关联队列 / 冲突待裁决 / 隔离影子重放
# ================================================================
class EventDecision(Base):
    """状态机对每条事件的评估结论，只追加。

    生产决策 replay_run_id 为空；隔离重放产生的影子步骤 replay_run_id 指向对应重放，
    decision=SHADOW。任何拒绝/冲突/无法关联都在此留下可读原因，而不是静默丢弃。
    """
    __tablename__ = "event_decisions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    raw_event_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("raw_events.id"), nullable=True
    )
    business_event_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("business_events.id"), nullable=True
    )
    # 非空 = 影子重放步骤；为空 = 生产摄入时的真实决策
    replay_run_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("replay_runs.id"), nullable=True
    )
    order_ref: Mapped[str | None] = mapped_column(String(100), nullable=True)
    payment_ref: Mapped[str | None] = mapped_column(String(100), nullable=True)
    channel_ref: Mapped[str | None] = mapped_column(String(100), nullable=True)
    event_type: Mapped[str] = mapped_column(String(50), nullable=False)
    source: Mapped[str] = mapped_column(String(20), nullable=False)
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    # ACCEPTED / REJECTED / DUPLICATE / UNLINKED / CONFLICT / SHADOW
    decision: Mapped[str] = mapped_column(String(20), nullable=False)
    reason_code: Mapped[str | None] = mapped_column(String(60), nullable=True)
    reason_detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    # 转移前后状态快照（拒绝时 to_state 与 from_state 相同）
    from_state: Mapped[str | None] = mapped_column(String(40), nullable=True)
    to_state: Mapped[str | None] = mapped_column(String(40), nullable=True)
    conflict_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("state_conflicts.id"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.clock_timestamp()
    )

    __table_args__ = (
        Index("ix_ed_order", "order_ref"),
        Index("ix_ed_payment", "payment_ref"),
        Index("ix_ed_decision", "decision"),
        Index("ix_ed_replay", "replay_run_id"),
        Index("ix_ed_event_time", "event_time"),
    )


class UnlinkedEvent(Base):
    """待关联队列：无法关联到任何订单/支付的原始事件。生命周期列可更新（非只追加）。

    原始报文仍保存在 raw_events（证据不可变），本表只承载“待人工关联/丢弃”的处置状态。
    """
    __tablename__ = "unlinked_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    raw_event_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("raw_events.id"), nullable=False, unique=True
    )
    idempotency_key: Mapped[str] = mapped_column(String(200), nullable=False)
    source: Mapped[str] = mapped_column(String(20), nullable=False)
    event_type: Mapped[str] = mapped_column(String(50), nullable=False)
    topic: Mapped[str] = mapped_column(String(100), nullable=False)
    # 报文中携带的关联线索（可能缺失或指向不存在的实体）
    order_ref_hint: Mapped[str | None] = mapped_column(String(100), nullable=True)
    payment_ref_hint: Mapped[str | None] = mapped_column(String(100), nullable=True)
    channel_ref_hint: Mapped[str | None] = mapped_column(String(100), nullable=True)
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    reason_code: Mapped[str] = mapped_column(String(60), nullable=False)
    reason_detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    # PENDING -> LINKED / DISCARDED
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="PENDING")
    linked_order_ref: Mapped[str | None] = mapped_column(String(100), nullable=True)
    linked_payment_ref: Mapped[str | None] = mapped_column(String(100), nullable=True)
    resolved_by: Mapped[str | None] = mapped_column(String(100), nullable=True)
    resolution_comment: Mapped[str | None] = mapped_column(Text, nullable=True)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.clock_timestamp()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.clock_timestamp()
    )

    __table_args__ = (
        Index("ix_unlinked_status", "status"),
        Index("ix_unlinked_event_time", "event_time"),
    )


class StateConflict(Base):
    """状态冲突：乱序/同业务时间互斥事件无法自动定序时，保留两条记录并待人工裁决。

    冲突期间两条业务事件都已只追加留存、均不覆盖状态；裁决后选定胜者驱动状态，败者置 REJECTED。
    本表为处置工作表，裁决列允许更新（非只追加）。
    """
    __tablename__ = "state_conflicts"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    order_ref: Mapped[str | None] = mapped_column(String(100), nullable=True)
    payment_ref: Mapped[str | None] = mapped_column(String(100), nullable=True)
    # SAME_TIMESTAMP / DUPLICATED_FACT
    conflict_type: Mapped[str] = mapped_column(String(30), nullable=False)
    # PENDING -> RESOLVED / DISCARDED
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="PENDING")
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    detail: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    # 裁决结果：胜出/落败的业务事件
    winning_event_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("business_events.id"), nullable=True
    )
    losing_event_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("business_events.id"), nullable=True
    )
    resolved_by: Mapped[str | None] = mapped_column(String(100), nullable=True)
    resolution_comment: Mapped[str | None] = mapped_column(Text, nullable=True)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.clock_timestamp()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.clock_timestamp()
    )

    __table_args__ = (
        Index("ix_conflict_payment_status", "payment_ref", "status"),
        Index("ix_conflict_order", "order_ref"),
    )


class ReplayRun(Base):
    """隔离影子重放运行记录，只追加。

    每次重放固化生产状态快照、影子终态、逐步骤影子决策与差异结论；
    write_back 恒为 False —— 影子重放只能对比，不能回写生产账本。
    """
    __tablename__ = "replay_runs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    run_ref: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    # 固定 SHADOW（隔离模式）
    mode: Mapped[str] = mapped_column(String(20), nullable=False, default="SHADOW")
    # ORDER / PAYMENT / UNLINKED / RAW_EVENT
    scope_type: Mapped[str] = mapped_column(String(20), nullable=False)
    scope_ref: Mapped[str] = mapped_column(String(100), nullable=False)
    trigger_raw_event_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("raw_events.id"), nullable=True
    )
    operator: Mapped[str] = mapped_column(String(100), nullable=False)
    production_state: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    shadow_state: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    # MATCHED / DIVERGED / BLOCKED
    outcome: Mapped[str] = mapped_column(String(20), nullable=False)
    divergence_detail: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    step_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # 恒为 False：显式记录“未回写生产”，作为隔离保证的审计字段
    write_back: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.clock_timestamp()
    )

    __table_args__ = (
        Index("ix_replay_scope", "scope_type", "scope_ref"),
    )
