"""事件类型与状态枚举（以字符串存储，避免数据库 ENUM 迁移成本）。"""


class EventSource:
    CDC = "CDC"              # 商城订单库变更捕获
    WEBHOOK = "WEBHOOK"      # 支付渠道异步推送
    POLL = "POLL"            # 渠道轮询补拉
    MANUAL = "MANUAL"        # 人工补单 / 手工调账

class EntityType:
    ORDER = "ORDER"
    PAYMENT_ATTEMPT = "PAYMENT_ATTEMPT"
    AUTHORIZATION = "AUTHORIZATION"
    CAPTURE = "CAPTURE"
    REFUND = "REFUND"
    CHARGEBACK = "CHARGEBACK"
    SETTLEMENT = "SETTLEMENT"
    BANK_TXN = "BANK_TXN"
    MANUAL_ADJ = "MANUAL_ADJ"
    FX_RATE = "FX_RATE"


class EventType:
    # 订单
    ORDER_CREATED = "ORDER_CREATED"
    ORDER_PAID = "ORDER_PAID"
    ORDER_STATUS_CHANGED = "ORDER_STATUS_CHANGED"
    # 支付生命周期
    PAYMENT_CREATED = "PAYMENT_CREATED"
    PAYMENT_AUTHORIZED = "PAYMENT_AUTHORIZED"
    PAYMENT_AUTH_EXPIRED = "PAYMENT_AUTH_EXPIRED"
    PAYMENT_CAPTURED = "PAYMENT_CAPTURED"
    PAYMENT_FAILED = "PAYMENT_FAILED"
    REFUND_REQUESTED = "REFUND_REQUESTED"
    REFUND_SUCCEEDED = "REFUND_SUCCEEDED"
    CHARGEBACK_OPENED = "CHARGEBACK_OPENED"
    CHARGEBACK_REVERSED = "CHARGEBACK_REVERSED"
    # 结算 / 银行
    SETTLEMENT_BATCH_ISSUED = "SETTLEMENT_BATCH_ISSUED"
    SETTLEMENT_BATCH_REVISED = "SETTLEMENT_BATCH_REVISED"
    BANK_DEPOSIT_RECEIVED = "BANK_DEPOSIT_RECEIVED"
    # 人工 / 汇率
    MANUAL_ADJUSTMENT = "MANUAL_ADJUSTMENT"
    FX_RATE_PUBLISHED = "FX_RATE_PUBLISHED"
    FX_RATE_CORRECTED = "FX_RATE_CORRECTED"


class PaymentState:
    CREATED = "CREATED"
    AUTHORIZED = "AUTHORIZED"
    CAPTURED = "CAPTURED"
    PARTIALLY_REFUNDED = "PARTIALLY_REFUNDED"
    REFUNDED = "REFUNDED"
    CHARGEBACK = "CHARGEBACK"
    CHARGEBACK_REVERSED = "CAPTURED"  # 拒付反转后回到捕获态（复用状态，事件轨迹保留区别）
    FAILED = "FAILED"


class BatchStatus:
    OPEN = "OPEN"
    CLOSED = "CLOSED"


class MatchStatus:
    PROPOSED = "PROPOSED"
    CONFIRMED = "CONFIRMED"


class DiscrepancyKind:
    AMOUNT = "AMOUNT"      # 金额差
    FEE = "FEE"            # 手续费差
    FX = "FX"              # 汇率差
    TIMING = "TIMING"      # 时间差
    UNKNOWN = "UNKNOWN"    # 未知差（禁止自动核销）


class DiscrepancyStatus:
    OPEN = "OPEN"
    CLAIMED = "CLAIMED"
    RESOLVED = "RESOLVED"   # 已创建调账建议/已调账，待批次关闭
    CLOSED = "CLOSED"


class ActionType:
    CLAIM = "CLAIM"
    ATTACH = "ATTACH"
    ADJUST_SUGGEST = "ADJUST_SUGGEST"
    ADJUST_POST = "ADJUST_POST"
    CLOSE = "CLOSE"
    REOPEN = "REOPEN"


class ReportStatus:
    DRAFT = "DRAFT"
    CLOSED = "CLOSED"


class VersionStatus:
    CURRENT = "CURRENT"
    SUPERSEDED = "SUPERSEDED"


# 账本科目（双分录记账）
class Account:
    REVENUE = "REVENUE"                       # 收入（贷）
    AR = "AR"                                 # 应收账款（订单侧）
    SETTLEMENT_RECEIVABLE = "SETTLEMENT_RECEIVABLE"  # 渠道结算应收
    CASH = "CASH"                             # 银行资金
    PAYMENT_FEE = "PAYMENT_FEE"               # 手续费
    REFUND_PAYABLE = "REFUND_PAYABLE"         # 退款
    CHARGEBACK = "CHARGEBACK"                 # 拒付
    FX_GAIN_LOSS = "FX_GAIN_LOSS"             # 汇率更正损益
    MANUAL_ADJUSTMENT = "MANUAL_ADJUSTMENT"   # 手工调账
    ROUNDING = "ROUNDING"                     # 舍入差


# ================================================================
# 支付状态机（特性 04）：接受/拒绝决策、待关联队列、冲突待裁决、隔离影子重放
# ================================================================

class EventDecision:
    """单条事件经状态机评估后的处置结论（只追加留痕）。"""
    ACCEPTED = "ACCEPTED"            # 接受：转移合法，驱动（或不改变）当前状态
    REJECTED = "REJECTED"            # 拒绝：违反类型约束/业务时间顺序
    DUPLICATE = "DUPLICATE"          # 同一业务事实重复到达（两层幂等拦截，状态机不重复处理）
    UNLINKED = "UNLINKED"            # 无法关联到任何订单/支付，进入待关联队列
    CONFLICT = "CONFLICT"            # 与既有事件同业务时间且互斥，保留两条并待人工裁决
    SHADOW = "SHADOW"                # 隔离重放产生的影子步骤（不回写生产账本）


class RejectReason:
    """拒绝/特殊处置原因码（机器可读，前端可直接映射中文说明）。"""
    DUP_BUSINESS_KEY = "DUP_BUSINESS_KEY"                  # 业务键重复，账本保持不变
    DUP_RAW_MESSAGE = "DUP_RAW_MESSAGE"                    # 来源消息重复（L1 幂等）
    UNLINKED_NO_ORDER_OR_PAYMENT = "UNLINKED_NO_ORDER_OR_PAYMENT"  # 缺少订单号/支付号/渠道参考，无法关联
    UNLINKED_UNKNOWN_ORDER = "UNLINKED_UNKNOWN_ORDER"      # 引用的订单/支付在系统中不存在
    ILLEGAL_BEFORE_CREATED = "ILLEGAL_BEFORE_CREATED"      # 支付尚未创建即收到生命周期事件
    ILLEGAL_AUTH_AFTER_CAPTURE = "ILLEGAL_AUTH_AFTER_CAPTURE"      # 已捕获后不允许再授权
    ILLEGAL_CAPTURE_WITHOUT_AUTH = "ILLEGAL_CAPTURE_WITHOUT_AUTH"  # 未授权即捕获（且无授权事件）
    ILLEGAL_CAPTURE_AFTER_REFUND = "ILLEGAL_CAPTURE_AFTER_REFUND"  # 已全额退款后不允许再捕获
    ILLEGAL_REFUND_EXCEEDS_CAPTURE = "ILLEGAL_REFUND_EXCEEDS_CAPTURE"  # 退款累计超过捕获金额
    ILLEGAL_CHARGEBACK_WITHOUT_CAPTURE = "ILLEGAL_CHARGEBACK_WITHOUT_CAPTURE"  # 未捕获即拒付
    ILLEGAL_REVERSAL_WITHOUT_CHARGEBACK = "ILLEGAL_REVERSAL_WITHOUT_CHARGEBACK"  # 无拒付却来拒付反转
    ILLEGAL_AUTH_EXPIRED_AFTER_CAPTURE = "ILLEGAL_AUTH_EXPIRED_AFTER_CAPTURE"  # 已捕获后授权过期无效
    ILLEGAL_FAILED_AFTER_CAPTURE = "ILLEGAL_FAILED_AFTER_CAPTURE"  # 已捕获后支付失败通知无效
    OUT_OF_ORDER_OLDER_EVENT = "OUT_OF_ORDER_OLDER_EVENT"  # 业务时间早于当前驱动事件，迟到不覆盖
    CONFLICT_SAME_TIMESTAMP = "CONFLICT_SAME_TIMESTAMP"    # 同业务时间互斥事件，待裁决
    CONFLICT_DUPLICATED_FACT = "CONFLICT_DUPLICATED_FACT"  # 同一事实不同取值（如金额不一致），待裁决
    CONFLICT_PENDING = "CONFLICT_PENDING"                  # 该支付存在未裁决冲突，新事件先挂起
    UNKNOWN_EVENT_TYPE = "UNKNOWN_EVENT_TYPE"              # 状态机不认识的事件类型


class UnlinkedStatus:
    """待关联队列生命周期（该表可更新，非只追加）。"""
    PENDING = "PENDING"        # 待人工关联
    LINKED = "LINKED"          # 已人工关联到订单/支付，重新进入状态机
    DISCARDED = "DISCARDED"    # 人工判定为无效事件，丢弃（原始 raw_event 仍保留）


class ConflictStatus:
    """状态冲突裁决状态（该表可更新，非只追加）。"""
    PENDING = "PENDING"        # 待裁决：两条互斥记录都保留
    RESOLVED = "RESOLVED"      # 已裁决：选定胜者，败者标记 REJECTED
    DISCARDED = "DISCARDED"    # 人工判定整组无效


class ReplayMode:
    """重放隔离级别。"""
    SHADOW = "SHADOW"          # 影子重放：只生成影子状态，禁止任何生产回写


class ReplayOutcome:
    MATCHED = "MATCHED"        # 影子终态与生产当前状态一致
    DIVERGED = "DIVERGED"      # 影子终态与生产当前状态不一致，需人工对比
    BLOCKED = "BLOCKED"        # 重放过程中存在拒绝/冲突，影子状态不完整


# 支付生命周期事件集合（授权/捕获/退款/拒付/反转，以及创建/失败/过期）
PAYMENT_LIFECYCLE_EVENTS = frozenset({
    "PAYMENT_CREATED",
    "PAYMENT_AUTHORIZED",
    "PAYMENT_AUTH_EXPIRED",
    "PAYMENT_CAPTURED",
    "PAYMENT_FAILED",
    "REFUND_REQUESTED",
    "REFUND_SUCCEEDED",
    "CHARGEBACK_OPENED",
    "CHARGEBACK_REVERSED",
})

# 会真正驱动支付状态机迁移的事件（REFUND_REQUESTED 仅登记意图，不改累计金额态）
PAYMENT_STATE_DRIVING_EVENTS = frozenset({
    "PAYMENT_CREATED",
    "PAYMENT_AUTHORIZED",
    "PAYMENT_AUTH_EXPIRED",
    "PAYMENT_CAPTURED",
    "PAYMENT_FAILED",
    "REFUND_SUCCEEDED",
    "CHARGEBACK_OPENED",
    "CHARGEBACK_REVERSED",
})
