"""接收就绪数据快照：消费组接收配置的生效判定与阻塞原因收口。

消息接入（kafka_connector）与主题注册（topic_registry）共享同一份快照：

- 消息接入读取消费组接收数据配置，写入配置版本与偏移阈值，并在
  「标记生效」「修改阈值」时触发生效核对；
- 主题注册在「确认上线」时向同一份快照提供主题上线版本与主题范围，
  在「标记下线」时写入下线回执，并回查消息接入写入的配置版本。

每条消费组配置只保留一份快照（原地更新、data_version 单调递增），
因此任意时刻生效判定只有一个版本。阈值放行与主题下线回执的冲突统一
收口为「主题状态优先」：只要主题不处于已上线，即使偏移阈值达标也不放行，
避免接收端提前消费并丢偏移。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.store import store

SNAPSHOT_TABLE = "receive_ready_snapshots"
CONNECTOR_MODULE = "kafka_connector"
TOPIC_MODULE = "topic_registry"

VERDICT_PENDING = "待核对"
VERDICT_EFFECTIVE = "已生效"
VERDICT_BLOCKED = "已阻塞"
VERDICT_DISABLED = "已停用"

REQUIRED_FIELDS = ["配置编号", "集群地址", "消费组名"]


def _now() -> str:
    return datetime.now().isoformat()


class ReceiveReadySnapshotService:
    """维护接收就绪数据快照：生成、核对、跨模块同步与中断恢复。"""

    def __init__(self) -> None:
        store.rows(SNAPSHOT_TABLE)  # 确保快照表存在
        self._bootstrap()

    # ───────────────────────────────
    # 关联解析与查找
    # ───────────────────────────────

    @staticmethod
    def resolve_topic_code(config_code: str) -> str | None:
        """配置编号遵循 KAFK-XXXX ↔ TOPI-XXXX 约定；不符合约定视为未绑定主题。"""
        code = (config_code or "").strip()
        if code.startswith("KAFK-"):
            return "TOPI-" + code[len("KAFK-"):]
        return None

    def get_snapshot(self, config_code: str) -> dict[str, Any] | None:
        for snap in store.rows(SNAPSHOT_TABLE):
            if snap.get("config_code") == config_code:
                return snap
        return None

    def list_snapshots(self) -> list[dict[str, Any]]:
        return list(store.rows(SNAPSHOT_TABLE))

    def snapshots_for_topic(self, topic_code: str) -> list[dict[str, Any]]:
        return [s for s in store.rows(SNAPSHOT_TABLE) if s.get("topic_code") == topic_code]

    def _find_connector(self, config_code: str) -> dict[str, Any] | None:
        for row in store.rows(CONNECTOR_MODULE):
            if row.get("配置编号") == config_code:
                return row
        return None

    def _find_topic(self, topic_code: str | None) -> dict[str, Any] | None:
        if not topic_code:
            return None
        for row in store.rows(TOPIC_MODULE):
            if row.get("主题编号") == topic_code:
                return row
        return None

    @staticmethod
    def _topic_scope(topic: dict[str, Any] | None) -> str | None:
        """主题范围：主题注册向快照提供的适用边界说明。"""
        if topic is None:
            return None
        return f"{topic.get('主题名称', '')}（分区键：{topic.get('分区键列', '-')}）"

    # ───────────────────────────────
    # 生成与重核对（消息接入写链路）
    # ───────────────────────────────

    def rebuild_snapshot(self, connector: dict[str, Any], *, trigger: str) -> dict[str, Any]:
        """读取消费组接收数据配置，生成或原地更新接收就绪数据快照。

        生效判断与阻塞原因落在同一份快照里；配置版本、主题上线版本、
        偏移阈值与适用范围都以快照记录为准。
        """
        config_code = str(connector.get("配置编号", ""))
        snap = self.get_snapshot(config_code)
        if snap is None:
            snap = {
                "id": max((int(s.get("id", 0)) for s in store.rows(SNAPSHOT_TABLE)), default=0) + 1,
                "config_code": config_code,
                "data_version": 0,
                "topic_online_version": None,
                "topic_online_confirmed_at": None,
                "offline_receipt": None,
                "pending_action": None,
                "first_offset": None,
                "effective_confirmed_at": None,
            }
            store.rows(SNAPSHOT_TABLE).append(snap)

        topic_code = self.resolve_topic_code(config_code)
        topic = self._find_topic(topic_code)

        snap.update({
            "consumer_group": connector.get("消费组名"),
            "config_version": int(connector.get("_config_version", 1)),
            "topic_code": topic_code,
            "topic_scope": self._topic_scope(topic),
            "offset_policy": connector.get("偏移策略"),
            "offset_threshold": connector.get("批量大小"),
            "last_trigger": trigger,
            "updated_at": _now(),
        })

        # 主题已上线且主题注册已给出上线版本时，把版本确认进同一份快照
        if topic is not None and topic.get("status") == "已上线":
            online_version = int(topic.get("_online_version", 0) or 0)
            if online_version and snap.get("topic_online_version") != online_version:
                snap["topic_online_version"] = online_version
                snap["topic_online_confirmed_at"] = _now()
                snap["offline_receipt"] = None

        verdict, blocked_reason, threshold_check = self._judge(connector, snap, topic_code, topic)
        snap["threshold_check"] = threshold_check
        snap["verdict"] = verdict
        snap["blocked_reason"] = blocked_reason
        snap["data_version"] = int(snap.get("data_version", 0)) + 1

        if verdict == VERDICT_EFFECTIVE:
            snap["first_offset"] = self._first_offset(connector, topic)
            snap["effective_confirmed_at"] = _now()
            # 核对通过但仍有挂起动作未应用时，快照不算完成
            snap["completed"] = not snap.get("pending_action")
        elif verdict == VERDICT_DISABLED:
            snap["first_offset"] = None
            snap["completed"] = True
        else:
            # 阻塞/待核对：没有首条消费位置，标记为尚未完成项，等待恢复
            snap["first_offset"] = None
            snap["completed"] = False
        return snap

    def _judge(
        self,
        connector: dict[str, Any],
        snap: dict[str, Any],
        topic_code: str | None,
        topic: dict[str, Any] | None,
    ) -> tuple[str, str | None, str]:
        """按快照的规则、阈值与适用范围核对生效判定。

        返回 (生效判定, 阻塞原因, 阈值核对结果)。冲突收口为主题状态优先：
        主题未上线或下线回执已到达时，阈值达标也不放行。
        """
        if connector.get("status") == "已停用":
            return VERDICT_DISABLED, None, "skipped"

        missing = [f for f in REQUIRED_FIELDS if not str(connector.get(f) or "").strip()]
        if missing:
            return VERDICT_BLOCKED, f"配置必填字段缺失：{'、'.join(missing)}", "skipped"

        # 适用范围：绑定了注册主题的配置必须通过主题前置校验
        if topic_code:
            if topic is None:
                return VERDICT_BLOCKED, f"关联主题 {topic_code} 未注册", "skipped"
            if topic.get("status") != "已上线":
                receipt = snap.get("offline_receipt")
                suffix = "；主题下线回执已到达，阈值放行被回执收口" if receipt else ""
                return (
                    VERDICT_BLOCKED,
                    f"主题 {topic_code} 当前状态「{topic.get('status')}」，未上线；"
                    f"主题上线版本须先于消费组生效版本被确认{suffix}",
                    "skipped",
                )
            if not snap.get("topic_online_version"):
                return (
                    VERDICT_BLOCKED,
                    f"主题 {topic_code} 上线版本尚未确认到快照，先生效会造成版本错位",
                    "skipped",
                )

        # 偏移阈值核对：批量大小可解析时必须为正；不可解析视为未设置阈值
        threshold_check = "skipped"
        threshold = str(connector.get("批量大小") or "").strip()
        if threshold:
            try:
                if int(threshold) < 1:
                    return VERDICT_BLOCKED, f"偏移阈值未达标：批量大小 {threshold} 小于 1", "failed"
                threshold_check = "passed"
            except ValueError:
                threshold_check = "skipped"

        return VERDICT_EFFECTIVE, None, threshold_check

    def _first_offset(self, connector: dict[str, Any], topic: dict[str, Any] | None) -> str:
        """首条消费位置：按偏移策略在生效判定时一次性确定。"""
        policy = str(connector.get("偏移策略") or "").strip().lower()
        if policy == "latest" and topic is not None:
            offsets = [
                int(row.get("消费偏移", 0) or 0)
                for row in store.rows("watermark_track")
                if row.get("数据主题") == topic.get("主题编号")
                and str(row.get("消费偏移", "")).isdigit()
            ]
            return str(max(offsets)) if offsets else "0"
        return "0"

    # ───────────────────────────────
    # 主题注册写链路（上线版本 / 主题范围 / 下线回执）
    # ───────────────────────────────

    def sync_topic_online(self, topic: dict[str, Any]) -> list[dict[str, Any]]:
        """主题确认上线：向同一份快照提供上线版本与主题范围，并重核对关联配置。

        消息接入已写入配置版本，这里读取该版本后把上线版本写进同一份快照；
        此前因主题未上线被阻塞且挂起了「标记生效」的配置，核对通过后直接恢复接收。
        """
        topic_code = topic.get("主题编号")
        online_version = int(topic.get("_online_version", 0) or 0)
        affected: list[dict[str, Any]] = []
        for snap in self.list_snapshots():
            if snap.get("topic_code") != topic_code:
                continue
            snap["topic_online_version"] = online_version
            snap["topic_online_confirmed_at"] = _now()
            snap["topic_scope"] = self._topic_scope(topic)
            snap["offline_receipt"] = None
            connector = self._find_connector(str(snap.get("config_code", "")))
            if connector is None:
                continue
            self.rebuild_snapshot(connector, trigger="主题上线确认")
            self.apply_pending_action(connector, snap)
            affected.append(snap)
        return affected

    def sync_topic_offline(self, topic: dict[str, Any]) -> list[dict[str, Any]]:
        """主题标记下线：写入下线回执并重核对关联配置。

        下线回执优先于阈值放行：已生效的关联配置统一回退为待校验，
        快照里只保留一个生效判定（已阻塞），data_version 递增。
        """
        topic_code = topic.get("主题编号")
        receipt = {
            "topic_code": topic_code,
            "received_at": _now(),
            "online_version": topic.get("_online_version"),
        }
        affected: list[dict[str, Any]] = []
        for snap in self.list_snapshots():
            if snap.get("topic_code") != topic_code:
                continue
            snap["offline_receipt"] = receipt
            connector = self._find_connector(str(snap.get("config_code", "")))
            if connector is None:
                continue
            self.rebuild_snapshot(connector, trigger="主题下线回执")
            if connector.get("status") == "已生效":
                connector["status"] = "待校验"
                connector["pending"] = True
                snap["pending_action"] = "标记生效"
            affected.append(snap)
        return affected

    # ───────────────────────────────
    # 中断恢复：仅处理尚未完成项
    # ───────────────────────────────

    def resume_pending(self) -> dict[str, Any]:
        """恢复接收就绪数据快照：只重核对尚未完成（含挂起动作）的快照项。"""
        processed: list[str] = []
        newly_effective: list[str] = []
        still_blocked: list[dict[str, Any]] = []
        for snap in self.list_snapshots():
            if snap.get("completed") and not snap.get("pending_action"):
                continue
            connector = self._find_connector(str(snap.get("config_code", "")))
            if connector is None:
                continue
            self.rebuild_snapshot(connector, trigger="中断恢复")
            processed.append(str(snap.get("config_code")))
            if snap.get("verdict") == VERDICT_EFFECTIVE:
                newly_effective.append(str(snap.get("config_code")))
                self.apply_pending_action(connector, snap)
            else:
                still_blocked.append({
                    "config_code": snap.get("config_code"),
                    "blocked_reason": snap.get("blocked_reason"),
                })
        return {
            "processed": processed,
            "newly_effective": newly_effective,
            "still_blocked": still_blocked,
        }

    def apply_pending_action(self, connector: dict[str, Any], snap: dict[str, Any]) -> None:
        """快照核对通过后，应用此前挂起的「标记生效」请求。"""
        if snap.get("verdict") != VERDICT_EFFECTIVE:
            return
        if snap.get("pending_action") != "标记生效":
            return
        if connector.get("status") == "已停用":
            return
        connector["status"] = "已生效"
        connector["pending"] = False
        snap["pending_action"] = None
        snap["completed"] = True

    # ───────────────────────────────
    # 基线初始化
    # ───────────────────────────────

    def _bootstrap(self) -> None:
        """启动时为存量主题与配置建立快照基线，修正历史错位。"""
        for topic in store.rows(TOPIC_MODULE):
            if topic.get("status") == "已上线" and not topic.get("_online_version"):
                topic["_online_version"] = 1
        for connector in store.rows(CONNECTOR_MODULE):
            snap = self.rebuild_snapshot(connector, trigger="基线初始化")
            if connector.get("status") == "已生效" and snap.get("verdict") != VERDICT_EFFECTIVE:
                # 存量错位：配置已生效但快照核对不通过，以快照为准回退
                connector["status"] = "待校验"
                connector["pending"] = True
                snap["pending_action"] = "标记生效"


snapshot_service = ReceiveReadySnapshotService()
