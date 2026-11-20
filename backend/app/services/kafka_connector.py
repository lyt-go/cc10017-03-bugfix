"""消息接入业务规则：状态流转、字段校验与筛选口径都收在这里。

增强：消费组接收数据配置的「标记生效」不再只看字段完整性，而是读取配置
生成接收就绪数据快照，由快照统一核对主题上线版本与偏移阈值；生效判断与
阻塞原因落在同一份快照里，阻塞时返回具体是哪个主题在阻塞。
"""
from __future__ import annotations

from typing import Any

from app.services.receive_ready import (
    VERDICT_DISABLED,
    VERDICT_EFFECTIVE,
    snapshot_service,
)
from app.store import store

MODULE = "kafka_connector"
LIST_FIELDS = ["配置编号", "集群地址", "消费组名", "偏移策略", "反序列化器", "批量大小", "超时时间", "死信队列"]
REQUIRED_FIELDS = ["配置编号", "集群地址", "消费组名"]
STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用配置": "已停用"}
NEGATIVE_ACTIONS = ["停用配置"]


class KafkaConnectorService:
    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        for field, expected in (filters or {}).items():
            rows = [row for row in rows if expected.casefold() in str(row.get(field, "")).casefold()]
        if keyword:
            rows = [row for row in rows if keyword.casefold() in str(row.get("配置编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in LIST_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        entry["_config_version"] = 1
        rows.append(entry)
        # 登记即生成接收就绪数据快照，后续核对都落在这一份快照上
        snapshot_service.rebuild_snapshot(entry, trigger="登记配置")
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"接入配置 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于消息接入可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"

        if action == "标记生效":
            return self._mark_effective(entry)

        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        snap = snapshot_service.rebuild_snapshot(entry, trigger=action)
        if action == "停用配置":
            snap["verdict"] = VERDICT_DISABLED
            snap["completed"] = True
            snap["pending_action"] = None
        entry["_snapshot"] = snap
        return entry, f"接入配置已{action}"

    def _mark_effective(self, entry: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        """标记生效：以接收就绪数据快照的核对结果为准。

        主题上线版本必须先于消费组生效版本被确认到快照；核对不通过时
        配置保持原状态，返回的阻塞原因指明具体主题。
        """
        snap = snapshot_service.rebuild_snapshot(entry, trigger="标记生效")
        if snap.get("verdict") != VERDICT_EFFECTIVE:
            snap["pending_action"] = "标记生效"
            reason = snap.get("blocked_reason") or "接收就绪核对未通过"
            return None, f"接入配置未生效：{reason}"
        entry["status"] = "已生效"
        entry["pending"] = False
        entry["abnormal"] = False
        snap["pending_action"] = None
        snap["completed"] = True
        entry["_snapshot"] = snap
        message = (
            f"接入配置已标记生效，首条消费位置 {snap.get('first_offset')}"
            f"（配置版本 v{snap.get('config_version')}，主题上线版本 v{snap.get('topic_online_version') or '-' }）"
        )
        return entry, message

    def update_threshold(self, entry_id: int, values: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        """修改偏移阈值（批量大小/超时时间）：配置版本递增并重核对快照。

        修改后若不再满足生效条件，已生效配置回退为待校验，阻塞原因写进快照。
        """
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"接入配置 {entry_id} 不存在或已归档"
        updated = []
        for field in ("批量大小", "超时时间"):
            value = str(values.get(field) or "").strip()
            if value:
                entry[field] = value
                updated.append(field)
        if not updated:
            return None, "未提供可更新的阈值字段（批量大小、超时时间）"
        entry["_config_version"] = int(entry.get("_config_version", 1)) + 1
        snap = snapshot_service.rebuild_snapshot(entry, trigger="修改阈值")
        entry["_snapshot"] = snap
        message = f"阈值已更新（{'、'.join(updated)}），配置版本 v{entry['_config_version']}"
        if entry.get("status") == "已生效" and snap.get("verdict") != VERDICT_EFFECTIVE:
            entry["status"] = "待校验"
            entry["pending"] = True
            snap["pending_action"] = "标记生效"
            message += f"；新阈值下核对未通过，配置已回退为待校验：{snap.get('blocked_reason')}"
        else:
            # 阈值修复后核对通过，应用此前挂起的生效请求
            snapshot_service.apply_pending_action(entry, snap)
        return entry, message

    # ───────────────────────────────
    # 接收就绪数据快照查询
    # ───────────────────────────────

    def list_snapshots(self) -> list[dict[str, Any]]:
        return snapshot_service.list_snapshots()

    def get_snapshot(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        return snapshot_service.get_snapshot(str(entry.get("配置编号", "")))

    def resume_snapshots(self) -> dict[str, Any]:
        """中断恢复：仅处理尚未完成的快照项。"""
        return snapshot_service.resume_pending()
