"""消息接入接口：维护接入配置，覆盖提交校验、标记生效、停用配置等动作。"""
from __future__ import annotations

import csv
import io

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services.kafka_connector import KafkaConnectorService

router = APIRouter(prefix="/api/kafka_connector", tags=["消息接入"])

service = KafkaConnectorService()

LIST_FIELDS = ["配置编号", "集群地址", "消费组名", "偏移策略", "反序列化器", "批量大小", "超时时间", "死信队列"]
STATUSES = ["草稿", "待校验", "已生效", "已停用"]


def query_filters(request: Request) -> dict[str, str]:
    """只接受当前模块声明过的字段，忽略分页等保留参数。"""
    return {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in LIST_FIELDS and value.strip()
    }


@router.get("", response_model=PageResult[dict])
def list_entries(
    request: Request,
    keyword: str | None = Query(default=None, description="按配置编号检索"),
    status: str | None = Query(default=None, description="草稿、待校验、已生效、已停用"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    """按页面字段、配置编号关键字与状态过滤消息接入列表。"""
    items, total = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/snapshots")
def list_snapshots() -> dict:
    """接收就绪数据快照列表：消息接入工作台与主题注册回查共用同一份数据。"""
    return {"items": service.list_snapshots(), "total": len(service.list_snapshots())}


@router.post("/snapshots/resume", response_model=ActionResult)
def resume_snapshots() -> ActionResult:
    """中断恢复接收就绪数据快照：仅处理尚未完成项，已完成的快照保持原样。"""
    result = service.resume_snapshots()
    message = (
        f"已恢复处理 {len(result['processed'])} 份未完成快照，"
        f"新生效 {len(result['newly_effective'])} 份，仍阻塞 {len(result['still_blocked'])} 份"
    )
    return ActionResult(ok=True, message=message, extra=result)


@router.get("/export")
def export_entries(
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    """下载当前筛选条件下的 UTF-8 CSV；必须声明在 /{entry_id} 之前。"""
    items, _ = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=1, size=1000000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *LIST_FIELDS, "status"])
    for item in items:
        row = []
        for value in [item.get("id"), *(item.get(field, "") for field in LIST_FIELDS), item.get("status", "")]:
            text = "" if value is None else str(value)
            row.append("'" + text if text.startswith(("=", "+", "-", "@")) else text)
        writer.writerow(row)
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="kafka_connector.csv"'})


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条接入配置明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"接入配置 {entry_id} 不存在或已归档")
    return entry


@router.get("/{entry_id}/snapshot", response_model=dict)
def get_snapshot(entry_id: int) -> dict:
    """查看该消费组配置的接收就绪数据快照：配置版本、生效判定与阻塞原因。"""
    snapshot = service.get_snapshot(entry_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail=f"接入配置 {entry_id} 的接收就绪数据快照不存在")
    return snapshot


@router.put("/{entry_id}/threshold", response_model=ActionResult)
def update_threshold(entry_id: int, payload: dict[str, object]) -> ActionResult:
    """修改偏移阈值（批量大小/超时时间）：配置版本递增并重核对快照。"""
    entry, message = service.update_threshold(entry_id, payload)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry, extra={"snapshot": entry.get("_snapshot")})


@router.post("", response_model=ActionResult)
def create_entry(payload: dict[str, object]) -> ActionResult:
    """登记一条接入配置，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="接入配置已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    """对单条接入配置执行提交校验、标记生效、停用配置；不允许的动作会被拦下并说明原因。"""
    entry, message = service.run_action(entry_id, payload.action.strip())
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry, extra={"snapshot": entry.get("_snapshot")})
