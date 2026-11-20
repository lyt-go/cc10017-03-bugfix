"""接收就绪数据快照测试：生效判定、阻塞原因、冲突收口与中断恢复。"""
from __future__ import annotations

import unittest

from app.services.kafka_connector import KafkaConnectorService
from app.services.receive_ready import snapshot_service
from app.services.topic_registry import TopicRegistryService
from app.store import store


class ReceiveReadyCase(unittest.TestCase):
    """构造互不影响的主题/配置对。"""

    def setUp(self) -> None:
        self.topic_svc = TopicRegistryService()
        self.kafka_svc = KafkaConnectorService()
        self._seq = 0

    def make_pair(self, suffix: str):
        topic, _ = self.topic_svc.create_entry({
            "主题编号": f"TOPI-{suffix}", "主题名称": f"测试主题{suffix}", "消息格式": "JSON",
        })
        conn, _ = self.kafka_svc.create_entry({
            "配置编号": f"KAFK-{suffix}", "集群地址": "kafka-rt:9092",
            "消费组名": f"cg-{suffix}", "偏移策略": "earliest",
            "批量大小": "100", "超时时间": "5000",
        })
        return topic, conn


class TestSnapshotBaseline(ReceiveReadyCase):
    """基线：种子数据在启动时生成快照，历史错位被修正。"""

    def test_seed_snapshots_exist(self) -> None:
        snap_ok = snapshot_service.get_snapshot("KAFK-0001")
        self.assertIsNotNone(snap_ok)
        self.assertEqual(snap_ok["verdict"], "已生效")
        self.assertEqual(snap_ok["topic_online_version"], 1)
        self.assertTrue(snap_ok["completed"])

    def test_seed_blocked_snapshot_keeps_reason(self) -> None:
        """KAFK-0003 关联的 TOPI-0003 未上线：快照保留配置版本与阻塞原因。"""
        snap = snapshot_service.get_snapshot("KAFK-0003")
        self.assertIsNotNone(snap)
        self.assertEqual(snap["verdict"], "已阻塞")
        self.assertIn("TOPI-0003", snap["blocked_reason"])
        self.assertEqual(snap["config_version"], 1)
        self.assertFalse(snap["completed"])


class TestEffectiveJudgement(ReceiveReadyCase):
    """生效判定：主题上线版本是前置条件，判定与阻塞原因落在同一份快照。"""

    def test_mark_effective_blocked_when_topic_offline(self) -> None:
        topic, conn = self.make_pair("RT01")
        entry, msg = self.kafka_svc.run_action(conn["id"], "标记生效")
        self.assertIsNone(entry)
        self.assertIn("TOPI-RT01", msg)
        snap = snapshot_service.get_snapshot("KAFK-RT01")
        self.assertEqual(snap["verdict"], "已阻塞")
        self.assertIn("TOPI-RT01", snap["blocked_reason"])
        self.assertIsNone(snap["first_offset"])
        # 配置状态不被提前置为已生效
        self.assertNotEqual(store.find("kafka_connector", conn["id"])["status"], "已生效")

    def test_topic_online_releases_pending_effective(self) -> None:
        topic, conn = self.make_pair("RT02")
        self.kafka_svc.run_action(conn["id"], "标记生效")  # 阻塞并挂起
        entry, _ = self.topic_svc.run_action(topic["id"], "确认上线")
        self.assertEqual(entry["status"], "已上线")
        snap = snapshot_service.get_snapshot("KAFK-RT02")
        self.assertEqual(snap["verdict"], "已生效")
        self.assertEqual(snap["topic_online_version"], 1)
        self.assertIsNotNone(snap["first_offset"])
        # 主题上线版本先于消费组生效版本被确认到快照
        self.assertLessEqual(snap["topic_online_confirmed_at"], snap["effective_confirmed_at"])
        # 挂起的「标记生效」在主题上线后被应用
        self.assertEqual(store.find("kafka_connector", conn["id"])["status"], "已生效")

    def test_unbound_config_passes_without_topic(self) -> None:
        conn, _ = self.kafka_svc.create_entry({
            "配置编号": "自定义-RT07", "集群地址": "kafka:9092", "消费组名": "cg-x",
        })
        entry, _ = self.kafka_svc.run_action(conn["id"], "标记生效")
        self.assertIsNotNone(entry)
        self.assertEqual(entry["status"], "已生效")
        snap = snapshot_service.get_snapshot("自定义-RT07")
        self.assertIsNone(snap["topic_code"])
        self.assertEqual(snap["verdict"], "已生效")

    def test_snapshot_is_single_per_config(self) -> None:
        topic, conn = self.make_pair("RT08")
        for _ in range(3):
            snapshot_service.rebuild_snapshot(store.find("kafka_connector", conn["id"]), trigger="重复核对")
        snaps = [s for s in snapshot_service.list_snapshots() if s["config_code"] == "KAFK-RT08"]
        self.assertEqual(len(snaps), 1)


class TestOfflineReceiptConflict(ReceiveReadyCase):
    """冲突收口：主题下线回执优先于阈值放行，快照只保留一个生效判定。"""

    def test_offline_receipt_overrides_threshold_release(self) -> None:
        topic, conn = self.make_pair("RT03")
        self.topic_svc.run_action(topic["id"], "确认上线")
        entry, _ = self.kafka_svc.run_action(conn["id"], "标记生效")
        self.assertIsNotNone(entry)
        snap = snapshot_service.get_snapshot("KAFK-RT03")
        dv_before = snap["data_version"]
        self.assertIsNotNone(snap["first_offset"])

        self.topic_svc.run_action(topic["id"], "标记下线")
        snap = snapshot_service.get_snapshot("KAFK-RT03")
        self.assertEqual(snap["verdict"], "已阻塞")
        self.assertIsNone(snap["first_offset"])
        self.assertGreater(snap["data_version"], dv_before)
        self.assertIn("下线", snap["blocked_reason"])
        self.assertIsNotNone(snap["offline_receipt"])
        # 已生效配置随下线回执回退
        self.assertEqual(store.find("kafka_connector", conn["id"])["status"], "待校验")

    def test_reonline_bumps_version_and_restores(self) -> None:
        topic, conn = self.make_pair("RT04")
        self.topic_svc.run_action(topic["id"], "确认上线")
        self.kafka_svc.run_action(conn["id"], "标记生效")
        self.topic_svc.run_action(topic["id"], "标记下线")
        entry, msg = self.topic_svc.run_action(topic["id"], "确认上线")  # 重新上线
        self.assertIsNotNone(entry)
        self.assertEqual(entry["_online_version"], 2)
        snap = snapshot_service.get_snapshot("KAFK-RT04")
        self.assertEqual(snap["verdict"], "已生效")
        self.assertEqual(snap["topic_online_version"], 2)
        self.assertEqual(store.find("kafka_connector", conn["id"])["status"], "已生效")


class TestResumePending(ReceiveReadyCase):
    """中断恢复：仅处理尚未完成项。"""

    def test_resume_only_processes_incomplete(self) -> None:
        topic, conn = self.make_pair("RT05")
        self.kafka_svc.run_action(conn["id"], "标记生效")  # 阻塞 → 未完成
        completed_before = {
            s["config_code"]: s["data_version"]
            for s in snapshot_service.list_snapshots() if s.get("completed")
        }
        result = snapshot_service.resume_pending()
        self.assertIn("KAFK-RT05", result["processed"])
        self.assertIn("KAFK-RT05", [b["config_code"] for b in result["still_blocked"]])
        # 已完成的快照不被重处理
        for code, dv in completed_before.items():
            snap = snapshot_service.get_snapshot(code)
            if snap.get("completed"):
                self.assertEqual(snap["data_version"], dv)

    def test_resume_applies_after_topic_online(self) -> None:
        topic, conn = self.make_pair("RT09")
        self.kafka_svc.run_action(conn["id"], "标记生效")  # 阻塞挂起
        # 模拟中断：主题上线了但快照尚未重核对
        topic_row = store.find("topic_registry", topic["id"])
        topic_row["status"] = "已上线"
        topic_row["_online_version"] = 1
        result = snapshot_service.resume_pending()
        self.assertIn("KAFK-RT09", result["newly_effective"])
        snap = snapshot_service.get_snapshot("KAFK-RT09")
        self.assertEqual(snap["verdict"], "已生效")
        self.assertEqual(store.find("kafka_connector", conn["id"])["status"], "已生效")


class TestThresholdUpdate(ReceiveReadyCase):
    """修改阈值：配置版本递增并按快照规则重核对。"""

    def test_threshold_update_rechecks_and_demotes(self) -> None:
        topic, conn = self.make_pair("RT06")
        self.topic_svc.run_action(topic["id"], "确认上线")
        self.kafka_svc.run_action(conn["id"], "标记生效")

        entry, msg = self.kafka_svc.update_threshold(conn["id"], {"批量大小": "0"})
        self.assertIsNotNone(entry)
        snap = snapshot_service.get_snapshot("KAFK-RT06")
        self.assertEqual(snap["verdict"], "已阻塞")
        self.assertEqual(snap["threshold_check"], "failed")
        self.assertEqual(snap["config_version"], 2)
        self.assertEqual(store.find("kafka_connector", conn["id"])["status"], "待校验")

        entry, msg = self.kafka_svc.update_threshold(conn["id"], {"批量大小": "200"})
        snap = snapshot_service.get_snapshot("KAFK-RT06")
        self.assertEqual(snap["verdict"], "已生效")
        self.assertEqual(snap["config_version"], 3)
        # 阈值修复后经恢复流程重新生效
        snapshot_service.resume_pending()
        self.assertEqual(store.find("kafka_connector", conn["id"])["status"], "已生效")


class TestTopicSnapshotFeedback(ReceiveReadyCase):
    """主题注册回查：读取消息接入写入同一份快照的配置版本。"""

    def test_feedback_contains_config_and_online_version(self) -> None:
        topic, conn = self.make_pair("RT10")
        entry, _ = self.topic_svc.run_action(topic["id"], "确认上线")
        feedback = self.topic_svc.get_topic_snapshots(topic["id"])
        self.assertEqual(feedback["topic_code"], "TOPI-RT10")
        self.assertEqual(feedback["online_version"], 1)
        self.assertEqual(len(feedback["snapshots"]), 1)
        item = feedback["snapshots"][0]
        self.assertEqual(item["config_code"], "KAFK-RT10")
        self.assertEqual(item["config_version"], 1)
        self.assertEqual(item["topic_online_version"], 1)
        # 动作响应里也带着同一份回查结果
        self.assertEqual(entry["_snapshot_feedback"]["snapshots"][0]["config_code"], "KAFK-RT10")


if __name__ == "__main__":
    unittest.main()
