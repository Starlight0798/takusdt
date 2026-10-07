from __future__ import annotations

import os as _os

_os.environ.setdefault("DASHBOARD_BASE_URL", "https://monitor.example.com")

import asyncio
import base64
import hashlib
import hmac
import json
import os
import sqlite3
import tempfile
import time
from types import SimpleNamespace
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import httpx
from fastapi.testclient import TestClient

from app.main import (
    NotificationChannelCreate,
    Store,
    STATIC_DIR,
    aggregate_records,
    create_app,
    decimal_text,
    deliver_pending_operations,
    error_alert_content,
    extract_portfolio_id,
    format_operation_notification,
    fetch_binance_order_history,
    fetch_binance_order_history_page,
    fetch_binance_position_history,
    fetch_binance_symbol_precisions,
    fetch_binance_trade_history,
    fetch_leader_performances,
    fetch_leader_finance,
    fetch_leader_name,
    keyed_records,
    is_network_failure,
    monitor_poll_offset_seconds,
    next_pool_rebuild_streak,
    notification_operation_details,
    format_push_delay,
    operation_action,
    poll_all,
    poll_monitor,
    process_monitor_trade_alerts,
    reference_leverage_for_operation,
    refresh_monitor_drawdowns,
    safe_error,
    send_dingtalk_message,
    send_extra_notification_channels,
    send_feishu_message,
    trade_alert_key,
)


SOURCE_URL = "https://www.binance.com/zh-CN/copy-trading/lead-details/5075281354358777856?timeRange=7D"
BATCH_SOURCE_URL = "https://www.binance.com/zh-CN/copy-trading/lead-details/5075281354358777857?timeRange=7D"


async def empty_position_history(_, __: str, ___: int, ____: int) -> list[dict[str, object]]:
    return []


async def wait_for_monitor_initialization(app, monitor_id: int) -> None:
    for _ in range(100):
        monitor = app.state.store.get_monitor(monitor_id)
        if monitor and monitor["initialized"]:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("等待后台检查完成超时")


async def wait_for_operation_count(app, monitor_id: int, count: int) -> None:
    for _ in range(100):
        if len(app.state.store.recent_operations(monitor_id, 10)) == count:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("等待后台检查写入操作超时")


async def wait_for_operation_status(app, monitor_id: int, status: str) -> None:
    for _ in range(100):
        operations = app.state.store.recent_operations(monitor_id, 10)
        if any(operation["notification_status"] == status for operation in operations):
            return
        await asyncio.sleep(0.01)
    raise AssertionError("等待操作通知状态更新超时")


async def wait_for_background_tasks(app) -> None:
    for _ in range(100):
        if not app.state.background_tasks:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("等待后台增强任务完成超时")


def sample_record(time: int, side: str = "SELL", profit: str = "0") -> dict[str, object]:
    return {
        "time": time,
        "symbol": "XAUUSDT",
        "side": side,
        "price": 4667.13,
        "quantity": 2949.62616,
        "qty": 0.632,
        "baseAsset": "XAU",
        "positionSide": "SHORT",
        "realizedProfit": profit,
        "realizedProfitAsset": "USDT",
    }


class MainTests(unittest.TestCase):
    def test_extract_portfolio_id(self) -> None:
        self.assertEqual(extract_portfolio_id(SOURCE_URL), "5075281354358777856")

    def test_operation_actions_use_requested_colors(self) -> None:
        self.assertEqual(operation_action("SELL", "LONG"), ("平多", "close_long"))
        self.assertEqual(operation_action("SELL", "SHORT"), ("开空", "open_short"))
        self.assertEqual(operation_action("BUY", "LONG"), ("开多", "open_long"))
        self.assertEqual(operation_action("BUY", "SHORT"), ("平空", "close_short"))
        self.assertEqual(operation_action("BUY", "BOTH"), ("做多", "open_long"))
        self.assertEqual(operation_action("SELL", "BOTH"), ("做空", "open_short"))
        self.assertEqual(operation_action("SELL", "SHORT", "0"), ("开空", "open_short"))
        self.assertEqual(operation_action("BUY", "LONG", "3.5"), ("平空", "close_short"))
        self.assertEqual(operation_action("SELL", "SHORT", "-1.2"), ("平多", "close_long"))

    def test_operation_color_selectors_match_action_keys(self) -> None:
        page = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
        self.assertIn(".trade.open_long, .trade.close_short", page)
        self.assertIn(".trade.close_long, .trade.open_short", page)

    def test_operation_summary_uses_the_action_label_only_once(self) -> None:
        page = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
        self.assertIn('id="operation-keyword-filter"', page)
        self.assertIn("${verb} ${symbolLinkHtml(operation.symbol)}", page)
        self.assertNotIn("${verb}${actionHtml(operation)}", page)

    def test_monitor_management_has_filter_and_operation_navigation(self) -> None:
        page = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
        self.assertIn('id="monitor-keyword-filter"', page)
        self.assertIn('id="monitor-mode-filter"', page)
        self.assertIn('id="monitor-add-content" class="hidden"', page)
        self.assertIn("grid-template-columns: repeat(2, minmax(0, 1fr))", page)
        self.assertIn("data-monitor-operations", page)
        self.assertNotIn('id="add-monitor-url" required placeholder="https://www.binance.com/zh-CN/copy-trading/lead-details/..." value=', page)

    def test_operations_refreshes_content_every_thirty_seconds(self) -> None:
        page = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
        self.assertIn("operationRefreshTimer = setInterval", page)
        self.assertIn("}, 30000);", page)
        self.assertIn('route.parameters.get("monitor-name")', page)
        self.assertIn('route.parameters.get("symbol-link")', page)
        self.assertIn('id="operation-time-filter"', page)
        self.assertIn('value="1">最近 24 小时', page)
        self.assertIn("matchesOperationFilters(operation, filters, config.key)", page)
        self.assertIn("Date.now() - hours * 60 * 60 * 1000", page)
        self.assertIn("仓位历史参考杠杆", page)
        self.assertIn("operation.reference_leverage", page)
        self.assertNotIn('if (state.currentView === "operations") await loadOperations();', page)

    def test_dashboard_exposes_the_build_version(self) -> None:
        page = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
        self.assertIn('id="sidebar-version"', page)
        self.assertIn('data.version || "dev"', page)
        self.assertNotIn('data.version || "dev").slice', page)
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ,
            {
                "ADMIN_PASSWORD": "test-admin-password",
                "APP_VERSION": "2026-08-28+abcdef123456",
            },
        ):
            app = create_app(Path(directory) / "monitor.db", start_poller=False)
            with TestClient(app) as client:
                client.post(
                    "/api/auth/login",
                    json={"username": "admin", "password": "test-admin-password"},
                )
                self.assertEqual(
                    client.get("/api/dashboard").json()["version"], "2026-08-28+abcdef123456"
                )

    def test_trade_alerts_are_disabled_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, {"ADMIN_PASSWORD": "test-admin-password"}, clear=False
        ):
            os.environ.pop("TRADE_ALERTS_ENABLED", None)
            app = create_app(Path(directory) / "monitor.db", start_poller=False)
            with TestClient(app):
                self.assertFalse(app.state.trade_alerts_enabled)

    def test_disabled_trade_alerts_clear_stale_errors(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "monitor.db")
            store.initialize("test-admin-password")
            monitor = store.create_monitor(
                "Leader", "", SOURCE_URL, "5075281354358777856"
            )
            store.set_monitor_trade_alert_error(monitor["id"], "trade alert query failed")
            store.clear_trade_alert_errors()
            self.assertIsNone(store.get_monitor(monitor["id"])["last_trade_alert_error"])
            store.close()

    def test_overview_shows_latest_operations_before_summaries(self) -> None:
        page = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
        self.assertLess(page.index("<h3>最新操作</h3>"), page.index("<h3>项目表现摘要</h3>"))
        self.assertIn("<th>操作记录</th>", page)
        self.assertIn("data-operation-history", page)
        self.assertIn('"symbol-link": operationHistory.dataset.symbol', page)
        self.assertNotIn("<h3>监控状态</h3>", page)
        self.assertIn("qualified.slice(0, 8)", page)
        self.assertIn("30D 已实现盈亏", page)
        self.assertIn(".metric-line { display: grid; grid-template-columns: minmax(200px, 300px) minmax(0, 1fr);", page)
        self.assertGreaterEqual(page.count("monitorLinkHtml("), 4)

    def test_notification_and_session_controls_are_present(self) -> None:
        page = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
        self.assertIn("data-notification-panel", page)
        self.assertIn('id="session-settings-form"', page)
        self.assertIn('id="poll-settings-form"', page)
        self.assertIn('id="poll-interval-seconds"', page)
        self.assertIn("/api/settings/poll", page)
        self.assertIn('id="notification-block-form"', page)
        self.assertIn('id="operation-symbol-summary"', page)
        self.assertIn('data-view="notification-blocks"', page)
        self.assertIn('id="operation-block-toggle"', page)
        self.assertIn('data-operation-block-toggle', page)
        self.assertNotIn('id="overview-error-list"', page)
        self.assertIn('class="stat stat-link" type="button" data-go="monitors"', page)
        self.assertIn("item.monitor_id", page)
        self.assertIn('data-notification-panel="dingtalk"', page)
        self.assertIn('data-notification-panel="feishu"', page)
        self.assertIn('id="telegram-channel-form"', page)
        self.assertIn('id="telegram-channel-list"', page)
        self.assertIn('id="dingtalk-channel-form"', page)
        self.assertIn('id="feishu-channel-form"', page)
        self.assertIn('id="email-channel-list"', page)
        self.assertIn("默认 SMTP 邮箱", page)
        self.assertNotIn('id="telegram-form"', page)
        self.assertNotIn('id="test-telegram"', page)
        self.assertNotIn("默认 Telegram Bot", page)
        self.assertIn('data-view="security"><span class="nav-icon">◇</span>系统设置', page)
        self.assertIn('security: "系统设置"', page)
        self.assertNotIn("账户安全", page)
        self.assertIn("data-channel-test", page)
        self.assertIn('blocked: "已屏蔽"', page)
        monitors_page = page[page.index('id="view-monitors"'):page.index('id="view-notification-blocks"')]
        self.assertNotIn('id="notification-block-form"', monitors_page)
        self.assertIn("docker compose logs copy-watch | grep '初始密码'", page)

    def test_safe_error_includes_unexpected_exception_detail(self) -> None:
        self.assertEqual(safe_error(ValueError("invalid monitor state")), "ValueError：invalid monitor state")

    def test_error_alert_identifies_the_monitor_and_links_to_its_operations(self) -> None:
        subject, text, markdown_text = error_alert_content(
            {"id": 7, "name": "Leader"}, "Binance 查询", "TimeoutError：timed out"
        )
        self.assertIn("Leader（监控 ID: 7）", subject)
        self.assertIn("对象: Leader（监控 ID: 7）", text)
        self.assertIn("操作记录: https://monitor.example.com/#operations?monitor-name=Leader&monitor-id=7", text)
        self.assertIn("### ⚠️ 策略监控异常", markdown_text)
        self.assertIn("👤 **Leader（监控 ID: 7）** · 事件：Binance 查询", markdown_text)
        self.assertIn("> TimeoutError：timed out", markdown_text)
        self.assertIn(
            "[操作记录](https://monitor.example.com/#operations?monitor-name=Leader&monitor-id=7)",
            markdown_text,
        )

    def test_dashboard_groups_all_current_monitor_errors_and_log_sources(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, {"ADMIN_PASSWORD": "test-admin-password"}
        ):
            app = create_app(Path(directory) / "monitor.db", start_poller=False)
            with TestClient(app) as client:
                client.post(
                    "/api/auth/login",
                    json={"username": "admin", "password": "test-admin-password"},
                )
                monitor = app.state.store.create_monitor(
                    "Leader", "Test note", SOURCE_URL, "5075281354358777856"
                )
                app.state.store.set_monitor_error(monitor["id"], "order query failed")
                app.state.store.set_monitor_drawdown_error(monitor["id"], "drawdown query failed")
                app.state.store.set_monitor_leverage_error(monitor["id"], "leverage query failed")
                app.state.store.set_monitor_trade_alert_error(monitor["id"], "trade alert query failed")
                app.state.store.log_notification(monitor["id"], "sent", "error alert sent")
                app.state.store.log_event(monitor["id"], "error", "Binance 查询", "order query failed")

                dashboard = client.get("/api/dashboard").json()

        self.assertEqual(dashboard["metrics"]["error_count"], 1)
        self.assertEqual(
            {item["scope"] for item in dashboard["current_errors"]},
            {"订单查询", "带单表现", "参考杠杆", "成交预警"},
        )
        self.assertTrue(
            all(
                item["monitor_id"] == monitor["id"] and item["monitor_name"] == "Leader"
                for item in dashboard["current_errors"]
            )
        )
        self.assertEqual(dashboard["notification_attempts"][0]["monitor_id"], monitor["id"])
        self.assertEqual(dashboard["system_logs"][0]["monitor_id"], monitor["id"])

    def test_extra_notification_channels_are_private_and_independently_configurable(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, {"ADMIN_PASSWORD": "test-admin-password"}
        ):
            app = create_app(Path(directory) / "monitor.db", start_poller=False)
            with TestClient(app) as client:
                client.post(
                    "/api/auth/login",
                    json={"username": "admin", "password": "test-admin-password"},
                )
                telegram_one = client.post(
                    "/api/notification-channels",
                    json={
                        "kind": "telegram",
                        "name": "交易群",
                        "bot_token": "token-one",
                        "chat_id": "-1001",
                    },
                )
                telegram_two = client.post(
                    "/api/notification-channels",
                    json={
                        "kind": "telegram",
                        "name": "备份群",
                        "bot_token": "token-two",
                        "chat_id": "-1002",
                        "enabled": False,
                    },
                )
                dingtalk = client.post(
                    "/api/notification-channels",
                    json={
                        "kind": "dingtalk",
                        "name": "钉钉告警",
                        "webhook_url": "https://oapi.dingtalk.com/robot/send?access_token=secret-token",
                        "secret": "SECabc",
                    },
                )
                feishu = client.post(
                    "/api/notification-channels",
                    json={
                        "kind": "feishu",
                        "name": "飞书告警",
                        "webhook_url": "https://open.feishu.cn/open-apis/bot/v2/hook/secret-token",
                        "secret": "feishu-secret",
                    },
                )
                self.assertEqual(telegram_one.status_code, 201)
                self.assertEqual(telegram_two.status_code, 201)
                self.assertEqual(dingtalk.status_code, 201)
                self.assertEqual(feishu.status_code, 201)
                self.assertEqual(
                    client.post(
                        "/api/notification-channels",
                        json={
                            "kind": "dingtalk",
                            "name": "错误地址",
                            "webhook_url": "https://example.com/robot/send?access_token=x",
                        },
                    ).status_code,
                    422,
                )
                self.assertEqual(
                    client.post(
                        "/api/notification-channels",
                        json={
                            "kind": "feishu",
                            "name": "错误地址",
                            "webhook_url": "https://example.com/open-apis/bot/v2/hook/x",
                        },
                    ).status_code,
                    422,
                )

                dashboard = client.get("/api/dashboard").json()
                channels = dashboard["notification_channels"]
                self.assertEqual(len(channels), 4)
                self.assertFalse(any("token-one" in json.dumps(channel) for channel in channels))
                self.assertFalse(any("secret-token" in json.dumps(channel) for channel in channels))
                self.assertFalse(any("SECabc" in json.dumps(channel) for channel in channels))
                self.assertFalse(any("feishu-secret" in json.dumps(channel) for channel in channels))
                self.assertTrue(next(channel for channel in channels if channel["name"] == "交易群")["enabled"])
                self.assertFalse(next(channel for channel in channels if channel["name"] == "备份群")["enabled"])

                disabled = client.patch(
                    f"/api/notification-channels/{dingtalk.json()['id']}", json={"enabled": False}
                )
                self.assertEqual(disabled.status_code, 200)
                self.assertFalse(disabled.json()["enabled"])

    def test_extra_channels_receive_notifications_and_are_logged_by_name(self) -> None:
        sent_channels = []

        async def fake_telegram(_, channel, text: str, parse_mode=None) -> None:
            sent_channels.append((channel["kind"], channel["name"], text, parse_mode))

        async def fake_dingtalk(
            _, channel, title: str, text: str, *, markdown: bool = False
        ) -> None:
            sent_channels.append((channel["kind"], channel["name"], title, text, markdown))

        async def fake_feishu(_, channel, title: str, text: str) -> None:
            sent_channels.append((channel["kind"], channel["name"], title, text))

        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "monitor.db")
            store.initialize("test-admin-password")
            telegram = store.create_notification_channel(
                NotificationChannelCreate(
                    kind="telegram", name="交易群", bot_token="token", chat_id="123"
                )
            )
            dingtalk = store.create_notification_channel(
                NotificationChannelCreate(
                    kind="dingtalk",
                    name="钉钉群",
                    webhook_url="https://oapi.dingtalk.com/robot/send?access_token=token",
                )
            )
            feishu = store.create_notification_channel(
                NotificationChannelCreate(
                    kind="feishu",
                    name="飞书群",
                    webhook_url="https://open.feishu.cn/open-apis/bot/v2/hook/token",
                )
            )
            app = SimpleNamespace(state=SimpleNamespace(store=store))
            with (
                patch("app.main.send_telegram_channel_message", new=fake_telegram),
                patch("app.main.send_dingtalk_message", new=fake_dingtalk),
                patch("app.main.send_feishu_message", new=fake_feishu),
            ):
                errors = asyncio.run(
                    send_extra_notification_channels(
                        app, None, "标题", "纯文本", "<b>HTML</b>", "操作详情"
                    )
                )
                errors.extend(
                    asyncio.run(
                        send_extra_notification_channels(
                            app,
                            None,
                            "标题",
                            "纯文本",
                            "<b>HTML</b>",
                            "操作详情",
                            "### 纯文本",
                        )
                    )
                )

            attempts = store.notification_attempts()
            store.close()
            self.assertEqual(errors, [])
            self.assertEqual(
                {(kind, name) for kind, name, *_ in sent_channels},
                {("telegram", "交易群"), ("dingtalk", "钉钉群"), ("feishu", "飞书群")},
            )
            self.assertIn(("dingtalk", "钉钉群", "标题", "### 纯文本", True), sent_channels)
            self.assertIn(("dingtalk", "钉钉群", "标题", "纯文本", False), sent_channels)
            self.assertEqual(
                {item["channel_name"] for item in attempts},
                {"Telegram · 交易群", "钉钉 · 钉钉群", "飞书 · 飞书群"},
            )
            self.assertEqual(telegram["name"], "交易群")
            self.assertEqual(dingtalk["name"], "钉钉群")
            self.assertEqual(feishu["name"], "飞书群")

    def test_dingtalk_message_uses_signed_markdown_webhook(self) -> None:
        captured = {}

        async def handler(request: httpx.Request) -> httpx.Response:
            captured["url"] = request.url
            captured["payload"] = json.loads(request.content)
            return httpx.Response(200, json={"errcode": 0, "errmsg": "ok"})

        async def send() -> None:
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                app = SimpleNamespace(state=SimpleNamespace(http=client))
                await send_dingtalk_message(
                    app,
                    {
                        "kind": "dingtalk",
                        "name": "钉钉群",
                        "config": {
                            "webhook_url": "https://oapi.dingtalk.com/robot/send?access_token=token",
                            "secret": "SECabc",
                        },
                    },
                    "测试标题",
                    "第一行\n第二行",
                )

        asyncio.run(send())
        timestamp = captured["url"].params["timestamp"]
        expected_sign = base64.b64encode(
            hmac.new(
                b"SECabc", f"{timestamp}\nSECabc".encode("utf-8"), hashlib.sha256
            ).digest()
        ).decode("ascii")
        self.assertEqual(captured["url"].host, "oapi.dingtalk.com")
        self.assertEqual(captured["url"].params["access_token"], "token")
        self.assertEqual(captured["url"].params["sign"], expected_sign)
        self.assertEqual(captured["payload"]["msgtype"], "markdown")
        self.assertIn("第一行\n\n第二行", captured["payload"]["markdown"]["text"])

    def test_feishu_message_uses_signed_rich_text_webhook(self) -> None:
        captured = {}

        async def handler(request: httpx.Request) -> httpx.Response:
            captured["url"] = request.url
            captured["payload"] = json.loads(request.content)
            return httpx.Response(200, json={"code": 0, "msg": "success"})

        async def send() -> None:
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                app = SimpleNamespace(state=SimpleNamespace(http=client))
                await send_feishu_message(
                    app,
                    {
                        "kind": "feishu",
                        "name": "飞书群",
                        "config": {
                            "webhook_url": "https://open.feishu.cn/open-apis/bot/v2/hook/token",
                            "secret": "feishu-secret",
                        },
                    },
                    "测试标题",
                    "第一行\n第二行 https://monitor.example.com/#operations?monitor-id=7",
                )

        asyncio.run(send())
        timestamp = captured["payload"]["timestamp"]
        expected_sign = base64.b64encode(
            hmac.new(
                f"{timestamp}\nfeishu-secret".encode("utf-8"), digestmod=hashlib.sha256
            ).digest()
        ).decode("ascii")
        self.assertEqual(captured["url"].host, "open.feishu.cn")
        self.assertEqual(captured["url"].path, "/open-apis/bot/v2/hook/token")
        self.assertEqual(captured["payload"]["sign"], expected_sign)
        self.assertEqual(captured["payload"]["msg_type"], "post")
        self.assertEqual(
            captured["payload"]["content"]["post"]["zh_cn"]["content"],
            [
                [{"tag": "text", "text": "第一行"}],
                [
                    {"tag": "text", "text": "第二行 "},
                    {
                        "tag": "a",
                        "text": "https://monitor.example.com/#operations?monitor-id=7",
                        "href": "https://monitor.example.com/#operations?monitor-id=7",
                    },
                ],
            ],
        )

    def test_extra_feishu_channel_test_endpoint_sends_and_logs_the_named_channel(self) -> None:
        sent_channels = []

        async def fake_feishu(_, channel, title: str, text: str) -> None:
            sent_channels.append((channel["name"], title, text))

        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, {"ADMIN_PASSWORD": "test-admin-password"}
        ):
            app = create_app(Path(directory) / "monitor.db", start_poller=False)
            with patch("app.main.send_feishu_message", new=fake_feishu), TestClient(app) as client:
                client.post(
                    "/api/auth/login",
                    json={"username": "admin", "password": "test-admin-password"},
                )
                channel = client.post(
                    "/api/notification-channels",
                    json={
                        "kind": "feishu",
                        "name": "飞书告警",
                        "webhook_url": "https://open.feishu.cn/open-apis/bot/v2/hook/token",
                    },
                ).json()
                response = client.post(f"/api/notification-channels/{channel['id']}/test")

                self.assertEqual(response.status_code, 200)
                self.assertEqual(sent_channels[0][0], "飞书告警")
                self.assertEqual(
                    app.state.store.notification_attempts()[0]["channel_name"], "飞书 · 飞书告警"
                )

    def test_notification_channel_schema_migration_preserves_existing_channels(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "monitor.db"
            connection = sqlite3.connect(database_path)
            connection.executescript(
                """
                CREATE TABLE notification_channels (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    kind TEXT NOT NULL CHECK (kind IN ('telegram', 'dingtalk')),
                    name TEXT NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    config_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX notification_channels_kind_idx
                    ON notification_channels (kind, enabled, id DESC);
                """
            )
            connection.execute(
                """
                INSERT INTO notification_channels (kind, name, enabled, config_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    "dingtalk",
                    "已有钉钉",
                    1,
                    json.dumps({"webhook_url": "https://oapi.dingtalk.com/robot/send?access_token=x"}),
                    "2026-01-01T00:00:00+00:00",
                    "2026-01-01T00:00:00+00:00",
                ),
            )
            connection.commit()
            connection.close()

            store = Store(database_path)
            store.initialize("test-admin-password")
            existing = store.notification_channels()
            created = store.create_notification_channel(
                NotificationChannelCreate(
                    kind="feishu",
                    name="飞书群",
                    webhook_url="https://open.feishu.cn/open-apis/bot/v2/hook/token",
                )
            )
            schema = store.connection.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'notification_channels'"
            ).fetchone()["sql"]
            store.close()

        self.assertEqual(existing[0]["name"], "已有钉钉")
        self.assertEqual(created["kind"], "feishu")
        self.assertIn("'feishu'", schema)

    def test_legacy_default_telegram_is_imported_as_a_managed_channel(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "monitor.db"
            store = Store(database_path)
            store.initialize("test-admin-password")
            store.update_telegram_settings("legacy-token", "-1001", True)
            store.close()

            migrated = Store(database_path)
            migrated.initialize("test-admin-password")
            channels = migrated.notification_channels()
            delivered = migrated.notification_channel_for_delivery(channels[0]["id"])
            migrated.initialize("test-admin-password")

            self.assertFalse(migrated.public_telegram_settings()["enabled"])
            self.assertEqual(len(migrated.notification_channels()), 1)
            migrated.close()

        self.assertEqual(channels[0]["name"], "已导入 Telegram Bot")
        self.assertTrue(channels[0]["enabled"])
        self.assertEqual(delivered["config"]["bot_token"], "legacy-token")
        self.assertEqual(delivered["config"]["chat_id"], "-1001")

    def test_no_effective_channel_skips_pending_notifications(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "monitor.db")
            store.initialize("test-admin-password")
            monitor = store.create_monitor("Leader", "", SOURCE_URL, "5075281354358777856")
            store.save_baseline(monitor["id"], keyed_records([sample_record(1)]))
            store.save_new_operations(monitor["id"], keyed_records([sample_record(2)]))

            result = asyncio.run(
                deliver_pending_operations(
                    SimpleNamespace(state=SimpleNamespace(store=store)), store.get_monitor(monitor["id"])
                )
            )
            operation = store.recent_operations(monitor["id"])[0]
            logs = store.system_logs()
            attempts = store.notification_attempts()
            store.close()

        self.assertEqual(result, {"status": "skipped", "count": 1})
        self.assertEqual(operation["notification_status"], "skipped")
        self.assertEqual(logs[0]["event"], "通知跳过")
        self.assertEqual(attempts, [])

    def test_regular_order_history_only_requests_the_latest_24_hours(self) -> None:
        requested_payload = {}

        async def handler(request: httpx.Request) -> httpx.Response:
            requested_payload.update(json.loads(request.content))
            return httpx.Response(200, json={"code": "000000", "data": {"list": []}})

        async def request_history() -> list[dict[str, object]]:
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                return await fetch_binance_order_history(client, "5075281354358777856")

        with patch("app.main.time.time", return_value=1_800_000_000.123):
            self.assertEqual(asyncio.run(request_history()), [])
        self.assertEqual(requested_payload["endTime"], 1_800_000_000_123)
        self.assertEqual(requested_payload["startTime"], 1_799_913_600_123)

    def test_order_history_failure_is_not_retried_within_the_poll(self) -> None:
        request_count = 0

        async def handler(_: httpx.Request) -> httpx.Response:
            nonlocal request_count
            request_count += 1
            return httpx.Response(503)

        async def request_history() -> None:
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                await fetch_binance_order_history_page(client, "5075281354358777856", 0, 1)

        with self.assertRaises(httpx.HTTPStatusError):
            asyncio.run(request_history())
        self.assertEqual(request_count, 1)

    def test_order_history_timeout_is_cancelled_and_retried_once(self) -> None:
        request_count = 0

        async def handler(_: httpx.Request) -> httpx.Response:
            nonlocal request_count
            request_count += 1
            if request_count == 1:
                await asyncio.sleep(0.03)
            return httpx.Response(200, json={"code": "000000", "data": {"list": []}})

        async def request_history() -> list[dict[str, object]]:
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                return await fetch_binance_order_history(client, "5075281354358777856")

        with patch("app.main.ORDER_HISTORY_TIMEOUT_SECONDS", 0.01):
            self.assertEqual(asyncio.run(request_history()), [])
        self.assertEqual(request_count, 2)

    def test_metadata_retry_logs_each_attempt_and_recovers(self) -> None:
        attempts: list[str] = []

        async def flaky_drawdowns(
            _: httpx.AsyncClient, portfolio_id: str
        ) -> dict[str, float | None]:
            attempts.append(portfolio_id)
            if len(attempts) == 1:
                raise httpx.ConnectError("网络请求失败")
            return {"drawdown_7d": 0.031, "drawdown_30d": 0.244, "drawdown_90d": None}

        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, {"ADMIN_PASSWORD": "test-admin-password"}
        ):
            app = create_app(
                Path(directory) / "monitor.db",
                start_poller=False,
                drawdown_fetcher=flaky_drawdowns,
            )
            with TestClient(app):
                store = app.state.store
                monitor = store.create_monitor(
                    "Leader", "Test note", SOURCE_URL, "5075281354358777856"
                )
                with patch("app.main.METADATA_RETRY_DELAY_SECONDS", 0.0):
                    asyncio.run(refresh_monitor_drawdowns(app, monitor))

                logs = [
                    entry
                    for entry in store.system_logs()
                    if entry["event"] == "Binance 带单表现"
                ]
                state = store.get_monitor(monitor["id"])
                store.close()

        self.assertEqual(len(attempts), 2)
        self.assertEqual([entry["level"] for entry in logs], ["info", "warning"])
        self.assertIn("第 1/3 次尝试失败", logs[1]["message"])
        self.assertIn("ConnectError", logs[1]["message"])
        self.assertIn("网络请求失败", logs[1]["message"])
        self.assertIn("0 秒后重试", logs[1]["message"])
        self.assertIn("重试成功：第 2/3 次尝试成功", logs[0]["message"])
        self.assertEqual(state["drawdown_7d"], 0.031)
        self.assertEqual(state["last_drawdown_error"], None)

    def test_metadata_retry_exhaustion_reports_error_with_attempt_count(self) -> None:
        attempts: list[str] = []

        async def broken_drawdowns(
            _: httpx.AsyncClient, portfolio_id: str
        ) -> dict[str, float | None]:
            attempts.append(portfolio_id)
            raise httpx.ConnectError("网络请求失败")

        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, {"ADMIN_PASSWORD": "test-admin-password"}
        ):
            app = create_app(
                Path(directory) / "monitor.db",
                start_poller=False,
                drawdown_fetcher=broken_drawdowns,
            )
            with TestClient(app):
                store = app.state.store
                monitor = store.create_monitor(
                    "Leader", "Test note", SOURCE_URL, "5075281354358777856"
                )
                with patch("app.main.METADATA_RETRY_DELAY_SECONDS", 0.0):
                    asyncio.run(refresh_monitor_drawdowns(app, monitor))

                logs = [
                    entry
                    for entry in store.system_logs()
                    if entry["event"] == "Binance 带单表现"
                ]
                state = store.get_monitor(monitor["id"])
                store.close()

        self.assertEqual(len(attempts), 3)
        self.assertEqual(
            [entry["level"] for entry in logs], ["error", "warning", "warning"]
        )
        self.assertIn("第 1/3 次尝试失败", logs[2]["message"])
        self.assertIn("第 2/3 次尝试失败", logs[1]["message"])
        self.assertIn("Binance 带单表现查询失败（已重试 3 次）", logs[0]["message"])
        self.assertIn("Binance 带单表现查询失败（已重试 3 次）", state["last_drawdown_error"])

    def test_poll_monitor_skips_writes_when_monitor_deleted_midflight(self) -> None:
        async def fetch_records(
            _: httpx.AsyncClient, __: str
        ) -> list[dict[str, object]]:
            return []

        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, {"ADMIN_PASSWORD": "test-admin-password"}
        ):
            app = create_app(
                Path(directory) / "monitor.db",
                start_poller=False,
                fetcher=fetch_records,
            )
            with TestClient(app):
                store = app.state.store
                monitor = store.create_monitor(
                    "Leader", "Test note", SOURCE_URL, "5075281354358777856"
                )
                store.delete_monitor(monitor["id"])
                result = asyncio.run(poll_monitor(app, monitor))
                errors = [
                    entry
                    for entry in store.system_logs()
                    if entry["level"] == "error"
                ]
                store.close()

        self.assertEqual(result, {"monitor_id": monitor["id"], "status": "deleted"})
        self.assertEqual(errors, [])

    def test_trade_alerts_skip_deleted_monitor(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, {"ADMIN_PASSWORD": "test-admin-password"}
        ):
            app = create_app(Path(directory) / "monitor.db", start_poller=False)
            with TestClient(app):
                store = app.state.store
                monitor = store.create_monitor(
                    "Leader", "Test note", SOURCE_URL, "5075281354358777856"
                )
                store.delete_monitor(monitor["id"])
                result = asyncio.run(process_monitor_trade_alerts(app, monitor, []))
                store.close()

        self.assertEqual(result, {"status": "deleted", "count": 0})

    def test_log_writes_fall_back_to_system_level_for_deleted_monitor(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "monitor.db")
            store.initialize("test-admin-password")
            monitor = store.create_monitor(
                "Leader", "Test note", SOURCE_URL, "5075281354358777856"
            )
            store.delete_monitor(monitor["id"])

            store.log_event(monitor["id"], "error", "Binance 查询", "删除后写入")
            store.log_notification(monitor["id"], "sent", "已发送：异常告警", "钉钉")

            logs = store.system_logs()
            attempts = store.notification_attempts()
            store.close()

        self.assertEqual(logs[0]["monitor_id"], None)
        self.assertIn("删除后写入", logs[0]["message"])
        self.assertEqual(attempts[0]["monitor_id"], None)
        self.assertEqual(attempts[0]["status"], "sent")

    def test_monitor_poll_offsets_spread_sources_across_the_interval(self) -> None:
        self.assertEqual(
            [monitor_poll_offset_seconds(index, 3) for index in range(3)],
            [0.0, 20 / 3, 40 / 3],
        )
        self.assertEqual(monitor_poll_offset_seconds(0, 1), 0.0)
        self.assertEqual(monitor_poll_offset_seconds(1, 3, 5), 5 / 3)

    def test_slow_monitor_does_not_delay_the_next_scheduled_monitor(self) -> None:
        first_monitor_release = asyncio.Event()
        second_monitor_started = asyncio.Event()

        async def fake_fetcher(_, portfolio_id: str):
            if portfolio_id == "5075281354358777856":
                await first_monitor_release.wait()
            else:
                second_monitor_started.set()
            return []

        async def fake_leader_name(_, __: str, portfolio_id: str):
            return f"带单员 {portfolio_id[-4:]}"

        async def fake_drawdowns(_, __: str):
            return {
                "drawdown_7d": 0.0, "drawdown_30d": 0.0, "drawdown_90d": 0.0,
                "roi_7d": None, "roi_30d": None, "roi_90d": None,
                "win_rate_7d": None, "win_rate_30d": None, "win_rate_90d": None,
                "pnl_7d": None, "pnl_30d": None, "pnl_90d": None,
                "win_orders_7d": None, "win_orders_30d": None, "win_orders_90d": None,
                "total_orders_7d": None, "total_orders_30d": None, "total_orders_90d": None,
            }

        async def fake_finance(_, __: str):
            return ("18256.38", "25212.35")

        async def fake_symbol_precisions(_):
            return {}

        async def run_poll(app):
            task = asyncio.create_task(poll_all(app))
            try:
                await asyncio.wait_for(second_monitor_started.wait(), timeout=0.2)
            finally:
                first_monitor_release.set()
            return await asyncio.wait_for(task, timeout=0.2)

        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, {"ADMIN_PASSWORD": "test-admin-password"}
        ):
            app = create_app(
                Path(directory) / "monitor.db",
                start_poller=False,
                fetcher=fake_fetcher,
                position_history_fetcher=empty_position_history,
                leader_name_fetcher=fake_leader_name,
                drawdown_fetcher=fake_drawdowns,
                leader_finance_fetcher=fake_finance,
                symbol_precision_fetcher=fake_symbol_precisions,
            )
            with TestClient(app) as client, patch(
                "app.main.monitor_poll_offset_seconds", side_effect=lambda index, _, __: index * 0.01
            ):
                first_monitor = app.state.store.create_monitor(
                    "Leader 1", "", SOURCE_URL, "5075281354358777856"
                )
                second_monitor = app.state.store.create_monitor(
                    "Leader 2", "", BATCH_SOURCE_URL, "5075281354358777857"
                )
                app.state.store.save_baseline(first_monitor["id"], [])
                app.state.store.save_baseline(second_monitor["id"], [])

                result = client.portal.call(run_poll, app)

                self.assertEqual([item["status"] for item in result], ["ok", "ok"])

    def test_poll_all_records_specific_unexpected_monitor_error(self) -> None:
        async def invalid_fetcher(_, __: str):
            return None

        async def fake_leader_name(_, __: str, portfolio_id: str):
            return f"带单员 {portfolio_id[-4:]}"

        async def fake_drawdowns(_, __: str):
            return {
                "drawdown_7d": 0.0, "drawdown_30d": 0.0, "drawdown_90d": 0.0,
                "roi_7d": None, "roi_30d": None, "roi_90d": None,
                "win_rate_7d": None, "win_rate_30d": None, "win_rate_90d": None,
                "pnl_7d": None, "pnl_30d": None, "pnl_90d": None,
                "win_orders_7d": None, "win_orders_30d": None, "win_orders_90d": None,
                "total_orders_7d": None, "total_orders_30d": None, "total_orders_90d": None,
            }

        async def fake_finance(_, __: str):
            return ("18256.38", "25212.35")

        async def fake_symbol_precisions(_):
            return {}

        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, {"ADMIN_PASSWORD": "test-admin-password"}
        ):
            app = create_app(
                Path(directory) / "monitor.db",
                start_poller=False,
                fetcher=invalid_fetcher,
                position_history_fetcher=empty_position_history,
                leader_name_fetcher=fake_leader_name,
                drawdown_fetcher=fake_drawdowns,
                leader_finance_fetcher=fake_finance,
                symbol_precision_fetcher=fake_symbol_precisions,
            )
            with TestClient(app) as client:
                monitor = app.state.store.create_monitor(
                    "Leader", "", SOURCE_URL, "5075281354358777856"
                )
                result = client.portal.call(poll_all, app)

                self.assertEqual(result[0]["status"], "error")
                self.assertIn("TypeError", result[0]["error"])
                self.assertIn("TypeError", app.state.store.get_monitor(monitor["id"])["last_error"])
                self.assertTrue(
                    any(
                        log["event"] == "轮询处理" and "TypeError" in log["message"]
                        for log in app.state.store.system_logs()
                    )
                )

    def test_position_history_failure_does_not_block_operations(self) -> None:
        async def fake_fetcher(_, __: str):
            return [sample_record(1)]

        async def failed_position_history(_, __: str, ___: int, ____: int):
            raise httpx.ConnectError("position history unavailable")

        async def fake_leader_name(_, __: str, portfolio_id: str):
            return f"带单员 {portfolio_id[-4:]}"

        async def fake_drawdowns(_, __: str):
            return {
                "drawdown_7d": 0.0, "drawdown_30d": 0.0, "drawdown_90d": 0.0,
                "roi_7d": None, "roi_30d": None, "roi_90d": None,
                "win_rate_7d": None, "win_rate_30d": None, "win_rate_90d": None,
                "pnl_7d": None, "pnl_30d": None, "pnl_90d": None,
                "win_orders_7d": None, "win_orders_30d": None, "win_orders_90d": None,
                "total_orders_7d": None, "total_orders_30d": None, "total_orders_90d": None,
            }

        async def fake_finance(_, __: str):
            return ("18256.38", "25212.35")

        async def fake_symbol_precisions(_):
            return {}

        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, {"ADMIN_PASSWORD": "test-admin-password"}
        ):
            app = create_app(
                Path(directory) / "monitor.db",
                start_poller=False,
                fetcher=fake_fetcher,
                position_history_fetcher=failed_position_history,
                leader_name_fetcher=fake_leader_name,
                drawdown_fetcher=fake_drawdowns,
                leader_finance_fetcher=fake_finance,
                symbol_precision_fetcher=fake_symbol_precisions,
            )
            with TestClient(app) as client:
                monitor = app.state.store.create_monitor(
                    "Leader", "", SOURCE_URL, "5075281354358777856"
                )
                result = client.portal.call(poll_all, app)
                client.portal.call(wait_for_background_tasks, app)

                self.assertEqual(result[0]["status"], "baseline")
                self.assertIsNone(app.state.store.recent_operations(monitor["id"])[0]["reference_leverage"])
                self.assertIn(
                    "ConnectError", app.state.store.get_monitor(monitor["id"])["last_leverage_error"]
                )
                self.assertTrue(
                    any(
                        log["event"] == "Binance 仓位历史查询"
                        for log in app.state.store.system_logs()
                    )
                )

    def test_regular_poll_skips_leverage_history_without_new_operations(self) -> None:
        position_history_calls = 0
        records = [sample_record(int(time.time() * 1000))]

        async def fake_fetcher(_, __: str):
            return records

        async def fake_position_history(_, __: str, ___: int, ____: int):
            nonlocal position_history_calls
            position_history_calls += 1
            return []

        async def fake_leader_name(_, __: str, portfolio_id: str):
            return f"带单员 {portfolio_id[-4:]}"

        async def fake_drawdowns(_, __: str):
            return {
                "drawdown_7d": 0.0, "drawdown_30d": 0.0, "drawdown_90d": 0.0,
                "roi_7d": None, "roi_30d": None, "roi_90d": None,
                "win_rate_7d": None, "win_rate_30d": None, "win_rate_90d": None,
                "pnl_7d": None, "pnl_30d": None, "pnl_90d": None,
                "win_orders_7d": None, "win_orders_30d": None, "win_orders_90d": None,
                "total_orders_7d": None, "total_orders_30d": None, "total_orders_90d": None,
            }

        async def fake_finance(_, __: str):
            return ("18256.38", "25212.35")

        async def fake_symbol_precisions(_):
            return {}

        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, {"ADMIN_PASSWORD": "test-admin-password"}
        ):
            app = create_app(
                Path(directory) / "monitor.db",
                start_poller=False,
                fetcher=fake_fetcher,
                position_history_fetcher=fake_position_history,
                leader_name_fetcher=fake_leader_name,
                drawdown_fetcher=fake_drawdowns,
                leader_finance_fetcher=fake_finance,
                symbol_precision_fetcher=fake_symbol_precisions,
            )
            with TestClient(app) as client:
                monitor = app.state.store.create_monitor(
                    "Leader", "", SOURCE_URL, "5075281354358777856"
                )
                app.state.store.save_baseline(monitor["id"], keyed_records(records))

                result = client.portal.call(poll_all, app)
                client.portal.call(wait_for_background_tasks, app)

                self.assertEqual(result[0]["new_count"], 0)
                self.assertEqual(position_history_calls, 0)

    def test_poll_limits_reference_leverage_wait_before_notification(self) -> None:
        position_fetch_started = False
        sent_messages = []
        metadata_started = asyncio.Event()
        metadata_release = asyncio.Event()

        async def fake_fetcher(_, __: str):
            return [sample_record(2)]

        async def slow_position_history(_, __: str, ___: int, ____: int):
            nonlocal position_fetch_started
            position_fetch_started = True
            await asyncio.Event().wait()

        async def fake_leader_name(_, __: str, portfolio_id: str):
            return f"带单员 {portfolio_id[-4:]}"

        async def fake_drawdowns(_, __: str):
            return {
                "drawdown_7d": 0.0, "drawdown_30d": 0.0, "drawdown_90d": 0.0,
                "roi_7d": None, "roi_30d": None, "roi_90d": None,
                "win_rate_7d": None, "win_rate_30d": None, "win_rate_90d": None,
                "pnl_7d": None, "pnl_30d": None, "pnl_90d": None,
                "win_orders_7d": None, "win_orders_30d": None, "win_orders_90d": None,
                "total_orders_7d": None, "total_orders_30d": None, "total_orders_90d": None,
            }

        async def fake_finance(_, __: str):
            return ("18256.38", "25212.35")

        async def fake_symbol_precisions(_):
            return {}

        async def fake_send_telegram(_, text: str) -> None:
            self.assertFalse(position_fetch_started)
            sent_messages.append(text)

        async def fake_error_alert(*_) -> None:
            return None

        async def fake_refresh_metadata(_, __) -> None:
            self.assertEqual(len(sent_messages), 1)
            metadata_started.set()
            await metadata_release.wait()

        async def run_poll(app):
            return await asyncio.wait_for(poll_all(app), timeout=0.2)

        async def release_metadata() -> None:
            await asyncio.wait_for(metadata_started.wait(), timeout=0.2)
            metadata_release.set()

        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, {"ADMIN_PASSWORD": "test-admin-password"}
        ):
            app = create_app(
                Path(directory) / "monitor.db",
                start_poller=False,
                fetcher=fake_fetcher,
                position_history_fetcher=slow_position_history,
                leader_name_fetcher=fake_leader_name,
                drawdown_fetcher=fake_drawdowns,
                leader_finance_fetcher=fake_finance,
                symbol_precision_fetcher=fake_symbol_precisions,
            )
            with (
                patch("app.main.REFERENCE_LEVERAGE_TIMEOUT_SECONDS", 0.01),
                patch("app.main.send_telegram_html", new=fake_send_telegram),
                patch("app.main.send_error_alert", new=fake_error_alert),
                patch("app.main.refresh_monitor_metadata", new=fake_refresh_metadata),
                TestClient(app) as client,
            ):
                monitor = app.state.store.create_monitor(
                    "Leader", "", SOURCE_URL, "5075281354358777856"
                )
                app.state.store.save_baseline(monitor["id"], keyed_records([sample_record(1)]))
                app.state.store.update_telegram_settings("token", "123", True)

                result = client.portal.call(run_poll, app)
                client.portal.call(release_metadata)
                client.portal.call(wait_for_background_tasks, app)

                self.assertEqual(result[0]["notification"]["status"], "sent")
                self.assertEqual(len(sent_messages), 1)
                self.assertTrue(position_fetch_started)
                operation = app.state.store.recent_operations(monitor["id"])[0]
                self.assertIsNone(operation["reference_leverage"])
                self.assertIn(
                    "TimeoutError", app.state.store.get_monitor(monitor["id"])["last_leverage_error"]
                )

    def test_trade_alert_is_sent_before_the_official_order_returns(self) -> None:
        order_release = asyncio.Event()
        alert_sent = asyncio.Event()
        sent_messages = []
        old_order = sample_record(1)
        trade_time = int(time.time() * 1000) + 10_000
        new_trade = {**sample_record(trade_time), "fee": "-0.1", "feeAsset": "USDT"}

        async def slow_order_history(_, __: str):
            await order_release.wait()
            return [{**sample_record(trade_time + 500), "price": "4667.13", "qty": "0.632"}]

        async def fake_trade_history(_, __: str):
            return [new_trade]

        async def fake_leader_name(_, __: str, portfolio_id: str):
            return f"带单员 {portfolio_id[-4:]}"

        async def fake_drawdowns(_, __: str):
            return {
                "drawdown_7d": 0.0, "drawdown_30d": 0.0, "drawdown_90d": 0.0,
                "roi_7d": None, "roi_30d": None, "roi_90d": None,
                "win_rate_7d": None, "win_rate_30d": None, "win_rate_90d": None,
                "pnl_7d": None, "pnl_30d": None, "pnl_90d": None,
                "win_orders_7d": None, "win_orders_30d": None, "win_orders_90d": None,
                "total_orders_7d": None, "total_orders_30d": None, "total_orders_90d": None,
            }

        async def fake_finance(_, __: str):
            return ("18256.38", "25212.35")

        async def fake_symbol_precisions(_):
            return {}

        async def fake_send_telegram(_, html_text: str) -> None:
            sent_messages.append(html_text)
            if "成交预警" in html_text:
                alert_sent.set()

        async def fake_error_alert(*_) -> None:
            return None

        async def run_poll(app):
            task = asyncio.create_task(poll_all(app))
            await asyncio.wait_for(alert_sent.wait(), timeout=0.2)
            self.assertFalse(order_release.is_set())
            order_release.set()
            return await asyncio.wait_for(task, timeout=0.2)

        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, {"ADMIN_PASSWORD": "test-admin-password"}
        ):
            app = create_app(
                Path(directory) / "monitor.db",
                start_poller=False,
                fetcher=slow_order_history,
                trade_history_fetcher=fake_trade_history,
                trade_alerts_enabled=True,
                position_history_fetcher=empty_position_history,
                leader_name_fetcher=fake_leader_name,
                drawdown_fetcher=fake_drawdowns,
                leader_finance_fetcher=fake_finance,
                symbol_precision_fetcher=fake_symbol_precisions,
            )
            with (
                patch("app.main.send_telegram_html", new=fake_send_telegram),
                patch("app.main.send_error_alert", new=fake_error_alert),
                TestClient(app) as client,
            ):
                monitor = app.state.store.create_monitor(
                    "Leader", "", SOURCE_URL, "5075281354358777856"
                )
                app.state.store.save_baseline(monitor["id"], keyed_records([old_order]))
                app.state.store.save_trade_alert_baseline(monitor["id"], [])
                app.state.store.update_telegram_settings("token", "123", True)

                result = client.portal.call(run_poll, app)
                client.portal.call(wait_for_background_tasks, app)

                self.assertEqual(result[0]["trade_alert"]["status"], "sent")
                self.assertEqual(len(sent_messages), 1)
                self.assertIn("成交预警", sent_messages[0])
                self.assertEqual(
                    app.state.store.recent_operations(monitor["id"], 1)[0]["notification_status"],
                    "prealerted",
                )

    def test_slow_trade_alert_source_does_not_delay_or_duplicate_official_notification(self) -> None:
        trade_release = asyncio.Event()
        sent_messages = []
        old_order = sample_record(1)
        trade_time = int(time.time() * 1000) + 10_000
        new_order = {**sample_record(trade_time + 500), "price": "4667.13", "qty": "0.632"}
        matching_trade = {**sample_record(trade_time), "fee": "-0.1", "feeAsset": "USDT"}

        async def fast_order_history(_, __: str):
            return [new_order]

        async def slow_trade_history(_, __: str):
            await trade_release.wait()
            return [matching_trade]

        async def fake_drawdowns(_, __: str):
            return {
                "drawdown_7d": 0.0, "drawdown_30d": 0.0, "drawdown_90d": 0.0,
                "roi_7d": None, "roi_30d": None, "roi_90d": None,
                "win_rate_7d": None, "win_rate_30d": None, "win_rate_90d": None,
                "pnl_7d": None, "pnl_30d": None, "pnl_90d": None,
                "win_orders_7d": None, "win_orders_30d": None, "win_orders_90d": None,
                "total_orders_7d": None, "total_orders_30d": None, "total_orders_90d": None,
            }

        async def fake_finance(_, __: str):
            return ("18256.38", "25212.35")

        async def fake_symbol_precisions(_):
            return {}

        async def fake_send_telegram(_, html_text: str) -> None:
            sent_messages.append(html_text)

        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, {"ADMIN_PASSWORD": "test-admin-password"}
        ):
            app = create_app(
                Path(directory) / "monitor.db",
                start_poller=False,
                fetcher=fast_order_history,
                trade_history_fetcher=slow_trade_history,
                trade_alerts_enabled=True,
                position_history_fetcher=empty_position_history,
                drawdown_fetcher=fake_drawdowns,
                leader_finance_fetcher=fake_finance,
                symbol_precision_fetcher=fake_symbol_precisions,
            )
            with TestClient(app) as client, patch(
                "app.main.send_telegram_html", new=fake_send_telegram
            ):
                monitor = app.state.store.create_monitor(
                    "Leader", "", SOURCE_URL, "5075281354358777856"
                )
                app.state.store.save_baseline(monitor["id"], keyed_records([old_order]))
                app.state.store.save_trade_alert_baseline(monitor["id"], [])
                app.state.store.update_telegram_settings("token", "123", True)

                result = client.portal.call(
                    lambda current_app: asyncio.wait_for(poll_all(current_app), timeout=0.2), app
                )
                self.assertEqual(result[0]["notification"]["status"], "sent")
                self.assertEqual(result[0]["trade_alert"]["status"], "empty")
                self.assertEqual(len(sent_messages), 1)

                trade_release.set()
                client.portal.call(wait_for_background_tasks, app)
                self.assertEqual(len(sent_messages), 1)

    def test_keyed_records_keeps_duplicate_records_distinct(self) -> None:
        record = sample_record(1)
        keys = [key for key, _ in keyed_records([record, record])]
        self.assertNotEqual(keys[0], keys[1])

    def test_reference_leverage_does_not_change_operation_key(self) -> None:
        record = sample_record(1)
        enriched_record = {**record, "referenceLeverage": "20"}
        self.assertEqual(keyed_records([record])[0][0], keyed_records([enriched_record])[0][0])

    def test_reference_leverage_matches_position_symbol_side_and_time(self) -> None:
        record = sample_record(150)
        positions = [
            {"symbol": "XAUUSDT", "side": "Long", "opened": 100, "closed": 200, "leverage": "5"},
            {"symbol": "XAUUSDT", "side": "Short", "opened": 100, "closed": 200, "leverage": "12"},
            {"symbol": "XAUUSDT", "side": "Short", "opened": 300, "closed": 400, "leverage": "50"},
        ]
        self.assertEqual(reference_leverage_for_operation(record, positions), "12")

        both_sided_record = {**record, "positionSide": "BOTH", "side": "SELL"}
        self.assertEqual(
            reference_leverage_for_operation(
                both_sided_record,
                [{"symbol": "XAUUSDT", "side": "Long", "opened": 100, "closed": 200, "leverage": "7"}],
            ),
            "7",
        )

    def test_aggregate_records_matches_binance_displayed_operation(self) -> None:
        first = sample_record(1, profit="3.5")
        first["price"] = 10
        first["qty"] = 1
        first["quantity"] = 10
        second = sample_record(1, profit="1.2")
        second["price"] = 20
        second["qty"] = 2
        second["quantity"] = 40
        grouped = aggregate_records([first, second])
        self.assertEqual(len(grouped), 1)
        self.assertEqual(grouped[0]["qty"], "3")
        self.assertEqual(grouped[0]["quantity"], "50")
        self.assertEqual(grouped[0]["price"], "16.66666666666666666666666667")
        self.assertEqual(grouped[0]["realizedProfit"], "4.7")

    def test_decimal_text_preserves_integer_trailing_zeros(self) -> None:
        self.assertEqual(decimal_text(Decimal("50")), "50")

    def test_notification_log_details_include_operation_context(self) -> None:
        record = sample_record(1)
        operation = {
            "occurred_at": 1,
            "symbol": record["symbol"],
            "side": record["side"],
            "position_side": record["positionSide"],
            "qty": record["qty"],
            "base_asset": record["baseAsset"],
            "price": record["price"],
            "quantity": record["quantity"],
        }
        details = notification_operation_details(operation)
        self.assertIn("XAUUSDT 开空", details)
        self.assertIn("数量 0.632 XAU", details)
        self.assertIn("均价 4667.13 USDT", details)

    def test_operation_notification_includes_push_delay(self) -> None:
        operation = {
            "occurred_at": 1000,
            "side": "SELL",
            "position_side": "LONG",
            "realized_profit": "1",
            "qty": "1",
            "base_asset": "ETH",
            "symbol": "ETHUSDT",
            "price": "10",
            "quantity": "10",
        }
        text, html_text, markdown_text = format_operation_notification(
            {"name": "Leader", "url": SOURCE_URL}, operation, None, now_ms=126_000
        )
        self.assertIn("（距推送约 2 分 05 秒）", text)
        self.assertIn("（距推送约 2 分 05 秒）", html_text)
        self.assertIn("🕒 1970-01-01 08:00:01（距推送约 2 分 05 秒）", markdown_text)

        self.assertEqual(format_push_delay(0), "暂无")
        self.assertEqual(format_push_delay(None), "暂无")
        self.assertEqual(format_push_delay(1000, now_ms=1500), "0 秒")
        self.assertEqual(format_push_delay(1000, now_ms=3_661_000), "1 小时 1 分 00 秒")
        self.assertEqual(format_push_delay(2000, now_ms=1000), "0 秒")

    def test_operation_notification_pnl_links_and_performance(self) -> None:
        operation = {
            "occurred_at": 1,
            "symbol": "XAUUSDT",
            "side": "SELL",
            "position_side": "SHORT",
            "qty": "0.632",
            "base_asset": "XAU",
            "price": "4667.13",
            "quantity": "2949.62616",
            "reference_leverage": "12",
            "realized_profit": "3.5",
            "profit_asset": "USDT",
        }
        performance = {
            "periods": {
                "7d": {"win_rate": 100.0, "max_drawdown": 0.01},
                "30d": {
                    "win_rate": 75.0,
                    "max_drawdown": 2.5,
                    "roi": 16.6,
                    "pnl": 3030.76,
                    "win_orders": 30,
                    "total_orders": 40,
                },
                "90d": {"win_rate": 60.0, "max_drawdown": None},
            }
        }
        text, html_text, markdown_text = format_operation_notification(
            {
                "id": 7,
                "name": "Leader",
                "url": SOURCE_URL,
                "margin_balance": "18256.38058624",
                "aum_amount": "25212.35392926",
            },
            operation,
            performance,
        )

        self.assertNotIn("参考杠杆", text)
        self.assertIn("带单人: Leader\n", text)
        self.assertNotIn("monitor.example.com", text)
        self.assertIn("操作: ↘ 平多", text)
        self.assertIn("合约: XAUUSDT\n", text)
        self.assertNotIn("binance.com", text)
        self.assertIn("本次实现盈亏: +3.5 USDT", text)
        self.assertIn("带单余额: 18,256.38 USDT", text)
        self.assertIn("资产管理规模: 25,212.35 USDT", text)
        self.assertIn("带单余额: 18,256.38 USDT", html_text)
        self.assertIn("资产管理规模: 25,212.35 USDT", html_text)
        self.assertIn("7D 胜率: 100.0% | 收益率: 暂无 | 最大回撤: 0.01%", text)
        self.assertIn("30D 胜率: 75.0% | 收益率: +16.60% | 最大回撤: 2.5%", text)
        self.assertIn("90D 胜率: 60.0% | 收益率: 暂无 | 最大回撤: 暂无", text)
        self.assertIn("30D 已实现盈亏：+3,030.76 USDT · 胜场：30/40", text)
        self.assertIn("### ↘ 平多 · XAUUSDT", markdown_text)
        self.assertIn("👤 **Leader**\n\n🕒 ", markdown_text)
        self.assertIn("- 📦 数量：0.632 XAU", markdown_text)
        self.assertIn("- 💰 均价：**4,667.13** USDT", markdown_text)
        self.assertIn("- 🧮 总值：**2,949.63** USDT", markdown_text)
        self.assertNotIn("参考杠杆", markdown_text)
        self.assertIn("- 💵 本次实现盈亏：**+3.5 USDT**", markdown_text)
        self.assertIn("> 带单余额：**18,256.38 USDT**", markdown_text)
        self.assertIn("> 资产管理规模：**25,212.35 USDT**", markdown_text)
        self.assertIn("> 30D　胜率 **75.0%** ｜ 收益率 **+16.60%** ｜ 回撤 2.5%", markdown_text)
        self.assertIn("> 90D　胜率 **60.0%** ｜ 收益率 暂无 ｜ 回撤 暂无", markdown_text)
        self.assertIn("> 30D 已实现盈亏 **+3,030.76 USDT** · 胜场 **30/40**", markdown_text)
        self.assertNotIn("带单地址", text)
        self.assertNotIn("UTC+8", text)
        self.assertNotIn("<a ", html_text)
        open_operation = {
            **operation,
            "side": "BUY",
            "position_side": "LONG",
            "realized_profit": "0",
        }
        open_text, open_html, open_markdown = format_operation_notification(
            {
                "id": 7,
                "name": "Leader",
                "url": SOURCE_URL,
                "margin_balance": "18256.38058624",
            },
            open_operation,
            performance,
        )
        self.assertIn("仓位: 161.6 USDT/千U余额", open_text)
        self.assertIn("仓位: 161.6 USDT/千U余额", open_html)
        self.assertIn("- 📌 仓位：**161.6 USDT/千U余额**", open_markdown)
        self.assertNotIn("仓位:", text)
        no_data_text, _, no_data_markdown = format_operation_notification(
            {"name": "Leader", "url": SOURCE_URL},
            {**operation, "reference_leverage": None, "realized_profit": "0"},
            None,
        )
        self.assertNotIn("参考杠杆", no_data_text)
        self.assertIn("本次实现盈亏: 暂无", no_data_text)
        self.assertNotIn("带单余额", no_data_text)
        self.assertNotIn("资产管理规模", no_data_text)
        self.assertNotIn("资产规模", no_data_markdown)
        self.assertNotIn("参考杠杆", no_data_markdown)
        self.assertIn("- 💵 本次实现盈亏：暂无", no_data_markdown)

        with patch("app.main.DASHBOARD_BASE_URL", ""):
            plain_text, plain_html, _ = format_operation_notification(
                {"id": 7, "name": "Leader", "url": SOURCE_URL}, operation, performance
            )
            _, alert_text, alert_markdown = error_alert_content(
                {"id": 7, "name": "Leader"}, "订单查询", "boom"
            )
        self.assertIn("带单人: Leader" + chr(10), plain_text)
        self.assertNotIn("#operations", plain_text + plain_html + alert_text + alert_markdown)

    def test_order_history_response_without_success_flag_is_accepted(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.url.path.rsplit("/", 1)[-1], "order-history")
            self.assertEqual(json.loads(request.content)["portfolioId"], "5075281354358777856")
            return httpx.Response(
                200,
                json={
                    "code": "000000",
                    "data": {
                        "indexValue": "next-page",
                        "list": [
                            {
                                "symbol": "XAUUSDT",
                                "baseAsset": "XAU",
                                "quoteAsset": "USDT",
                                "side": "SELL",
                                "positionSide": "SHORT",
                                "executedQty": "2.5",
                                "avgPrice": "10.4",
                                "totalPnl": "3.2",
                                "orderUpdateTime": 1,
                            }
                        ],
                    },
                },
            )

        async def request_page() -> tuple[list[dict[str, object]], str | None]:
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                return await fetch_binance_order_history_page(
                    client, "5075281354358777856", 0, 2
                )

        records, index_value = asyncio.run(request_page())
        self.assertEqual(index_value, "next-page")
        self.assertEqual(
            records,
            [
                {
                    "time": 1,
                    "symbol": "XAUUSDT",
                    "side": "SELL",
                    "positionSide": "SHORT",
                    "price": "10.4",
                    "qty": "2.5",
                    "baseAsset": "XAU",
                    "quantity": "26",
                    "quantityAsset": "USDT",
                    "realizedProfit": "3.2",
                    "realizedProfitAsset": "USDT",
                }
            ],
        )

    def test_trade_history_response_without_success_flag_is_accepted(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.url.path.rsplit("/", 1)[-1], "trade-history")
            self.assertEqual(
                json.loads(request.content),
                {"portfolioId": "5075281354358777856", "pageNumber": 1, "pageSize": 100},
            )
            return httpx.Response(
                200,
                json={
                    "code": "000000",
                    "data": {
                        "list": [
                            {
                                "time": 1,
                                "symbol": "XAUUSDT",
                                "baseAsset": "XAU",
                                "quantityAsset": "USDT",
                                "side": "SELL",
                                "positionSide": "SHORT",
                                "qty": "2.5",
                                "price": "10.4",
                                "quantity": "26",
                                "fee": "-0.1",
                                "feeAsset": "USDT",
                                "realizedProfit": "3.2",
                                "realizedProfitAsset": "USDT",
                            }
                        ]
                    },
                },
            )

        async def request_trades() -> list[dict[str, object]]:
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                return await fetch_binance_trade_history(client, "5075281354358777856")

        records = asyncio.run(request_trades())
        self.assertEqual(records[0]["time"], 1)
        self.assertEqual(records[0]["qty"], "2.5")
        self.assertEqual(records[0]["fee"], "-0.1")
        self.assertEqual(records[0]["realizedProfit"], "3.2")

    def test_trade_alert_baseline_deduplicates_and_suppresses_later_order_notification(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "monitor.db")
            store.initialize("test-admin-password")
            monitor = store.create_monitor(
                "Leader", "", SOURCE_URL, "5075281354358777856"
            )
            trade_time = int(time.time() * 1000) + 10_000
            baseline = {**sample_record(trade_time - 20_000), "fee": "-0.1", "feeAsset": "USDT"}
            fresh_trade = {**sample_record(trade_time), "fee": "-0.2", "feeAsset": "USDT"}

            self.assertEqual(store.save_trade_alert_baseline(monitor["id"], [baseline]), 1)
            self.assertEqual(store.save_new_trade_alerts(monitor["id"], [baseline], True), 0)
            self.assertEqual(store.save_new_trade_alerts(monitor["id"], [fresh_trade], True), 1)
            alert = store.pending_trade_alerts(monitor["id"], 1)[0]
            self.assertEqual(alert["alert_key"], trade_alert_key(fresh_trade))
            store.mark_trade_alerts_notified(monitor["id"], [alert["alert_key"]])

            official_order = {**sample_record(trade_time + 500), "price": "4667.13", "qty": "0.632"}
            store.save_new_operations(monitor["id"], keyed_records([official_order]))

            self.assertEqual(
                store.recent_operations(monitor["id"], 1)[0]["notification_status"], "prealerted"
            )
            store.close()

    def test_trade_alert_ignores_history_returned_after_the_baseline(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "monitor.db")
            store.initialize("test-admin-password")
            monitor = store.create_monitor(
                "Leader", "", SOURCE_URL, "5075281354358777856"
            )
            store.save_trade_alert_baseline(monitor["id"], [])
            historical_trade = {
                **sample_record(int(time.time() * 1000) - 60_000),
                "fee": "-0.1",
                "feeAsset": "USDT",
            }

            self.assertEqual(store.save_new_trade_alerts(monitor["id"], [historical_trade], True), 0)
            self.assertEqual(store.pending_trade_alerts(monitor["id"], 1), [])
            store.close()

    def test_trade_alert_migration_skips_pending_history_from_the_previous_version(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "monitor.db")
            store.initialize("test-admin-password")
            monitor = store.create_monitor(
                "Leader", "", SOURCE_URL, "5075281354358777856"
            )
            future_trade = {
                **sample_record(int(time.time() * 1000) + 10_000),
                "fee": "-0.1",
                "feeAsset": "USDT",
            }
            store.save_trade_alert_baseline(monitor["id"], [])
            self.assertEqual(store.save_new_trade_alerts(monitor["id"], [future_trade], True), 1)
            with store.lock:
                store.connection.execute(
                    "DELETE FROM settings WHERE key = 'trade_alert_history_format_version'"
                )
                store.connection.execute(
                    "UPDATE monitors SET trade_alerts_started_at = NULL WHERE id = ?",
                    (monitor["id"],),
                )
                store.connection.commit()

            store._migrate_trade_alert_history()

            with store.lock:
                status = store.connection.execute(
                    "SELECT notification_status FROM trade_alerts WHERE monitor_id = ?",
                    (monitor["id"],),
                ).fetchone()[0]
            self.assertEqual(status, "skipped")
            self.assertGreater(store.get_monitor(monitor["id"])["trade_alerts_started_at"], 0)
            store.close()

    def test_position_history_paginates_and_preserves_leverage(self) -> None:
        requested_pages = []

        async def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.url.path.rsplit("/", 1)[-1], "position-history")
            page_number = json.loads(request.content)["pageNumber"]
            requested_pages.append(page_number)
            rows = [{"positionId": str(index), "leverage": "4"} for index in range(100)]
            if page_number == 2:
                rows = [{"positionId": "100", "leverage": "20"}]
            return httpx.Response(200, json={"code": "000000", "data": {"total": 101, "list": rows}})

        async def request_positions() -> list[dict[str, object]]:
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                return await fetch_binance_position_history(client, "5075281354358777856", 0, 2)

        positions = asyncio.run(request_positions())
        self.assertEqual(requested_pages, [1, 2])
        self.assertEqual(len(positions), 101)
        self.assertEqual(positions[-1]["leverage"], "20")

    def test_symbol_precisions_use_binance_display_precision(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.url.path, "/fapi/v1/exchangeInfo")
            self.assertEqual(request.url.params["showall"], "true")
            return httpx.Response(
                200,
                json={
                    "symbols": [
                        {"symbol": "BTCUSDT", "pricePrecision": 2, "quantityPrecision": 3},
                        {"symbol": "XAUUSDT", "pricePrecision": 2, "quantityPrecision": 3},
                        {"symbol": "MUUSDT", "pricePrecision": 5, "quantityPrecision": 2},
                    ]
                },
            )

        async def request_precisions() -> dict[str, tuple[int, int]]:
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                return await fetch_binance_symbol_precisions(client)

        self.assertEqual(
            asyncio.run(request_precisions()),
            {"BTCUSDT": (2, 3), "XAUUSDT": (2, 3), "MUUSDT": (5, 2)},
        )

    def test_fetch_leader_performances_parses_official_payload(self) -> None:
        payloads = {
            "7D": {
                "code": "000000",
                "data": {
                    "roi": 12.3456,
                    "mdd": 4.5,
                    "winRate": 66.67,
                    "pnl": 1234.5,
                    "winOrders": 20,
                    "totalOrder": 30,
                },
            },
            "30D": {
                "code": "000000",
                "data": {
                    "roi": None,
                    "mdd": None,
                    "winRate": None,
                    "pnl": None,
                    "winOrders": None,
                    "totalOrder": None,
                },
            },
            "90D": {"code": "000000", "data": {}},
        }

        async def handler(request: httpx.Request) -> httpx.Response:
            self.assertTrue(request.url.path.endswith("lead-portfolio/performance"))
            time_range = request.url.params["timeRange"]
            self.assertNotIn("dataType", request.url.params)
            return httpx.Response(200, json=payloads[time_range])

        async def request_performances() -> dict[str, float | int | None]:
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                return await fetch_leader_performances(client, "5075281354358777856")

        self.assertEqual(
            asyncio.run(request_performances()),
            {
                "drawdown_7d": 4.5,
                "drawdown_30d": None,
                "drawdown_90d": None,
                "roi_7d": 12.35,
                "roi_30d": None,
                "roi_90d": None,
                "win_rate_7d": 66.67,
                "win_rate_30d": None,
                "win_rate_90d": None,
                "pnl_7d": 1234.5,
                "pnl_30d": None,
                "pnl_90d": None,
                "win_orders_7d": 20,
                "win_orders_30d": None,
                "win_orders_90d": None,
                "total_orders_7d": 30,
                "total_orders_30d": None,
                "total_orders_90d": None,
            },
        )

    def test_official_performance_round_trips_through_store(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "monitor.db")
            store.initialize("test-admin-password")
            monitor = store.create_monitor(
                "Leader", "", SOURCE_URL, "5075281354358777856"
            )
            store.update_monitor_performance(
                monitor["id"],
                {
                    "drawdown_7d": 1.5,
                    "drawdown_30d": 2.5,
                    "drawdown_90d": None,
                    "roi_7d": 12.34,
                    "roi_30d": 56.78,
                    "roi_90d": None,
                    "win_rate_7d": 61.1,
                    "win_rate_30d": 55.5,
                    "win_rate_90d": None,
                    "win_orders_7d": 11,
                    "win_orders_30d": 45,
                    "win_orders_90d": None,
                    "total_orders_7d": 18,
                    "total_orders_30d": 81,
                    "total_orders_90d": None,
                    "pnl_7d": 100.25,
                    "pnl_30d": -50.5,
                    "pnl_90d": None,
                },
            )
            periods = store.performance(monitor["id"])[0]["periods"]
            self.assertEqual(periods["7d"]["roi"], 12.34)
            self.assertEqual(periods["30d"]["roi"], 56.78)
            self.assertIsNone(periods["90d"]["roi"])
            self.assertEqual(periods["30d"]["win_rate"], 55.5)
            self.assertEqual(periods["30d"]["win_orders"], 45)
            self.assertEqual(periods["30d"]["total_orders"], 81)
            self.assertEqual(periods["30d"]["max_drawdown"], 2.5)
            self.assertEqual(periods["30d"]["pnl"], -50.5)
            store.close()
    def test_leader_name_uses_detail_endpoint_nickname(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            self.assertTrue(request.url.path.endswith("lead-portfolio/detail"))
            self.assertEqual(request.url.params["portfolioId"], "5075281354358777856")
            return httpx.Response(200, json={"code": "000000", "data": {"nickname": "Leader"}})

        async def request_name() -> str:
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                return await fetch_leader_name(client, SOURCE_URL, "5075281354358777856")

        self.assertEqual(asyncio.run(request_name()), "Leader")


    def test_fetch_leader_finance_parses_detail_fields(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            self.assertTrue(request.url.path.endswith("lead-portfolio/detail"))
            self.assertEqual(request.url.params["portfolioId"], "5075281354358777856")
            return httpx.Response(
                200,
                json={
                    "code": "000000",
                    "data": {
                        "nickname": "Leader",
                        "marginBalance": "18256.38058624",
                        "aumAmount": "25212.35392926",
                    },
                },
            )

        async def request_finance() -> tuple[str, str]:
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                return await fetch_leader_finance(client, "5075281354358777856")

        self.assertEqual(
            asyncio.run(request_finance()), ("18256.38058624", "25212.35392926")
        )

    def test_update_monitor_finance_persists_values(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "monitor.db")
            store.initialize(None)
            monitor = store.create_monitor("Leader", "", SOURCE_URL, "5075281354358777856")
            self.assertEqual(monitor["margin_balance"], "")
            self.assertEqual(monitor["aum_amount"], "")

            store.update_monitor_finance(monitor["id"], "18256.38058624", "25212.35392926")
            updated = store.get_monitor(monitor["id"])
            self.assertIsNotNone(updated)
            self.assertEqual(updated["margin_balance"], "18256.38058624")
            self.assertEqual(updated["aum_amount"], "25212.35392926")
            store.close()

    def test_generated_admin_login_and_password_change(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, {"ADMIN_PASSWORD": ""}
        ):
            database_path = Path(directory) / "monitor.db"
            store = Store(database_path)
            generated_password = store.initialize(None)
            store.close()
            self.assertIsNotNone(generated_password)
            self.assertFalse((Path(directory) / "initial-admin-password.txt").exists())
            app = create_app(database_path, start_poller=False)
            with TestClient(app) as client:
                self.assertEqual(client.get("/api/dashboard").status_code, 401)
                login = client.post(
                    "/api/auth/login", json={"username": "admin", "password": generated_password}
                )
                self.assertEqual(login.status_code, 200)
                self.assertEqual(login.json()["user"]["username"], "admin")
                self.assertEqual(client.get("/api/auth/me").status_code, 200)

                changed = client.post(
                    "/api/auth/password",
                    json={
                        "current_password": generated_password,
                        "new_password": "new-password-with-sufficient-length",
                    },
                )
                self.assertEqual(changed.status_code, 200)
                self.assertEqual(client.get("/api/dashboard").status_code, 401)
                self.assertEqual(
                    client.post(
                        "/api/auth/login",
                        json={"username": "admin", "password": "new-password-with-sufficient-length"},
                    ).status_code,
                    200,
                )

    def test_session_lifetime_setting_revokes_existing_sessions(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, {"ADMIN_PASSWORD": "test-admin-password"}
        ):
            app = create_app(Path(directory) / "monitor.db", start_poller=False)
            with TestClient(app) as client:
                self.assertEqual(
                    client.post("/api/settings/session", json={"session_ttl_hours": 24}).status_code,
                    401,
                )
                self.assertEqual(
                    client.post(
                        "/api/auth/login",
                        json={"username": "admin", "password": "test-admin-password"},
                    ).status_code,
                    200,
                )

                updated = client.post("/api/settings/session", json={"session_ttl_hours": 24})

                self.assertEqual(updated.status_code, 200)
                self.assertEqual(updated.json()["session_ttl_hours"], 24)
                self.assertEqual(client.get("/api/dashboard").status_code, 401)
                login = client.post(
                    "/api/auth/login",
                    json={"username": "admin", "password": "test-admin-password"},
                )
                self.assertIn("Max-Age=86400", login.headers["set-cookie"])
                self.assertEqual(
                    client.get("/api/dashboard").json()["session"], {"session_ttl_hours": 24}
                )

    def test_poll_interval_setting_is_authenticated_and_persistent(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, {"ADMIN_PASSWORD": "test-admin-password"}
        ):
            database_path = Path(directory) / "monitor.db"
            app = create_app(database_path, start_poller=False)
            with TestClient(app) as client:
                self.assertEqual(
                    client.post("/api/settings/poll", json={"poll_interval_seconds": 5}).status_code,
                    401,
                )
                client.post(
                    "/api/auth/login",
                    json={"username": "admin", "password": "test-admin-password"},
                )
                updated = client.post("/api/settings/poll", json={"poll_interval_seconds": 5})
                self.assertEqual(updated.status_code, 200)
                self.assertEqual(updated.json(), {"poll_interval_seconds": 5})
                self.assertTrue(app.state.poll_settings_changed.is_set())
                self.assertEqual(client.get("/api/dashboard").json()["poll_interval_seconds"], 5)
                self.assertEqual(
                    client.post("/api/settings/poll", json={"poll_interval_seconds": 4}).status_code,
                    422,
                )

            restored = Store(database_path)
            restored.initialize("test-admin-password")
            self.assertEqual(restored.poll_interval_seconds(), 5)
            restored.close()

    def test_prune_old_operations_removes_expired_records(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "monitor.db")
            store.initialize("test-admin-password")
            monitor = store.create_monitor("Leader", "", SOURCE_URL, "5075281354358777856")
            store.save_baseline(monitor["id"], keyed_records([sample_record(1)]))
            self.assertEqual(store.prune_old_operations(2), 1)
            self.assertEqual(store.overview_metrics()["operation_count"], 0)
            store.close()

    def test_startup_retains_operations_and_deduplication_for_thirty_days(self) -> None:
        day_milliseconds = 24 * 60 * 60 * 1000
        now = int(time.time() * 1000)
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "monitor.db"
            store = Store(database_path)
            store.initialize("test-admin-password")
            monitor = store.create_monitor("Leader", "", SOURCE_URL, "5075281354358777856")
            store.save_baseline(
                monitor["id"],
                keyed_records(
                    [
                        sample_record(now - 30 * day_milliseconds - 60_000),
                        sample_record(now - 30 * day_milliseconds + 60_000),
                    ]
                ),
            )
            store.close()

            app = create_app(database_path, start_poller=False)
            with TestClient(app):
                self.assertEqual(app.state.store.overview_metrics()["operation_count"], 1)
                self.assertEqual(
                    app.state.store.connection.execute(
                        "SELECT COUNT(*) FROM known_operations"
                    ).fetchone()[0],
                    1,
                )

    def test_operation_history_migration_rebuilds_existing_baseline(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "monitor.db"
            store = Store(database_path)
            store.initialize("test-admin-password")
            monitor = store.create_monitor("Leader", "", SOURCE_URL, "5075281354358777856")
            store.save_baseline(monitor["id"], keyed_records([sample_record(1)]))
            store.connection.execute(
                "DELETE FROM settings WHERE key = 'operation_history_format_version'"
            )
            store.connection.commit()
            store.close()

            migrated = Store(database_path)
            migrated.initialize("test-admin-password")
            self.assertFalse(migrated.get_monitor(monitor["id"])["initialized"])
            self.assertEqual(migrated.overview_metrics()["operation_count"], 0)
            self.assertEqual(
                migrated.connection.execute("SELECT COUNT(*) FROM known_operations").fetchone()[0], 0
            )
            migrated.close()

    def test_monitor_modes_control_listening_and_operation_notifications(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "monitor.db")
            store.initialize("test-admin-password")
            monitor = store.create_monitor("Leader", "", SOURCE_URL, "5075281354358777856")
            monitor_id = monitor["id"]

            store.save_baseline(monitor_id, keyed_records([sample_record(1)]))
            store.save_new_operations(monitor_id, keyed_records([sample_record(2)]))
            self.assertEqual(store.recent_operations(monitor_id)[0]["notification_status"], "pending")

            silent = store.update_monitor(monitor_id, None, "silent")
            self.assertTrue(silent["enabled"])
            self.assertFalse(silent["notification_enabled"])
            self.assertEqual(silent["mode"], "silent")
            self.assertEqual(store.recent_operations(monitor_id)[0]["notification_status"], "skipped")
            store.save_new_operations(
                monitor_id,
                keyed_records([sample_record(3)]),
                silent["notification_enabled"],
            )
            self.assertEqual(store.recent_operations(monitor_id)[0]["notification_status"], "skipped")

            stopped = store.update_monitor(monitor_id, None, "stopped")
            self.assertFalse(stopped["enabled"])
            self.assertFalse(stopped["notification_enabled"])
            self.assertEqual(stopped["mode"], "stopped")

            notify = store.update_monitor(monitor_id, None, "notify")
            self.assertTrue(notify["enabled"])
            self.assertTrue(notify["notification_enabled"])
            self.assertEqual(notify["mode"], "notify")
            store.close()

    def test_notification_block_marks_matching_operations_and_never_backfills(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "monitor.db")
            store.initialize("test-admin-password")
            monitor = store.create_monitor("Leader", "", SOURCE_URL, "5075281354358777856")
            monitor_id = monitor["id"]

            store.save_baseline(monitor_id, keyed_records([sample_record(1)]))
            block = store.create_notification_block(monitor_id, "XAUUSDT")
            self.assertEqual(block["monitor_id"], monitor_id)
            self.assertEqual(block["symbol"], "XAUUSDT")

            store.save_new_operations(monitor_id, keyed_records([sample_record(2)]))
            self.assertEqual(store.recent_operations(monitor_id)[0]["notification_status"], "blocked")

            btc_record = {**sample_record(3), "symbol": "BTCUSDT", "baseAsset": "BTC"}
            store.save_new_operations(monitor_id, keyed_records([btc_record]))
            self.assertEqual(store.recent_operations(monitor_id)[0]["notification_status"], "pending")
            btc_block = store.create_notification_block(monitor_id, "BTCUSDT")
            self.assertEqual(store.recent_operations(monitor_id)[0]["notification_status"], "blocked")

            self.assertTrue(store.delete_notification_block(block["id"]))
            store.save_new_operations(monitor_id, keyed_records([sample_record(4)]))
            statuses = {
                operation["occurred_at"]: operation["notification_status"]
                for operation in store.recent_operations(monitor_id, 10)
            }
            self.assertEqual(statuses[2], "blocked")
            self.assertEqual(statuses[4], "pending")
            self.assertEqual(store.notification_blocks()[0]["id"], btc_block["id"])
            store.close()

    def test_notification_block_api_validates_and_exposes_rules(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, {"ADMIN_PASSWORD": "test-admin-password"}
        ):
            app = create_app(Path(directory) / "monitor.db", start_poller=False)
            with TestClient(app) as client:
                self.assertEqual(
                    client.post(
                        "/api/auth/login",
                        json={"username": "admin", "password": "test-admin-password"},
                    ).status_code,
                    200,
                )
                monitor = app.state.store.create_monitor(
                    "Leader", "策略", SOURCE_URL, "5075281354358777856"
                )
                created = client.post(
                    "/api/notification-blocks",
                    json={"monitor_id": monitor["id"], "symbol": "btcusdt"},
                )
                self.assertEqual(created.status_code, 201)
                self.assertEqual(created.json()["symbol"], "BTCUSDT")
                self.assertEqual(created.json()["monitor_name"], "Leader")
                self.assertEqual(
                    client.post(
                        "/api/notification-blocks",
                        json={"monitor_id": monitor["id"], "symbol": "BTCUSDT"},
                    ).status_code,
                    409,
                )
                self.assertEqual(
                    client.post(
                        "/api/notification-blocks",
                        json={"monitor_id": monitor["id"], "symbol": "BTC-USDT"},
                    ).status_code,
                    422,
                )
                self.assertEqual(
                    client.get("/api/dashboard").json()["notification_blocks"][0]["id"],
                    created.json()["id"],
                )
                self.assertEqual(
                    client.delete(f"/api/notification-blocks/{created.json()['id']}").status_code,
                    204,
                )
                self.assertEqual(client.get("/api/dashboard").json()["notification_blocks"], [])

    def test_pending_operations_prioritize_the_latest_record(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "monitor.db")
            store.initialize("test-admin-password")
            monitor = store.create_monitor("Leader", "", SOURCE_URL, "5075281354358777856")
            store.save_new_operations(
                monitor["id"],
                keyed_records([sample_record(1), sample_record(3), sample_record(2)]),
            )
            self.assertEqual(
                [operation["occurred_at"] for operation in store.pending_operations(monitor["id"], 3)],
                [3, 2, 1],
            )
            store.close()

    def test_reset_record_state_rebuilds_monitors_from_a_clean_baseline(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "monitor.db")
            store.initialize("test-admin-password")
            monitor = store.create_monitor("Leader", "note", SOURCE_URL, "5075281354358777856")
            store.save_baseline(monitor["id"], keyed_records([sample_record(1)]))
            store.save_new_operations(monitor["id"], keyed_records([sample_record(2)]))
            store.log_notification(monitor["id"], "sent", "sent operation")

            result = store.reset_record_state()

            self.assertEqual(result, {"operation_count": 2, "notification_count": 1, "monitor_count": 1})
            reset_monitor = store.get_monitor(monitor["id"])
            self.assertFalse(reset_monitor["initialized"])
            self.assertEqual(reset_monitor["mode"], "notify")
            self.assertIsNone(reset_monitor["last_success_at"])
            self.assertEqual(store.recent_operations(), [])
            self.assertEqual(store.notification_attempts(), [])
            self.assertEqual(
                store.connection.execute("SELECT COUNT(*) FROM known_operations").fetchone()[0], 0
            )
            self.assertEqual(store.monitor_urls(), [SOURCE_URL])
            store.close()

    def test_clear_logs_removes_notification_and_system_history(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "monitor.db")
            store.initialize("test-admin-password")
            store.log_notification(None, "sent", "notification log")
            store.log_event(None, "info", "system event", "system log")

            result = store.clear_logs()

            self.assertEqual(result, {"notification_count": 1, "system_log_count": 1})
            self.assertEqual(store.notification_attempts(), [])
            self.assertEqual(store.system_logs(), [])
            store.close()

    def test_reset_and_export_routes_require_a_session(self) -> None:
        records = [sample_record(1)]

        async def fake_fetcher(_, __: str):
            return records

        async def fake_leader_name(_, __: str, portfolio_id: str):
            return f"带单员 {portfolio_id[-4:]}"

        async def fake_drawdowns(_, __: str):
            return {
                "drawdown_7d": 0.0, "drawdown_30d": 0.0, "drawdown_90d": 0.0,
                "roi_7d": None, "roi_30d": None, "roi_90d": None,
                "win_rate_7d": None, "win_rate_30d": None, "win_rate_90d": None,
                "pnl_7d": None, "pnl_30d": None, "pnl_90d": None,
                "win_orders_7d": None, "win_orders_30d": None, "win_orders_90d": None,
                "total_orders_7d": None, "total_orders_30d": None, "total_orders_90d": None,
            }

        async def fake_finance(_, __: str):
            return ("18256.38", "25212.35")

        async def fake_symbol_precisions(_):
            return {}

        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, {"ADMIN_PASSWORD": "test-admin-password"}
        ):
            app = create_app(
                Path(directory) / "monitor.db",
                start_poller=False,
                fetcher=fake_fetcher,
                position_history_fetcher=empty_position_history,
                leader_name_fetcher=fake_leader_name,
                drawdown_fetcher=fake_drawdowns,
                leader_finance_fetcher=fake_finance,
                symbol_precision_fetcher=fake_symbol_precisions,
            )
            with TestClient(app) as client:
                self.assertEqual(client.post("/api/operations/reset", json={}).status_code, 401)
                self.assertEqual(client.get("/api/monitors/export").status_code, 401)
                self.assertEqual(client.delete("/api/logs").status_code, 401)
                self.assertEqual(
                    client.post(
                        "/api/auth/login",
                        json={"username": "admin", "password": "test-admin-password"},
                    ).status_code,
                    200,
                )
                app.state.store.create_monitor("Leader", "", SOURCE_URL, "5075281354358777856")
                exported = client.get("/api/monitors/export")
                self.assertEqual(exported.text, f"{SOURCE_URL}\n")
                self.assertEqual(
                    exported.headers["content-disposition"],
                    'attachment; filename="binance-copy-watch-sources.txt"',
                )
                monitor = app.state.store.list_monitors()[0]
                app.state.store.save_baseline(monitor["id"], keyed_records(records))
                app.state.store.save_new_operations(
                    monitor["id"], keyed_records([sample_record(2)])
                )
                app.state.store.log_notification(monitor["id"], "sent", "sent operation")

                reset = client.post("/api/operations/reset", json={})
                self.assertEqual(reset.status_code, 200)
                self.assertEqual(
                    reset.json(),
                    {"operation_count": 2, "notification_count": 1, "monitor_count": 1, "queued_count": 1},
                )
                client.portal.call(wait_for_monitor_initialization, app, monitor["id"])
                operations = client.get("/api/operations").json()["operations"]
                self.assertEqual(len(operations), 1)
                self.assertEqual(operations[0]["notification_status"], "baseline")
                self.assertEqual(app.state.store.notification_attempts(), [])

                app.state.store.log_notification(None, "sent", "notification log")
                cleared_logs = client.delete("/api/logs")
                self.assertEqual(cleared_logs.status_code, 200)
                self.assertGreaterEqual(cleared_logs.json()["system_log_count"], 1)
                self.assertEqual(app.state.store.notification_attempts(), [])
                self.assertEqual(app.state.store.system_logs(), [])

    def test_monitor_mode_migration_defaults_existing_addresses_to_notifications(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "monitor.db"
            connection = sqlite3.connect(database_path)
            connection.execute(
                """
                CREATE TABLE monitors (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    note TEXT NOT NULL DEFAULT '',
                    url TEXT NOT NULL UNIQUE,
                    portfolio_id TEXT NOT NULL UNIQUE,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    initialized INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    last_checked_at TEXT,
                    last_success_at TEXT,
                    last_error TEXT
                )
                """
            )
            connection.execute(
                """
                INSERT INTO monitors (name, note, url, portfolio_id, enabled, initialized, created_at)
                VALUES ('Leader', '', ?, '5075281354358777856', 1, 0, '2026-01-01T00:00:00+00:00')
                """,
                (SOURCE_URL,),
            )
            connection.commit()
            connection.close()

            store = Store(database_path)
            store.initialize("test-admin-password")
            monitor = store.list_monitors()[0]
            self.assertTrue(monitor["notification_enabled"])
            self.assertEqual(monitor["mode"], "notify")
            store.close()

    def test_batch_monitor_creation_returns_partial_results(self) -> None:
        async def fake_fetcher(_, __: str):
            return []

        async def fake_leader_name(_, __: str, portfolio_id: str):
            return f"带单员 {portfolio_id[-4:]}"

        async def fake_drawdowns(_, __: str):
            return {
                "drawdown_7d": 0.0, "drawdown_30d": 0.0, "drawdown_90d": 0.0,
                "roi_7d": None, "roi_30d": None, "roi_90d": None,
                "win_rate_7d": None, "win_rate_30d": None, "win_rate_90d": None,
                "pnl_7d": None, "pnl_30d": None, "pnl_90d": None,
                "win_orders_7d": None, "win_orders_30d": None, "win_orders_90d": None,
                "total_orders_7d": None, "total_orders_30d": None, "total_orders_90d": None,
            }

        async def fake_finance(_, __: str):
            return ("18256.38", "25212.35")

        async def fake_symbol_precisions(_):
            return {}

        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, {"ADMIN_PASSWORD": "test-admin-password"}
        ):
            app = create_app(
                Path(directory) / "monitor.db",
                start_poller=False,
                fetcher=fake_fetcher,
                position_history_fetcher=empty_position_history,
                leader_name_fetcher=fake_leader_name,
                drawdown_fetcher=fake_drawdowns,
                leader_finance_fetcher=fake_finance,
                symbol_precision_fetcher=fake_symbol_precisions,
            )
            with TestClient(app) as client:
                self.assertEqual(
                    client.post(
                        "/api/auth/login",
                        json={"username": "admin", "password": "test-admin-password"},
                    ).status_code,
                    200,
                )
                result = client.post(
                    "/api/monitors/batch",
                    json={
                        "note": "批量观察",
                        "urls": [
                            SOURCE_URL,
                            "invalid-url",
                            SOURCE_URL.replace("timeRange=7D", "timeRange=30D"),
                            BATCH_SOURCE_URL,
                        ],
                    },
                )
                self.assertEqual(result.status_code, 201)
                body = result.json()
                self.assertEqual(len(body["created"]), 2)
                self.assertEqual(len(body["errors"]), 2)
                self.assertEqual(body["created"][0]["note"], "批量观察")
                self.assertTrue(any(item["url"] == "invalid-url" for item in body["errors"]))
                self.assertTrue(any("已存在" in item["detail"] for item in body["errors"]))
                self.assertEqual(len(client.get("/api/dashboard").json()["monitors"]), 2)

    def test_initial_batch_sends_only_each_sources_latest_operation(self) -> None:
        records = [sample_record(1), sample_record(2)]
        sent_messages = []

        async def fake_fetcher(_, __: str):
            return records

        async def fake_leader_name(_, __: str, portfolio_id: str):
            return f"带单员 {portfolio_id[-4:]}"

        async def fake_drawdowns(_, __: str):
            return {
                "drawdown_7d": 0.0, "drawdown_30d": 0.0, "drawdown_90d": 0.0,
                "roi_7d": None, "roi_30d": None, "roi_90d": None,
                "win_rate_7d": None, "win_rate_30d": None, "win_rate_90d": None,
                "pnl_7d": None, "pnl_30d": None, "pnl_90d": None,
                "win_orders_7d": None, "win_orders_30d": None, "win_orders_90d": None,
                "total_orders_7d": None, "total_orders_30d": None, "total_orders_90d": None,
            }

        async def fake_finance(_, __: str):
            return ("18256.38", "25212.35")

        async def fake_symbol_precisions(_):
            return {}

        async def fake_send_telegram(_, text: str) -> None:
            sent_messages.append(text)

        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, {"ADMIN_PASSWORD": "test-admin-password"}
        ):
            app = create_app(
                Path(directory) / "monitor.db",
                start_poller=False,
                fetcher=fake_fetcher,
                position_history_fetcher=empty_position_history,
                leader_name_fetcher=fake_leader_name,
                drawdown_fetcher=fake_drawdowns,
                leader_finance_fetcher=fake_finance,
                symbol_precision_fetcher=fake_symbol_precisions,
            )
            with patch("app.main.send_telegram_html", new=fake_send_telegram), TestClient(app) as client:
                self.assertEqual(
                    client.post(
                        "/api/auth/login",
                        json={"username": "admin", "password": "test-admin-password"},
                    ).status_code,
                    200,
                )
                app.state.store.update_telegram_settings("token", "123", True)
                created = client.post(
                    "/api/monitors/batch",
                    json={"note": "首次通知", "urls": [SOURCE_URL, BATCH_SOURCE_URL]},
                ).json()["created"]
                for monitor in created:
                    client.portal.call(wait_for_operation_status, app, monitor["id"], "sent")
                    statuses = {
                        operation["occurred_at"]: operation["notification_status"]
                        for operation in app.state.store.recent_operations(monitor["id"], 10)
                    }
                    self.assertEqual(statuses, {2: "sent", 1: "baseline"})
                self.assertEqual(len(sent_messages), 2)

    def test_monitor_notes_operations_filter_and_performance(self) -> None:
        now = int(time.time() * 1000)
        records = [sample_record(now - 1_000, profit="3.5")]

        async def fake_fetcher(_, portfolio_id: str):
            self.assertEqual(portfolio_id, "5075281354358777856")
            return records

        async def fake_position_history(_, portfolio_id: str, __: int, ___: int):
            self.assertEqual(portfolio_id, "5075281354358777856")
            return [
                {
                    "symbol": "XAUUSDT",
                    "side": "Short",
                    "opened": now - 2_000,
                    "closed": now + 1_000,
                    "leverage": "12",
                }
            ]

        async def fake_leader_name(_, __: str, portfolio_id: str):
            self.assertEqual(portfolio_id, "5075281354358777856")
            return "自动带单员"

        async def fake_drawdowns(_, portfolio_id: str):
            self.assertEqual(portfolio_id, "5075281354358777856")
            return {
                "drawdown_7d": 4.55, "drawdown_30d": 8.33, "drawdown_90d": 12.34,
                "roi_7d": 12.0, "roi_30d": 34.5, "roi_90d": 56.7,
                "win_rate_7d": 50.0, "win_rate_30d": 62.5, "win_rate_90d": 55.0,
                "pnl_7d": 111.1, "pnl_30d": 2222.2, "pnl_90d": 33333.3,
                "win_orders_7d": 5, "win_orders_30d": 25, "win_orders_90d": 77,
                "total_orders_7d": 10, "total_orders_30d": 40, "total_orders_90d": 140,
            }

        async def fake_finance(_, portfolio_id: str):
            self.assertEqual(portfolio_id, "5075281354358777856")
            return ("18256.38058624", "25212.35392926")

        async def fake_symbol_precisions(_):
            return {"XAUUSDT": (2, 3)}

        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, {"ADMIN_PASSWORD": "test-admin-password"}
        ):
            app = create_app(
                Path(directory) / "monitor.db",
                start_poller=False,
                fetcher=fake_fetcher,
                position_history_fetcher=fake_position_history,
                leader_name_fetcher=fake_leader_name,
                drawdown_fetcher=fake_drawdowns,
                leader_finance_fetcher=fake_finance,
                symbol_precision_fetcher=fake_symbol_precisions,
            )
            with TestClient(app) as client:
                self.assertEqual(
                    client.post(
                        "/api/auth/login",
                        json={"username": "admin", "password": "test-admin-password"},
                    ).status_code,
                    200,
                )
                created = client.post(
                    "/api/monitors",
                    json={"note": "趋势观察", "url": SOURCE_URL},
                )
                self.assertEqual(created.status_code, 201)
                monitor_id = created.json()["id"]
                self.assertEqual(created.json()["name"], "自动带单员")
                self.assertEqual(created.json()["note"], "趋势观察")
                self.assertTrue(created.json()["check_queued"])
                client.portal.call(wait_for_monitor_initialization, app, monitor_id)
                client.portal.call(wait_for_background_tasks, app)
                initial_performance = client.get("/api/performance").json()["items"][0]
                self.assertEqual(initial_performance["periods"]["7d"]["max_drawdown"], 4.55)
                self.assertEqual(
                    client.get("/api/operations").json()["operations"][0]["reference_leverage"], "12"
                )
                refreshed_monitor = next(
                    item
                    for item in client.get("/api/dashboard").json()["monitors"]
                    if item["id"] == monitor_id
                )
                self.assertEqual(refreshed_monitor["margin_balance"], "18256.38058624")
                self.assertEqual(refreshed_monitor["aum_amount"], "25212.35392926")

                silent = client.patch(f"/api/monitors/{monitor_id}", json={"mode": "silent"})
                self.assertEqual(silent.status_code, 200)
                self.assertEqual(silent.json()["mode"], "silent")
                self.assertTrue(silent.json()["enabled"])
                self.assertFalse(silent.json()["notification_enabled"])

                records.insert(0, sample_record(now, "BUY", "-1.2"))
                detected = client.post(f"/api/monitors/{monitor_id}/check")
                self.assertEqual(detected.status_code, 202)
                client.portal.call(wait_for_operation_count, app, monitor_id, 2)
                client.portal.call(wait_for_background_tasks, app)
                self.assertEqual(client.get("/api/operations").json()["operations"][0]["action"], "平空")
                self.assertEqual(
                    client.get("/api/operations").json()["operations"][0]["reference_leverage"], "12"
                )

                edited = client.patch(
                    f"/api/monitors/{monitor_id}", json={"note": "已更新备注", "mode": "stopped"}
                )
                self.assertEqual(edited.json()["note"], "已更新备注")
                self.assertEqual(edited.json()["mode"], "stopped")
                self.assertEqual(client.post(f"/api/monitors/{monitor_id}/check").status_code, 409)
                operations = client.get(f"/api/operations?monitor_id={monitor_id}").json()["operations"]
                self.assertEqual(len(operations), 2)
                self.assertEqual(operations[0]["action"], "平空")
                self.assertEqual(operations[1]["action"], "平多")
                self.assertEqual(operations[0]["notification_status"], "skipped")
                performance = client.get("/api/performance").json()["items"][0]
                expected = {
                    "7d": {"win_rate": 50.0, "roi": 12.0, "max_drawdown": 4.55, "pnl": 111.1},
                    "30d": {"win_rate": 62.5, "roi": 34.5, "max_drawdown": 8.33, "pnl": 2222.2},
                    "90d": {"win_rate": 55.0, "roi": 56.7, "max_drawdown": 12.34, "pnl": 33333.3},
                }
                for period, values in expected.items():
                    for key, value in values.items():
                        self.assertEqual(performance["periods"][period][key], value)
                self.assertEqual(performance["periods"]["30d"]["win_orders"], 25)
                self.assertEqual(performance["periods"]["30d"]["total_orders"], 40)

                email_settings = client.post(
                    "/api/settings/email",
                    json={
                        "host": "smtp.example.com",
                        "port": 465,
                        "username": "alerts@example.com",
                        "password": "smtp-test-password",
                        "from_address": "watch@example.com",
                        "to_address": "owner@example.com",
                        "security": "ssl",
                        "enabled": False,
                    },
                )
                self.assertEqual(email_settings.status_code, 200)
                self.assertTrue(email_settings.json()["configured"])
                self.assertTrue(email_settings.json()["password_configured"])
                self.assertFalse(email_settings.json()["enabled"])
                self.assertEqual(email_settings.json()["to_address"], "owner@example.com")

                telegram_settings = client.post(
                    "/api/settings/telegram",
                    json={"bot_token": "test-token", "chat_id": "123", "enabled": False},
                )
                self.assertEqual(telegram_settings.status_code, 200)
                self.assertFalse(telegram_settings.json()["enabled"])

                cleared = client.delete("/api/operations")
                self.assertEqual(cleared.status_code, 200)
                self.assertEqual(cleared.json()["deleted_count"], 2)
                self.assertEqual(client.get("/api/operations").json()["operations"], [])
                resumed = client.patch(f"/api/monitors/{monitor_id}", json={"mode": "notify"})
                self.assertEqual(resumed.json()["mode"], "notify")
                self.assertEqual(client.post(f"/api/monitors/{monitor_id}/check").status_code, 202)
                dashboard = client.get("/api/dashboard").json()
                self.assertEqual(dashboard["system_logs"][0]["event"], "历史记录清理")

    def test_saving_enabled_telegram_channel_sends_test_message(self) -> None:
        sent_messages = []

        async def fake_send_telegram(_, text: str) -> None:
            sent_messages.append(text)

        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, {"ADMIN_PASSWORD": "test-admin-password"}
        ):
            app = create_app(Path(directory) / "monitor.db", start_poller=False)
            with patch("app.main.send_telegram_text", new=fake_send_telegram), TestClient(app) as client:
                self.assertEqual(
                    client.post(
                        "/api/auth/login",
                        json={"username": "admin", "password": "test-admin-password"},
                    ).status_code,
                    200,
                )
                saved = client.post(
                    "/api/settings/telegram",
                    json={"bot_token": "test-token", "chat_id": "123", "enabled": True},
                )
                self.assertEqual(saved.status_code, 200)
                self.assertEqual(saved.json()["test_status"], "sent")
                self.assertEqual(len(sent_messages), 1)

                unchanged = client.post(
                    "/api/settings/telegram",
                    json={"bot_token": "", "chat_id": "123", "enabled": True},
                )
                self.assertEqual(unchanged.status_code, 200)
                self.assertNotIn("test_status", unchanged.json())
                self.assertEqual(len(sent_messages), 1)


class HttpPoolSelfHealTest(unittest.TestCase):
    def test_network_failure_detection(self) -> None:
        ok = {"status": "ok"}
        server_busy = {
            "status": "error",
            "error": "Binance 查询失败: Binance 未返回可用的订单记录：The system is currently busy",
        }
        order_timeout = {
            "status": "error",
            "error": "Binance 查询失败: Binance 订单请求两次均在 8.5 秒内未完成",
        }
        connect_error = {"status": "error", "error": "Binance 查询失败: ConnectError：网络请求失败"}
        drawdown_timeout = {
            "status": "error",
            "error": "Binance 带单表现查询失败（已重试 3 次）: TimeoutError：未提供错误详情",
        }
        self.assertFalse(is_network_failure(ok))
        self.assertFalse(is_network_failure(server_busy))
        self.assertTrue(is_network_failure(order_timeout))
        self.assertTrue(is_network_failure(connect_error))
        self.assertTrue(is_network_failure(drawdown_timeout))

    def test_streak_increments_on_quota_and_resets(self) -> None:
        quarter_fail = [{"status": "error", "error": "TimeoutError"}] + [{"status": "ok"}] * 3
        below_quota = [{"status": "error", "error": "TimeoutError"}] + [{"status": "ok"}] * 99
        clean = [{"status": "ok"}] * 4
        streak = next_pool_rebuild_streak(0, quarter_fail)
        self.assertEqual(streak, 1)
        self.assertEqual(next_pool_rebuild_streak(streak, quarter_fail), 2)
        self.assertEqual(next_pool_rebuild_streak(2, below_quota), 0)
        self.assertEqual(next_pool_rebuild_streak(1, clean), 0)
        self.assertEqual(next_pool_rebuild_streak(3, []), 0)

    def test_single_monitor_round_still_counts(self) -> None:
        results = [{"status": "error", "error": "Binance 订单请求两次均在 8.5 秒内未完成"}]
        self.assertEqual(next_pool_rebuild_streak(0, results), 1)
