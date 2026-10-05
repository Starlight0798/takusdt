from __future__ import annotations

import asyncio
import base64
import hashlib
import html
import hmac
import json
import logging
import math
import os
import re
import secrets
import smtplib
import sqlite3
import threading
import time
import traceback
from collections import Counter, defaultdict
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Literal
from urllib.parse import parse_qs, parse_qsl, unquote, urlencode, urlparse
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI, HTTPException, Request, Response, status
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field


BINANCE_LEADER_DETAIL_URL = (
    "https://www.binance.com/bapi/futures/v1/friendly/future/"
    "copy-trade/lead-portfolio/detail"
)
BINANCE_ORDER_HISTORY_URL = (
    "https://www.binance.com/bapi/futures/v1/friendly/future/"
    "copy-trade/lead-portfolio/order-history"
)
BINANCE_TRADE_HISTORY_URL = (
    "https://www.binance.com/bapi/futures/v1/public/future/"
    "copy-trade/lead-portfolio/trade-history"
)
BINANCE_POSITION_HISTORY_URL = (
    "https://www.binance.com/bapi/futures/v1/friendly/future/"
    "copy-trade/lead-portfolio/position-history"
)
BINANCE_EXCHANGE_INFO_URL = "https://www.binance.com/fapi/v1/exchangeInfo?showall=true"
BINANCE_LEADER_CHART_URL = (
    "https://www.binance.com/bapi/futures/v1/public/future/"
    "copy-trade/lead-portfolio/chart-data"
)
POLL_INTERVAL_SECONDS = 20
MIN_POLL_INTERVAL_SECONDS = 5
MAX_POLL_INTERVAL_SECONDS = 60 * 60
MAX_PENDING_PER_NOTIFICATION = 20
INITIAL_HISTORY_DAYS = 7
REGULAR_HISTORY_HOURS = 24
OPERATION_RETENTION_DAYS = 30
MAX_INITIAL_HISTORY_PAGES = 60
MAX_POSITION_HISTORY_PAGES = 60
ORDER_HISTORY_TIMEOUT_SECONDS = 8.5
REFERENCE_LEVERAGE_TIMEOUT_SECONDS = 5.0
METADATA_TIMEOUT_SECONDS = 5.0
METADATA_FETCH_ATTEMPTS = 3
METADATA_RETRY_DELAY_SECONDS = 2.0
OPERATION_HISTORY_FORMAT_VERSION = "2"
TRADE_ALERT_HISTORY_FORMAT_VERSION = "2"
DEFAULT_SESSION_TTL_HOURS = 7 * 24
MAX_SESSION_TTL_HOURS = 30 * 24
SESSION_COOKIE = "copy_watch_session"
PASSWORD_ITERATIONS = 600_000
STATIC_DIR = Path(__file__).parent / "static"
# 面板公网地址，用于通知中的操作记录链接；未配置时通知不附带面板链接
DASHBOARD_BASE_URL = os.getenv("DASHBOARD_BASE_URL", "").strip().rstrip("/")
LOGGER = logging.getLogger("copy-watch")
SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
MONITOR_ERROR_FIELDS = (
    ("last_error", "订单查询"),
    ("last_drawdown_error", "回撤查询"),
    ("last_leverage_error", "参考杠杆"),
    ("last_trade_alert_error", "成交预警"),
)

Fetcher = Callable[[httpx.AsyncClient, str], Awaitable[list[dict[str, Any]]]]
PositionHistoryFetcher = Callable[
    [httpx.AsyncClient, str, int, int], Awaitable[list[dict[str, Any]]]
]
LeaderNameFetcher = Callable[[httpx.AsyncClient, str, str], Awaitable[str]]
DrawdownFetcher = Callable[[httpx.AsyncClient, str], Awaitable[dict[str, float | None]]]
LeaderFinanceFetcher = Callable[[httpx.AsyncClient, str], Awaitable[tuple[str, str]]]
SymbolPrecisionFetcher = Callable[[httpx.AsyncClient], Awaitable[dict[str, tuple[int, int]]]]


class MonitorCreate(BaseModel):
    note: str = Field(default="", max_length=300)
    url: str = Field(min_length=1, max_length=1000)


class MonitorBatchCreate(BaseModel):
    note: str = Field(default="", max_length=300)
    urls: list[str] = Field(min_length=1, max_length=20)


class MonitorUpdate(BaseModel):
    note: str | None = Field(default=None, max_length=300)
    mode: Literal["notify", "silent", "stopped"] | None = None


class NotificationBlockCreate(BaseModel):
    monitor_id: int = Field(ge=1)
    symbol: str = Field(min_length=1, max_length=30)


class TelegramSettingsUpdate(BaseModel):
    bot_token: str | None = Field(default=None, max_length=200)
    chat_id: str | None = Field(default=None, max_length=100)
    enabled: bool | None = None


class NotificationChannelCreate(BaseModel):
    kind: Literal["telegram", "dingtalk", "feishu"]
    name: str = Field(min_length=1, max_length=80)
    bot_token: str | None = Field(default=None, max_length=200)
    chat_id: str | None = Field(default=None, max_length=100)
    webhook_url: str | None = Field(default=None, max_length=2000)
    secret: str | None = Field(default=None, max_length=512)
    enabled: bool = True


class NotificationChannelUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=80)
    bot_token: str | None = Field(default=None, max_length=200)
    chat_id: str | None = Field(default=None, max_length=100)
    webhook_url: str | None = Field(default=None, max_length=2000)
    secret: str | None = Field(default=None, max_length=512)
    enabled: bool | None = None


class EmailSettingsUpdate(BaseModel):
    host: str | None = Field(default=None, max_length=255)
    port: int | None = Field(default=None, ge=1, le=65535)
    username: str | None = Field(default=None, max_length=255)
    password: str | None = Field(default=None, max_length=512)
    from_address: str | None = Field(default=None, max_length=255)
    to_address: str | None = Field(default=None, max_length=255)
    security: Literal["ssl", "starttls", "plain"] | None = None
    enabled: bool | None = None


class SessionSettingsUpdate(BaseModel):
    session_ttl_hours: int = Field(ge=1, le=MAX_SESSION_TTL_HOURS)


class PollSettingsUpdate(BaseModel):
    poll_interval_seconds: int = Field(
        ge=MIN_POLL_INTERVAL_SECONDS, le=MAX_POLL_INTERVAL_SECONDS
    )


class LoginPayload(BaseModel):
    username: str = Field(min_length=1, max_length=80)
    password: str = Field(min_length=1, max_length=256)


class PasswordChangePayload(BaseModel):
    current_password: str = Field(min_length=1, max_length=256)
    new_password: str = Field(min_length=12, max_length=256)


class PollError(Exception):
    pass


def notification_channel_configured(kind: str, config: dict[str, str]) -> bool:
    if kind == "telegram":
        return bool(config.get("bot_token") and config.get("chat_id"))
    if kind in {"dingtalk", "feishu"}:
        return bool(config.get("webhook_url"))
    return False


def validate_dingtalk_webhook_url(value: str) -> str:
    webhook_url = value.strip()
    parsed = urlparse(webhook_url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "oapi.dingtalk.com"
        or parsed.path != "/robot/send"
        or not parse_qs(parsed.query).get("access_token")
    ):
        raise ValueError("钉钉机器人 Webhook 必须是包含 access_token 的 oapi.dingtalk.com HTTPS 地址")
    return webhook_url


def validate_feishu_webhook_url(value: str) -> str:
    webhook_url = value.strip()
    parsed = urlparse(webhook_url)
    hook_prefix = "/open-apis/bot/v2/hook/"
    if (
        parsed.scheme != "https"
        or parsed.hostname != "open.feishu.cn"
        or not parsed.path.startswith(hook_prefix)
        or not parsed.path[len(hook_prefix) :]
    ):
        raise ValueError("飞书机器人 Webhook 必须是 open.feishu.cn 的 HTTPS 地址")
    return webhook_url


def notification_channel_public(row: sqlite3.Row) -> dict[str, Any]:
    try:
        config = json.loads(row["config_json"])
    except (TypeError, json.JSONDecodeError):
        config = {}
    if not isinstance(config, dict):
        config = {}
    channel = {
        "id": row["id"],
        "kind": row["kind"],
        "name": row["name"],
        "enabled": bool(row["enabled"]),
        "configured": notification_channel_configured(row["kind"], config),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }
    if row["kind"] == "telegram":
        channel["chat_id"] = str(config.get("chat_id") or "")
        channel["bot_token_configured"] = bool(config.get("bot_token"))
    elif row["kind"] in {"dingtalk", "feishu"}:
        channel["webhook_configured"] = bool(config.get("webhook_url"))
        channel["secret_configured"] = bool(config.get("secret"))
    return channel


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def password_hash(password: str) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, PASSWORD_ITERATIONS)
    salt_text = base64.urlsafe_b64encode(salt).decode("ascii")
    digest_text = base64.urlsafe_b64encode(digest).decode("ascii")
    return f"pbkdf2_sha256${PASSWORD_ITERATIONS}${salt_text}${digest_text}"


def verify_password(password: str, encoded: str) -> bool:
    try:
        algorithm, iterations_text, salt_text, digest_text = encoded.split("$", 3)
        if algorithm != "pbkdf2_sha256":
            return False
        salt = base64.urlsafe_b64decode(salt_text.encode("ascii"))
        expected = base64.urlsafe_b64decode(digest_text.encode("ascii"))
        actual = hashlib.pbkdf2_hmac(
            "sha256", password.encode("utf-8"), salt, int(iterations_text)
        )
    except (ValueError, TypeError):
        return False
    return hmac.compare_digest(actual, expected)


def extract_portfolio_id(url: str) -> str:
    parsed = urlparse(url.strip())
    hostname = (parsed.hostname or "").lower()
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("地址必须以 http:// 或 https:// 开头")
    if hostname not in {"binance.com", "www.binance.com"}:
        raise ValueError("目前仅支持 Binance 带单员详情地址")

    parts = [unquote(part) for part in parsed.path.split("/") if part]
    try:
        portfolio_id = parts[parts.index("lead-details") + 1]
    except (ValueError, IndexError) as error:
        raise ValueError("未从地址中识别到 Binance portfolioId") from error

    if not re.fullmatch(r"[A-Za-z0-9_-]{6,128}", portfolio_id):
        raise ValueError("Binance portfolioId 格式无效")
    return portfolio_id


def normalize_symbol(value: str) -> str:
    symbol = value.strip().upper()
    if not re.fullmatch(r"[A-Z0-9]{3,30}", symbol):
        raise ValueError("合约代码格式无效")
    return symbol


async def create_monitor_from_url(app: FastAPI, note: str, url: str) -> dict[str, Any]:
    normalized_url = url.strip()
    portfolio_id = extract_portfolio_id(normalized_url)
    name = await app.state.leader_name_fetcher(app.state.http, normalized_url, portfolio_id)
    monitor = app.state.store.create_monitor(name, note, normalized_url, portfolio_id)
    monitor["check_queued"] = queue_monitor_check(app, monitor["id"])
    return monitor


def keyed_records(records: Iterable[dict[str, Any]]) -> list[tuple[str, dict[str, Any]]]:
    occurrences: Counter[str] = Counter()
    result: list[tuple[str, dict[str, Any]]] = []
    for record in records:
        # Supplemental position data must not make an already known order look new.
        identity = {key: value for key, value in record.items() if key != "referenceLeverage"}
        canonical = json.dumps(identity, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        operation_key = f"{digest}:{occurrences[digest]}"
        occurrences[digest] += 1
        result.append((operation_key, record))
    return result


def trade_alert_key(record: dict[str, Any]) -> str:
    """Deduplicate exact trade-history duplicates without inventing an order ID."""
    canonical = json.dumps(record, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def aggregate_records(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], dict[str, Any]] = {}
    for record in records:
        occurred_at, symbol, side, position_side, _, *_ = operation_values(record)
        key = (
            occurred_at,
            symbol,
            side,
            position_side,
            str(record.get("baseAsset") or ""),
            str(record.get("quantityAsset") or "USDT"),
            str(record.get("realizedProfitAsset") or "USDT"),
        )
        grouped_record = grouped.get(key)
        if grouped_record is None:
            grouped[key] = dict(record)
            continue
        for field in ("qty", "quantity", "realizedProfit", "fee"):
            grouped_record[field] = decimal_text(
                decimal_value(grouped_record.get(field)) + decimal_value(record.get(field))
            )
    for record in grouped.values():
        quantity = decimal_value(record.get("quantity"))
        qty = decimal_value(record.get("qty"))
        if qty:
            record["price"] = decimal_text(quantity / qty)
    return list(grouped.values())


def safe_error(error: Exception) -> str:
    def compact(value: Any, fallback: str = "") -> str:
        text = " ".join(str(value or "").split())
        return text[:240] if text else fallback

    if isinstance(error, httpx.HTTPStatusError):
        response_message = ""
        try:
            payload = error.response.json()
            if isinstance(payload, dict):
                response_message = compact(payload.get("message") or payload.get("msg"))
        except (json.JSONDecodeError, ValueError):
            pass
        return (
            f"HTTP {error.response.status_code}：{response_message}"
            if response_message
            else f"HTTP {error.response.status_code}"
        )
    if isinstance(error, httpx.TimeoutException):
        return f"{type(error).__name__}：请求超时"
    if isinstance(error, httpx.RequestError):
        return f"{type(error).__name__}：{compact(error, '网络请求失败')}"
    if isinstance(error, PollError):
        return compact(error, "未提供错误详情")
    return f"{type(error).__name__}：{compact(error, '未提供错误详情')}"


def decimal_value(value: Any) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def decimal_text(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def operation_values(record: dict[str, Any]) -> tuple[int, str, str, str, str, str, str, str, str, str]:
    try:
        occurred_at = int(record.get("time") or 0)
    except (TypeError, ValueError):
        occurred_at = 0
    return (
        occurred_at,
        str(record.get("symbol") or "-"),
        str(record.get("side") or "-"),
        str(record.get("positionSide") or "-"),
        str(record.get("price") or "-"),
        str(record.get("qty") or "-"),
        str(record.get("baseAsset") or ""),
        str(record.get("quantity") or "-"),
        str(record.get("realizedProfit") or "0"),
        str(record.get("realizedProfitAsset") or "USDT"),
    )


def operation_action(
    side: str, position_side: str, realized_profit: Any = None
) -> tuple[str, str]:
    profit = decimal_value(realized_profit)
    if profit.is_finite() and profit:
        return {
            "BUY": ("平空", "close_short"),
            "SELL": ("平多", "close_long"),
        }.get(side.upper(), (f"{side} / {position_side}", "unknown"))
    mapping = {
        ("BUY", "LONG"): ("开多", "open_long"),
        ("SELL", "LONG"): ("平多", "close_long"),
        ("SELL", "SHORT"): ("开空", "open_short"),
        ("BUY", "SHORT"): ("平空", "close_short"),
    }
    if position_side.upper() == "BOTH":
        # Binance labels one-way-mode orders by execution direction, even when PnL is realized.
        return {
            "BUY": ("做多", "open_long"),
            "SELL": ("做空", "open_short"),
        }.get(side.upper(), (f"{side} / {position_side}", "unknown"))
    return mapping.get(
        (side.upper(), position_side.upper()), (f"{side} / {position_side}", "unknown")
    )


def wilson_lower_bound(wins: int, settled_records: int) -> float | None:
    if not settled_records:
        return None
    proportion = wins / settled_records
    z = 1.96
    denominator = 1 + z * z / settled_records
    centre = proportion + z * z / (2 * settled_records)
    adjustment = z * math.sqrt(
        proportion * (1 - proportion) / settled_records + z * z / (4 * settled_records**2)
    )
    return max(0.0, (centre - adjustment) / denominator * 100)


def sample_reliability(settled_records: int) -> str:
    if settled_records >= 50:
        return "高"
    if settled_records >= 20:
        return "中"
    if settled_records:
        return "低"
    return "暂无"


def period_performance(
    records: list[tuple[int, dict[str, Any]]], cutoff: int
) -> dict[str, Any]:
    total_records = 0
    settled_records = 0
    wins = 0
    losses = 0
    pnl_by_asset: defaultdict[str, Decimal] = defaultdict(Decimal)
    for occurred_at, payload in records:
        if occurred_at < cutoff:
            continue
        total_records += 1
        profit = decimal_value(payload.get("realizedProfit"))
        if not profit:
            continue
        settled_records += 1
        if profit > 0:
            wins += 1
        else:
            losses += 1
        pnl_by_asset[str(payload.get("realizedProfitAsset") or "USDT")] += profit

    conservative_win_rate = wilson_lower_bound(wins, settled_records)
    return {
        "total_records": total_records,
        "settled_records": settled_records,
        "wins": wins,
        "losses": losses,
        "win_rate": round(wins / settled_records * 100, 1) if settled_records else None,
        "conservative_win_rate": (
            round(conservative_win_rate, 1) if conservative_win_rate is not None else None
        ),
        "sample_reliability": sample_reliability(settled_records),
        "pnl": [
            {"asset": asset, "amount": decimal_text(amount)}
            for asset, amount in sorted(pnl_by_asset.items())
        ],
    }


class Store:
    def __init__(self, database_path: Path) -> None:
        database_path.parent.mkdir(parents=True, exist_ok=True)
        self.database_path = database_path
        self.connection = sqlite3.connect(database_path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.lock = threading.RLock()

    def _ensure_column(self, table: str, name: str, definition: str) -> None:
        columns = {row["name"] for row in self.connection.execute(f"PRAGMA table_info({table})")}
        if name not in columns:
            self.connection.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")

    def initialize(self, configured_admin_password: str | None) -> str | None:
        with self.lock:
            self.connection.executescript(
                """
                PRAGMA foreign_keys = ON;

                CREATE TABLE IF NOT EXISTS monitors (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    note TEXT NOT NULL DEFAULT '',
                    url TEXT NOT NULL UNIQUE,
                    portfolio_id TEXT NOT NULL UNIQUE,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    notification_enabled INTEGER NOT NULL DEFAULT 1,
                    initialized INTEGER NOT NULL DEFAULT 0,
                    notify_latest_baseline INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    last_checked_at TEXT,
                    last_success_at TEXT,
                    last_error TEXT,
                    drawdown_7d REAL,
                    drawdown_30d REAL,
                    drawdown_90d REAL,
                    drawdown_updated_at TEXT,
                    last_drawdown_error TEXT,
                    last_leverage_error TEXT,
                    trade_alerts_initialized INTEGER NOT NULL DEFAULT 0,
                    trade_alerts_started_at INTEGER,
                    last_trade_alert_error TEXT,
                    margin_balance TEXT NOT NULL DEFAULT '',
                    aum_amount TEXT NOT NULL DEFAULT ''
                );

                CREATE TABLE IF NOT EXISTS operations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    monitor_id INTEGER NOT NULL,
                    operation_key TEXT NOT NULL,
                    occurred_at INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    position_side TEXT NOT NULL,
                    price TEXT NOT NULL,
                    qty TEXT NOT NULL,
                    base_asset TEXT NOT NULL,
                    quantity TEXT NOT NULL,
                    realized_profit TEXT NOT NULL DEFAULT '0',
                    profit_asset TEXT NOT NULL DEFAULT 'USDT',
                    reference_leverage TEXT,
                    payload_json TEXT NOT NULL,
                    notification_status TEXT NOT NULL,
                    discovered_at TEXT NOT NULL,
                    notified_at TEXT,
                    UNIQUE (monitor_id, operation_key),
                    FOREIGN KEY (monitor_id) REFERENCES monitors(id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS operations_monitor_pending_idx
                    ON operations (monitor_id, notification_status, occurred_at);
                CREATE INDEX IF NOT EXISTS operations_recent_idx
                    ON operations (occurred_at DESC, id DESC);

                CREATE TABLE IF NOT EXISTS known_operations (
                    monitor_id INTEGER NOT NULL,
                    operation_key TEXT NOT NULL,
                    occurred_at INTEGER NOT NULL,
                    PRIMARY KEY (monitor_id, operation_key),
                    FOREIGN KEY (monitor_id) REFERENCES monitors(id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS known_operations_occurred_idx
                    ON known_operations (occurred_at);

                CREATE TABLE IF NOT EXISTS notification_blocks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    monitor_id INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE (monitor_id, symbol),
                    FOREIGN KEY (monitor_id) REFERENCES monitors(id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS notification_blocks_monitor_idx
                    ON notification_blocks (monitor_id);

                CREATE TABLE IF NOT EXISTS trade_alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    monitor_id INTEGER NOT NULL,
                    alert_key TEXT NOT NULL,
                    occurred_at INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    position_side TEXT NOT NULL,
                    price TEXT NOT NULL,
                    qty TEXT NOT NULL,
                    base_asset TEXT NOT NULL,
                    quantity TEXT NOT NULL,
                    realized_profit TEXT NOT NULL DEFAULT '0',
                    profit_asset TEXT NOT NULL DEFAULT 'USDT',
                    payload_json TEXT NOT NULL,
                    notification_status TEXT NOT NULL,
                    discovered_at TEXT NOT NULL,
                    notified_at TEXT,
                    UNIQUE (monitor_id, alert_key),
                    FOREIGN KEY (monitor_id) REFERENCES monitors(id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS trade_alerts_monitor_pending_idx
                    ON trade_alerts (monitor_id, notification_status, occurred_at);
                CREATE INDEX IF NOT EXISTS trade_alerts_match_idx
                    ON trade_alerts (monitor_id, symbol, side, position_side, occurred_at);

                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS notification_channels (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    kind TEXT NOT NULL CHECK (kind IN ('telegram', 'dingtalk', 'feishu')),
                    name TEXT NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    config_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS notification_channels_kind_idx
                    ON notification_channels (kind, enabled, id DESC);

                CREATE TABLE IF NOT EXISTS notification_attempts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    monitor_id INTEGER,
                    channel_name TEXT,
                    status TEXT NOT NULL,
                    message TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (monitor_id) REFERENCES monitors(id) ON DELETE SET NULL
                );

                CREATE TABLE IF NOT EXISTS system_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    monitor_id INTEGER,
                    level TEXT NOT NULL,
                    event TEXT NOT NULL,
                    message TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (monitor_id) REFERENCES monitors(id) ON DELETE SET NULL
                );
                CREATE INDEX IF NOT EXISTS system_logs_recent_idx
                    ON system_logs (id DESC);

                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT NOT NULL UNIQUE,
                    password_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS sessions (
                    token_hash TEXT PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    expires_at INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
                );
                """
            )
            self._ensure_column("monitors", "note", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(
                "monitors", "notification_enabled", "INTEGER NOT NULL DEFAULT 1"
            )
            self._ensure_column(
                "monitors", "notify_latest_baseline", "INTEGER NOT NULL DEFAULT 0"
            )
            self._ensure_column("monitors", "drawdown_7d", "REAL")
            self._ensure_column("monitors", "drawdown_30d", "REAL")
            self._ensure_column("monitors", "drawdown_90d", "REAL")
            self._ensure_column("monitors", "roi_7d", "REAL")
            self._ensure_column("monitors", "roi_30d", "REAL")
            self._ensure_column("monitors", "roi_90d", "REAL")
            self._ensure_column("monitors", "drawdown_updated_at", "TEXT")
            self._ensure_column("monitors", "last_drawdown_error", "TEXT")
            self._ensure_column("monitors", "last_leverage_error", "TEXT")
            self._ensure_column(
                "monitors", "trade_alerts_initialized", "INTEGER NOT NULL DEFAULT 0"
            )
            self._ensure_column("monitors", "trade_alerts_started_at", "INTEGER")
            self._ensure_column("monitors", "last_trade_alert_error", "TEXT")
            self._ensure_column("monitors", "margin_balance", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column("monitors", "aum_amount", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column("operations", "realized_profit", "TEXT NOT NULL DEFAULT '0'")
            self._ensure_column("operations", "profit_asset", "TEXT NOT NULL DEFAULT 'USDT'")
            self._ensure_column("operations", "reference_leverage", "TEXT")
            self._ensure_column("notification_attempts", "channel_name", "TEXT")
            self._migrate_notification_channels_schema()
            self._migrate_legacy_telegram_channel()
            self.connection.execute(
                """
                INSERT OR IGNORE INTO known_operations (monitor_id, operation_key, occurred_at)
                SELECT monitor_id, operation_key, occurred_at FROM operations
                """
            )
            self._migrate_operation_history_format()
            self._migrate_trade_alert_history()

            has_user = self.connection.execute("SELECT 1 FROM users LIMIT 1").fetchone()
            if not has_user:
                password = configured_admin_password or secrets.token_urlsafe(18)
                now = utc_now()
                self.connection.execute(
                    """
                    INSERT INTO users (username, password_hash, created_at, updated_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    ("admin", password_hash(password), now, now),
                )
            self.connection.commit()
        return password if not has_user and not configured_admin_password else None

    def _migrate_notification_channels_schema(self) -> None:
        row = self.connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'notification_channels'"
        ).fetchone()
        if not row or "'feishu'" in (row["sql"] or ""):
            return
        self.connection.executescript(
            """
            DROP INDEX IF EXISTS notification_channels_kind_idx;
            CREATE TABLE notification_channels_replacement (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT NOT NULL CHECK (kind IN ('telegram', 'dingtalk', 'feishu')),
                name TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                config_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            INSERT INTO notification_channels_replacement
                (id, kind, name, enabled, config_json, created_at, updated_at)
            SELECT id, kind, name, enabled, config_json, created_at, updated_at
            FROM notification_channels;
            DROP TABLE notification_channels;
            ALTER TABLE notification_channels_replacement RENAME TO notification_channels;
            CREATE INDEX notification_channels_kind_idx
                ON notification_channels (kind, enabled, id DESC);
            """
        )

    def _migrate_legacy_telegram_channel(self) -> None:
        settings = {
            row["key"]: row["value"]
            for row in self.connection.execute("SELECT key, value FROM settings")
        }
        token = settings.get("telegram_bot_token", "")
        chat_id = settings.get("telegram_chat_id", "")
        if settings.get("telegram_enabled", "1") != "1" or not token or not chat_id:
            return
        existing = self.connection.execute(
            "SELECT 1 FROM notification_channels WHERE kind = 'telegram' AND name = ?",
            ("已导入 Telegram Bot",),
        ).fetchone()
        if not existing:
            now = utc_now()
            self.connection.execute(
                """
                INSERT INTO notification_channels (kind, name, enabled, config_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    "telegram",
                    "已导入 Telegram Bot",
                    1,
                    json.dumps({"bot_token": token, "chat_id": chat_id}, ensure_ascii=True),
                    now,
                    now,
                ),
            )
        self.connection.execute(
            """
            INSERT INTO settings (key, value) VALUES ('telegram_enabled', '0')
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """
        )

    def _migrate_operation_history_format(self) -> None:
        row = self.connection.execute(
            "SELECT value FROM settings WHERE key = 'operation_history_format_version'"
        ).fetchone()
        if row and row["value"] == OPERATION_HISTORY_FORMAT_VERSION:
            return
        self.connection.execute("DELETE FROM operations")
        self.connection.execute("DELETE FROM known_operations")
        self.connection.execute("DELETE FROM trade_alerts")
        self.connection.execute(
            """
            UPDATE monitors
            SET initialized = 0, notify_latest_baseline = 0, trade_alerts_initialized = 0,
                trade_alerts_started_at = NULL,
                last_checked_at = NULL, last_success_at = NULL, last_error = NULL,
                last_trade_alert_error = NULL
            """
        )
        self.connection.execute(
            """
            INSERT INTO settings (key, value) VALUES ('operation_history_format_version', ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (OPERATION_HISTORY_FORMAT_VERSION,),
        )

    def _migrate_trade_alert_history(self) -> None:
        row = self.connection.execute(
            "SELECT value FROM settings WHERE key = 'trade_alert_history_format_version'"
        ).fetchone()
        if row and row["value"] == TRADE_ALERT_HISTORY_FORMAT_VERSION:
            return
        started_at = int(time.time() * 1000)
        # Older versions could see a shifting trade-history page and mistake old fills for new ones.
        self.connection.execute(
            "UPDATE trade_alerts SET notification_status = 'skipped' WHERE notification_status = 'pending'"
        )
        self.connection.execute(
            """
            UPDATE monitors
            SET trade_alerts_started_at = ?, last_trade_alert_error = NULL
            WHERE trade_alerts_initialized = 1
            """,
            (started_at,),
        )
        self.connection.execute(
            """
            INSERT INTO settings (key, value) VALUES ('trade_alert_history_format_version', ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (TRADE_ALERT_HISTORY_FORMAT_VERSION,),
        )

    def close(self) -> None:
        with self.lock:
            self.connection.close()

    def prune_old_operations(self, cutoff: int) -> int:
        with self.lock:
            cursor = self.connection.execute("DELETE FROM operations WHERE occurred_at < ?", (cutoff,))
            self.connection.execute("DELETE FROM known_operations WHERE occurred_at < ?", (cutoff,))
            self.connection.execute("DELETE FROM trade_alerts WHERE occurred_at < ?", (cutoff,))
            self.connection.commit()
        return cursor.rowcount

    def clear_operations(self) -> int:
        with self.lock:
            cursor = self.connection.execute("DELETE FROM operations")
            self.connection.commit()
        return cursor.rowcount

    def reset_record_state(self) -> dict[str, int]:
        with self.lock:
            operation_count = self.connection.execute("SELECT COUNT(*) FROM operations").fetchone()[0]
            notification_count = self.connection.execute(
                "SELECT COUNT(*) FROM notification_attempts"
            ).fetchone()[0]
            monitor_count = self.connection.execute("SELECT COUNT(*) FROM monitors").fetchone()[0]
            self.connection.execute("DELETE FROM operations")
            self.connection.execute("DELETE FROM known_operations")
            self.connection.execute("DELETE FROM trade_alerts")
            self.connection.execute("DELETE FROM notification_attempts")
            self.connection.execute(
                """
                UPDATE monitors
                SET initialized = 0, notify_latest_baseline = 0, trade_alerts_initialized = 0,
                    trade_alerts_started_at = NULL,
                    last_checked_at = NULL, last_success_at = NULL,
                    last_error = NULL, drawdown_7d = NULL, drawdown_30d = NULL,
                    drawdown_90d = NULL, drawdown_updated_at = NULL, last_drawdown_error = NULL,
                    roi_7d = NULL, roi_30d = NULL, roi_90d = NULL,
                    last_leverage_error = NULL, last_trade_alert_error = NULL
                """
            )
            self.connection.commit()
        return {
            "operation_count": operation_count,
            "notification_count": notification_count,
            "monitor_count": monitor_count,
        }

    def _monitor_from_row(self, row: sqlite3.Row) -> dict[str, Any]:
        monitor = dict(row)
        monitor["enabled"] = bool(monitor["enabled"])
        monitor["notification_enabled"] = bool(monitor["notification_enabled"])
        monitor["initialized"] = bool(monitor["initialized"])
        monitor["notify_latest_baseline"] = bool(monitor["notify_latest_baseline"])
        monitor["trade_alerts_initialized"] = bool(monitor["trade_alerts_initialized"])
        monitor["note"] = monitor.get("note") or ""
        monitor["mode"] = (
            "notify"
            if monitor["enabled"] and monitor["notification_enabled"]
            else "silent"
            if monitor["enabled"]
            else "stopped"
        )
        monitor["current_errors"] = [
            {"scope": scope, "message": str(monitor[field])}
            for field, scope in MONITOR_ERROR_FIELDS
            if monitor.get(field)
        ]
        return monitor

    def list_monitors(self) -> list[dict[str, Any]]:
        with self.lock:
            rows = self.connection.execute(
                """
                SELECT monitors.*,
                (SELECT COUNT(*) FROM operations
                 WHERE operations.monitor_id = monitors.id
                   AND operations.notification_status = 'pending')
                + (SELECT COUNT(*) FROM trade_alerts
                   WHERE trade_alerts.monitor_id = monitors.id
                     AND trade_alerts.notification_status = 'pending') AS pending_count
                FROM monitors
                ORDER BY id DESC
                """
            ).fetchall()
        return [self._monitor_from_row(row) for row in rows]

    def monitor_urls(self) -> list[str]:
        with self.lock:
            rows = self.connection.execute("SELECT url FROM monitors ORDER BY id").fetchall()
        return [str(row["url"]) for row in rows]

    def get_monitor(self, monitor_id: int) -> dict[str, Any] | None:
        with self.lock:
            row = self.connection.execute(
                """
                SELECT monitors.*,
                       (SELECT COUNT(*) FROM operations
                        WHERE operations.monitor_id = monitors.id
                          AND operations.notification_status = 'pending')
                       + (SELECT COUNT(*) FROM trade_alerts
                          WHERE trade_alerts.monitor_id = monitors.id
                            AND trade_alerts.notification_status = 'pending') AS pending_count
                FROM monitors
                WHERE id = ?
                """,
                (monitor_id,),
            ).fetchone()
        return self._monitor_from_row(row) if row else None

    def create_monitor(self, name: str, note: str, url: str, portfolio_id: str) -> dict[str, Any]:
        with self.lock:
            cursor = self.connection.execute(
                """
                INSERT INTO monitors (name, note, url, portfolio_id, notify_latest_baseline, created_at)
                VALUES (?, ?, ?, ?, 1, ?)
                """,
                (name, note, url, portfolio_id, utc_now()),
            )
            self.connection.commit()
        monitor = self.get_monitor(cursor.lastrowid)
        assert monitor is not None
        return monitor

    def notification_blocks(self) -> list[dict[str, Any]]:
        with self.lock:
            rows = self.connection.execute(
                """
                SELECT notification_blocks.id, notification_blocks.monitor_id,
                       notification_blocks.symbol, notification_blocks.created_at,
                       monitors.name AS monitor_name, monitors.note AS monitor_note
                FROM notification_blocks
                JOIN monitors ON monitors.id = notification_blocks.monitor_id
                ORDER BY notification_blocks.id DESC
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def create_notification_block(self, monitor_id: int, symbol: str) -> dict[str, Any] | None:
        with self.lock:
            cursor = self.connection.cursor()
            cursor.execute(
                """
                INSERT OR IGNORE INTO notification_blocks (monitor_id, symbol, created_at)
                VALUES (?, ?, ?)
                """,
                (monitor_id, symbol, utc_now()),
            )
            if not cursor.rowcount:
                self.connection.commit()
                return None
            block_id = cursor.lastrowid
            cursor.execute(
                """
                UPDATE operations
                SET notification_status = 'blocked'
                WHERE monitor_id = ? AND symbol = ? AND notification_status = 'pending'
                """,
                (monitor_id, symbol),
            )
            cursor.execute(
                """
                UPDATE trade_alerts
                SET notification_status = 'blocked'
                WHERE monitor_id = ? AND symbol = ? AND notification_status = 'pending'
                """,
                (monitor_id, symbol),
            )
            self.connection.commit()
        return next(
            (block for block in self.notification_blocks() if block["id"] == block_id),
            None,
        )

    def delete_notification_block(self, block_id: int) -> bool:
        with self.lock:
            cursor = self.connection.execute("DELETE FROM notification_blocks WHERE id = ?", (block_id,))
            self.connection.commit()
        return cursor.rowcount > 0

    @staticmethod
    def _blocked_symbols(cursor: sqlite3.Cursor, monitor_id: int) -> set[str]:
        rows = cursor.execute(
            "SELECT symbol FROM notification_blocks WHERE monitor_id = ?", (monitor_id,)
        ).fetchall()
        return {str(row["symbol"]) for row in rows}

    def is_notification_blocked(self, monitor_id: int, symbol: str) -> bool:
        with self.lock:
            row = self.connection.execute(
                """
                SELECT 1 FROM notification_blocks
                WHERE monitor_id = ? AND symbol = ?
                LIMIT 1
                """,
                (monitor_id, symbol),
            ).fetchone()
        return row is not None

    def mark_operations_blocked(self, monitor_id: int, operation_keys: list[str]) -> None:
        if not operation_keys:
            return
        placeholders = ",".join("?" for _ in operation_keys)
        with self.lock:
            self.connection.execute(
                f"""
                UPDATE operations
                SET notification_status = 'blocked'
                WHERE monitor_id = ? AND operation_key IN ({placeholders})
                  AND notification_status = 'pending'
                """,
                (monitor_id, *operation_keys),
            )
            self.connection.commit()

    def update_monitor(
        self, monitor_id: int, note: str | None, mode: str | None
    ) -> dict[str, Any] | None:
        if note is None and mode is None:
            return self.get_monitor(monitor_id)
        with self.lock:
            if note is not None:
                self.connection.execute(
                    "UPDATE monitors SET note = ? WHERE id = ?", (note, monitor_id)
                )
            if mode is not None:
                enabled, notification_enabled = {
                    "notify": (1, 1),
                    "silent": (1, 0),
                    "stopped": (0, 0),
                }[mode]
                self.connection.execute(
                    "UPDATE monitors SET enabled = ?, notification_enabled = ? WHERE id = ?",
                    (enabled, notification_enabled, monitor_id),
                )
                if not notification_enabled:
                    self.connection.execute(
                        """
                        UPDATE operations
                        SET notification_status = 'skipped'
                        WHERE monitor_id = ? AND notification_status = 'pending'
                        """,
                        (monitor_id,),
                    )
                    self.connection.execute(
                        """
                        UPDATE trade_alerts
                        SET notification_status = 'skipped'
                        WHERE monitor_id = ? AND notification_status = 'pending'
                        """,
                        (monitor_id,),
                    )
            self.connection.commit()
        return self.get_monitor(monitor_id)

    def update_monitor_name(self, monitor_id: int, name: str) -> dict[str, Any] | None:
        with self.lock:
            self.connection.execute(
                "UPDATE monitors SET name = ? WHERE id = ?", (name, monitor_id)
            )
            self.connection.commit()
        return self.get_monitor(monitor_id)

    def delete_monitor(self, monitor_id: int) -> bool:
        with self.lock:
            cursor = self.connection.execute("DELETE FROM monitors WHERE id = ?", (monitor_id,))
            self.connection.commit()
        return cursor.rowcount > 0

    def set_monitor_error(self, monitor_id: int, error: str) -> bool:
        message = error[:300]
        with self.lock:
            row = self.connection.execute(
                "SELECT last_error FROM monitors WHERE id = ?", (monitor_id,)
            ).fetchone()
            self.connection.execute(
                """
                UPDATE monitors
                SET last_checked_at = ?, last_error = ?
                WHERE id = ?
                """,
                (utc_now(), message, monitor_id),
            )
            self.connection.commit()
        return bool(row and row["last_error"] != message)

    def update_monitor_drawdowns(self, monitor_id: int, drawdowns: dict[str, float | None]) -> None:
        with self.lock:
            self.connection.execute(
                """
                UPDATE monitors
                SET drawdown_7d = ?, drawdown_30d = ?, drawdown_90d = ?,
                    roi_7d = ?, roi_30d = ?, roi_90d = ?,
                    drawdown_updated_at = ?, last_drawdown_error = NULL
                WHERE id = ?
                """,
                (
                    drawdowns.get("7d"),
                    drawdowns.get("30d"),
                    drawdowns.get("90d"),
                    drawdowns.get("roi_7d"),
                    drawdowns.get("roi_30d"),
                    drawdowns.get("roi_90d"),
                    utc_now(),
                    monitor_id,
                ),
            )
            self.connection.commit()

    def update_monitor_finance(self, monitor_id: int, margin_balance: str, aum_amount: str) -> None:
        with self.lock:
            self.connection.execute(
                "UPDATE monitors SET margin_balance = ?, aum_amount = ? WHERE id = ?",
                (margin_balance, aum_amount, monitor_id),
            )
            self.connection.commit()

    def set_monitor_drawdown_error(self, monitor_id: int, error: str) -> bool:
        message = error[:300]
        with self.lock:
            row = self.connection.execute(
                "SELECT last_drawdown_error FROM monitors WHERE id = ?", (monitor_id,)
            ).fetchone()
            self.connection.execute(
                "UPDATE monitors SET last_drawdown_error = ? WHERE id = ?",
                (message, monitor_id),
            )
            self.connection.commit()
        return bool(row and row["last_drawdown_error"] != message)

    def set_monitor_leverage_error(self, monitor_id: int, error: str) -> bool:
        message = error[:300]
        with self.lock:
            row = self.connection.execute(
                "SELECT last_leverage_error FROM monitors WHERE id = ?", (monitor_id,)
            ).fetchone()
            self.connection.execute(
                "UPDATE monitors SET last_leverage_error = ? WHERE id = ?",
                (message, monitor_id),
            )
            self.connection.commit()
        return bool(row and row["last_leverage_error"] != message)

    def clear_monitor_leverage_error(self, monitor_id: int) -> None:
        with self.lock:
            self.connection.execute(
                "UPDATE monitors SET last_leverage_error = NULL WHERE id = ?", (monitor_id,)
            )
            self.connection.commit()

    def set_monitor_trade_alert_error(self, monitor_id: int, error: str) -> bool:
        message = error[:300]
        with self.lock:
            row = self.connection.execute(
                "SELECT last_trade_alert_error FROM monitors WHERE id = ?", (monitor_id,)
            ).fetchone()
            self.connection.execute(
                "UPDATE monitors SET last_trade_alert_error = ? WHERE id = ?",
                (message, monitor_id),
            )
            self.connection.commit()
        return bool(row and row["last_trade_alert_error"] != message)

    def clear_monitor_trade_alert_error(self, monitor_id: int) -> None:
        with self.lock:
            self.connection.execute(
                "UPDATE monitors SET last_trade_alert_error = NULL WHERE id = ?", (monitor_id,)
            )
            self.connection.commit()

    def clear_trade_alert_errors(self) -> None:
        with self.lock:
            self.connection.execute("UPDATE monitors SET last_trade_alert_error = NULL")
            self.connection.commit()

    def _insert_operation(
        self,
        cursor: sqlite3.Cursor,
        monitor_id: int,
        operation_key: str,
        record: dict[str, Any],
        notification_status: str,
    ) -> bool:
        values = operation_values(record)
        reference_leverage = str(record.get("referenceLeverage") or "") or None
        cursor.execute(
            """
            INSERT OR IGNORE INTO known_operations (monitor_id, operation_key, occurred_at)
            VALUES (?, ?, ?)
            """,
            (monitor_id, operation_key, values[0]),
        )
        if not cursor.rowcount:
            return False
        cursor.execute(
            """
            INSERT INTO operations (
                monitor_id, operation_key, occurred_at, symbol, side, position_side,
                price, qty, base_asset, quantity, realized_profit, profit_asset,
                reference_leverage, payload_json, notification_status, discovered_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                monitor_id,
                operation_key,
                *values,
                reference_leverage,
                json.dumps(record, ensure_ascii=False, separators=(",", ":")),
                notification_status,
                utc_now(),
            ),
        )
        return cursor.rowcount > 0

    def save_baseline(
        self,
        monitor_id: int,
        records: Iterable[tuple[str, dict[str, Any]]],
        notify_latest: bool = False,
    ) -> int:
        records = list(records)
        latest_key = (
            max(records, key=lambda item: (int(item[1].get("time") or 0), item[0]))[0]
            if notify_latest and records
            else None
        )
        inserted = 0
        with self.lock:
            cursor = self.connection.cursor()
            blocked_symbols = self._blocked_symbols(cursor, monitor_id)
            for operation_key, record in records:
                inserted += self._insert_operation(
                    cursor,
                    monitor_id,
                    operation_key,
                    record,
                    (
                        "blocked"
                        if operation_key == latest_key
                        and str(record.get("symbol") or "-") in blocked_symbols
                        else "pending"
                        if operation_key == latest_key
                        else "baseline"
                    ),
                )
            now = utc_now()
            cursor.execute(
                """
                UPDATE monitors
                SET initialized = 1, notify_latest_baseline = 0,
                    last_checked_at = ?, last_success_at = ?, last_error = NULL
                WHERE id = ?
                """,
                (now, now, monitor_id),
            )
            self.connection.commit()
        return inserted

    def save_new_operations(
        self,
        monitor_id: int,
        records: Iterable[tuple[str, dict[str, Any]]],
        notification_enabled: bool = True,
    ) -> int:
        inserted = 0
        with self.lock:
            cursor = self.connection.cursor()
            blocked_symbols = self._blocked_symbols(cursor, monitor_id)
            for operation_key, record in records:
                notification_status = (
                    "blocked"
                    if str(record.get("symbol") or "-") in blocked_symbols
                    else "skipped"
                )
                if notification_enabled and notification_status != "blocked":
                    notification_status = (
                        "prealerted"
                        if self._has_sent_trade_alert(cursor, monitor_id, record)
                        else "pending"
                    )
                inserted += self._insert_operation(
                    cursor,
                    monitor_id,
                    operation_key,
                    record,
                    notification_status,
                )
            now = utc_now()
            cursor.execute(
                """
                UPDATE monitors
                SET last_checked_at = ?, last_success_at = ?, last_error = NULL
                WHERE id = ?
                """,
                (now, now, monitor_id),
            )
            self.connection.commit()
        return inserted

    def _has_sent_trade_alert(
        self, cursor: sqlite3.Cursor, monitor_id: int, record: dict[str, Any]
    ) -> bool:
        occurred_at, symbol, side, position_side, *_ = operation_values(record)
        # The trade endpoint rounds execution times to seconds while order-history keeps milliseconds.
        row = cursor.execute(
            """
            SELECT 1 FROM trade_alerts
            WHERE monitor_id = ? AND notification_status = 'sent'
              AND symbol = ? AND side = ? AND position_side = ?
              AND occurred_at BETWEEN ? AND ?
            LIMIT 1
            """,
            (monitor_id, symbol, side, position_side, occurred_at - 2_000, occurred_at + 2_000),
        ).fetchone()
        return row is not None

    def save_trade_alert_baseline(
        self, monitor_id: int, records: Iterable[dict[str, Any]]
    ) -> int:
        inserted = 0
        started_at = int(time.time() * 1000)
        with self.lock:
            cursor = self.connection.cursor()
            for record in records:
                inserted += self._insert_trade_alert(cursor, monitor_id, record, "baseline")
            cursor.execute(
                """
                UPDATE monitors
                SET trade_alerts_initialized = 1, trade_alerts_started_at = ?,
                    last_trade_alert_error = NULL
                WHERE id = ?
                """,
                (started_at, monitor_id),
            )
            self.connection.commit()
        return inserted

    def save_new_trade_alerts(
        self, monitor_id: int, records: Iterable[dict[str, Any]], notification_enabled: bool
    ) -> int:
        inserted = 0
        with self.lock:
            cursor = self.connection.cursor()
            row = cursor.execute(
                "SELECT trade_alerts_started_at FROM monitors WHERE id = ?", (monitor_id,)
            ).fetchone()
            started_at = int(row["trade_alerts_started_at"] or 0) if row else 0
            if not started_at:
                return 0
            blocked_symbols = self._blocked_symbols(cursor, monitor_id)
            for record in records:
                if operation_values(record)[0] < started_at:
                    continue
                notification_status = "skipped"
                if operation_values(record)[1] in blocked_symbols:
                    notification_status = "blocked"
                elif notification_enabled and not self._has_sent_operation(cursor, monitor_id, record):
                    notification_status = "pending"
                inserted += self._insert_trade_alert(
                    cursor,
                    monitor_id,
                    record,
                    notification_status,
                )
            self.connection.commit()
        return inserted

    def _has_sent_operation(
        self, cursor: sqlite3.Cursor, monitor_id: int, record: dict[str, Any]
    ) -> bool:
        occurred_at, symbol, side, position_side, *_ = operation_values(record)
        row = cursor.execute(
            """
            SELECT 1 FROM operations
            WHERE monitor_id = ? AND notification_status = 'sent'
              AND symbol = ? AND side = ? AND position_side = ?
              AND occurred_at BETWEEN ? AND ?
            LIMIT 1
            """,
            (monitor_id, symbol, side, position_side, occurred_at - 2_000, occurred_at + 2_000),
        ).fetchone()
        return row is not None

    def _insert_trade_alert(
        self,
        cursor: sqlite3.Cursor,
        monitor_id: int,
        record: dict[str, Any],
        notification_status: str,
    ) -> bool:
        occurred_at, symbol, side, position_side, price, qty, base_asset, quantity, profit, asset = (
            operation_values(record)
        )
        cursor.execute(
            """
            INSERT OR IGNORE INTO trade_alerts (
                monitor_id, alert_key, occurred_at, symbol, side, position_side, price, qty,
                base_asset, quantity, realized_profit, profit_asset, payload_json,
                notification_status, discovered_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                monitor_id,
                trade_alert_key(record),
                occurred_at,
                symbol,
                side,
                position_side,
                price,
                qty,
                base_asset,
                quantity,
                profit,
                asset,
                json.dumps(record, ensure_ascii=False, separators=(",", ":")),
                notification_status,
                utc_now(),
            ),
        )
        return cursor.rowcount > 0

    def pending_trade_alerts(self, monitor_id: int, limit: int) -> list[dict[str, Any]]:
        with self.lock:
            rows = self.connection.execute(
                """
                SELECT alert_key, occurred_at, symbol, side, position_side, price, qty,
                       base_asset, quantity, realized_profit, profit_asset
                FROM trade_alerts
                WHERE monitor_id = ? AND notification_status = 'pending'
                ORDER BY occurred_at DESC, id DESC
                LIMIT ?
                """,
                (monitor_id, limit),
            ).fetchall()
        return [{**dict(row), "reference_leverage": None} for row in rows]

    def mark_trade_alerts_notified(self, monitor_id: int, alert_keys: list[str]) -> None:
        if not alert_keys:
            return
        placeholders = ",".join("?" for _ in alert_keys)
        with self.lock:
            self.connection.execute(
                f"""
                UPDATE trade_alerts
                SET notification_status = 'sent', notified_at = ?
                WHERE monitor_id = ? AND alert_key IN ({placeholders})
                """,
                (utc_now(), monitor_id, *alert_keys),
            )
            self.connection.commit()

    def mark_trade_alerts_skipped(self, monitor_id: int, alert_keys: list[str]) -> None:
        if not alert_keys:
            return
        placeholders = ",".join("?" for _ in alert_keys)
        with self.lock:
            self.connection.execute(
                f"""
                UPDATE trade_alerts
                SET notification_status = 'skipped'
                WHERE monitor_id = ? AND alert_key IN ({placeholders})
                """,
                (monitor_id, *alert_keys),
            )
            self.connection.commit()

    def mark_trade_alerts_blocked(self, monitor_id: int, alert_keys: list[str]) -> None:
        if not alert_keys:
            return
        placeholders = ",".join("?" for _ in alert_keys)
        with self.lock:
            self.connection.execute(
                f"""
                UPDATE trade_alerts
                SET notification_status = 'blocked'
                WHERE monitor_id = ? AND alert_key IN ({placeholders})
                  AND notification_status = 'pending'
                """,
                (monitor_id, *alert_keys),
            )
            self.connection.commit()

    def update_operation_reference_leverages(
        self, monitor_id: int, records: Iterable[tuple[str, dict[str, Any]]]
    ) -> None:
        updates = [
            (str(record["referenceLeverage"]), monitor_id, operation_key)
            for operation_key, record in records
            if record.get("referenceLeverage")
        ]
        if not updates:
            return
        with self.lock:
            self.connection.executemany(
                """
                UPDATE operations
                SET reference_leverage = ?
                WHERE monitor_id = ? AND operation_key = ?
                """,
                updates,
            )
            self.connection.commit()

    def pending_operations(self, monitor_id: int, limit: int) -> list[dict[str, Any]]:
        with self.lock:
            rows = self.connection.execute(
                """
                SELECT operation_key, occurred_at, symbol, side, position_side, price, qty,
                       base_asset, quantity, realized_profit, profit_asset, reference_leverage,
                       payload_json
                FROM operations
                WHERE monitor_id = ? AND notification_status = 'pending'
                ORDER BY occurred_at DESC, id DESC
                LIMIT ?
                """,
                (monitor_id, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def mark_notified(self, monitor_id: int, operation_keys: list[str]) -> None:
        if not operation_keys:
            return
        placeholders = ",".join("?" for _ in operation_keys)
        with self.lock:
            self.connection.execute(
                f"""
                UPDATE operations
                SET notification_status = 'sent', notified_at = ?
                WHERE monitor_id = ? AND operation_key IN ({placeholders})
                """,
                (utc_now(), monitor_id, *operation_keys),
            )
            self.connection.commit()

    def mark_notification_skipped(self, monitor_id: int, operation_keys: list[str]) -> None:
        if not operation_keys:
            return
        placeholders = ",".join("?" for _ in operation_keys)
        with self.lock:
            self.connection.execute(
                f"""
                UPDATE operations
                SET notification_status = 'skipped'
                WHERE monitor_id = ? AND operation_key IN ({placeholders})
                """,
                (monitor_id, *operation_keys),
            )
            self.connection.commit()

    def recent_operations(
        self, monitor_id: int | None = None, limit: int = 80
    ) -> list[dict[str, Any]]:
        limit = max(1, min(limit, 5000))
        conditions = ""
        values: tuple[int, ...] = (limit,)
        if monitor_id is not None:
            conditions = "WHERE operations.monitor_id = ?"
            values = (monitor_id, limit)
        with self.lock:
            rows = self.connection.execute(
                f"""
                SELECT operations.id, operations.monitor_id, operations.occurred_at,
                        operations.symbol, operations.side, operations.position_side,
                        operations.price, operations.qty, operations.base_asset,
                        operations.quantity, operations.realized_profit, operations.profit_asset,
                        operations.reference_leverage,
                        operations.payload_json, operations.notification_status,
                       operations.discovered_at, operations.notified_at,
                       monitors.name AS monitor_name, monitors.note AS monitor_note, monitors.url
                FROM operations
                JOIN monitors ON monitors.id = operations.monitor_id
                {conditions}
                ORDER BY operations.occurred_at DESC, operations.id DESC
                LIMIT ?
                """,
                values,
            ).fetchall()
        result = []
        for row in rows:
            operation = dict(row)
            try:
                payload = json.loads(operation.pop("payload_json"))
            except json.JSONDecodeError:
                payload = {}
            operation["realized_profit"] = str(payload.get("realizedProfit", operation["realized_profit"]))
            operation["profit_asset"] = str(
                payload.get("realizedProfitAsset", operation["profit_asset"])
            )
            operation["quote_asset"] = str(payload.get("quantityAsset") or "USDT")
            operation["action"], operation["action_key"] = operation_action(
                operation["side"], operation["position_side"], operation["realized_profit"]
            )
            result.append(operation)
        return result

    def overview_metrics(self) -> dict[str, int]:
        with self.lock:
            monitor_count = self.connection.execute("SELECT COUNT(*) FROM monitors").fetchone()[0]
            active_count = self.connection.execute(
                "SELECT COUNT(*) FROM monitors WHERE enabled = 1"
            ).fetchone()[0]
            error_count = self.connection.execute(
                """
                SELECT COUNT(*) FROM monitors
                WHERE last_error IS NOT NULL OR last_drawdown_error IS NOT NULL
                   OR last_leverage_error IS NOT NULL OR last_trade_alert_error IS NOT NULL
                """
            ).fetchone()[0]
            pending_count = self.connection.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM operations WHERE notification_status = 'pending')
                    + (SELECT COUNT(*) FROM trade_alerts WHERE notification_status = 'pending')
                """
            ).fetchone()[0]
            operation_count = self.connection.execute("SELECT COUNT(*) FROM operations").fetchone()[0]
        return {
            "monitor_count": monitor_count,
            "active_count": active_count,
            "error_count": error_count,
            "pending_count": pending_count,
            "operation_count": operation_count,
        }

    def performance(self, monitor_id: int | None = None) -> list[dict[str, Any]]:
        if monitor_id is None:
            monitors = {monitor["id"]: monitor for monitor in self.list_monitors()}
        else:
            monitor = self.get_monitor(monitor_id)
            if not monitor:
                return []
            monitors = {monitor_id: monitor}
        records_by_monitor: dict[int, list[tuple[int, dict[str, Any]]]] = defaultdict(list)
        with self.lock:
            if monitor_id is None:
                rows = self.connection.execute(
                    "SELECT monitor_id, occurred_at, payload_json FROM operations"
                ).fetchall()
            else:
                rows = self.connection.execute(
                    "SELECT monitor_id, occurred_at, payload_json FROM operations WHERE monitor_id = ?",
                    (monitor_id,),
                ).fetchall()
        for row in rows:
            try:
                payload = json.loads(row["payload_json"])
            except json.JSONDecodeError:
                continue
            records_by_monitor[row["monitor_id"]].append((row["occurred_at"], payload))

        now_ms = int(time.time() * 1000)
        result = []
        for monitor_id, monitor in monitors.items():
            records = records_by_monitor[monitor_id]
            periods = {}
            for days in (7, 30, 90):
                period = period_performance(records, now_ms - days * 24 * 60 * 60 * 1000)
                period["max_drawdown"] = monitor.get(f"drawdown_{days}d")
                period["roi"] = monitor.get(f"roi_{days}d")
                periods[f"{days}d"] = period
            result.append(
                {
                    "monitor_id": monitor_id,
                    "name": monitor["name"],
                    "note": monitor["note"],
                    "url": monitor["url"],
                    "stored_records": len(records),
                    "periods": periods,
                    "drawdown_updated_at": monitor.get("drawdown_updated_at"),
                }
            )
        return sorted(
            result,
            key=lambda item: (
                item["periods"]["30d"]["win_rate"] is not None,
                item["periods"]["30d"]["win_rate"] or 0,
                item["periods"]["30d"]["settled_records"],
            ),
            reverse=True,
        )

    def notification_attempts(self, limit: int = 200) -> list[dict[str, Any]]:
        with self.lock:
            rows = self.connection.execute(
                """
                SELECT notification_attempts.monitor_id, notification_attempts.channel_name,
                       notification_attempts.status, notification_attempts.message,
                       notification_attempts.created_at, monitors.name AS monitor_name
                FROM notification_attempts
                LEFT JOIN monitors ON monitors.id = notification_attempts.monitor_id
                ORDER BY notification_attempts.id DESC
                LIMIT ?
                """,
                (max(1, min(limit, 500)),),
            ).fetchall()
        return [dict(row) for row in rows]

    def clear_logs(self) -> dict[str, int]:
        with self.lock:
            notification_count = self.connection.execute(
                "DELETE FROM notification_attempts"
            ).rowcount
            system_log_count = self.connection.execute("DELETE FROM system_logs").rowcount
            self.connection.commit()
        return {"notification_count": notification_count, "system_log_count": system_log_count}

    def log_notification(
        self, monitor_id: int | None, attempt_status: str, message: str, channel_name: str | None = None
    ) -> None:
        with self.lock:
            try:
                self.connection.execute(
                    """
                    INSERT INTO notification_attempts (monitor_id, channel_name, status, message, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (monitor_id, channel_name[:100] if channel_name else None, attempt_status, message[:300], utc_now()),
                )
            except sqlite3.IntegrityError:
                # 外键失效说明监控刚被删除：降级为系统级记录，避免记账中断业务流程
                self.connection.execute(
                    """
                    INSERT INTO notification_attempts (monitor_id, channel_name, status, message, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (None, channel_name[:100] if channel_name else None, attempt_status, message[:300], utc_now()),
                )
            self.connection.commit()

    def log_event(self, monitor_id: int | None, level: str, event: str, message: str) -> None:
        with self.lock:
            try:
                self.connection.execute(
                    """
                    INSERT INTO system_logs (monitor_id, level, event, message, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (monitor_id, level[:20], event[:80], message[:500], utc_now()),
                )
            except sqlite3.IntegrityError:
                # 外键失效说明监控刚被删除：降级为系统级日志，避免日志写入中断业务流程
                self.connection.execute(
                    """
                    INSERT INTO system_logs (monitor_id, level, event, message, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (None, level[:20], event[:80], message[:500], utc_now()),
                )
            self.connection.commit()

    def system_logs(self, limit: int = 200) -> list[dict[str, Any]]:
        with self.lock:
            rows = self.connection.execute(
                """
                SELECT system_logs.monitor_id, system_logs.level, system_logs.event, system_logs.message,
                       system_logs.created_at, monitors.name AS monitor_name
                FROM system_logs
                LEFT JOIN monitors ON monitors.id = system_logs.monitor_id
                ORDER BY system_logs.id DESC
                LIMIT ?
                """,
                (max(1, min(limit, 500)),),
            ).fetchall()
        return [dict(row) for row in rows]

    def notification_channels(self) -> list[dict[str, Any]]:
        with self.lock:
            rows = self.connection.execute(
                "SELECT * FROM notification_channels ORDER BY kind, id DESC"
            ).fetchall()
        return [notification_channel_public(row) for row in rows]

    def enabled_extra_notification_channels(self) -> list[dict[str, Any]]:
        with self.lock:
            rows = self.connection.execute(
                "SELECT * FROM notification_channels WHERE enabled = 1 ORDER BY id"
            ).fetchall()
        channels = []
        for row in rows:
            try:
                config = json.loads(row["config_json"])
            except (TypeError, json.JSONDecodeError):
                continue
            if not isinstance(config, dict):
                continue
            if notification_channel_configured(row["kind"], config):
                channels.append(
                    {"id": row["id"], "kind": row["kind"], "name": row["name"], "config": config}
                )
        return channels

    def get_notification_channel(self, channel_id: int) -> dict[str, Any] | None:
        with self.lock:
            row = self.connection.execute(
                "SELECT * FROM notification_channels WHERE id = ?", (channel_id,)
            ).fetchone()
        return notification_channel_public(row) if row else None

    def notification_channel_for_delivery(self, channel_id: int) -> dict[str, Any] | None:
        with self.lock:
            row = self.connection.execute(
                "SELECT * FROM notification_channels WHERE id = ?", (channel_id,)
            ).fetchone()
        if not row:
            return None
        try:
            config = json.loads(row["config_json"])
        except (TypeError, json.JSONDecodeError):
            return None
        if not isinstance(config, dict):
            return None
        if not notification_channel_configured(row["kind"], config):
            return None
        return {"id": row["id"], "kind": row["kind"], "name": row["name"], "config": config}

    def create_notification_channel(self, payload: NotificationChannelCreate) -> dict[str, Any]:
        name = payload.name.strip()
        if not name:
            raise ValueError("通知渠道名称不能为空")
        config = {
            "bot_token": (payload.bot_token or "").strip(),
            "chat_id": (payload.chat_id or "").strip(),
            "webhook_url": (payload.webhook_url or "").strip(),
            "secret": (payload.secret or "").strip(),
        }
        if payload.kind == "telegram" and not notification_channel_configured("telegram", config):
            raise ValueError("请填写 Telegram Bot Token 和 Chat ID")
        if payload.kind == "dingtalk":
            config["webhook_url"] = validate_dingtalk_webhook_url(config["webhook_url"])
        if payload.kind == "feishu":
            config["webhook_url"] = validate_feishu_webhook_url(config["webhook_url"])
        now = utc_now()
        with self.lock:
            cursor = self.connection.execute(
                """
                INSERT INTO notification_channels (kind, name, enabled, config_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    payload.kind,
                    name,
                    int(payload.enabled),
                    json.dumps(config, ensure_ascii=True),
                    now,
                    now,
                ),
            )
            self.connection.commit()
        return self.get_notification_channel(cursor.lastrowid) or {}

    def update_notification_channel(
        self, channel_id: int, payload: NotificationChannelUpdate
    ) -> dict[str, Any] | None:
        with self.lock:
            row = self.connection.execute(
                "SELECT * FROM notification_channels WHERE id = ?", (channel_id,)
            ).fetchone()
            if not row:
                return None
            try:
                config = json.loads(row["config_json"])
            except (TypeError, json.JSONDecodeError):
                config = {}
            if not isinstance(config, dict):
                config = {}
            for field in ("bot_token", "chat_id", "webhook_url", "secret"):
                value = getattr(payload, field)
                if value is not None:
                    config[field] = value.strip()
            if row["kind"] == "telegram" and not notification_channel_configured("telegram", config):
                raise ValueError("请填写 Telegram Bot Token 和 Chat ID")
            if row["kind"] == "dingtalk":
                config["webhook_url"] = validate_dingtalk_webhook_url(str(config.get("webhook_url") or ""))
            if row["kind"] == "feishu":
                config["webhook_url"] = validate_feishu_webhook_url(str(config.get("webhook_url") or ""))
            name = payload.name.strip() if payload.name is not None else row["name"]
            if not name:
                raise ValueError("通知渠道名称不能为空")
            self.connection.execute(
                """
                UPDATE notification_channels
                SET name = ?, enabled = ?, config_json = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    name,
                    int(payload.enabled) if payload.enabled is not None else row["enabled"],
                    json.dumps(config, ensure_ascii=True),
                    utc_now(),
                    channel_id,
                ),
            )
            self.connection.commit()
        return self.get_notification_channel(channel_id)

    def delete_notification_channel(self, channel_id: int) -> bool:
        with self.lock:
            cursor = self.connection.execute("DELETE FROM notification_channels WHERE id = ?", (channel_id,))
            self.connection.commit()
        return cursor.rowcount > 0

    def telegram_settings(self) -> dict[str, str]:
        with self.lock:
            rows = self.connection.execute("SELECT key, value FROM settings").fetchall()
        return {row["key"]: row["value"] for row in rows}

    def public_telegram_settings(self) -> dict[str, Any]:
        settings = self.telegram_settings()
        return {
            "bot_token_configured": bool(settings.get("telegram_bot_token")),
            "chat_id": settings.get("telegram_chat_id", ""),
            "enabled": settings.get("telegram_enabled", "1") == "1",
        }

    def update_telegram_settings(
        self, bot_token: str | None, chat_id: str | None, enabled: bool | None
    ) -> None:
        updates: list[tuple[str, str]] = []
        if bot_token is not None and bot_token.strip():
            updates.append(("telegram_bot_token", bot_token.strip()))
        if chat_id is not None:
            updates.append(("telegram_chat_id", chat_id.strip()))
        if enabled is not None:
            updates.append(("telegram_enabled", "1" if enabled else "0"))
        if not updates:
            return
        with self.lock:
            self.connection.executemany(
                """
                INSERT INTO settings (key, value) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                updates,
            )
            self.connection.commit()

    def public_email_settings(self) -> dict[str, Any]:
        settings = self.telegram_settings()
        host = settings.get("email_host", "")
        username = settings.get("email_username", "")
        to_address = settings.get("email_to_address", "")
        return {
            "configured": bool(host and username and settings.get("email_password") and to_address),
            "host": host,
            "port": int(settings.get("email_port", "465")),
            "username": username,
            "from_address": settings.get("email_from_address", username),
            "to_address": to_address,
            "security": settings.get("email_security", "ssl"),
            "password_configured": bool(settings.get("email_password")),
            "enabled": settings.get("email_enabled", "1") == "1",
        }

    def session_ttl_hours(self) -> int:
        try:
            value = int(self.telegram_settings().get("session_ttl_hours", DEFAULT_SESSION_TTL_HOURS))
        except (TypeError, ValueError):
            return DEFAULT_SESSION_TTL_HOURS
        return value if 1 <= value <= MAX_SESSION_TTL_HOURS else DEFAULT_SESSION_TTL_HOURS

    def public_session_settings(self) -> dict[str, int]:
        return {"session_ttl_hours": self.session_ttl_hours()}

    def poll_interval_seconds(self) -> int:
        try:
            value = int(self.telegram_settings().get("poll_interval_seconds", POLL_INTERVAL_SECONDS))
        except (TypeError, ValueError):
            return POLL_INTERVAL_SECONDS
        return value if MIN_POLL_INTERVAL_SECONDS <= value <= MAX_POLL_INTERVAL_SECONDS else POLL_INTERVAL_SECONDS

    def update_poll_interval_seconds(self, poll_interval_seconds: int) -> None:
        with self.lock:
            self.connection.execute(
                """
                INSERT INTO settings (key, value) VALUES ('poll_interval_seconds', ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (str(poll_interval_seconds),),
            )
            self.connection.commit()

    def update_session_ttl_hours(self, session_ttl_hours: int) -> None:
        with self.lock:
            self.connection.execute(
                """
                INSERT INTO settings (key, value) VALUES ('session_ttl_hours', ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (str(session_ttl_hours),),
            )
            self.connection.commit()

    def update_email_settings(self, payload: EmailSettingsUpdate) -> None:
        updates: list[tuple[str, str]] = []
        mapping = {
            "host": "email_host",
            "username": "email_username",
            "from_address": "email_from_address",
            "to_address": "email_to_address",
            "security": "email_security",
        }
        for field, key in mapping.items():
            value = getattr(payload, field)
            if value is not None:
                updates.append((key, value.strip()))
        if payload.port is not None:
            updates.append(("email_port", str(payload.port)))
        if payload.password is not None and payload.password.strip():
            updates.append(("email_password", payload.password.strip()))
        if payload.enabled is not None:
            updates.append(("email_enabled", "1" if payload.enabled else "0"))
        if not updates:
            return
        with self.lock:
            self.connection.executemany(
                """
                INSERT INTO settings (key, value) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                updates,
            )
            self.connection.commit()

    def authenticate(self, username: str, password: str) -> dict[str, Any] | None:
        with self.lock:
            row = self.connection.execute(
                "SELECT id, username, password_hash FROM users WHERE username = ?",
                (username,),
            ).fetchone()
        if not row or not verify_password(password, row["password_hash"]):
            return None
        return {"id": row["id"], "username": row["username"]}

    def create_session(self, user_id: int) -> tuple[str, int]:
        token = secrets.token_urlsafe(32)
        token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
        ttl_seconds = self.session_ttl_hours() * 60 * 60
        with self.lock:
            self.connection.execute("DELETE FROM sessions WHERE expires_at <= ?", (int(time.time()),))
            self.connection.execute(
                """
                INSERT INTO sessions (token_hash, user_id, expires_at, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (token_hash, user_id, int(time.time()) + ttl_seconds, utc_now()),
            )
            self.connection.commit()
        return token, ttl_seconds

    def session_user(self, token: str | None) -> dict[str, Any] | None:
        if not token:
            return None
        token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
        with self.lock:
            row = self.connection.execute(
                """
                SELECT users.id, users.username
                FROM sessions
                JOIN users ON users.id = sessions.user_id
                WHERE sessions.token_hash = ? AND sessions.expires_at > ?
                """,
                (token_hash, int(time.time())),
            ).fetchone()
        return dict(row) if row else None

    def revoke_session(self, token: str | None) -> None:
        if not token:
            return
        token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
        with self.lock:
            self.connection.execute("DELETE FROM sessions WHERE token_hash = ?", (token_hash,))
            self.connection.commit()

    def revoke_all_sessions(self) -> None:
        with self.lock:
            self.connection.execute("DELETE FROM sessions")
            self.connection.commit()

    def change_password(self, user_id: int, current_password: str, new_password: str) -> bool:
        with self.lock:
            row = self.connection.execute(
                "SELECT password_hash FROM users WHERE id = ?", (user_id,)
            ).fetchone()
            if not row or not verify_password(current_password, row["password_hash"]):
                return False
            self.connection.execute(
                "UPDATE users SET password_hash = ?, updated_at = ? WHERE id = ?",
                (password_hash(new_password), utc_now(), user_id),
            )
            self.connection.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
            self.connection.commit()
        return True


def binance_headers(portfolio_id: str) -> dict[str, str]:
    return {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Origin": "https://www.binance.com",
        "Referer": f"https://www.binance.com/zh-CN/copy-trading/lead-details/{portfolio_id}",
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        ),
        "clienttype": "web",
    }


def order_history_record(order: dict[str, Any]) -> dict[str, Any]:
    try:
        occurred_at = int(order.get("orderUpdateTime") or order.get("orderTime") or 0)
    except (TypeError, ValueError) as error:
        raise PollError("Binance 订单时间无效") from error
    price = decimal_value(order.get("avgPrice"))
    qty = decimal_value(order.get("executedQty"))
    if not occurred_at or not price or not qty:
        raise PollError("Binance 订单数据不完整")
    quote_asset = str(order.get("quoteAsset") or "USDT")
    return {
        "time": occurred_at,
        "symbol": str(order.get("symbol") or "-"),
        "side": str(order.get("side") or "-"),
        "positionSide": str(order.get("positionSide") or "-"),
        "price": decimal_text(price),
        "qty": decimal_text(qty),
        "baseAsset": str(order.get("baseAsset") or ""),
        "quantity": decimal_text(price * qty),
        "quantityAsset": quote_asset,
        "realizedProfit": decimal_text(decimal_value(order.get("totalPnl"))),
        "realizedProfitAsset": quote_asset,
    }


def trade_history_record(trade: dict[str, Any]) -> dict[str, Any]:
    try:
        occurred_at = int(trade.get("time") or 0)
    except (TypeError, ValueError) as error:
        raise PollError("Binance 成交预警时间无效") from error
    price = decimal_value(trade.get("price"))
    qty = decimal_value(trade.get("qty"))
    if not occurred_at or not price or not qty:
        raise PollError("Binance 成交预警数据不完整")
    quote_asset = str(trade.get("quantityAsset") or "USDT")
    return {
        "time": occurred_at,
        "symbol": str(trade.get("symbol") or "-"),
        "side": str(trade.get("side") or "-"),
        "positionSide": str(trade.get("positionSide") or "-"),
        "price": decimal_text(price),
        "qty": decimal_text(qty),
        "baseAsset": str(trade.get("baseAsset") or ""),
        "quantity": decimal_text(decimal_value(trade.get("quantity")) or price * qty),
        "quantityAsset": quote_asset,
        "realizedProfit": decimal_text(decimal_value(trade.get("realizedProfit"))),
        "realizedProfitAsset": str(trade.get("realizedProfitAsset") or quote_asset),
        "fee": decimal_text(decimal_value(trade.get("fee"))),
        "feeAsset": str(trade.get("feeAsset") or quote_asset),
    }


async def fetch_binance_order_history_page(
    client: httpx.AsyncClient,
    portfolio_id: str,
    start_time: int,
    end_time: int,
    index_value: str | None = None,
) -> tuple[list[dict[str, Any]], str | None]:
    payload = {
        "portfolioId": portfolio_id,
        "startTime": start_time,
        "endTime": end_time,
        "pageSize": 100,
    }
    if index_value:
        payload["indexValue"] = index_value
    response: httpx.Response | None = None
    for attempt in range(2):
        try:
            response = await asyncio.wait_for(
                client.post(
                    BINANCE_ORDER_HISTORY_URL,
                    headers=binance_headers(portfolio_id),
                    json=payload,
                    timeout=ORDER_HISTORY_TIMEOUT_SECONDS,
                ),
                timeout=ORDER_HISTORY_TIMEOUT_SECONDS,
            )
            break
        except (asyncio.TimeoutError, httpx.TimeoutException) as error:
            if attempt:
                raise PollError("Binance 订单请求两次均在 8.5 秒内未完成") from error
    if response is None:
        raise PollError("Binance 订单请求未返回响应")
    response.raise_for_status()
    try:
        result = response.json()
    except json.JSONDecodeError as error:
        raise PollError("Binance 返回了非 JSON 数据") from error
    if not isinstance(result, dict) or result.get("success") is False or result.get("code") not in {
        None,
        "000000",
    }:
        message = result.get("message") if isinstance(result, dict) else None
        raise PollError(f"Binance 未返回可用的订单记录{f'：{message}' if message else ''}")
    data = result.get("data")
    orders = data.get("list") if isinstance(data, dict) else None
    if not isinstance(orders, list) or not all(isinstance(order, dict) for order in orders):
        raise PollError("Binance 订单数据格式无效")
    return [order_history_record(order) for order in orders], str(data.get("indexValue") or "") or None


async def fetch_binance_order_history(
    client: httpx.AsyncClient, portfolio_id: str
) -> list[dict[str, Any]]:
    end_time = int(time.time() * 1000)
    start_time = end_time - REGULAR_HISTORY_HOURS * 60 * 60 * 1000
    records, _ = await fetch_binance_order_history_page(
        client, portfolio_id, start_time, end_time
    )
    return records


async def fetch_binance_trade_history(
    client: httpx.AsyncClient, portfolio_id: str
) -> list[dict[str, Any]]:
    response = await client.post(
        BINANCE_TRADE_HISTORY_URL,
        headers=binance_headers(portfolio_id),
        json={"portfolioId": portfolio_id, "pageNumber": 1, "pageSize": 100},
        timeout=ORDER_HISTORY_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    try:
        result = response.json()
    except json.JSONDecodeError as error:
        raise PollError("Binance 返回了非 JSON 成交预警数据") from error
    if not isinstance(result, dict) or result.get("success") is False or result.get("code") not in {
        None,
        "000000",
    }:
        message = result.get("message") if isinstance(result, dict) else None
        raise PollError(f"Binance 未返回可用的成交预警{f'：{message}' if message else ''}")
    data = result.get("data")
    trades = data.get("list") if isinstance(data, dict) else None
    if not isinstance(trades, list) or not all(isinstance(trade, dict) for trade in trades):
        raise PollError("Binance 成交预警数据格式无效")
    return [trade_history_record(trade) for trade in trades]


async def empty_trade_history(_: httpx.AsyncClient, __: str) -> list[dict[str, Any]]:
    return []


async def fetch_binance_position_history(
    client: httpx.AsyncClient, portfolio_id: str, start_time: int, end_time: int
) -> list[dict[str, Any]]:
    positions: list[dict[str, Any]] = []
    for page_number in range(1, MAX_POSITION_HISTORY_PAGES + 1):
        payload = {
            "portfolioId": portfolio_id,
            "startTime": start_time,
            "endTime": end_time,
            "pageSize": 100,
            "pageNumber": page_number,
        }
        last_error: Exception | None = None
        for attempt in range(3):
            try:
                response = await client.post(
                    BINANCE_POSITION_HISTORY_URL,
                    headers=binance_headers(portfolio_id),
                    json=payload,
                )
                response.raise_for_status()
                try:
                    result = response.json()
                except json.JSONDecodeError as error:
                    raise PollError("Binance 返回了非 JSON 仓位记录") from error
                if not isinstance(result, dict) or result.get("success") is False or result.get("code") not in {
                    None,
                    "000000",
                }:
                    message = result.get("message") if isinstance(result, dict) else None
                    raise PollError(f"Binance 未返回可用的仓位记录{f'：{message}' if message else ''}")
                data = result.get("data")
                page_positions = data.get("list") if isinstance(data, dict) else None
                if not isinstance(page_positions, list) or not all(
                    isinstance(position, dict) for position in page_positions
                ):
                    raise PollError("Binance 仓位数据格式无效")
                total = int(data.get("total") or len(page_positions))
                break
            except (httpx.HTTPError, PollError, TypeError, ValueError) as error:
                last_error = error
                if attempt < 2:
                    await asyncio.sleep(attempt + 1)
        else:
            assert last_error is not None
            raise last_error
        positions.extend(page_positions)
        if not page_positions or len(positions) >= total:
            break
    return positions


def timestamp_value(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def reference_leverage_for_operation(
    record: dict[str, Any], positions: Iterable[dict[str, Any]]
) -> str | None:
    occurred_at = timestamp_value(record.get("time"))
    symbol = str(record.get("symbol") or "")
    position_side = str(record.get("positionSide") or "").upper()
    if not occurred_at or not symbol:
        return None

    candidates: list[tuple[int, int, str]] = []
    for position in positions:
        if str(position.get("symbol") or "") != symbol:
            continue
        side = str(position.get("side") or "").upper()
        if position_side in {"LONG", "SHORT"} and side != position_side:
            continue
        opened_at = timestamp_value(position.get("opened"))
        closed_at = timestamp_value(position.get("closed")) or timestamp_value(position.get("updateTime"))
        if not opened_at or not closed_at or not opened_at <= occurred_at <= max(opened_at, closed_at):
            continue
        leverage = decimal_value(position.get("leverage"))
        if not leverage.is_finite() or leverage <= 0:
            continue
        candidates.append((opened_at, max(opened_at, closed_at), decimal_text(leverage)))
    if not candidates:
        return None
    _, _, leverage = min(
        candidates, key=lambda item: min(abs(occurred_at - item[0]), abs(occurred_at - item[1]))
    )
    return leverage


async def fetch_binance_symbol_precisions(
    client: httpx.AsyncClient,
) -> dict[str, tuple[int, int]]:
    response = await client.get(BINANCE_EXCHANGE_INFO_URL)
    response.raise_for_status()
    try:
        payload = response.json()
    except json.JSONDecodeError as error:
        raise PollError("Binance 合约精度数据不是 JSON") from error
    symbols = payload.get("symbols") if isinstance(payload, dict) else None
    if not isinstance(symbols, list):
        raise PollError("Binance 合约精度数据格式无效")
    result = {}
    for symbol in symbols:
        if not isinstance(symbol, dict):
            continue
        name = symbol.get("symbol")
        price_precision = symbol.get("pricePrecision")
        quantity_precision = symbol.get("quantityPrecision")
        if not isinstance(name, str):
            continue
        try:
            result[name] = (
                max(0, min(int(price_precision), 12)),
                max(0, min(int(quantity_precision), 12)),
            )
        except (TypeError, ValueError):
            continue
    return result


async def fetch_leader_drawdown(
    client: httpx.AsyncClient, portfolio_id: str, time_range: str
) -> tuple[float | None, float | None]:
    response = await client.get(
        BINANCE_LEADER_CHART_URL,
        params={"portfolioId": portfolio_id, "dataType": "ROI", "timeRange": time_range},
        headers=binance_headers(portfolio_id),
    )
    response.raise_for_status()
    try:
        payload = response.json()
    except json.JSONDecodeError as error:
        raise PollError("Binance 回撤数据不是 JSON") from error
    if not isinstance(payload, dict) or payload.get("success") is False or payload.get("code") not in {
        None,
        "000000",
    }:
        message = payload.get("message") if isinstance(payload, dict) else None
        raise PollError(f"Binance 未返回可用的回撤数据{f'：{message}' if message else ''}")
    data = payload.get("data")
    if not isinstance(data, list):
        raise PollError("Binance 回撤数据格式无效")

    points: list[tuple[int, Decimal]] = []
    for point in data:
        if not isinstance(point, dict):
            continue
        try:
            timestamp = int(point.get("dateTime"))
            value = Decimal(str(point.get("value")))
        except (InvalidOperation, TypeError, ValueError):
            continue
        if value.is_finite():
            points.append((timestamp, value))
    if not points:
        return None, None

    points.sort(key=lambda point: point[0])
    period_roi = points[-1][1]
    peak = points[0][1]
    maximum_drawdown = Decimal("0")
    for _, value in points:
        equity_at_peak = Decimal("100") + peak
        if equity_at_peak > 0:
            maximum_drawdown = max(
                maximum_drawdown,
                (peak - value) / equity_at_peak * Decimal("100"),
            )
        peak = max(peak, value)
    return (
        float(maximum_drawdown.quantize(Decimal("0.01"))),
        float(period_roi.quantize(Decimal("0.01"))),
    )


async def fetch_leader_drawdowns(
    client: httpx.AsyncClient, portfolio_id: str
) -> dict[str, float | None]:
    values = await asyncio.gather(
        *(fetch_leader_drawdown(client, portfolio_id, f"{days}D") for days in (7, 30, 90))
    )
    drawdowns: dict[str, float | None] = {}
    for days, (drawdown, roi) in zip((7, 30, 90), values, strict=True):
        drawdowns[f"{days}d"] = drawdown
        drawdowns[f"roi_{days}d"] = roi
    return drawdowns


async def fetch_leader_finance(
    client: httpx.AsyncClient, portfolio_id: str
) -> tuple[str, str]:
    response = await client.get(
        BINANCE_LEADER_DETAIL_URL,
        params={"portfolioId": portfolio_id},
        headers=binance_headers(portfolio_id),
    )
    response.raise_for_status()
    payload = response.json()
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        raise PollError("带单详情数据结构异常")
    return (
        str(data.get("marginBalance") or ""),
        str(data.get("aumAmount") or ""),
    )


async def fetch_initial_history(app: FastAPI, portfolio_id: str) -> list[dict[str, Any]]:
    if app.state.fetcher is not fetch_binance_order_history:
        return await app.state.fetcher(app.state.http, portfolio_id)

    end_time = int(time.time() * 1000)
    start_time = end_time - INITIAL_HISTORY_DAYS * 24 * 60 * 60 * 1000
    records: list[dict[str, Any]] = []
    index_value: str | None = None
    seen_indexes = set()
    for _ in range(MAX_INITIAL_HISTORY_PAGES):
        try:
            page_records, next_index_value = await fetch_binance_order_history_page(
                app.state.http, portfolio_id, start_time, end_time, index_value
            )
        except PollError:
            if records:
                break
            raise
        records.extend(page_records)
        if not page_records or not next_index_value or next_index_value in seen_indexes:
            break
        seen_indexes.add(next_index_value)
        index_value = next_index_value
    return records


async def fetch_leader_name(client: httpx.AsyncClient, url: str, portfolio_id: str) -> str:
    fallback = f"带单员 {portfolio_id[-8:]}"
    try:
        response = await client.get(
            BINANCE_LEADER_DETAIL_URL,
            params={"portfolioId": portfolio_id},
            headers=binance_headers(portfolio_id),
        )
        response.raise_for_status()
        payload = response.json()
        data = payload.get("data") if isinstance(payload, dict) else None
        name = data.get("nickname") if isinstance(data, dict) else None
        if isinstance(name, str) and 1 <= len(name.strip()) <= 80:
            return name.strip()
    except (httpx.HTTPError, json.JSONDecodeError):
        pass

    try:
        response = await client.get(url, headers=binance_headers(portfolio_id))
        if response.status_code != status.HTTP_200_OK:
            return fallback
        content = response.text[:2_000_000]
    except httpx.RequestError:
        return fallback

    patterns = (
        r'"(?:nickName|nickname|leaderName|displayName|userName)"\s*:\s*"((?:\\.|[^"\\])*)"',
        r'<meta[^>]+property="og:title"[^>]+content="([^"]+)"',
        r'<title>\s*([^<]+?)\s*</title>',
    )
    for pattern in patterns:
        match = re.search(pattern, content, re.IGNORECASE)
        if not match:
            continue
        candidate = html.unescape(match.group(1))
        try:
            candidate = json.loads(f'"{candidate}"')
        except json.JSONDecodeError:
            pass
        candidate = candidate.strip()
        if 1 <= len(candidate) <= 80 and "binance.com" not in candidate.lower():
            return candidate
    return fallback


def format_operation_time(value: int) -> str:
    if not value:
        return "未知时间"
    return datetime.fromtimestamp(value / 1000, timezone.utc).astimezone(SHANGHAI_TZ).strftime(
        "%Y-%m-%d %H:%M:%S"
    )




def dashboard_monitor_url(monitor: dict[str, Any]) -> str | None:
    if not DASHBOARD_BASE_URL or monitor.get("id") is None:
        return None
    parameters = {
        "monitor-name": str(monitor.get("name") or ""),
        "monitor-id": str(monitor["id"]),
    }
    return f"{DASHBOARD_BASE_URL}/#operations?{urlencode(parameters)}"


def error_alert_content(
    monitor: dict[str, Any] | None, event: str, message: str
) -> tuple[str, str]:
    monitor_id = monitor.get("id") if monitor else None
    monitor_name = str(monitor.get("name") or "未命名带单人") if monitor else "系统"
    target = f"{monitor_name}（监控 ID: {monitor_id}）" if monitor_id is not None else monitor_name
    lines = ["策略监控异常", f"对象: {target}", f"事件: {event}", f"详情: {message}"]
    monitor_url = dashboard_monitor_url(monitor) if monitor else None
    if monitor_url:
        lines.append(f"操作记录: {monitor_url}")
    return f"[策略监控异常] {target} {event}", "\n".join(lines)


def notification_action(operation: dict[str, Any]) -> str:
    action, action_key = operation_action(
        operation["side"], operation["position_side"], operation.get("realized_profit")
    )
    icon = {
        "open_long": "↗",
        "close_long": "↘",
        "open_short": "↙",
        "close_short": "↖",
    }.get(action_key, "•")
    return f"{icon} {action}"


def notification_realized_profit(operation: dict[str, Any]) -> str:
    profit = decimal_value(operation.get("realized_profit"))
    if not profit or not profit.is_finite():
        return "暂无"
    amount = decimal_text(profit)
    if profit > 0:
        amount = f"+{amount}"
    return f"{amount} {operation.get('profit_asset') or 'USDT'}"


def format_finance_amount(value: Any) -> str | None:
    amount = decimal_value(value)
    if not amount or not amount.is_finite():
        return None
    quantized = amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return f"{quantized:,} USDT"


def notification_period_roi(period: dict[str, Any]) -> str:
    raw = period.get("roi")
    if raw is None:
        return "暂无"
    roi = decimal_value(raw)
    if roi is None or not roi.is_finite():
        return "暂无"
    return f"{'+' if roi > 0 else ''}{roi.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)}%"


def notification_period_lines(performance: dict[str, Any] | None) -> list[str]:
    periods = performance.get("periods", {}) if performance else {}
    lines = []
    for label, key in (("7D", "7d"), ("30D", "30d"), ("90D", "90d")):
        period = periods.get(key, {})
        win_rate = period.get("win_rate")
        drawdown = period.get("max_drawdown")
        roi = notification_period_roi(period)
        lines.append(
            f"{label} 胜率: {f'{win_rate}%' if win_rate is not None else '暂无'}"
            f" | 收益率: {roi}"
            f" | 最大回撤: {f'{drawdown}%' if drawdown is not None else '暂无'}"
        )
    current = periods.get("30d", {})
    pnl = current.get("pnl") or []
    pnl_text = " · ".join(
        f"{item.get('amount')} {item.get('asset')}"
        for item in pnl
        if isinstance(item, dict) and item.get("amount") is not None and item.get("asset")
    ) or "暂无"
    conservative_win_rate = current.get("conservative_win_rate")
    lines.append(
        f"30D 已实现盈亏：{pnl_text} · 保守胜率："
        f"{f'{conservative_win_rate}%' if conservative_win_rate is not None else '暂无'}"
    )
    return lines
def open_position_line(
    operation: dict[str, Any], action_key: str, margin_balance: Decimal | None
) -> str | None:
    """开仓操作折算每 1000 USDT 带单余额对应的持仓，便于直接跟单。"""
    if action_key not in ("open_long", "open_short"):
        return None
    if margin_balance is None:
        return None
    total = decimal_value(operation.get("quantity"))
    if not total or not total.is_finite() or total <= 0:
        return None
    per_thousand = total / margin_balance * Decimal("1000")
    return f"仓位: {per_thousand.quantize(Decimal('0.1'), rounding=ROUND_HALF_UP)} USDT/千U余额"


def format_operation_notification(
    monitor: dict[str, Any],
    operation: dict[str, Any],
    performance: dict[str, Any] | None,
    *,
    trade_alert: bool = False,
) -> tuple[str, str]:
    action = notification_action(operation)
    amount = f"{operation['qty']} {operation['base_asset']}".strip()
    symbol = str(operation["symbol"])
    leverage = str(operation.get("reference_leverage") or "")
    margin_balance = decimal_value(monitor.get("margin_balance"))
    if not margin_balance.is_finite() or margin_balance <= 0:
        margin_balance = None
    period_lines = notification_period_lines(performance)
    action_key = operation_action(
        operation["side"], operation["position_side"], operation.get("realized_profit")
    )[1]
    position_line = open_position_line(operation, action_key, margin_balance)
    heading = "Binance Copy Watch · 成交预警" if trade_alert else "Binance Copy Watch"
    source_note = "说明: 此为分笔成交预警，官方订单记录将在 Binance 返回后同步。"
    margin_balance_text = format_finance_amount(monitor.get("margin_balance"))
    aum_amount_text = format_finance_amount(monitor.get("aum_amount"))
    finance_lines = [
        *( [f"带单余额: {margin_balance_text}"] if margin_balance_text else [] ),
        *( [f"资产管理规模: {aum_amount_text}"] if aum_amount_text else [] ),
    ]
    lines = [
        heading,
        *([source_note] if trade_alert else []),
        f"带单人: {monitor['name']}",
        f"时间: {format_operation_time(operation['occurred_at'])}",
        f"操作: {action}",
        f"合约: {symbol}",
        f"数量: {amount}",
        f"均价: {operation['price']} USDT",
        f"总值: {operation['quantity']} USDT",
        *([position_line] if position_line else []),
        f"参考杠杆: {f'{leverage}x' if leverage else '暂无'}",
        f"本次实现盈亏: {notification_realized_profit(operation)}",
        *finance_lines,
        "带单人表现:",
        *period_lines,
    ]
    html_lines = [
        f"<b>{heading}</b>",
        *([html.escape(source_note)] if trade_alert else []),
        f"带单人: {html.escape(str(monitor['name']))}",
        f"时间: {html.escape(format_operation_time(operation['occurred_at']))}",
        f"操作: {html.escape(action)}",
        f"合约: {html.escape(symbol)}",
        f"数量: {html.escape(amount)}",
        f"均价: {html.escape(str(operation['price']))} USDT",
        f"总值: {html.escape(str(operation['quantity']))} USDT",
        *([html.escape(position_line)] if position_line else []),
        f"参考杠杆: {html.escape(f'{leverage}x' if leverage else '暂无')}",
        f"本次实现盈亏: {html.escape(notification_realized_profit(operation))}",
        *(html.escape(line) for line in finance_lines),
        "<b>带单人表现:</b>",
        *(html.escape(line) for line in period_lines),
    ]
    return "\n".join(lines), "\n".join(html_lines)


def notification_operation_details(operation: dict[str, Any]) -> str:
    action = operation_action(
        operation["side"], operation["position_side"], operation.get("realized_profit")
    )[0]
    amount = f"{operation['qty']} {operation['base_asset']}".strip()
    leverage = str(operation.get("reference_leverage") or "")
    return (
        f"{format_operation_time(operation['occurred_at'])} | {operation['symbol']} {action}"
        f" | 数量 {amount} | 均价 {operation['price']} USDT | 总值 {operation['quantity']} USDT"
        f" | 杠杆 {f'{leverage}x' if leverage else '暂无'}"
        f" | 已实现盈亏 {notification_realized_profit(operation)}"
    )


async def send_telegram_message(app: FastAPI, text: str, parse_mode: str | None = None) -> None:
    settings = app.state.store.telegram_settings()
    token = settings.get("telegram_bot_token", "")
    chat_id = settings.get("telegram_chat_id", "")
    if not token or not chat_id:
        raise PollError("请先保存 Telegram Bot Token 和 Chat ID")
    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    if parse_mode:
        payload["parse_mode"] = parse_mode
    response = await app.state.http.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json=payload,
    )
    response.raise_for_status()
    try:
        payload = response.json()
    except json.JSONDecodeError as error:
        raise PollError("Telegram 返回了非 JSON 数据") from error
    if not payload.get("ok"):
        raise PollError("Telegram 拒绝了通知请求")


async def send_telegram_text(app: FastAPI, text: str) -> None:
    await send_telegram_message(app, text)


async def send_telegram_html(app: FastAPI, html_text: str) -> None:
    await send_telegram_message(app, html_text, "HTML")


async def send_telegram_channel_message(
    app: FastAPI, channel: dict[str, Any], text: str, parse_mode: str | None = None
) -> None:
    config = channel["config"]
    token = str(config.get("bot_token") or "")
    chat_id = str(config.get("chat_id") or "")
    if not token or not chat_id:
        raise PollError("Telegram Bot Token 或 Chat ID 未配置")
    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    if parse_mode:
        payload["parse_mode"] = parse_mode
    response = await app.state.http.post(
        f"https://api.telegram.org/bot{token}/sendMessage", json=payload
    )
    response.raise_for_status()
    try:
        response_payload = response.json()
    except json.JSONDecodeError as error:
        raise PollError("Telegram 返回了非 JSON 数据") from error
    if not response_payload.get("ok"):
        raise PollError("Telegram 拒绝了通知请求")


async def send_dingtalk_message(app: FastAPI, channel: dict[str, Any], title: str, text: str) -> None:
    config = channel["config"]
    webhook_url = validate_dingtalk_webhook_url(str(config.get("webhook_url") or ""))
    parameters = parse_qsl(urlparse(webhook_url).query, keep_blank_values=True)
    secret = str(config.get("secret") or "")
    if secret:
        timestamp = str(int(time.time() * 1000))
        string_to_sign = f"{timestamp}\n{secret}"
        signature = base64.b64encode(
            hmac.new(secret.encode("utf-8"), string_to_sign.encode("utf-8"), hashlib.sha256).digest()
        ).decode("ascii")
        parameters.extend((("timestamp", timestamp), ("sign", signature)))
    payload = {
        "msgtype": "markdown",
        "markdown": {"title": title[:100], "text": f"### {title}\n\n{text.replace(chr(10), chr(10) * 2)}"},
    }
    try:
        response = await app.state.http.post(webhook_url, params=parameters, json=payload)
    except httpx.HTTPError as error:
        raise PollError(f"钉钉机器人请求失败: {type(error).__name__}") from error
    if response.is_error:
        raise PollError(f"钉钉机器人返回 HTTP {response.status_code}")
    try:
        response_payload = response.json()
    except json.JSONDecodeError as error:
        raise PollError("钉钉机器人返回了非 JSON 数据") from error
    if str(response_payload.get("errcode", 0)) != "0":
        error_code = str(response_payload.get("errcode"))[:40]
        error_message = str(response_payload.get("errmsg") or "未知错误")[:160]
        raise PollError(f"钉钉机器人拒绝了通知请求: {error_code} {error_message}")


async def send_feishu_message(app: FastAPI, channel: dict[str, Any], title: str, text: str) -> None:
    config = channel["config"]
    webhook_url = validate_feishu_webhook_url(str(config.get("webhook_url") or ""))
    paragraphs = []
    for line in text.splitlines():
        nodes = []
        cursor = 0
        for match in re.finditer(r"https?://[^\s]+", line):
            if match.start() > cursor:
                nodes.append({"tag": "text", "text": line[cursor : match.start()]})
            url = match.group()
            nodes.append({"tag": "a", "text": url, "href": url})
            cursor = match.end()
        if cursor < len(line):
            nodes.append({"tag": "text", "text": line[cursor:]})
        paragraphs.append(nodes or [{"tag": "text", "text": " "}])
    payload: dict[str, Any] = {
        "msg_type": "post",
        "content": {
            "post": {
                "zh_cn": {
                    "title": title[:100],
                    "content": paragraphs,
                }
            }
        },
    }
    secret = str(config.get("secret") or "")
    if secret:
        timestamp = str(int(time.time()))
        string_to_sign = f"{timestamp}\n{secret}"
        payload["timestamp"] = timestamp
        payload["sign"] = base64.b64encode(
            hmac.new(string_to_sign.encode("utf-8"), digestmod=hashlib.sha256).digest()
        ).decode("ascii")
    try:
        response = await app.state.http.post(webhook_url, json=payload)
    except httpx.HTTPError as error:
        raise PollError(f"飞书机器人请求失败: {type(error).__name__}") from error
    if response.is_error:
        raise PollError(f"飞书机器人返回 HTTP {response.status_code}")
    try:
        response_payload = response.json()
    except json.JSONDecodeError as error:
        raise PollError("飞书机器人返回了非 JSON 数据") from error
    if str(response_payload.get("code", 0)) != "0":
        error_code = str(response_payload.get("code"))[:40]
        error_message = str(response_payload.get("msg") or "未知错误")[:160]
        raise PollError(f"飞书机器人拒绝了通知请求: {error_code} {error_message}")


def notification_channel_label(channel: dict[str, Any]) -> str:
    kind = {"telegram": "Telegram", "dingtalk": "钉钉", "feishu": "飞书"}[channel["kind"]]
    return f"{kind} · {channel['name']}"


async def send_extra_notification_channels(
    app: FastAPI,
    monitor_id: int | None,
    subject: str,
    text: str,
    html_text: str | None,
    details: str,
) -> list[str]:
    errors = []
    for channel in app.state.store.enabled_extra_notification_channels():
        label = notification_channel_label(channel)
        try:
            if channel["kind"] == "telegram":
                await send_telegram_channel_message(
                    app, channel, html_text or text, "HTML" if html_text else None
                )
            elif channel["kind"] == "dingtalk":
                await send_dingtalk_message(app, channel, subject, text)
            else:
                await send_feishu_message(app, channel, subject, text)
            app.state.store.log_notification(monitor_id, "sent", f"{label} 已发送：{details}", label)
        except Exception as error:
            error_message = safe_error(error)
            errors.append(f"{label}：{error_message}")
            app.state.store.log_notification(
                monitor_id, "error", f"{label} 发送失败：{details}；{error_message}", label
            )
    return errors


async def send_telegram_test_message(app: FastAPI) -> str:
    message = "策略监控中心已连接。后续 Binance 新操作会在轮询后发送到这里。"
    await send_telegram_text(app, message)
    app.state.store.log_notification(None, "sent", "Telegram 测试消息已发送", "Telegram 默认")
    return "Telegram 测试消息已发送"


def send_email_sync(
    settings: dict[str, str], subject: str, text: str, html_text: str | None = None
) -> None:
    host = settings.get("email_host", "")
    username = settings.get("email_username", "")
    password = settings.get("email_password", "")
    to_address = settings.get("email_to_address", "")
    if not host or not username or not password or not to_address:
        raise PollError("请先完成 SMTP 邮箱配置")
    try:
        port = int(settings.get("email_port", "465"))
    except ValueError as error:
        raise PollError("SMTP 端口无效") from error

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = settings.get("email_from_address") or username
    message["To"] = to_address
    message.set_content(text)
    if html_text:
        message.add_alternative(html_text, subtype="html")
    security = settings.get("email_security", "ssl")
    if security == "ssl":
        with smtplib.SMTP_SSL(host, port, timeout=20) as smtp:
            smtp.login(username, password)
            smtp.send_message(message)
        return
    with smtplib.SMTP(host, port, timeout=20) as smtp:
        smtp.ehlo()
        if security == "starttls":
            smtp.starttls()
            smtp.ehlo()
        smtp.login(username, password)
        smtp.send_message(message)


async def send_email_text(
    app: FastAPI, subject: str, text: str, html_text: str | None = None
) -> None:
    await asyncio.to_thread(send_email_sync, app.state.store.telegram_settings(), subject, text, html_text)


def enabled_notification_channels(settings: dict[str, str]) -> tuple[bool, bool]:
    telegram_enabled = bool(
        settings.get("telegram_enabled", "1") == "1"
        and settings.get("telegram_bot_token")
        and settings.get("telegram_chat_id")
    )
    email_enabled = bool(
        settings.get("email_enabled", "1") == "1"
        and settings.get("email_host")
        and settings.get("email_username")
        and settings.get("email_password")
        and settings.get("email_to_address")
    )
    return telegram_enabled, email_enabled


def has_enabled_notification_channel(app: FastAPI) -> bool:
    telegram_enabled, email_enabled = enabled_notification_channels(app.state.store.telegram_settings())
    return telegram_enabled or email_enabled or bool(app.state.store.enabled_extra_notification_channels())


async def send_error_alert(
    app: FastAPI, monitor: dict[str, Any] | None, event: str, message: str
) -> None:
    if monitor and not monitor["notification_enabled"]:
        return
    telegram_enabled, email_enabled = enabled_notification_channels(app.state.store.telegram_settings())
    if not telegram_enabled and not email_enabled and not app.state.store.enabled_extra_notification_channels():
        return
    monitor_id = monitor.get("id") if monitor else None
    subject, text = error_alert_content(monitor, event, message)
    if telegram_enabled:
        try:
            await send_telegram_text(app, text)
            app.state.store.log_notification(
                monitor_id, "sent", "异常告警已发送至 Telegram", "Telegram 默认"
            )
        except Exception as error:
            app.state.store.log_notification(
                monitor_id,
                "error",
                f"异常告警 Telegram 发送失败: {safe_error(error)}",
                "Telegram 默认",
            )
    if email_enabled:
        try:
            await send_email_text(app, subject, text)
            app.state.store.log_notification(monitor_id, "sent", "异常告警邮件已发送", "SMTP 邮箱")
        except Exception as error:
            app.state.store.log_notification(
                monitor_id,
                "error",
                f"异常告警邮件发送失败: {safe_error(error)}",
                "SMTP 邮箱",
            )
    await send_extra_notification_channels(app, monitor_id, subject, text, None, "异常告警")


async def deliver_pending_operations(app: FastAPI, monitor: dict[str, Any]) -> dict[str, Any]:
    operations = app.state.store.pending_operations(monitor["id"], MAX_PENDING_PER_NOTIFICATION)
    if not operations:
        return {"status": "empty", "count": 0}

    if not monitor["notification_enabled"]:
        operation_keys = [operation["operation_key"] for operation in operations]
        app.state.store.mark_notification_skipped(monitor["id"], operation_keys)
        return {"status": "skipped", "count": len(operations)}

    settings = app.state.store.telegram_settings()
    telegram_enabled, email_enabled = enabled_notification_channels(settings)
    if not has_enabled_notification_channel(app):
        operation_keys = [operation["operation_key"] for operation in operations]
        app.state.store.mark_notification_skipped(monitor["id"], operation_keys)
        app.state.store.log_event(
            monitor["id"], "info", "通知跳过", f"通知渠道未启用，跳过 {len(operations)} 条新操作"
        )
        return {"status": "skipped", "count": len(operations)}

    delivered_count = 0
    performance_items = app.state.store.performance(monitor["id"])
    performance = performance_items[0] if performance_items else None
    for operation in operations:
        if app.state.store.is_notification_blocked(monitor["id"], operation["symbol"]):
            app.state.store.mark_operations_blocked(monitor["id"], [operation["operation_key"]])
            continue
        text, html_text = format_operation_notification(monitor, operation, performance)
        action = operation_action(
            operation["side"], operation["position_side"], operation.get("realized_profit")
        )[0]
        subject = f"[策略监控] {monitor['name']} 新增 {operation['symbol']} {action}"
        details = notification_operation_details(operation)
        errors = []
        if telegram_enabled:
            try:
                await send_telegram_html(app, html_text)
                app.state.store.log_notification(
                    monitor["id"], "sent", f"Telegram 已发送：{details}", "Telegram 默认"
                )
            except Exception as error:
                error_message = safe_error(error)
                errors.append(f"Telegram：{error_message}")
                app.state.store.log_notification(
                    monitor["id"],
                    "error",
                    f"Telegram 发送失败：{details}；{error_message}",
                    "Telegram 默认",
                )
        if email_enabled:
            try:
                await send_email_text(app, subject, text, html_text)
                app.state.store.log_notification(
                    monitor["id"], "sent", f"邮件已发送：{details}", "SMTP 邮箱"
                )
            except Exception as error:
                error_message = safe_error(error)
                errors.append(f"邮件：{error_message}")
                app.state.store.log_notification(
                    monitor["id"], "error", f"邮件发送失败：{details}；{error_message}", "SMTP 邮箱"
                )
        errors.extend(
            await send_extra_notification_channels(
                app, monitor["id"], subject, text, html_text, details
            )
        )
        if errors:
            message = "；".join(errors)
            error_message = f"通知发送失败: {message}"
            is_new_error = app.state.store.set_monitor_error(monitor["id"], error_message)
            app.state.store.log_event(monitor["id"], "error", "通知发送", error_message)
            if is_new_error:
                await send_error_alert(app, monitor, "通知发送", error_message)
            return {"status": "error", "count": delivered_count, "error": message}

        app.state.store.mark_notified(monitor["id"], [operation["operation_key"]])
        delivered_count += 1
    return {"status": "sent", "count": delivered_count}


async def deliver_pending_trade_alerts(app: FastAPI, monitor: dict[str, Any]) -> dict[str, Any]:
    alerts = app.state.store.pending_trade_alerts(monitor["id"], MAX_PENDING_PER_NOTIFICATION)
    if not alerts:
        return {"status": "empty", "count": 0}

    alert_keys = [alert["alert_key"] for alert in alerts]
    if not monitor["notification_enabled"]:
        app.state.store.mark_trade_alerts_skipped(monitor["id"], alert_keys)
        return {"status": "skipped", "count": len(alerts)}

    settings = app.state.store.telegram_settings()
    telegram_enabled, email_enabled = enabled_notification_channels(settings)
    if not has_enabled_notification_channel(app):
        app.state.store.mark_trade_alerts_skipped(monitor["id"], alert_keys)
        app.state.store.log_event(
            monitor["id"], "info", "成交预警跳过", f"通知渠道未启用，跳过 {len(alerts)} 条成交预警"
        )
        return {"status": "skipped", "count": len(alerts)}

    delivered_count = 0
    performance_items = app.state.store.performance(monitor["id"])
    performance = performance_items[0] if performance_items else None
    for alert in alerts:
        if app.state.store.is_notification_blocked(monitor["id"], alert["symbol"]):
            app.state.store.mark_trade_alerts_blocked(monitor["id"], [alert["alert_key"]])
            continue
        text, html_text = format_operation_notification(
            monitor, alert, performance, trade_alert=True
        )
        subject = (
            f"[策略监控][成交预警] {monitor['name']} {alert['symbol']} "
            f"{operation_action(alert['side'], alert['position_side'], alert.get('realized_profit'))[0]}"
        )
        details = notification_operation_details(alert)
        errors = []
        if telegram_enabled:
            try:
                await send_telegram_html(app, html_text)
                app.state.store.log_notification(
                    monitor["id"], "sent", f"成交预警 Telegram 已发送：{details}", "Telegram 默认"
                )
            except Exception as error:
                error_message = safe_error(error)
                errors.append(f"Telegram：{error_message}")
                app.state.store.log_notification(
                    monitor["id"],
                    "error",
                    f"成交预警 Telegram 发送失败：{details}；{error_message}",
                    "Telegram 默认",
                )
        if email_enabled:
            try:
                await send_email_text(app, subject, text, html_text)
                app.state.store.log_notification(
                    monitor["id"], "sent", f"成交预警邮件已发送：{details}", "SMTP 邮箱"
                )
            except Exception as error:
                error_message = safe_error(error)
                errors.append(f"邮件：{error_message}")
                app.state.store.log_notification(
                    monitor["id"], "error", f"成交预警邮件发送失败：{details}；{error_message}", "SMTP 邮箱"
                )
        errors.extend(
            await send_extra_notification_channels(
                app, monitor["id"], subject, text, html_text, f"成交预警 {details}"
            )
        )
        if errors:
            message = "；".join(errors)
            error_message = f"成交预警发送失败: {message}"
            is_new_error = app.state.store.set_monitor_trade_alert_error(
                monitor["id"], error_message
            )
            app.state.store.log_event(monitor["id"], "error", "成交预警发送", error_message)
            if is_new_error:
                await send_error_alert(app, monitor, "成交预警发送", error_message)
            return {"status": "error", "count": delivered_count, "error": message}

        app.state.store.mark_trade_alerts_notified(monitor["id"], [alert["alert_key"]])
        delivered_count += 1
    app.state.store.clear_monitor_trade_alert_error(monitor["id"])
    return {"status": "sent", "count": delivered_count}


async def fetch_metadata_with_retry(
    app: FastAPI,
    monitor: dict[str, Any],
    event_label: str,
    fetch: Callable[[], Awaitable[Any]],
) -> Any:
    """带单元数据请求统一重试，并把每次尝试写入系统运行日志。"""
    for attempt in range(1, METADATA_FETCH_ATTEMPTS + 1):
        try:
            result = await asyncio.wait_for(fetch(), timeout=METADATA_TIMEOUT_SECONDS)
        except Exception as error:
            if attempt >= METADATA_FETCH_ATTEMPTS:
                raise
            app.state.store.log_event(
                monitor["id"],
                "warning",
                event_label,
                (
                    f"第 {attempt}/{METADATA_FETCH_ATTEMPTS} 次尝试失败: {safe_error(error)}，"
                    f"{METADATA_RETRY_DELAY_SECONDS:.0f} 秒后重试"
                ),
            )
            await asyncio.sleep(METADATA_RETRY_DELAY_SECONDS)
            continue
        if attempt > 1:
            app.state.store.log_event(
                monitor["id"],
                "info",
                event_label,
                f"重试成功：第 {attempt}/{METADATA_FETCH_ATTEMPTS} 次尝试成功",
            )
        return result
    raise AssertionError("metadata retry loop must return or raise")


async def refresh_monitor_drawdowns(app: FastAPI, monitor: dict[str, Any]) -> None:
    try:
        drawdowns = await fetch_metadata_with_retry(
            app,
            monitor,
            "Binance 回撤查询",
            lambda: app.state.drawdown_fetcher(app.state.http, monitor["portfolio_id"]),
        )
    except Exception as error:
        message = f"Binance 回撤查询失败（已重试 {METADATA_FETCH_ATTEMPTS} 次）: {safe_error(error)}"
        is_new_error = app.state.store.set_monitor_drawdown_error(monitor["id"], message)
        app.state.store.log_event(monitor["id"], "error", "Binance 回撤查询", message)
        if is_new_error:
            await send_error_alert(app, monitor, "Binance 回撤查询", message)
        return
    app.state.store.update_monitor_drawdowns(monitor["id"], drawdowns)


async def refresh_monitor_finance(app: FastAPI, monitor: dict[str, Any]) -> None:
    try:
        margin_balance, aum_amount = await fetch_metadata_with_retry(
            app,
            monitor,
            "Binance 带单余额",
            lambda: app.state.leader_finance_fetcher(app.state.http, monitor["portfolio_id"]),
        )
    except Exception as error:
        message = f"Binance 带单余额查询失败（已重试 {METADATA_FETCH_ATTEMPTS} 次）: {safe_error(error)}"
        app.state.store.log_event(monitor["id"], "error", "Binance 带单余额", message)
        return
    app.state.store.update_monitor_finance(monitor["id"], margin_balance, aum_amount)


async def fetch_monitor_reference_positions(
    app: FastAPI,
    monitor: dict[str, Any],
    start_time: int,
    end_time: int,
) -> list[dict[str, Any]]:
    try:
        positions = await asyncio.wait_for(
            app.state.position_history_fetcher(
                app.state.http, monitor["portfolio_id"], start_time, end_time
            ),
            timeout=REFERENCE_LEVERAGE_TIMEOUT_SECONDS,
        )
    except Exception as error:
        message = f"Binance 仓位历史查询失败: {safe_error(error)}"
        is_new_error = app.state.store.set_monitor_leverage_error(monitor["id"], message)
        if is_new_error:
            app.state.store.log_event(monitor["id"], "error", "Binance 仓位历史查询", message)
            if not isinstance(error, TimeoutError):
                asyncio.create_task(send_error_alert(app, monitor, "Binance 仓位历史查询", message))
        return []
    app.state.store.clear_monitor_leverage_error(monitor["id"])
    return positions


def apply_operation_reference_leverages(
    records: list[dict[str, Any]], positions: list[dict[str, Any]]
) -> None:
    for record in records:
        leverage = reference_leverage_for_operation(record, positions)
        if leverage:
            record["referenceLeverage"] = leverage


async def refresh_binance_symbol_precisions(app: FastAPI) -> None:
    if app.state.symbol_precisions:
        return
    async with app.state.symbol_precision_lock:
        if app.state.symbol_precisions:
            return
        try:
            app.state.symbol_precisions = await asyncio.wait_for(
                app.state.symbol_precision_fetcher(app.state.http),
                timeout=METADATA_TIMEOUT_SECONDS,
            )
        except Exception as error:
            message = f"Binance 合约精度查询失败: {safe_error(error)}"
            if app.state.price_precision_error != message:
                app.state.price_precision_error = message
                app.state.store.log_event(None, "error", "Binance 合约精度查询", message)
                await send_error_alert(app, None, "Binance 合约精度查询", message)


async def refresh_monitor_metadata(app: FastAPI, monitor: dict[str, Any]) -> None:
    await asyncio.gather(
        refresh_monitor_drawdowns(app, monitor),
        refresh_monitor_finance(app, monitor),
        refresh_binance_symbol_precisions(app),
    )


async def enrich_operation_reference_leverages(
    app: FastAPI,
    monitor: dict[str, Any],
    operations: list[tuple[str, dict[str, Any]]],
    start_time: int,
    end_time: int,
) -> None:
    positions = await fetch_monitor_reference_positions(app, monitor, start_time, end_time)
    apply_operation_reference_leverages([record for _, record in operations], positions)
    app.state.store.update_operation_reference_leverages(monitor["id"], operations)


def queue_background_task(app: FastAPI, coroutine: Awaitable[None]) -> None:
    task = asyncio.create_task(coroutine)
    app.state.background_tasks.add(task)
    task.add_done_callback(app.state.background_tasks.discard)


def queue_monitor_metadata(app: FastAPI, monitor: dict[str, Any]) -> None:
    queue_background_task(app, refresh_monitor_metadata(app, monitor))


def queue_operation_reference_leverages(
    app: FastAPI,
    monitor: dict[str, Any],
    operations: list[tuple[str, dict[str, Any]]],
    start_time: int,
    end_time: int,
) -> None:
    queue_background_task(
        app, enrich_operation_reference_leverages(app, monitor, operations, start_time, end_time)
    )


async def process_monitor_trade_alerts(
    app: FastAPI, monitor: dict[str, Any], trades: list[dict[str, Any]]
) -> dict[str, Any]:
    if app.state.store.get_monitor(monitor["id"]) is None:
        # 成交预警任务飞行期间监控被删除：跳过写入，避免外键错误
        return {"status": "deleted", "count": 0}
    if not monitor["trade_alerts_initialized"]:
        baseline_count = app.state.store.save_trade_alert_baseline(monitor["id"], trades)
        return {"status": "baseline", "count": baseline_count}
    new_count = app.state.store.save_new_trade_alerts(
        monitor["id"], trades, monitor["notification_enabled"]
    )
    current_monitor = app.state.store.get_monitor(monitor["id"]) or monitor
    notification = await deliver_pending_trade_alerts(app, current_monitor)
    return {"new_count": new_count, **notification}


async def process_monitor_trade_alert_task(
    app: FastAPI, monitor: dict[str, Any], task: asyncio.Task[list[dict[str, Any]]]
) -> dict[str, Any]:
    try:
        trades = await task
    except Exception as error:
        message = f"Binance 成交预警查询失败: {safe_error(error)}"
        is_new_error = app.state.store.set_monitor_trade_alert_error(monitor["id"], message)
        app.state.store.log_event(monitor["id"], "error", "Binance 成交预警查询", message)
        if is_new_error:
            await send_error_alert(app, monitor, "Binance 成交预警查询", message)
        return {"status": "error", "count": 0, "error": message}
    trade_alert = await process_monitor_trade_alerts(app, monitor, trades)
    app.state.store.clear_monitor_trade_alert_error(monitor["id"])
    return trade_alert


async def poll_monitor(app: FastAPI, monitor: dict[str, Any]) -> dict[str, Any]:
    if monitor["name"] == f"带单员 {monitor['portfolio_id'][-8:]}":
        try:
            name = await app.state.leader_name_fetcher(
                app.state.http, monitor["url"], monitor["portfolio_id"]
            )
            if name != monitor["name"]:
                monitor = app.state.store.update_monitor_name(monitor["id"], name) or monitor
        except Exception:
            LOGGER.warning(
                "带单员名称更新失败: %s\n%s", monitor["portfolio_id"], traceback.format_exc()
            )
    end_time = int(time.time() * 1000)
    reference_start_time = end_time - (
        INITIAL_HISTORY_DAYS * 24 * 60 * 60 * 1000
        if not monitor["initialized"]
        else REGULAR_HISTORY_HOURS * 60 * 60 * 1000
    )
    order_task = asyncio.create_task(
        app.state.fetcher(app.state.http, monitor["portfolio_id"])
        if monitor["initialized"]
        else fetch_initial_history(app, monitor["portfolio_id"])
    )
    trade_alert = {"status": "empty", "count": 0}
    trade_task: asyncio.Task[list[dict[str, Any]]] | None = None
    late_trade_task = False
    if app.state.trade_alerts_enabled:
        trade_task = asyncio.create_task(
            app.state.trade_history_fetcher(app.state.http, monitor["portfolio_id"])
        )
        completed, _ = await asyncio.wait(
            (order_task, trade_task), return_when=asyncio.FIRST_COMPLETED
        )
        late_trade_task = trade_task not in completed
        if not late_trade_task:
            trade_alert = await process_monitor_trade_alert_task(app, monitor, trade_task)
    try:
        records = await order_task
    except Exception as error:
        if late_trade_task and trade_task:
            queue_background_task(app, process_monitor_trade_alert_task(app, monitor, trade_task))
        message = f"Binance 查询失败: {safe_error(error)}"
        is_new_error = app.state.store.set_monitor_error(monitor["id"], message)
        app.state.store.log_event(monitor["id"], "error", "Binance 查询", message)
        if is_new_error:
            await send_error_alert(app, monitor, "Binance 查询", message)
        return {"monitor_id": monitor["id"], "status": "error", "error": message}

    if app.state.store.get_monitor(monitor["id"]) is None:
        # 等待订单响应期间监控被删除：跳过本次写入，避免外键错误触发轮询告警
        return {"monitor_id": monitor["id"], "status": "deleted"}
    # Binance order-history already returns the same operation grouping as its UI.
    operations = keyed_records(records)
    if not monitor["initialized"]:
        baseline_count = app.state.store.save_baseline(
            monitor["id"], operations, monitor["notify_latest_baseline"]
        )
        app.state.store.update_operation_reference_leverages(monitor["id"], operations)
        current_monitor = app.state.store.get_monitor(monitor["id"]) or monitor
        notification = await deliver_pending_operations(app, current_monitor)
        result = {
            "monitor_id": monitor["id"],
            "status": "baseline",
            "baseline_count": baseline_count,
            "new_count": 0,
            "notification": notification,
            "trade_alert": trade_alert,
        }
        should_enrich_leverages = bool(operations)
    else:
        new_count = app.state.store.save_new_operations(
            monitor["id"], operations, monitor["notification_enabled"]
        )
        app.state.store.update_operation_reference_leverages(monitor["id"], operations)
        current_monitor = app.state.store.get_monitor(monitor["id"]) or monitor
        notification = await deliver_pending_operations(app, current_monitor)
        result = {
            "monitor_id": monitor["id"],
            "status": "ok",
            "new_count": new_count,
            "notification": notification,
            "trade_alert": trade_alert,
        }
        should_enrich_leverages = new_count > 0
    if should_enrich_leverages:
        queue_operation_reference_leverages(
            app, monitor, operations, reference_start_time, end_time
        )
    queue_monitor_metadata(app, monitor)
    if late_trade_task and trade_task:
        queue_background_task(app, process_monitor_trade_alert_task(app, monitor, trade_task))
    return result


def monitor_poll_offset_seconds(
    index: int, count: int, poll_interval_seconds: int = POLL_INTERVAL_SECONDS
) -> float:
    """Spread enabled monitors across each fixed poll interval."""
    return 0.0 if count < 2 else poll_interval_seconds * index / count


async def poll_scheduled_monitor(
    app: FastAPI, monitor: dict[str, Any], delay: float
) -> dict[str, Any]:
    if delay > 0:
        await asyncio.sleep(delay)
    try:
        return await poll_monitor(app, monitor)
    except Exception as error:
        message = f"轮询处理失败：{safe_error(error)}"
        is_new_error = app.state.store.set_monitor_error(monitor["id"], message)
        app.state.store.log_event(monitor["id"], "error", "轮询处理", message)
        if is_new_error:
            await send_error_alert(app, monitor, "轮询处理", message)
        return {"monitor_id": monitor["id"], "status": "error", "error": message}


async def poll_all(app: FastAPI) -> list[dict[str, Any]]:
    async with app.state.poll_lock:
        app.state.store.prune_old_operations(
            int(time.time() * 1000) - OPERATION_RETENTION_DAYS * 24 * 60 * 60 * 1000
        )
        monitors = [monitor for monitor in app.state.store.list_monitors() if monitor["enabled"]]
        poll_interval_seconds = app.state.store.poll_interval_seconds()
        results = await asyncio.gather(
            *(
                poll_scheduled_monitor(
                    app,
                    monitor,
                    monitor_poll_offset_seconds(index, len(monitors), poll_interval_seconds),
                )
                for index, monitor in enumerate(monitors)
            )
        )
        app.state.last_poll_at = utc_now()
        app.state.store.log_event(
            None,
            "info",
            "轮询完成",
            f"已检查 {len(results)} 个启用监控，异常 {sum(item['status'] == 'error' for item in results)} 个",
        )
        return results


def queue_monitor_check(app: FastAPI, monitor_id: int) -> bool:
    if monitor_id in app.state.queued_monitor_ids:
        return False
    app.state.queued_monitor_ids.add(monitor_id)
    queued_task = asyncio.create_task(run_queued_monitor_check(app, monitor_id))
    app.state.queued_monitor_tasks.add(queued_task)
    queued_task.add_done_callback(app.state.queued_monitor_tasks.discard)
    return True


async def run_queued_monitor_check(app: FastAPI, monitor_id: int) -> None:
    try:
        async with app.state.poll_lock:
            monitor = app.state.store.get_monitor(monitor_id)
            if monitor and monitor["enabled"]:
                await poll_monitor(app, monitor)
    except asyncio.CancelledError:
        raise
    except Exception as error:
        message = f"手动检查失败：{safe_error(error)}"
        app.state.store.log_event(monitor_id, "error", "手动检查", message)
        await send_error_alert(app, app.state.store.get_monitor(monitor_id), "手动检查", message)
    finally:
        app.state.queued_monitor_ids.discard(monitor_id)


async def poll_loop(app: FastAPI) -> None:
    while True:
        started_at = asyncio.get_running_loop().time()
        try:
            await poll_all(app)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            LOGGER.exception("轮询任务失败: %s", safe_error(error))
            message = f"轮询任务失败：{safe_error(error)}"
            app.state.store.log_event(None, "error", "轮询任务", message)
            await send_error_alert(app, None, "轮询任务", message)
        poll_interval_seconds = app.state.store.poll_interval_seconds()
        delay = max(0, poll_interval_seconds - (asyncio.get_running_loop().time() - started_at))
        if app.state.poll_settings_changed.is_set():
            app.state.poll_settings_changed.clear()
        elif delay:
            try:
                await asyncio.wait_for(app.state.poll_settings_changed.wait(), timeout=delay)
                app.state.poll_settings_changed.clear()
            except asyncio.TimeoutError:
                pass


def session_cookie_secure() -> bool:
    return os.getenv("COOKIE_SECURE", "0").strip().lower() in {"1", "true", "yes"}


def create_app(
    database_path: Path | None = None,
    *,
    start_poller: bool = True,
    fetcher: Fetcher = fetch_binance_order_history,
    trade_history_fetcher: Fetcher | None = None,
    trade_alerts_enabled: bool | None = None,
    position_history_fetcher: PositionHistoryFetcher = fetch_binance_position_history,
    leader_name_fetcher: LeaderNameFetcher = fetch_leader_name,
    drawdown_fetcher: DrawdownFetcher = fetch_leader_drawdowns,
    leader_finance_fetcher: LeaderFinanceFetcher = fetch_leader_finance,
    symbol_precision_fetcher: SymbolPrecisionFetcher = fetch_binance_symbol_precisions,
) -> FastAPI:
    data_dir = Path(os.getenv("DATA_DIR", "data"))
    database_path = database_path or data_dir / "monitor.db"

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        store = Store(database_path)
        generated_password = store.initialize(os.getenv("ADMIN_PASSWORD") or None)
        store.prune_old_operations(
            int(time.time() * 1000) - OPERATION_RETENTION_DAYS * 24 * 60 * 60 * 1000
        )
        if generated_password:
            LOGGER.warning("已生成初始管理员账号：admin；初始密码：%s", generated_password)
        app.state.store = store
        app.state.fetcher = fetcher
        app.state.trade_alerts_enabled = (
            trade_alerts_enabled
            if trade_alerts_enabled is not None
            else os.getenv("TRADE_ALERTS_ENABLED", "0").strip().lower() in {"1", "true", "yes"}
        )
        if not app.state.trade_alerts_enabled:
            app.state.store.clear_trade_alert_errors()
        app.state.trade_history_fetcher = trade_history_fetcher or (
            fetch_binance_trade_history
            if fetcher is fetch_binance_order_history
            else empty_trade_history
        )
        app.state.position_history_fetcher = position_history_fetcher
        app.state.leader_name_fetcher = leader_name_fetcher
        app.state.drawdown_fetcher = drawdown_fetcher
        app.state.leader_finance_fetcher = leader_finance_fetcher
        app.state.symbol_precision_fetcher = symbol_precision_fetcher
        app.state.http = httpx.AsyncClient(timeout=httpx.Timeout(20.0), follow_redirects=True)
        app.state.last_poll_at = None
        app.state.poll_lock = asyncio.Lock()
        app.state.poll_settings_changed = asyncio.Event()
        app.state.symbol_precisions = {}
        app.state.symbol_precision_lock = asyncio.Lock()
        app.state.price_precision_error = None
        app.state.queued_monitor_ids = set()
        app.state.queued_monitor_tasks = set()
        app.state.background_tasks = set()
        store.log_event(
            None, "info", "服务启动", f"轮询间隔 {store.poll_interval_seconds()} 秒"
        )
        task = asyncio.create_task(poll_loop(app)) if start_poller else None
        app.state.poll_task = task
        try:
            yield
        finally:
            if task:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            for queued_task in app.state.queued_monitor_tasks:
                queued_task.cancel()
            await asyncio.gather(*app.state.queued_monitor_tasks, return_exceptions=True)
            for background_task in app.state.background_tasks:
                background_task.cancel()
            await asyncio.gather(*app.state.background_tasks, return_exceptions=True)
            await app.state.http.aclose()
            store.close()

    app = FastAPI(title="策略监控中心", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    public_api_paths = {"/api/auth/login", "/api/auth/logout"}

    @app.middleware("http")
    async def authenticate_request(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        if request.url.path != "/health":
            user = request.app.state.store.session_user(request.cookies.get(SESSION_COOKIE))
            request.state.user = user
            if request.url.path.startswith("/api/") and request.url.path not in public_api_paths:
                if not user:
                    return JSONResponse(status_code=status.HTTP_401_UNAUTHORIZED, content={"detail": "登录已失效"})
        return await call_next(request)

    @app.get("/health", include_in_schema=False)
    async def health() -> dict[str, bool]:
        return {"ok": True}

    @app.get("/", include_in_schema=False)
    async def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    @app.post("/api/auth/login")
    async def login(payload: LoginPayload, request: Request) -> Response:
        user = request.app.state.store.authenticate(payload.username.strip(), payload.password)
        if not user:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="账号或密码不正确")
        token, ttl_seconds = request.app.state.store.create_session(user["id"])
        response = JSONResponse({"user": user})
        response.set_cookie(
            SESSION_COOKIE,
            token,
            max_age=ttl_seconds,
            httponly=True,
            secure=session_cookie_secure(),
            samesite="lax",
            path="/",
        )
        return response

    @app.post("/api/auth/logout")
    async def logout(request: Request) -> Response:
        request.app.state.store.revoke_session(request.cookies.get(SESSION_COOKIE))
        response = Response(status_code=status.HTTP_204_NO_CONTENT)
        response.delete_cookie(SESSION_COOKIE, path="/")
        return response

    @app.get("/api/auth/me")
    async def current_user(request: Request) -> dict[str, Any]:
        return {"user": request.state.user}

    @app.post("/api/auth/password")
    async def change_password(payload: PasswordChangePayload, request: Request) -> Response:
        if payload.current_password == payload.new_password:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="新密码不能与当前密码相同")
        if not request.app.state.store.change_password(
            request.state.user["id"], payload.current_password, payload.new_password
        ):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="当前密码不正确")
        response = JSONResponse({"message": "密码已更新，请使用新密码重新登录"})
        response.delete_cookie(SESSION_COOKIE, path="/")
        return response

    @app.get("/api/dashboard")
    async def dashboard(request: Request) -> dict[str, Any]:
        monitors = request.app.state.store.list_monitors()
        for monitor in monitors:
            monitor["check_queued"] = monitor["id"] in request.app.state.queued_monitor_ids
        current_errors = [
            {
                "monitor_id": monitor["id"],
                "monitor_name": monitor["name"],
                "monitor_note": monitor["note"],
                **error,
            }
            for monitor in monitors
            for error in monitor["current_errors"]
        ]
        operations = request.app.state.store.recent_operations(limit=25)
        for operation in operations:
            precision = request.app.state.symbol_precisions.get(operation["symbol"])
            operation["price_precision"] = precision[0] if precision else None
            operation["quantity_precision"] = precision[1] if precision else None
        return {
            "version": os.getenv("APP_VERSION", "dev"),
            "poll_interval_seconds": request.app.state.store.poll_interval_seconds(),
            "last_poll_at": request.app.state.last_poll_at,
            "metrics": request.app.state.store.overview_metrics(),
            "telegram": request.app.state.store.public_telegram_settings(),
            "email": request.app.state.store.public_email_settings(),
            "session": request.app.state.store.public_session_settings(),
            "monitors": monitors,
            "current_errors": current_errors,
            "operations": operations,
            "performance": request.app.state.store.performance(),
            "notification_blocks": request.app.state.store.notification_blocks(),
            "notification_channels": request.app.state.store.notification_channels(),
            "notification_attempts": request.app.state.store.notification_attempts(),
            "system_logs": request.app.state.store.system_logs(),
        }

    @app.get("/api/operations")
    async def operations(
        request: Request, monitor_id: int | None = None, limit: int = 200
    ) -> dict[str, Any]:
        operations = request.app.state.store.recent_operations(monitor_id, limit)
        for operation in operations:
            precision = request.app.state.symbol_precisions.get(operation["symbol"])
            operation["price_precision"] = precision[0] if precision else None
            operation["quantity_precision"] = precision[1] if precision else None
        return {"operations": operations}

    @app.delete("/api/operations")
    async def clear_operations(request: Request) -> dict[str, Any]:
        deleted_count = request.app.state.store.clear_operations()
        request.app.state.store.log_event(
            None, "info", "历史记录清理", f"已清理 {deleted_count} 条操作记录"
        )
        return {"deleted_count": deleted_count}

    @app.post("/api/operations/reset")
    async def reset_operation_state(request: Request) -> dict[str, int]:
        async with request.app.state.poll_lock:
            result = request.app.state.store.reset_record_state()
            request.app.state.last_poll_at = None
            queued_count = sum(
                queue_monitor_check(request.app, monitor["id"])
                for monitor in request.app.state.store.list_monitors()
                if monitor["enabled"]
            )
            request.app.state.store.log_event(
                None,
                "info",
                "记录状态重置",
                f"已清除 {result['operation_count']} 条操作和 {result['notification_count']} 条通知记录；"
                f"已排队重建 {queued_count} 个启用源的基线",
            )
        return {**result, "queued_count": queued_count}

    @app.get("/api/monitors/export")
    async def export_monitor_urls(request: Request) -> Response:
        content = "\n".join(request.app.state.store.monitor_urls())
        if content:
            content += "\n"
        return Response(
            content=content,
            media_type="text/plain; charset=utf-8",
            headers={"Content-Disposition": 'attachment; filename="binance-copy-watch-sources.txt"'},
        )

    @app.delete("/api/logs")
    async def clear_logs(request: Request) -> dict[str, int]:
        return request.app.state.store.clear_logs()

    @app.post("/api/notification-blocks", status_code=status.HTTP_201_CREATED)
    async def add_notification_block(
        payload: NotificationBlockCreate, request: Request
    ) -> dict[str, Any]:
        if not request.app.state.store.get_monitor(payload.monitor_id):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="带单人不存在")
        try:
            symbol = normalize_symbol(payload.symbol)
        except ValueError as error:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(error)) from error
        block = request.app.state.store.create_notification_block(payload.monitor_id, symbol)
        if not block:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="该带单人和合约已在屏蔽列表中")
        return block

    @app.delete("/api/notification-blocks/{block_id}", status_code=status.HTTP_204_NO_CONTENT)
    async def remove_notification_block(block_id: int, request: Request) -> Response:
        if not request.app.state.store.delete_notification_block(block_id):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="屏蔽规则不存在")
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    @app.get("/api/performance")
    async def performance(request: Request) -> dict[str, Any]:
        return {"items": request.app.state.store.performance()}

    @app.post("/api/monitors", status_code=status.HTTP_201_CREATED)
    async def add_monitor(payload: MonitorCreate, request: Request) -> dict[str, Any]:
        try:
            return await create_monitor_from_url(
                request.app, payload.note.strip(), payload.url
            )
        except ValueError as error:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(error)) from error
        except sqlite3.IntegrityError as error:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="该 Binance 地址已存在") from error

    @app.post("/api/monitors/batch", status_code=status.HTTP_201_CREATED)
    async def add_monitors_batch(
        payload: MonitorBatchCreate, request: Request
    ) -> dict[str, list[dict[str, Any]]]:
        created = []
        errors = []
        note = payload.note.strip()
        for raw_url in payload.urls:
            url = raw_url.strip()
            if not url:
                errors.append({"url": raw_url, "detail": "地址不能为空"})
                continue
            if len(url) > 1000:
                errors.append({"url": url, "detail": "地址长度不能超过 1000 个字符"})
                continue
            try:
                created.append(await create_monitor_from_url(request.app, note, url))
            except ValueError as error:
                errors.append({"url": url, "detail": str(error)})
            except sqlite3.IntegrityError:
                errors.append({"url": url, "detail": "该 Binance 地址已存在"})
            except Exception as error:
                errors.append({"url": url, "detail": f"带单员信息获取失败：{safe_error(error)}"})
        return {"created": created, "errors": errors}

    @app.patch("/api/monitors/{monitor_id}")
    async def edit_monitor(
        monitor_id: int, payload: MonitorUpdate, request: Request
    ) -> dict[str, Any]:
        monitor = request.app.state.store.update_monitor(
            monitor_id,
            payload.note.strip() if payload.note is not None else None,
            payload.mode,
        )
        if not monitor:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="监控地址不存在")
        return monitor

    @app.delete("/api/monitors/{monitor_id}", status_code=status.HTTP_204_NO_CONTENT)
    async def remove_monitor(monitor_id: int, request: Request) -> Response:
        if not request.app.state.store.delete_monitor(monitor_id):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="监控地址不存在")
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    @app.post("/api/monitors/{monitor_id}/check")
    async def check_monitor(monitor_id: int, request: Request) -> dict[str, Any]:
        monitor = request.app.state.store.get_monitor(monitor_id)
        if not monitor:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="监控地址不存在")
        if not monitor["enabled"]:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="该地址未启动监听")
        queue_monitor_check(request.app, monitor_id)
        return JSONResponse({"status": "queued"}, status_code=status.HTTP_202_ACCEPTED)

    @app.post("/api/notification-channels", status_code=status.HTTP_201_CREATED)
    async def create_notification_channel(
        payload: NotificationChannelCreate, request: Request
    ) -> dict[str, Any]:
        try:
            return request.app.state.store.create_notification_channel(payload)
        except ValueError as error:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(error)) from error

    @app.patch("/api/notification-channels/{channel_id}")
    async def update_notification_channel(
        channel_id: int, payload: NotificationChannelUpdate, request: Request
    ) -> dict[str, Any]:
        try:
            channel = request.app.state.store.update_notification_channel(channel_id, payload)
        except ValueError as error:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(error)) from error
        if not channel:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="通知渠道不存在")
        return channel

    @app.delete("/api/notification-channels/{channel_id}", status_code=status.HTTP_204_NO_CONTENT)
    async def delete_notification_channel(channel_id: int, request: Request) -> Response:
        if not request.app.state.store.delete_notification_channel(channel_id):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="通知渠道不存在")
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    @app.post("/api/notification-channels/{channel_id}/test")
    async def test_notification_channel(channel_id: int, request: Request) -> dict[str, str]:
        channel = request.app.state.store.notification_channel_for_delivery(channel_id)
        if not channel:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="通知渠道不存在或配置不完整")
        label = notification_channel_label(channel)
        try:
            if channel["kind"] == "telegram":
                await send_telegram_channel_message(
                    request.app, channel, "策略监控中心已连接。后续 Binance 新操作会发送到这里。"
                )
            elif channel["kind"] == "dingtalk":
                await send_dingtalk_message(
                    request.app,
                    channel,
                    "[策略监控] 钉钉机器人测试",
                    "策略监控中心已连接。后续 Binance 新操作会发送到这里。",
                )
            else:
                await send_feishu_message(
                    request.app,
                    channel,
                    "[策略监控] 飞书机器人测试",
                    "策略监控中心已连接。后续 Binance 新操作会发送到这里。",
                )
        except Exception as error:
            message = f"{label} 测试失败: {safe_error(error)}"
            request.app.state.store.log_notification(None, "error", message, label)
            raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=message) from error
        message = f"{label} 测试消息已发送"
        request.app.state.store.log_notification(None, "sent", message, label)
        return {"message": message}

    @app.post("/api/settings/telegram")
    async def save_telegram_settings(payload: TelegramSettingsUpdate, request: Request) -> dict[str, Any]:
        previous_settings = request.app.state.store.telegram_settings()
        was_enabled = enabled_notification_channels(previous_settings)[0]
        token_changed = bool(payload.bot_token and payload.bot_token.strip())
        chat_id_changed = (
            payload.chat_id is not None
            and payload.chat_id.strip() != previous_settings.get("telegram_chat_id", "")
        )
        request.app.state.store.update_telegram_settings(
            payload.bot_token, payload.chat_id, payload.enabled
        )
        settings = request.app.state.store.public_telegram_settings()
        is_enabled = enabled_notification_channels(request.app.state.store.telegram_settings())[0]
        if is_enabled and (not was_enabled or token_changed or chat_id_changed):
            try:
                settings["test_status"] = "sent"
                settings["test_message"] = await send_telegram_test_message(request.app)
            except Exception as error:
                message = f"Telegram 自动测试失败: {safe_error(error)}"
                request.app.state.store.log_notification(None, "error", message, "Telegram 默认")
                settings["test_status"] = "error"
                settings["test_message"] = message
        return settings

    @app.post("/api/settings/email")
    async def save_email_settings(
        payload: EmailSettingsUpdate, request: Request
    ) -> dict[str, Any]:
        request.app.state.store.update_email_settings(payload)
        return request.app.state.store.public_email_settings()

    @app.post("/api/settings/session")
    async def save_session_settings(
        payload: SessionSettingsUpdate, request: Request
    ) -> Response:
        request.app.state.store.update_session_ttl_hours(payload.session_ttl_hours)
        request.app.state.store.revoke_all_sessions()
        response = JSONResponse(
            {
                "session_ttl_hours": payload.session_ttl_hours,
                "message": "会话有效期已更新，全部会话已退出，请重新登录",
            }
        )
        response.delete_cookie(SESSION_COOKIE, path="/")
        return response

    @app.post("/api/settings/poll")
    async def save_poll_settings(
        payload: PollSettingsUpdate, request: Request
    ) -> dict[str, int]:
        request.app.state.store.update_poll_interval_seconds(payload.poll_interval_seconds)
        request.app.state.poll_settings_changed.set()
        request.app.state.store.log_event(
            None, "info", "轮询设置", f"轮询间隔已更新为 {payload.poll_interval_seconds} 秒"
        )
        return {"poll_interval_seconds": payload.poll_interval_seconds}

    @app.post("/api/telegram/test")
    async def test_telegram(request: Request) -> dict[str, str]:
        try:
            message = await send_telegram_test_message(request.app)
        except Exception as error:
            message = f"Telegram 测试失败: {safe_error(error)}"
            request.app.state.store.log_notification(None, "error", message, "Telegram 默认")
            raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=message) from error
        return {"message": message}

    @app.post("/api/email/test")
    async def test_email(request: Request) -> dict[str, str]:
        try:
            await send_email_text(
                request.app,
                "[策略监控] 邮件通知测试",
                "策略监控中心已连接。后续 Binance 新操作会通过邮件发送到这里。",
            )
        except Exception as error:
            message = f"邮件测试失败: {safe_error(error)}"
            request.app.state.store.log_notification(None, "error", message, "SMTP 邮箱")
            raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=message) from error
        request.app.state.store.log_notification(None, "sent", "邮件测试消息已发送", "SMTP 邮箱")
        return {"message": "邮件测试消息已发送"}

    return app


app = create_app()
