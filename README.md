# 策略监控中心

Docker 化的 Binance 跟单带单员监控系统。可管理多个 `lead-details` 地址，每 20 秒读取最新操作、通过 Telegram 和 SMTP 邮箱发送通知，并基于已实现盈亏展示策略表现。

## 核心功能

- 正式账号登录：首次容器启动自动生成 `admin` 随机密码并哈希保存，密码只输出一次到容器日志；账户安全页可设置 1 至 720 小时的会话有效期，保存后会退出全部现有会话。
- 多页面中文 WebUI：概览、监控管理、操作记录、通知中心和账户安全。概览中的带单人名称可直接跳转至该源的操作筛选记录。
- 监控管理的新增区域默认折叠；支持单个或每行一个 URL 的批量添加，添加后立即在后台建立近 7 天基线。已配置地址支持名称、备注、地址、portfolioId 关键词及监听通知模式筛选；每个地址可设置为启动监听并允许通知、启动监听但不允许通知或不启动监听。
- 操作记录使用上海时区（UTC+8）显示，最新记录在前；操作页每 30 秒仅请求并渲染记录内容，不会重载页面。Binance 单向持仓订单按页面口径显示“做多”或“做空”，对冲持仓显示开多、平多、开空或平空。
- 操作记录使用 Binance 原生页面同源的 `order-history` 订单接口，避免将缺少订单 ID 的分笔成交错误合并或拆分；同时以同一带单员的 `position-history` 按币种、持仓方向和时间匹配，显示仓位历史提供的参考杠杆。无可信匹配时显示“暂无”，不反推杠杆。
- 胜率仅统计 `realizedProfit` 不为零的已结算记录，并分别展示 7 天、30 天、90 天的当前可用样本、保守胜率和已实现盈亏。
- 7 天、30 天、90 天的胜率旁同时显示 Binance 每日 ROI 序列计算的最大回撤；该指标为日快照口径，可能低于盘中实际最大回撤。
- 首次添加时回溯最近 7 天建立基线，并仅推送该源最新一条操作；其余历史不会推送。后续轮询仅查询最近 24 小时；已发现的操作和去重基线持续保留，只有超过 30 天时才自动清理。新操作可推送到 Telegram、SMTP 邮箱或两者，两个渠道均可独立关闭。每条通知包含可直达 `#operations` 带单人/合约筛选的带单人链接、四向操作图标、参考杠杆、可点击的 Binance 合约链接、本次已实现盈亏（无已结算盈亏时显示“暂无”）、带单人的 7D/30D/90D 胜率和最大回撤，以及 30D 已实现盈亏与保守胜率。
- 成交预警是默认关闭的实验性通道：它读取 `trade-history`，可能被 Binance 与订单接口的共享限流影响。仅在明确配置 `TRADE_ALERTS_ENABLED=1` 且有独立限流预算时启用；页面操作记录始终只来自 `order-history`。首次启用与版本升级后的首轮只建立预警去重基线，早于该基线时刻的成交一律不补发。
- 通知渠道关闭期间发现的操作标记为未推送，不会在渠道重新启用后补发；启用后的每条新操作分别发送一条通知。
- 轮询任务按 20 秒间隔执行；多个启用地址在每个周期内等间隔错开查询，避免周期开始时集中请求。系统记录服务启动、轮询完成、查询异常和通知异常，并记录具体异常类型与详情；新的异常会推送到已启用的通知渠道。
- 概览保留最新操作与按 30D 胜率排序的前 8 名项目表现，展示 30D 已实现盈亏与保守胜率；通知中心以 Telegram、SMTP、通知发送日志和系统运行日志四个全宽分栏切换展示。
- 可从操作记录页面清理已显示的历史记录；系统保留近 30 天去重基线，因此清理不会导致旧成交重新写入或重复通知。
- 账户安全页可导出全部监控源 URL，也可重置操作、去重基线和通知发送记录。重置后会保留源、备注、监听模式和通知配置，并为启用源重新建立不通知的 7 天基线。
- 账户安全页可清空通知发送日志和系统运行日志；清空操作不会影响监控源、通知配置或操作去重状态。

## Docker Compose 部署

`docker-compose.yml` 默认使用 GitHub Actions 构建的镜像 `ghcr.io/jiemo9527/takusdt:latest`（`linux/amd64`、`linux/arm64`）。镜像暂未公开时，可先 `docker login ghcr.io`（令牌需 `read:packages`），或改为本地构建：

```bash
git clone https://github.com/jiemo9527/takusdt.git && cd takusdt
cp .env.example .env        # 按需填写 DASHBOARD_BASE_URL 等
docker compose up -d        # 使用 GHCR 镜像
# 或：docker compose build && docker compose up -d   （本地构建）
```

应用容器只在 Docker 内网暴露 `8000`，由 Nginx 容器监听宿主机 `80` 端口反代。访客 HTTPS 应由前置层负责，例如 Cloudflare 橙云代理，或宿主机上的 Caddy / Nginx 证书；Compose 默认 `COOKIE_SECURE=1`，因此必须通过 HTTPS 访问面板，否则无法登录。若只在内网以 HTTP 访问，把 `.env` 中的 `COOKIE_SECURE` 改为 `0`。

> 使用 Cloudflare Flexible 模式时，Cloudflare 与源站之间是明文 HTTP；需要端到端加密时请使用 Origin Certificate + **Full (strict)**，并限制源站 `80` 端口仅允许 CDN 回源 IP 访问。

### 环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `DASHBOARD_BASE_URL` | 空 | 面板公网地址，如 `https://monitor.example.com`。填写后通知中的带单人名称会链接到筛选后的操作记录；为空则不附带链接 |
| `COOKIE_SECURE` | `1` | 会话 Cookie 仅经 HTTPS 发送 |
| `ADMIN_PASSWORD` | 空 | 首次初始化时的管理员密码；为空则随机生成并仅在首次启动日志输出一次 |
| `TRADE_ALERTS_ENABLED` | `0` | 实验性成交预警，见上文 |
| `HTTP_PORT` | `80` | Nginx 映射到宿主机的端口 |
| `TAKUSDT_IMAGE` | `ghcr.io/jiemo9527/takusdt:latest` | 自定义镜像 |

### 验证与维护

```bash
docker compose ps
docker compose logs --tail=100 copy-watch nginx
curl -fsS http://127.0.0.1/health
docker compose logs copy-watch | grep '初始密码'   # 首次启动后查看管理员初始密码
docker compose pull && docker compose up -d        # 升级
```

日志中会出现一次 `已生成初始管理员账号：admin；初始密码：...`。首次登录后请在“账户安全”修改密码。

### 清空重建

以下操作会永久删除全部监控源、操作记录、通知配置、会话和日志：

```bash
docker compose down
rm -rf ./data
docker compose pull
docker compose up -d
```

新容器会生成新的初始管理员密码，并写入首次启动日志。

## 开发

```bash
python -m venv .venv && . .venv/bin/activate   # Windows: .venv\Scriptsctivate
pip install -r requirements.txt
python -m py_compile app/main.py tests/test_main.py
python -m unittest discover -v
DATA_DIR=./data COOKIE_SECURE=0 uvicorn app.main:app --port 8000
```

## 注意事项

- Binance 数据来自网站使用的公开接口，可能受接口变更、地区限制或 IP 风控影响；异常会显示在监控项状态中。网页 WAF 阻断名称抓取时，系统会暂时显示地址尾号作为回退名称。
- Binance 返回的记录没有独立成交 ID，完全相同且同一时间戳的后续成交无法由上游数据完全区分。
- 系统仅保存和展示近 30 天操作。30 天和 90 天指标会基于当前可用样本计算，不构成收益承诺或历史全量审计。

## 许可证

[MIT](LICENSE)
