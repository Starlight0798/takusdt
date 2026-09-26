# 策略监控中心开发指南

## 项目目标

这是一个单管理员、Docker 化的 Binance 跟单带单员监控服务。它每 20 秒检查已启用的带单员，保存最近 30 天操作、展示操作与表现，并将每条新操作发送到已启用的 Telegram 或 SMTP 邮箱渠道。

## 代码结构

- `app/main.py`：FastAPI 应用、SQLite 存储、Binance 查询、轮询、通知和全部 API。
- `app/static/index.html`：无构建步骤的中文单页控制台，内含 CSS 和 JavaScript。
- `tests/test_main.py`：标准库 `unittest` 测试；外部 HTTP 使用 `httpx.MockTransport`。
- `Dockerfile`：生产镜像定义；OCI source/revision 标签由 CI 注入。
- `docker-compose.yml`：运行定义，默认使用 GHCR 镜像（可用 `TAKUSDT_IMAGE` 覆盖）；Nginx 容器公开监听 `80` 并反代到应用容器。
- `nginx/default.conf`：通用 HTTP 反代配置（`server_name _`），访客 HTTPS 由前置 CDN 或反代负责。
- `.github/workflows/publish.yml`：推送 `main` 后运行测试，再构建并推送多架构 GHCR 镜像。

## 必须保持的行为

- 所有用户可见时间均为上海时区 `Asia/Shanghai`（UTC+8）。数据库中的 ISO 时间保持 UTC，Binance 的操作时间保持毫秒时间戳。
- `POLL_INTERVAL_SECONDS` 固定为 `20`。同一轮的多个启用地址按该周期等间隔错开查询。`poll_loop()` 以单调时钟计算剩余等待时间，单轮超时后应立即进入下一轮，而不能并发重叠轮询。
- SQLite 的 `operations` 仅保存最近 30 天。`known_operations` 是短期去重基线：清除历史记录时不得清除它，否则旧操作会重写入并可能重复通知。
- 首次添加监控会立即排队回溯最近 7 天并写为 `baseline`，并只将该源最新一条操作作为通知发送；其余历史绝不补发。后续轮询只查询最近 24 小时。多条待发送操作优先推送最新一条。
- 页面操作必须从 Binance `order-history` 获取，它已按原生页面订单口径聚合；不得用缺少订单 ID 的 `trade-history` 分笔记录猜测还原。
- `trade-history` 仅可在明确启用 `TRADE_ALERTS_ENABLED=1` 且有独立限流预算时用于低延迟“成交预警”，绝不写入 `operations` 或推断还原页面订单。它可能包含重复或分笔成交；精确重复仅发送一次预警，首轮仅建立不通知的预警基线。早于预警基线时刻的成交即使后续翻页出现也绝不发送。后续官方订单命中已发送预警时，写入 `operations` 并标记 `prealerted`，不得重复发送官方订单通知。
- Binance 成功响应可能只包含 `code: "000000"` 而没有 `success` 字段；不得把该响应判为失败。
- 带单员名称从 `friendly/future/copy-trade/lead-portfolio/detail` 的 `data.nickname` 获取。HTML 抓取仅作回退。
- 7D/30D/90D 最大回撤从 Binance `chart-data` 的每日 ROI 序列按页面同口径计算；它是日快照回撤，不代表盘中最大回撤。
- 每个地址仅有三种模式：`notify`（启动监听并允许通知）、`silent`（启动监听但不允许通知）和 `stopped`（不启动监听）。切换到 `silent` 或 `stopped` 时，已有 `pending` 操作必须标记为 `skipped`，重新启用不得补发。
- Compose 默认 `COOKIE_SECURE=1`；不要为应用容器增加宿主机端口映射。公网访问应经 HTTPS 前置层到 Nginx 的 `80` 端口。
- `DASHBOARD_BASE_URL` 为空时，通知不附带面板链接；不得在代码中写死任何部署域名。
- FastAPI 的 `/docs`、`/redoc`、`/openapi.json` 保持关闭。
- 通知渠道关闭或未配置时，新操作标记为 `skipped`（UI 显示“未推送”），重新开启时不得补发旧操作。
- 已启用渠道的通知必须一条新操作对应一条通知，包含带参数的操作页链接、四向操作图标、参考杠杆、Binance 合约超链接、本次已实现盈亏（无则“暂无”）、7D/30D/90D 胜率/最大回撤及 30D 已实现盈亏/保守胜率。发送失败时保留 `pending` 并按下一轮重试。
- 新的查询或通知异常写入 `system_logs` 并尝试发送异常告警；日志与告警必须包含具体异常类型和安全的错误详情，不得只写“未预期错误”；相同持续错误不得每分钟重复告警。

## SQLite 约定

- 所有数据库改动必须兼容已有 `/data/monitor.db`。使用 `CREATE TABLE IF NOT EXISTS`、`_ensure_column()` 或可重入的数据迁移。
- `operations` 是可见历史；`known_operations` 是去重缓存；两者都由 `prune_old_operations()` 仅清理超过 30 天的记录。
- `trade_alerts` 保存分笔成交预警及其短期去重基线，由 `prune_old_operations()` 一并仅清理超过 30 天的记录；它不是可见页面操作历史。
- `notification_attempts` 保存渠道发送结果，`system_logs` 保存轮询、异常、启动和清理事件。仪表盘各读取最多 200 条，数据库本身不在应用层自动截断。
- 仅在管理员明确触发“清空所有日志”时，才同时清除 `notification_attempts` 和 `system_logs`；清空后不得新增一条清理日志。
- 不要通过重置、删除或覆盖服务器 `data/` 解决普通问题。仅在用户明确要求清空运行数据时才这样做。

## API 和前端

- 所有 `/api/*`（登录和登出除外）都要求 HttpOnly 会话 Cookie。
- `GET /api/dashboard` 是前端的聚合读取接口；新增面板数据时优先扩展该接口。
- `DELETE /api/operations` 清除可见历史，保留 `known_operations`。
- 仅在管理员明确触发“重置记录状态”时，才清除 `operations`、`known_operations` 和通知发送记录；保留所有源与配置，将监控恢复为未初始化状态，并为启用源重建不通知的 7 天基线。
- 监控管理页面的新增区域默认折叠，支持单个和批量添加（每行一个 URL，最多 20 条）；已配置地址支持关键词和 `notify`、`silent`、`stopped` 单项模式筛选。地址信息和 7D/30D/90D 表现显示在同一张带单员卡片中，不要重新拆成独立表现页面。
- 操作记录页使用时间线，支持带单人、操作类型、合约和通知状态组合筛选；在该页时每 30 秒仅重新请求和渲染记录数据，不得重载页面。`#operations?monitor-id={ID}&monitor-name={NAME}&symbol-link={SYMBOL}` 必须自动套用带单人和合约筛选。每条记录的参考杠杆仅可由同源 `position-history` 按币种、持仓方向和时间匹配，不得从成交额反推；无可信匹配显示“暂无”。合约链接格式必须为 `https://www.binance.com/zh-CN/futures/{SYMBOL}`。
- 概览页的带单人名称必须可跳转至操作记录并筛选该源；项目表现摘要按 30D 排序显示前 8 名，包含 30D 已实现盈亏和保守胜率，不显示独立“监控状态”面板。
- 通知中心使用 Telegram 配置、SMTP 邮箱、通知发送日志和系统运行日志四个可切换的全宽分栏；Telegram 与 SMTP 配置即使已填写也必须支持独立启用/关闭。
- 会话有效期为可持久化的 1 至 720 小时设置；修改后必须撤销全部已有会话，重新登录后才使用新有效期。
- `index.html` 是手写页面；保持现有中文、紧凑企业控制台视觉和移动端 CSS 断点。不要引入前端构建系统或框架。

## 验证

每次修改后至少运行：

```powershell
python -m py_compile 'app/main.py' 'tests/test_main.py'
python -m unittest discover -v
```

- 增加或修复后端行为时，在 `tests/test_main.py` 中添加确定性测试。
- 不要对真实 Binance、Telegram 或 SMTP 端点写单元测试。
- 不要在仓库中写入服务器地址、SSH 端口、个人域名、令牌或其他部署私有信息；部署细节只放在各自的部署环境中。

## 发布

- 推送 `main` 后由 GitHub Actions 运行测试，通过后构建 `linux/amd64`、`linux/arm64` 镜像并推送 `ghcr.io/<owner>/takusdt:<sha7>` 与 `latest`，同时注入 OCI source/revision 标签和 `APP_VERSION`。
- 不要从落后于 GitHub 的工作目录手工构建发布镜像。
- 提交前检查 `git status --short`、`git diff --check` 和测试结果。不要提交 `data/`、`.env`、`__pycache__/` 或密钥。
- 不要通过删除 `data/` 解决普通问题；仅在管理员明确要求清空运行数据时才这样做。
- 初始 `admin` 密码仅在首次启动日志中输出一次，禁止恢复写入密码文件的旧行为。
