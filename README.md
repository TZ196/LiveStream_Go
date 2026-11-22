# LiveStream 实时直播互动系统

LiveStream 面向本地开发，目标包含用户认证、直播间管理、实时弹幕、点赞、在线连接统计和异步事件持久化。当前已接通账号流程：前端注册 → 登录 → 携带 JWT 查询当前用户 → 展示账号 → 退出。后端使用 Hertz、GORM 和 MySQL；房间与实时互动仍按后续阶段实现。

设计基线为 **V1.1，2026 年 11 月 18 日**，后续选型按决策记录更新。后端采用 Go、CloudWeGo Hertz 和 Gorilla WebSocket，MySQL 访问统一使用 GORM；前端采用 Vue 3、TypeScript 和 Vite，Redis、Kafka 随开发阶段接入。

从 [文档导航与职责](docs/README.md) 开始阅读。需求见 [需求规格](docs/requirements.md)，实施顺序见 [开发计划](docs/development-plan.md)，实际完成情况见 [项目状态](docs/status.md)。工程协作规则见 [AGENTS.md](AGENTS.md)。

## 运行当前后端

当前使用 Go 1.26.8、Hertz v0.10.6、GORM v1.31.2、MySQL 8.4.13。第一次在新工作区运行，先按 [MySQL 初始化说明](docs/learning/04-mysql-and-migration.md) 配置根目录 `.env`、启动 MySQL 并执行迁移。

本工作区已有本地 `.env`、随机 JWT 签名密钥与数据库版本 2，无需重新建表。新工作区还需填写样例中的 `LIVESTREAM_JWT_SECRET`：至少 32 随机字节的标准 Base64 编码，无默认密钥。MySQL 映射使用 `127.0.0.1:13306`，避开环境中已有的 3306 实例。日常在仓库根目录执行：

```bash
docker compose --env-file .env -f deployments/docker/compose.yaml up -d --wait mysql
set -a
source .env
set +a
cd backend
go run ./cmd/migrate && go run ./cmd/api
```

迁移命令按编号执行尚未成功的版本，成功后记录，失败停止；只有迁移命令成功，`&&` 后的 API 才会启动。已有版本 2 时会跳过已完成步骤，API 本身只检查结构，不自动改表。

在另一个终端执行 `curl -i http://127.0.0.1:8080/healthz`，预期得到 HTTP 200 和 `{"status":"ok"}`。端口可通过 `go run ./cmd/api -addr 127.0.0.1:8081` 覆盖。

GET /readyz 现在检查 MySQL：正常时返回 200，`stage` 为 `P1b`、`dependencies.mysql` 为 `ready`；运行期间数据库不可用返回 503，`/healthz` 仍只表示进程能响应。配置样例位于 [backend/configs/.env.example](backend/configs/.env.example)。本步文件分工、首次建表与故障处理见 [第四步学习记录](docs/learning/04-mysql-and-migration.md)。

基础请求过程见 [第一步学习记录](docs/learning/01-minimal-http-server.md)。

## 运行当前前端

保留后端终端，在另一个终端从仓库根目录执行：

```bash
cd frontend
npm ci
npm run dev
```

打开 http://127.0.0.1:5173 。已安装依赖且锁文件未变化时，可以直接执行 `npm run dev`。本轮使用 Node.js v26.13.0、npm 11.20.0；依赖具体版本由 package.json 和 package-lock.json 锁定。

页面现在可以实际注册和登录。注册成功后输入同一密码登录，页面会通过受保护接口读取并展示当前账号；可以刷新账号信息或退出。登录凭证仅保存在当前页面内存中，刷新页面需重新登录，30 分钟后结束会话。开发连接检查仍只确认 API 进程能响应。

用户名去除首尾 ASCII 空格并转小写，3–32 位字母、数字或下划线，以字母开头。密码为 12–128 个 Unicode 字符，保留空格。注册与登录按来源 IP 共享每个整分钟 10 次限制，过快操作会显示等待倒计时；通过本地 Vite 代理的浏览器共用代理的来源地址。

后端改用 8081 时，在 frontend 目录执行 `LIVESTREAM_API_TARGET=http://127.0.0.1:8081 npm run dev`，或按 [前端配置样例](frontend/.env.example) 建立 `.env.local` 后重启开发服务。

前端检查命令为 `npm run type-check` 和 `npm run build`。构建生成的 `frontend/dist/` 尚未接入后端静态托管；开发连接检查面板不显示在生产页面。文件分工、请求链路和当前边界见 [第三步学习记录](docs/learning/03-frontend-and-health.md)。

本轮请求经过各层的说明见 [第五步：账号流程](docs/learning/05-user-authentication.md)。下一阶段是创建、查看和关闭直播间，再接入 WebSocket 弹幕。当前没有音视频播放能力。
