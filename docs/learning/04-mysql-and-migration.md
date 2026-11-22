# 第四步：MySQL、GORM 连接池与独立迁移

日期：2026 年 11 月 20 日。本篇记录 P1b 的数据库准备任务：连接、用户表与独立迁移。其后的注册、密码计算、JWT 和业务 Repository 已在 [第五步](05-user-authentication.md) 接通，当前完成状态以项目状态为准。

## 先分清三个职责

MySQL 容器负责运行数据库、保存数据；`cmd/migrate` 负责按版本建立和修改表；`cmd/api` 连接已经准备好的数据库并提供 HTTP 服务。迁移完成后进程就退出，它不是新的微服务。

本次吸收参考项目审查中的 [ISSUE-01、ISSUE-10](../reviews/live-stream-danmaku-review.md)：启动不删表，迁移独立执行，版本固定，秘密从本地环境注入。

## 文件分工与阅读顺序

| 文件 | 作用 |
| --- | --- |
| [compose.yaml](../../deployments/docker/compose.yaml) | 运行 MySQL，映射端口，使用数据卷保留数据 |
| [config.go](../../backend/internal/config/config.go) | LoadMySQL 读取并校验数据库配置；Load 在此基础上处理 API 的 HTTP 配置 |
| [mysql.go](../../backend/internal/database/mysql.go) | 让 GORM 创建连接池，设置容量、检查连接与关闭资源 |
| [迁移入口](../../backend/cmd/migrate/main.go) | 读取配置、连接数据库、执行迁移、关闭连接池与返回退出码 |
| [迁移调度](../../backend/migrations/migrate.go) | 取得迁移锁，查成功版本，按序执行，每步成功后登记，失败停止 |
| [版本 1](../../backend/migrations/000001_create_users.go)、[版本 2](../../backend/migrations/000002_tighten_username_check.go) | 固定的建表与约束修改步骤 |
| [schema.go](../../backend/internal/database/schema.go) | 读取连续版本，检查当前程序所需版本和 users 列 |
| [API 入口](../../backend/cmd/api/main.go) | 连接并检查结构，通过后启动 HTTP，退出时关闭连接池 |
| [server.go](../../backend/internal/httpserver/server.go) | 就绪请求调用只读结构检查，将结果映射为 200 或 503 |

先读配置与连接，再沿着迁移入口理解版本调度，最后回到 API 的启动流程。当前目录仅有 Go 迁移步骤，通过独立命令执行。

## GORM 怎样创建连接池

连接池是一组可重复使用的数据库连接。请求需要查询时借用连接，操作结束后归还；不必每个请求都重新建立 TCP 连接和进行认证。

1. `LoadMySQL()` 得到地址、数据库名、账号和密码；配置合法不等于已经连通。
2. `database.Open()` 把配置交给 GORM 的 MySQL 适配器，调用 `gorm.Open()`。适配器内部创建底层连接池，应用不再先调用 `sql.OpenDB()`。
3. `db.DB()` 取回这个已经存在的池，设置连接上限、空闲数和复用时间。这个调用不会创建第二套池。
4. `PingContext()` 实际验证连通性和认证，同时接受超时与取消。为统一控制这一步，GORM 自动 Ping 和自动版本探测已经关闭。
5. API 或迁移命令结束时，由 `database.Close()` 关闭这个池。

`*gorm.DB` 是操作数据库的入口；底层 `*sql.DB` 管理连接资源。两者有分工，使用 GORM 并不意味着底层不再使用 Go 的连接池和 MySQL 驱动。我们的源码不再直接导入 database/sql，也没有维护第二套池。

连接数和超时预算以 [部署设计](../deployment.md) 为准，它们是保护参数，不是实测吞吐量。认证阶段已按这个边界建立 Repository、Service 和 Handler，具体调用见第五步。

## 独立迁移具体做什么

启动命令是 `go run ./cmd/migrate`，它不会启动 HTTP。执行顺序是：

```text
读取 MySQL 配置 → GORM 连接数据库 → 取得同库迁移锁
  → 准备 schema_migrations → 读取已成功版本
  → 执行下一个版本 → 成功后写入版本记录 → 继续下一版
  → 检查所需结构 → 释放锁、关闭连接池 → 退出 0
```

其中任一步出错就停止，不再执行后续版本，并以退出码 1 报告。版本 1 用 GORM 的 `Migrator().CreateTable()` 建 users；版本 2 用 GORM 的 `Exec()` 执行一条改表语句，修正用户名约束。迁移模型是固定的 `userV1`，不会引用未来不断变化的业务 User 模型。

`schema_migrations` 是成功账本，记录 `version` 与 `applied_at`，和用户数量无关。例如：

| 运行前 | 本次动作 | 成功后 |
| --- | --- | --- |
| 空数据库 | 创建记录表，执行版本 1，再执行 2 | 记录 1、2 |
| 已有版本 1 | 跳过 1，只执行 2 | 原版本 1 时间不变，新增 2 |
| 已有版本 1、2 | 跳过已完成步骤，检查结构 | 两条记录及用户数据不变 |
| 版本 2 执行失败 | 不登记 2，停止后续步骤 | 保留已成功的版本 1 |

迁移命令可以重复运行；这不等于任意失败都能盲目重试。已有表却没有成功记录时，命令会报错，要求先核对结构，不会擅自认为它已经迁移成功。

为什么使用数据库锁：若两个终端同时执行迁移，它们可能同时看到版本 1，接着同时改同一张表。命令在固定连接上取得同库命名锁，第二个命令立即退出；第一个结束后才能再执行。`Connection` 的回调就是在持有同一条连接期间运行这些步骤，回调外由入口关闭池。

版本 2 为什么还出现 SQL：GORM 有删除、创建约束的方法，但分两步执行可能在中途失败后留下没有约束的表。这里通过 GORM 执行一条 `ALTER TABLE` 原子替换 CHECK。它仍属于 GORM 的能力，不需要另外引入迁移库或连接池。选择与限制见 [GORM 决策](../decisions/0004-gorm-database-access.md)。

## users 表与 API 的边界

`id` 是用户编号，`username` 是登录名，`password_hash` 将来保存密码哈希，`created_at` 记录 UTC 创建时间。字段和约束以 [数据设计](../database.md) 为准。

唯一索引处理并发注册同名用户：两个请求可能都先查到“不存在”，最终仍需由数据库阻止第二次插入。用户名 CHECK 是另一条边界；版本 2 修正了旧正则允许末尾换行的问题。若已有非法用户名，迁移会失败，需要单独决定如何修复数据，不能自动改名或删用户。

API 的流程是 `Load → Open → CheckSchema → 启动 HTTP`，不调用迁移或 AutoMigrate。这样重启 API 不会顺便改表。`CheckSchema` 核对连续版本和所需列，users 查询使用 LIMIT 0，不读取账号记录；它不是对全部索引、类型和约束的完整审计。

## 怎么运行

以下从仓库根目录开始。本工作区已有 `.env` 和版本 2；迁移命令会跳过已成功步骤。

新工作区先复制 `backend/configs/.env.example` 为根目录 `.env`，权限设为 0600，在本地填写两个不同的随机密码。该文件被 Git 忽略，不能提交。Go 不自动读取它，需由 shell 导出环境变量；Compose 显式指定 `--env-file .env`。

本工作区的 `.env` 将 MySQL 配置为 `127.0.0.1:13306`，避开另一个占用 3306 的实例。容器内端口仍是 3306；新环境按自身端口情况配置。

```bash
docker compose --env-file .env -f deployments/docker/compose.yaml config --quiet
docker compose --env-file .env -f deployments/docker/compose.yaml up -d --wait mysql
set -a
source .env
set +a
cd backend
go run ./cmd/migrate && go run ./cmd/api
```

`&&` 表示迁移命令成功后才启动 API。也可以分别执行：先运行 `go run ./cmd/migrate`，读完结果并确认退出成功，再运行 `go run ./cmd/api`。迁移只需要四个 MySQL 环境变量，当前使用有应用库权限的应用账号；生产账号和升级窗口需要另行设计。

在另一个终端访问 `http://127.0.0.1:8080/healthz` 和 `/readyz`。当前 API 还需配置 JWT 密钥，前端注册登录用法见 README 和第五步。

停止数据库时，在仓库根目录执行 `docker compose --env-file .env -f deployments/docker/compose.yaml stop mysql`，下次启动会复用数据卷。不要用删除数据卷处理迁移失败；已有卷时，仅改 `.env` 也不会修改数据库里的账号密码。

## 出错时如何兜底

| 情况 | 行为与恢复条件 |
| --- | --- |
| 密码缺失、地址无效或无法连接 | 退出 1；修正配置或恢复数据库后重新运行 |
| 另一个迁移正在执行 | 退出 1；等待该命令结束，不能强行绕过锁 |
| 某版本的结构操作失败 | 停止，不登记该版本；检查实际结构与数据，解决原因后恢复 |
| DDL 完成，但成功记录写入失败 | 报错停止；核对结构并受控修复记录，不能假定表已经回滚 |
| 成功版本断号、超出程序支持范围，或所需列缺失 | 拒绝继续；核对迁移历史和所运行的代码版本，不强写版本号绕过 |
| API 运行时数据库故障 | readyz 返回 503，healthz 仍可返回 200；数据库恢复且检查通过后恢复就绪 |

MySQL DDL 会隐式提交，所以“失败停止”不等于“自动撤销之前所有修改”。命令不会自动重试、清库或修复未知结构；具体恢复约定见数据设计。进程中断或网络超时也需要确认已经发出的 DDL 是否生效。

## 验证与下一步

检查采用临时数据库与真实进程，覆盖新建、旧版本升级、重复执行、并发迁移锁、失败停止和数据保留；没有给项目增加测试框架或测试文件。实际结果、检查命令及范围见 [项目状态](../status.md)，对应 BASE-04、ENV-03 的用户表部分和 ENV-04。

先沿文件顺序理解这次数据库调整，再到第五步追踪注册请求如何经过 Handler、Service、Repository，以及密码哈希与用户保存。
