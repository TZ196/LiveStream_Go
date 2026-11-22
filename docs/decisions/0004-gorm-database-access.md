# MySQL 访问统一使用 GORM

日期：2026 年 11 月 20 日。状态：用户明确选定，应用于 P1b 及后续数据库访问。

## 场景与决定

后续注册、登录、房间管理需要常规查询和写入，Worker 还需要明确的持久化事务。用户要求使用 GORM，因此基础设施层提供 GORM 实例，业务查询和事务由 Repository 使用 GORM 实现。Service 定义业务规则，业务 Handler 负责 HTTP 输入输出，不直接操作数据库。

固定 GORM v1.31.2、gorm.io/driver/mysql v1.6.0，底层 MySQL 驱动仍为 v1.10.2。最初范围为数据库初始化、独立版本迁移、结构检查与生命周期；P1b 认证实现已建立用户 Repository，负责注册写入及登录、当前用户查询，见 [第五步学习记录](../learning/05-user-authentication.md)。

## 取舍与边界

直接使用 database/sql 可以明确控制 SQL 和逐列映射，但常规增删改查需要更多重复代码。GORM 提供查询构建和结构体映射，代价是需要理解模型映射、更新字段、事务等框架行为。两种方式的性能尚未对比，不把框架选择写成性能提升。

用户进一步确认由 GORM 创建连接池：向 MySQL 适配器传入 DSNConfig，不再由应用调用 sql.OpenDB 再传入已有池。通过 DB() 取得同一个底层池，原来的容量、UTC、超时和关闭规则继续适用。禁用初始化自动 Ping，显式使用 PingContext；固定 MySQL 8.4 后跳过自动版本探测，确保启动网络检查受调用方 context 控制。结构检查使用 WithContext 创建独立查询，不把请求 context 或查询条件保存在共享实例上。

迁移改为独立 `cmd/migrate` 命令，通过 GORM 的 Migrator 和 Exec 执行固定编号步骤。记录表兼容已有版本 1、2：按升序执行未成功的版本，每步完成后写入成功记录，任何错误立即停止。执行期间在同一连接持有 MySQL 命名锁，避免并发迁移；API/Worker 不调用 AutoMigrate。当前工作区只保留 Go 迁移步骤，已有成功记录仍有效，不维护第二套日常入口。用户数据不需要转换，模型不引入默认软删除字段。

版本 1 用固定模型及 Migrator.CreateTable 建表，避免未来业务模型变化悄悄改变历史迁移。版本 2 用 GORM.Exec 执行一条 ALTER 原子替换 CHECK；分别调用 DropConstraint、CreateConstraint 会在后一步失败时留下缺少约束的表。执行原始 DDL 仍通过 GORM，不引入另一套连接或迁移工具。[GORM 迁移接口](https://gorm.io/docs/migration.html)

GORM 不提供这里所需的版本成功账本与失败恢复流程，因此保留一个顺序调度函数。MySQL DDL 隐式提交，不能靠包裹 Transaction 实现整轮回滚；若 DDL 成功但登记失败，命令报错退出，核对结构后人工修复，不自动删表、补版本或跳过错误。完整执行与恢复约定见 [数据设计](../database.md)。数据库唯一约束、业务事务边界和消费幂等仍须显式设计。

当前关闭 GORM 自动 SQL 日志，错误由调用方处理，避免业务参数自动进入日志。复杂 SQL 可以使用 GORM 的参数化 Raw/Exec，仍封装在数据访问层，不另建一套绕过 GORM 的业务读写路径。

## 验证与恢复

复用 BASE-04、ENV-03 检查连接、探针、退出与数据保留，ENV-04 覆盖空库迁移、旧版本兼容、重复执行、并发锁与失败停止。使用真实隔离数据库验证，检查范围和结果统一登记在 [项目状态](../status.md)。本次不改变最终结构版本 2；已有成功版本跳过，API 不自动建表或清库恢复。

实现与讲解见 [第四步学习记录](../learning/04-mysql-and-migration.md)，连接参数见 [部署设计](../deployment.md)，GORM 连接池管理见 [官方文档](https://gorm.io/docs/connecting_to_the_database.html)。
