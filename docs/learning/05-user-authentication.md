# 第五步：把注册、登录和当前用户串起来

日期：2026 年 11 月 20 日。本轮按用户要求，一起实现 P1b 的注册、登录、JWT 鉴权、当前用户查询和前端账号页面。MySQL 与四列 users 表沿用前一步；没有新增表、改写迁移，也没有提前创建房间或接入 Redis/Kafka。

## 先操作一遍

按仓库 README 启动 MySQL、API 和前端。已有工作区的 .env 已补充随机 JWT 签名密钥；新工作区按配置样例准备，不使用固定默认值。

1. 打开前端，选择注册，输入用户名与密码。
2. 成功后页面切回登录，保留规范化用户名，清除密码；使用原密码登录。
3. 后端验证密码后返回 JWT，页面随后携带该凭证请求当前用户接口。
4. 查询成功才显示账号页面，可以刷新账号信息和退出。
5. 刷新页面、退出或登录到期后，需要重新登录。退出不会删除数据库中的账号。

这条流程已经经过真实 MySQL 与浏览器验证，具体环境、失败案例与边界在 [项目状态](../status.md) 中记录。不要把账号流程完成理解为房间或弹幕已经可用。

## 后端每层负责什么

| 文件 | 职责 | 阅读重点 |
| --- | --- | --- |
| [cmd/api/main.go](../../backend/cmd/api/main.go) | 组装连接池、Repository、Service、JWT 组件和 HTTP 服务 | main 负责把依赖连接起来 |
| [httpserver/auth.go](../../backend/internal/httpserver/auth.go) | 注册、登录、当前用户 Handler 与 Bearer 认证中间件 | HTTP 输入、身份传递、响应及错误映射 |
| [user/service.go](../../backend/internal/user/service.go) | 用户名规范化、密码规则、注册与登录流程 | 业务步骤顺序，不接触 Hertz 请求对象 |
| [user/password.go](../../backend/internal/user/password.go) | 生成随机盐、Argon2id 编码、验证密码 | 密码只参与计算，哈希与盐入库 |
| [user/repository.go](../../backend/internal/user/repository.go) | 用户数据模型、GORM 创建与查询、重复键映射 | 真正访问 MySQL 的位置 |
| [auth/token.go](../../backend/internal/auth/token.go) | 签发 JWT，固定条件验签并返回身份 | 不访问数据库，不依赖 Hertz |
| [httpserver/auth_limit.go](../../backend/internal/httpserver/auth_limit.go) | 注册登录的 IP 固定窗口限制 | 互斥保护计数，窗口和容量有界 |
| [httpserver/response.go](../../backend/internal/httpserver/response.go) | 业务响应格式、trace_id 和请求超时 | JSON 契约及故障定位 |

`database` 继续负责连接、结构检查和资源关闭，`migrations` 继续负责准备表。用户注册后的读写现在落在 User Repository，HTTP Handler 通过 Service 使用它。

## 一次注册怎样走完

浏览器将 username、password 放在 JSON 请求体中，提交 POST /api/v1/auth/register。Handler 确认 Content-Type、JSON 格式和字段，随后调用 User Service。

Service 去除用户名首尾 ASCII 空格，检查合法字符，再转小写；密码按 Unicode 字符数和 UTF-8 字节数检查，保留空格。随后取得密码计算名额，生成独立随机盐和 Argon2id 编码，构造用户记录，调用 Repository.Create。

Repository 通过 GORM 写入 users。数据库分配自增 ID，成功后 Handler 只返回 id、username、created_at；ID 转成十进制字符串，时间按 UTC 毫秒格式输出。响应不会包含密码或密码哈希，注册也不自动签发令牌。

两个请求可能同时注册同一个用户名。我们直接依靠数据库唯一索引裁决，第二个写入得到重复键错误，Repository 将它映射为用户名冲突，Handler 返回 409。先查询“是否存在”不能替代这个唯一约束。

## 登录与访问身份

登录 Handler 调用 Service.Login。Service 查出用户的编码哈希，按里面的盐和受支持参数重新计算输入密码，再作恒定时间的哈希比较。不存在的账号使用预先准备的同参数哈希完成一次校验；无效账号和错误密码都返回同一种凭据错误，不承诺整个请求耗时绝对相同。

密码验证成功后，Handler 调用令牌组件签发 JWT。令牌表达用户身份和到期时间；签名密钥从环境配置读入，原始密码不写入 JWT。算法、签发者、接收者与时间条件固定，具体字段以 [HTTP 契约](../api-spec.md) 为准。[JWT 库校验选项](https://golang-jwt.github.io/jwt/usage/parse/)

页面接着调用 GET /api/v1/users/me，Authorization 请求头携带 Bearer。认证中间件验签后取得 user_id，当前用户 Handler 用该身份调用 Service，再由 Repository 查询账号。客户端传来的 user_id 不会决定查询对象；此接口拒绝查询参数和请求体。

## 为什么密码和令牌要分别处理

密码哈希用于验证用户输入的密码，JWT 用于之后每次请求携带身份。用户日常请求不需要反复发送原始密码。

Argon2id 增加离线猜测密码的计算成本；每条记录有独立随机盐，所以相同密码也会得到不同编码。参数沿用已经确定的设计，不把配置值说成实际抗攻击能力或系统容量。[OWASP 密码存储指南](https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html)、[Go Argon2 包](https://pkg.go.dev/golang.org/x/crypto/argon2)

JWT 不需要为当前阶段增加会话存储和 Redis，但 V1 也不提供单个令牌的即时撤销。退出登录只清空本页面内存；其他客户端持有的有效凭证不因此撤销。页面刷新重新登录是当前明确的取舍，后续若要持久登录，需要单独设计刷新和撤销流程。

## 保护边界与出错路径

| 场景 | 当前处理 | 恢复方式 |
| --- | --- | --- |
| 输入不合规 | 400，前端也做基础提示 | 修改输入 |
| 同名注册 | 409，数据库保留唯一记录 | 换用户名或使用原账号登录 |
| 密码错误、账号不存在 | 统一 401 凭据错误 | 重新输入 |
| 凭证无效或过期 | 受保护接口返回 401，页面清除会话 | 重新登录 |
| 注册/登录太频繁 | 429 和 Retry-After，页面倒计时禁用提交 | 等待后主动操作 |
| 密码计算名额已满 | 429，不创建无限等待队列 | 短暂等待后重试 |
| MySQL 暂时不可用 | 503，当前用户查询不伪造数据 | 恢复数据库后重新查询 |
| 注册提交后网络中断或超时 | 前端提示结果尚未确认，不自动重复发送 | 先尝试登录确认是否已创建 |

IP 窗口、密码并发、请求体与超时的数值统一在 HTTP 契约维护。IP 计数只信任 TCP 对端，因此通过 Vite 代理的本地页面共享一个来源；部署其他代理时需要明确可信代理范围，不能直接相信客户端传来的转发头。

密码计算库本身不接收 context；已经开始的运算会完成后释放名额。我们限制输入、固定受支持参数和并发数量，不通过额外 goroutine 假装取消运算。数据库操作则传递请求 context，避免故障期间一直等待。

## 前端的使用与边界

用户不必先学习前端内部结构。使用时只需要知道：表单发真实请求，登录成功后展示服务端返回的当前用户；提交期间禁止重复操作，卸载页面取消请求，429 按服务器要求等待，其他错误通过页面提示表达。

实现入口分别是 [AuthPage.vue](../../frontend/src/pages/AuthPage.vue)、[AccountPage.vue](../../frontend/src/pages/AccountPage.vue) 和 [App.vue](../../frontend/src/App.vue)。请求集中在 [api/http.ts](../../frontend/src/api/http.ts) 与 [api/auth.ts](../../frontend/src/api/auth.ts)。令牌只保存于 App 当前页面内存，不进入 localStorage、sessionStorage、Cookie 或 URL。

## 怎么讲这个项目里的认证

场景是用户注册账号，再通过登录访问属于自己的账号信息。Handler、Service、Repository 分离 HTTP、业务和持久化；用户名唯一性由数据库解决并发冲突，密码使用带独立盐的 Argon2id，后续请求用固定校验条件的 JWT 识别身份。密码计算和 IP 尝试有明确上限，数据库错误返回可解释的失败，网络超时不假定注册一定失败。

证据对应 AUTH-01 至 AUTH-04、SEC-01 和 ENV-03。已经验证真实链路、异常输入、JWT 边界、并发冲突和故障恢复；UI 的 401、429 和网络中断另有受控模拟，不能说成生产事故。没有进行容量压测，不用“最多 4 个密码计算”等保护参数代替吞吐成绩。

下一阶段 P2 将复用这套用户身份，接入创建、列表、详情和关闭直播间，再让前端展示真实房间。
