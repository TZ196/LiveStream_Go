# 第三步：前端界面与健康接口联调

本篇记录 P1a、BASE-03 的前端骨架阶段：当时只展示账号界面，按钮禁用。当前 P1b 已接通真实注册登录，现行账号使用和调用过程见 [第五步学习记录](05-user-authentication.md)；下文按阶段保留健康请求与页面分工的教学说明。

## 运行与检查

当前 P1b 后端需先准备 MySQL 并导出环境配置，按 [仓库 README](../../README.md) 启动。另一个终端进入 frontend，首次运行 `npm ci`，再运行 `npm run dev`。浏览器打开 http://127.0.0.1:5173 。默认开发代理连接 127.0.0.1:8080，改变地址的方法见 [配置与部署](../deployment.md#当前前端开发)。

在 frontend 执行 `npm run type-check` 和 `npm run build`。前者检查类型，后者生成 dist 静态产物；两者用途不同。具体版本和实际结果统一记录在 [项目状态](../status.md)，此文档不重复维护完成清单。

## 文件分工与阅读顺序

| 文件 | 当前职责 |
| --- | --- |
| [index.html](../../frontend/index.html) | 提供页面容器和前端脚本入口 |
| [src/main.ts](../../frontend/src/main.ts) | 创建 Vue 应用，挂到 HTML 的 app 容器 |
| [src/App.vue](../../frontend/src/App.vue) | 组织页面和仅开发时显示的连接检查 |
| [src/pages/AuthPage.vue](../../frontend/src/pages/AuthPage.vue) | 切换登录、注册表单；本步不提交表单 |
| [src/components/HealthStatus.vue](../../frontend/src/components/HealthStatus.vue) | 显示检查中、成功、失败，接收手动重试操作 |
| [src/api/health.ts](../../frontend/src/api/health.ts) | 请求健康接口，核对返回的 status 是否为 ok |
| [src/api/http.ts](../../frontend/src/api/http.ts) | 使用 fetch 发起 GET，统一处理超时、取消、HTTP 错误及 JSON 解析 |
| [vite.config.ts](../../frontend/vite.config.ts) | 配置 Vue 编译插件、开发监听地址及代理 |
| [src/style.css](../../frontend/src/style.css) | 页面样式、表单样式与窄屏布局 |

package.json 负责命令和直接依赖版本，package-lock.json 锁定完整依赖树；tsconfig.json 负责类型检查规则。没有加入路由、全局状态库、UI 框架或测试框架，等出现真实需求再考虑。源码中的关键注释使用中文。

## 一次请求怎样走

页面显示后，Vue 调用 `onMounted` 注册的 `checkConnection`：

1. HealthStatus 将状态改为检查中，并禁止重复发起检查。
2. health.ts 调用 http.ts，请求相对路径 `/healthz`。
3. 浏览器访问前端开发地址的 `/healthz`，Vite 转发到配置的 Go 后端地址。
4. Hertz 返回 HTTP 200 和健康 JSON；http.ts 检查 HTTP 状态并解析 JSON。
5. health.ts 核对业务字段，组件再显示“后端可以响应”。任一步失败都显示错误提示。

页面只负责交互状态，HTTP 文件负责传输，健康文件负责该接口的响应约定。没有直接在两个组件中分别写 fetch。HTTP 200 不足以证明探针正确，例如误配代理返回 HTML 或其他 JSON 时必须报错。

使用开发代理后，浏览器面对的是同一个站点，不需要本步给后端增加跨域放行。生产代理不会随静态文件一起部署，后续按设计由 API 同源托管静态产物。Vite 行为依据其 [开发代理文档](https://vite.dev/config/server-options.html#server-proxy) 和 [环境变量文档](https://vite.dev/guide/env-and-mode.html)；Vue 应用组织依据 [官方入门文档](https://vuejs.org/guide/quick-start.html)。

## 边界与失败处理

- 同时最多一个健康请求；请求超过 5 秒取消，用户可手动重试，没有自动轮询或无限重试。5 秒是等待预算，不是性能实测成绩。
- 浏览器的 AbortSignal 合并请求超时和组件卸载信号；组件移除时调用 AbortController.abort，避免页面离开后继续等待。
- 不把服务器原始错误内容直接显示给用户。HTTP 错误、非法 JSON 和健康字段不符分别作为失败。结果表示最近一次检查；停止后端后，再次检查会更新为失败，不宣称自动监测服务变化。
- 登录和注册提交按钮禁用；表单还阻止默认提交，防止回车触发原生页面跳转。输入校验、真实注册登录和令牌管理在 P1b 实现，约定以 [领域模型](../domain-model.md#用户及输入口径) 和 [HTTP 契约](../api-spec.md) 为准。
- 健康面板只在开发模式显示。健康成功不代表认证、MySQL 或房间业务就绪，不承诺音视频播放。

这里选择浏览器自带 fetch，当前依赖少、请求简单；等业务接口出现再按需要扩展请求参数与认证。使用本地表单切换能看清页面状态变化；P2 有直播间列表与详情导航时再评估路由。

## 下次讲解顺序

先从浏览器看到的两个区域定位 App.vue，再看 AuthPage 中 `ref` 如何控制界面；最后沿 HealthStatus → health.ts → http.ts → Vite → Hertz 追踪一个真实请求。讲清这些后，再进入 P1b 的 MySQL 与用户认证小步骤。

本轮没有新增持久化测试文件或测试框架。检查由助手执行，用户可先阅读代码和页面；提交、推送与 PR 沿用先讲解、用户审阅的教学交付约定。
