# 变更记录

本文件记录 sign-ql 脚本集合中对使用方有影响的变更（行为、接口、依赖、环境、构建/部署、目录结构）。
最新条目在最上方，未发布的改动归入 `## [未发布]`。

## [未发布]

### 修复

- 3dmgame.py：修复 3DM 论坛签到脚本报错 `3dmgame论坛用户信息获取失败：list index out of range`。
  - 原因：站点对未携带（或携带失效）`yxd_token` cookie 的请求返回 156 字节的 JS 引导页（写入 `yxd_token` 后跳回原地址）。脚本不执行 JS，直接解析该页导致所有 xpath 取空，`user_info()` 抛 `IndexError`，而 `check_task()`、`check_task_doing()` 则误报“没有可接任务 / 没有进行中的任务”，整轮执行为空跑。
  - 变更：新增统一请求入口 `ThreeDMGame.request()`，自动解析引导页中的 `yxd_token` 写回 Cookie 请求头并重试（最多 3 次，间隔 1 秒）；响应正文同时出现「您需要先登录」与 `id="messagetext"` 提示框（Discuz 掉登录态提示页）时抛出 `CookieInvalid`，由调用方给出明确提示。原有 `self.session.get/post` 调用点全部改为走该入口；主流程按账号隔离异常，单个账号失败不再中断后续账号。
  - 影响面：`3dmgame.py` 全部请求（积分页、任务页、领奖、回帖）；通知内容中“用户信息获取失败”会附带具体原因。
  - 迁移：无需迁移，`bbs3dmck` 变量格式不变；cookie 真正失效时仍需重新抓取。
