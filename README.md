# OneBot 桥接插件

主副 Bot 桥接插件，支持将消息转发到副 Bot 并支持 OneBot v11 协议。

## 应用项目
- **[AstrBot](https://docs.astrbot.app/)**

## 功能特性

- 支持多种网络连接方式（支持同时配置多个）
  - **WebSocket 客户端**：主动连接到副 Bot 的 WebSocket 服务端
  - **WebSocket 服务器**：监听端口接收副 Bot 的 WebSocket 连接
  - **HTTP 服务器**：HTTP 接口接收副 Bot 的消息推送
  - **HTTP SSE 服务器**：支持 SSE 推送的 HTTP 服务器
  - **HTTP 客户端**：主动连接副 Bot 的 HTTP 服务端
- 支持群消息和私聊消息转发
- 支持多种触发方式
  - 命令前缀触发（去除前缀转发剩余内容）
  - 转发前缀触发（完整转发消息）
  - 关键字触发（完整转发消息）
- 支持每个连接独立配置转发规则
  - 可为每个 WebSocket 客户端/服务器、HTTP 客户端单独配置转发规则
  - 支持针对不同副 Bot 设置不同的触发条件和转发策略
  - **按连接定向投递**：命中哪个连接的规则，就只推给那条连接，不会重复投递
- 完整支持 OneBot v11 事件与消息格式
  - `message`：各类消息事件
  - `notice`：群成员增减、禁言、撤回、戳一戳等通知事件
  - `request`：加好友、加群请求
  - `meta_event`：`lifecycle` 连接事件与 `heartbeat` 心跳（客户端与服务端都会发送）
- 支持多种消息类型转换
  - 文本、图片、@、表情、语音、视频、回复、文件
  - JSON、XML、位置、联系人、骰子、猜拳、戳一戳、音乐、合并转发等
- 支持心跳检测和自动重连机制（WebSocket 客户端）
- 支持 Token 鉴权
  - WebSocket 客户端：以 `Authorization: Bearer <token>` 请求头下发
  - WebSocket 服务器 / HTTP 服务器 / HTTP SSE 服务器：支持 `Authorization: Bearer`、`access_token` 请求头与 `?access_token=` 查询参数
- 支持 `message_post_format` 选择 `array`（消息段数组）或 `string`（CQ 码）格式
  - 发送方向支持 CQ 码解析：`send_msg` 收到 `"...[CQ:image,file=x.png]"` 会自动解析成消息段
  - 可通过 `auto_escape` 参数关闭解析，按纯文本发送
- 兼容 `websockets` 12 ~ 16 版本（自动适配 `extra_headers` / `additional_headers` 及请求头读取方式）
- 支持多客户端配置
- 支持 OneBot API 调用
  - 消息 API：获取消息、撤回消息、获取合并转发消息、发送合并转发消息
  - 群组 API：获取群列表、获取群信息、获取群成员信息、获取群成员列表
  - 好友 API：获取好友列表
  - 群管理 / 请求处理：禁言、踢人、设置名片、处理加好友与加群请求等，直接透传给底层 OneBot 客户端
  - 底层客户端不可用时自动退回兼容占位数据，保证副 Bot 不因接口报错中断

## 安装

查 AstrBot 文档：[https://docs.astrobot.cn/](https://docs.astrobot.cn/)

## 配置说明

### 网络配置

插件支持 5 种网络连接方式，可在配置中添加多个：

#### WebSocket 客户端

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| name | 客户端名称 | ws_client |
| enable | 是否启用 | true |
| debug | 调试模式 | false |
| uri | WebSocket 服务端 URI 地址 | ws://localhost:3001/OneBotv11 |
| token | 访问令牌 | (空) |
| report_self_message | 是否上报机器人自己发送的消息 | false |
| enable_force_push_event | 是否推送 notice/request 事件 | true |
| message_post_format | 消息格式 (array/string) | array |
| heart_interval | 心跳间隔 (毫秒) | 30000 |
| ping_interval | Ping 间隔 (秒) | 10 |
| ping_timeout | Ping 超时 (秒) | 20 |
| connect_timeout | 连接超时 (秒) | 10 |
| close_timeout | 关闭超时 (秒) | 5 |
| max_reconnect_attempts | 最大重连次数 (0 为无限) | 0 |
| enable_group_forward | 启用群消息转发 | true |
| enable_private_forward | 启用私聊消息转发 | false |
| command_prefix | 命令前缀 (去除前缀转发) | (空) |
| forward_fprefix | 转发前缀 (完整转发) | [] |
| forward_keyword | 触发转发的关键字 | [] |

#### WebSocket 服务器

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| name | 服务器名称 | ws_server |
| enable | 是否启用 | true |
| debug | 调试模式 | false |
| host | 监听地址 | 127.0.0.1 |
| port | 监听端口 | 3001 |
| path | WebSocket 路径 | /OneBotv11 |
| token | 访问令牌 | (空) |
| report_self_message | 是否上报自身消息 | false |
| enable_force_push_event | 是否推送 notice/request 事件 | true |
| message_post_format | 消息格式 (array/string) | array |
| heart_interval | 心跳间隔 (毫秒) | 30000 |
| enable_group_forward | 启用群消息转发 | true |
| enable_private_forward | 启用私聊消息转发 | false |
| command_prefix | 命令前缀 (去除前缀转发) | (空) |
| forward_fprefix | 转发前缀 (完整转发) | [] |
| forward_keyword | 触发转发的关键字 | [] |

#### HTTP 服务器

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| name | 服务器名称 | http_server |
| enable | 是否启用 | true |
| debug | 调试模式 | false |
| host | 监听地址 | 127.0.0.1 |
| port | 监听端口 | 3000 |
| enable_cors | 是否启用 CORS 跨域 | true |
| enable_websocket | 是否启用同端口 WebSocket 端点 `/ws` | false |
| report_self_message | 是否上报自身消息 | false |
| enable_force_push_event | 是否推送 notice/request 事件 | true |
| message_post_format | 消息格式 (array/string) | array |
| token | 访问令牌 | (空) |

#### HTTP SSE 服务器

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| name | 服务器名称 | sse_server |
| enable | 是否启用 | true |
| debug | 调试模式 | false |
| host | 监听地址 | 127.0.0.1 |
| port | 监听端口 | 3002 |
| enable_cors | 是否启用 CORS 跨域 | true |
| report_self_message | 是否上报自身消息 | false |
| enable_force_push_event | 是否推送 notice/request 事件 | true |
| message_post_format | 消息格式 (array/string) | array |
| token | 访问令牌 | (空) |

#### HTTP 客户端

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| name | 客户端名称 | http_client |
| enable | 是否启用 | true |
| debug | 调试模式 | false |
| base_url | HTTP 服务端基础 URL | http://localhost:3000 |
| token | 访问令牌 | (空) |
| report_self_message | 是否上报自身消息 | false |
| enable_force_push_event | 是否推送 notice/request 事件 | true |
| message_post_format | 消息格式 (array/string) | array |
| timeout | 请求超时时间 (秒) | 30 |
| enable_group_forward | 启用群消息转发 | true |
| enable_private_forward | 启用私聊消息转发 | false |
| command_prefix | 命令前缀 (去除前缀转发) | (空) |
| forward_fprefix | 转发前缀 (完整转发) | [] |
| forward_keyword | 触发转发的关键字 | [] |

### 调试配置

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| debug | 调试模式 | false |

## 使用方法

### 触发方式说明

插件支持三种触发转发的方式：

1. **命令前缀触发**：配置 `command_prefix` 后，消息以该前缀开头时会去除前缀并转发剩余内容
   ```
   /yz 你好  ->  转发 "你好"
   ```

2. **转发前缀触发**：配置 `forward_fprefix` 后，消息以指定前缀开头时完整转发
   ```
   #你好  ->  转发 "#你好"
   ```

3. **关键字触发**：配置 `forward_keyword` 后，消息包含关键字时完整转发

### 多副 Bot 独立配置

插件支持为每个网络连接单独配置转发规则。当配置了多个副 Bot 时：

- 每个 WebSocket 客户端、WebSocket 服务器、HTTP 客户端都可以配置独立的：
  - `enable_group_forward` / `enable_private_forward`：是否转发群/私聊消息
  - `command_prefix`：命令前缀
  - `forward_fprefix`：转发前缀列表
  - `forward_keyword`：转发关键字列表

示例配置结构：
```json
{
  "websocket_clients": [
    {
      "__template_key": "WebSocket 客户端",
      "name": "bot1",
      "enable": true,
      "uri": "ws://localhost:3001/OneBotv11",
      "enable_group_forward": true,
      "command_prefix": "/cmd1"
    },
    {
      "__template_key": "HTTP 客户端",
      "name": "bot2",
      "enable": true,
      "base_url": "http://localhost:3000",
      "enable_group_forward": true,
      "forward_fprefix": ["#", "*"]
    }
  ]
}
```

### 帮助命令

本插件不注册对话命令，仅按配置的触发规则转发消息；转发规则见上方「触发方式说明」。

## 消息类型支持

插件支持以下消息类型的转换和转发：

- **基础消息**
  - 文本 (text)
  - 图片 (image)
  - @ (at)
  - 表情 (face)
  - 语音 (record)
  - 视频 (video)
  - 回复 (reply)
  - 文件 (file)

- **特殊消息**
  - JSON (json)
  - XML (xml)
  - Markdown (markdown)
  - 位置 (location)
  - 联系人 (contact)
  - 骰子 (dice)
  - 猜拳 (rps)
  - 戳一戳 (poke)
  - 音乐 (music)
  - 合并转发 (forward)
  - 转发节点 (node)

## OneBot API 支持

插件支持以下 OneBot v11 API 调用：

### 消息 API
- `get_msg` - 获取消息
- `delete_msg` - 撤回消息
- `get_forward_msg` - 获取合并转发消息
- `send_group_forward_msg` - 发送群合并转发消息
- `send_private_forward_msg` - 发送私聊合并转发消息

### 群组 API
- `get_group_list` - 获取群列表
- `get_group_info` - 获取群信息
- `get_group_member_info` - 获取群成员信息
- `get_group_member_list` - 获取群成员列表

### 好友 API
- `get_friend_list` - 获取好友列表

### 群管理与请求处理
以下动作会直接透传给底层 OneBot 客户端，客户端不可用时返回兼容占位数据：

- `set_group_ban` / `set_group_whole_ban` - 禁言
- `set_group_kick` - 踢出群成员
- `set_group_card` / `set_group_admin` - 设置名片 / 设置管理员
- `set_friend_add_request` / `set_group_add_request` - 处理加好友、加群请求
- `send_like` - 点赞
- `get_stranger_info` - 获取陌生人信息
- `get_status` / `can_send_image` / `can_send_record` - 状态与能力查询
- `get_image` / `get_record` - 获取图片、语音文件

## 注意事项

1. **插件启动条件**：必须至少配置一个有效的网络连接（WebSocket 客户端、HTTP 服务器、HTTP SSE 服务器、HTTP 客户端、WebSocket 服务器之一），否则插件不会启动
2. **转发私聊消息**：默认不转发私聊消息，如需转发请在对应连接配置中启用 `enable_private_forward`
3. **调试模式**：启用 `debug` 后会输出更多日志信息，便于排查问题
4. **多 Bot 互联**：可以同时配置多个网络连接方式，实现多 Bot 互联
5. **消息格式**：推荐使用 `array` 格式以获得完整的消息结构支持
6. **Token 验证**：如果配置了 token，请确保副 Bot 使用相同的 token 进行连接；WebSocket 客户端会以 `Authorization: Bearer <token>` 请求头下发，服务端三种传递方式（Bearer 请求头、`access_token` 请求头、URL 查询参数）都支持
7. **定向投递**：一条消息只会推给命中转发规则的连接；未配置任何触发条件（`command_prefix` / `forward_fprefix` / `forward_keyword`）的连接不会收到消息
8. **自身消息**：`report_self_message` 关闭时（默认），`self_id` 等于机器人自身账号的消息不会被推送，避免消息回灌形成循环
9. **路径校验**：WebSocket 服务器会校验 `path` 配置。客户端连接的具体路径与配置不一致时会被拒绝并记录日志；连接根路径 `/` 视为通配，保持向后兼容

## 版本信息

- **当前版本**：1.1.7
- **AstrBot 版本要求**：>=4.16,<5
- **作者**：苏月晅
- **项目地址**：[https://github.com/suyuexuan-cn/astrbot_plugin_onebot_bridge](https://github.com/suyuexuan-cn/astrbot_plugin_onebot_bridge)

## 作者

苏月晅  
[联系本作者](https://suyuexuan.cn/contact)

## 许可证

跟随 AstrBot 项目的许可证。
