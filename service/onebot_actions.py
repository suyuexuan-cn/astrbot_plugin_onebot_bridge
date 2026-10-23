"""OneBot v11 动作（action）统一分发。

优化前同一套逻辑在 WebSocket 服务器、HTTP 服务器、HTTP SSE 服务器里各写了一遍，
WebSocket 客户端又维护了一份更大的分发表，四份实现很容易改漏。
本模块把「发送消息 / 自身信息 / 版本信息 / 消息 API / 群组 API / 好友 API /
其他可透传动作」收敛为一份实现，并保持原有响应结构（status / retcode / data / echo）。
"""

import re
from typing import Any

from astrbot.api import logger

SEND_ACTIONS = frozenset({"send_msg", "send_group_msg", "send_private_msg"})
MESSAGE_ACTIONS = frozenset(
    {
        "get_msg",
        "delete_msg",
        "recall_message",
        "get_forward_msg",
        "send_group_forward_msg",
        "send_private_forward_msg",
    }
)
GROUP_ACTIONS = frozenset(
    {"get_group_list", "get_group_member_info", "get_group_member_list", "get_group_info"}
)
SELF_INFO_ACTIONS = frozenset({"get_self_info", "get_login_info"})

#: 直接透传给平台客户端的动作（群管理、请求处理等）
CLIENT_ACTIONS = frozenset(
    {
        "set_group_ban",
        "set_group_whole_ban",
        "set_group_kick",
        "set_group_card",
        "set_group_admin",
        "set_group_name",
        "set_group_leave",
        "set_group_special_title",
        "set_essence_msg",
        "delete_essence_msg",
        "send_group_sign",
        "send_like",
        "delete_friend",
        "set_friend_add_request",
        "set_group_add_request",
        "get_stranger_info",
        "get_group_honor_info",
        "get_group_msg_history",
        "get_friend_msg_history",
        "get_essence_msg_list",
        "get_image",
        "get_record",
        "get_cookies",
        "get_csrf_token",
    }
)

#: 平台不支持、但保持协议可用性的动作（成功返回，实际不做事）
NOOP_ACTIONS = frozenset(
    {
        "set_qq_profile",
        "set_qq_avatar",
        "delete_group_file",
    }
)

#: 占位数据，避免不支持的动作导致副 Bot 直接报错
PLACEHOLDER_STUBS = {
    "get_group_info": lambda p, bot_id: {
        "group_id": p.get("group_id"),
        "group_name": "未知群聊",
        "member_count": 0,
        "max_member_count": 0,
    },
    "get_stranger_info": lambda p, bot_id: {
        "user_id": p.get("user_id"),
        "nickname": "未知用户",
    },
    "get_group_honor_info": lambda p, bot_id: {
        "group_id": p.get("group_id"),
        "current_talkative": None,
        "talkative_list": [],
        "performer_list": [],
        "legend_list": [],
        "strong_newbie_list": [],
        "emotion_list": [],
    },
    "get_essence_msg_list": lambda p, bot_id: {"messages": []},
    "get_friend_msg_history": lambda p, bot_id: {"messages": []},
    "get_group_msg_history": lambda p, bot_id: {"messages": []},
    "download_file": lambda p, bot_id: {"file": p.get("url", "")},
    "upload_private_file": lambda p, bot_id: {"file_id": "0"},
    "upload_group_file": lambda p, bot_id: {"file_id": "0"},
    "create_group_file_folder": lambda p, bot_id: {"folder_id": "0"},
    "get_group_file_system_info": lambda p, bot_id: {
        "total_space": 10737418240,
        "used_space": 0,
    },
    "get_group_files_by_folder": lambda p, bot_id: {"files": [], "folders": []},
    "get_group_root_files": lambda p, bot_id: {"files": [], "folders": []},
    "get_group_file_url": lambda p, bot_id: {"url": ""},
    "get_record": lambda p, bot_id: {"url": p.get("file", "")},
    "get_image": lambda p, bot_id: {"url": p.get("file", "")},
    "get_cookies": lambda p, bot_id: {"cookies": ""},
    "get_csrf_token": lambda p, bot_id: {"token": 123456789},
    "get_guild_list": lambda p, bot_id: [],
    "get_online_clients": lambda p, bot_id: {
        "clients": [
            {
                "app_id": "QQ",
                "device_name": "Windows",
                "device_kind": "windows",
                "online_push_enabled": True,
                "push_strategy": 0,
                "client_id": "1",
                "app_name": "QQ",
                "app_version": "9.9.9",
                "os_version": "Windows 10",
                "guid": "1234567890",
            }
        ]
    },
    "get_guild_service_profile": lambda p, bot_id: {
        "guild_service_profile": {
            "user_id": bot_id,
            "nickname": "AstrBot",
            "tiny_id": str(bot_id),
            "avatar_url": f"https://q.qlogo.cn/g?b=qq&s=0&nk={bot_id}",
            "guild_count": 0,
            "joined_guild_count": 0,
        }
    },
    "_set_model_show": lambda p, bot_id: {
        "model": p.get("model", ""),
        "model_show": p.get("model_show", ""),
    },
    "get_guild_meta_by_guest": lambda p, bot_id: {
        "guild_id": p.get("guild_id"),
        "guild_name": "未知频道",
    },
    "get_guild_channel_list": lambda p, bot_id: [],
    "get_guild_member_list": lambda p, bot_id: {
        "members": [],
        "finished": True,
        "next_token": "",
    },
    "get_guild_member_profile": lambda p, bot_id: {
        "tiny_id": str(p.get("user_id", "")),
        "nickname": "未知用户",
    },
}

_CQ_ESCAPE = {"&": "&amp;", "[": "&#91;", "]": "&#93;"}
_CQ_UNESCAPE = (("&#44;", ","), ("&#91;", "["), ("&#93;", "]"), ("&amp;", "&"))
_CQ_PATTERN = re.compile(r"\[CQ:([A-Za-z0-9_.\-]+)((?:,[^\[\]]*)?)\]")


def _escape(value: Any, escape_comma: bool = False) -> str:
    text = str(value)
    for raw, escaped in _CQ_ESCAPE.items():
        text = text.replace(raw, escaped)
    if escape_comma:
        text = text.replace(",", "&#44;")
    return text


def _unescape(text: str) -> str:
    for escaped, raw in _CQ_UNESCAPE:
        text = text.replace(escaped, raw)
    return text


def _parse_cq_params(raw: str) -> dict[str, str]:
    params: dict[str, str] = {}
    for pair in raw.split(","):
        if not pair:
            continue
        key, _, value = pair.partition("=")
        if key:
            params[key.strip()] = _unescape(value)
    return params


def message_to_cq_string(message: Any) -> str:
    """把 OneBot 数组格式消息转换为 CQ 码字符串。

    用于 ``message_post_format = "string"`` 的客户端/服务端配置，
    优化前该配置项被忽略，消息始终以数组格式发送。
    """
    if isinstance(message, str):
        return message
    if not isinstance(message, (list, tuple)):
        return "" if message is None else str(message)

    parts: list[str] = []
    for segment in message:
        if not isinstance(segment, dict):
            parts.append(_escape(segment))
            continue

        seg_type = segment.get("type")
        seg_data = segment.get("data") or {}
        if not isinstance(seg_data, dict):
            seg_data = {}

        if seg_type == "text":
            parts.append(_escape(seg_data.get("text", "")))
            continue

        if seg_type in ("image", "record", "video"):
            # 优先 file，其次 url，避免同时带两个字段导致部分实现解析异常
            file = seg_data.get("file") or seg_data.get("url") or ""
            if file:
                parts.append(f"[CQ:{seg_type},file={_escape(file, True)}]")
            continue

        if seg_type == "at":
            qq = seg_data.get("qq", "")
            parts.append(f"[CQ:at,qq={_escape(qq, True)}]")
            continue

        if seg_type == "reply":
            seg_id = seg_data.get("id", "")
            parts.append(f"[CQ:reply,id={_escape(seg_id, True)}]")
            continue

        if not seg_type:
            continue

        params = [
            f"{key}={_escape(value, True)}"
            for key, value in seg_data.items()
            if value not in (None, "")
        ]
        parts.append(f"[CQ:{seg_type}{',' if params else ''}{','.join(params)}]")

    return "".join(parts)


def cq_string_to_message_array(message: Any) -> list[dict[str, Any]]:
    """把 CQ 码字符串解析成 OneBot 数组格式消息。

    优化前 ``send_msg`` 收到 ``"...[CQ:image,file=xxx]"`` 这类字符串时会被
    整段当作纯文本发出去，调用方看到的是"发送成功"但对方收到的是原始码。
    """
    if not isinstance(message, str):
        return message if isinstance(message, list) else []

    segments: list[dict[str, Any]] = []
    cursor = 0

    for match in _CQ_PATTERN.finditer(message):
        if match.start() > cursor:
            text = _unescape(message[cursor : match.start()])
            if text:
                segments.append({"type": "text", "data": {"text": text}})

        seg_type = match.group(1)
        seg_data = _parse_cq_params(match.group(2).lstrip(","))
        segments.append({"type": seg_type, "data": seg_data})
        cursor = match.end()

    if cursor < len(message):
        text = _unescape(message[cursor:])
        if text:
            segments.append({"type": "text", "data": {"text": text}})

    if not segments:
        segments.append({"type": "text", "data": {"text": message}})

    return segments


def should_skip_self_message(config: Any, message: Any, bot_id: Any) -> bool:
    """判断是否应跳过"自己发出的消息"。

    对应连接配置项 ``report_self_message``：关闭时不上报 self_id 等于机器人
    自身账号的消息，避免消息被回灌造成循环。
    """
    if not isinstance(message, dict):
        return False
    if getattr(config, "report_self_message", False):
        return False
    if message.get("post_type") != "message":
        return False

    self_id = str(bot_id or "")
    if not self_id or self_id == "0":
        return False

    return str(message.get("user_id", "")) == self_id


class OneBotActionDispatcher:
    """把 OneBot v11 动作分发到 AstrBot 的消息/群组/好友 API。

    ``dispatch`` 始终返回完整的 OneBot 响应结构，不再返回 ``None``；
    调用方不需要再各自拼一份"不支持的动作"兜底。
    """

    def __init__(self, bot_id: str, send_message_callback=None):
        self.bot_id = bot_id
        self.send_message_callback = send_message_callback

        self.message_api = None
        self.group_api = None
        self.friend_api = None
        self.raw_api = None

    def set_api_handlers(self, message_api, group_api, friend_api, raw_api=None):
        self.message_api = message_api
        self.group_api = group_api
        self.friend_api = friend_api
        self.raw_api = raw_api

    # ------------------------------------------------------------------ 响应构造
    @staticmethod
    def _ok(data: Any = None, echo: Any = None) -> dict[str, Any]:
        return {
            "status": "ok",
            "retcode": 0,
            "data": {} if data is None else data,
            "echo": echo,
        }

    @staticmethod
    def _fail(message: str, echo: Any = None, retcode: int = 1) -> dict[str, Any]:
        return {
            "status": "failed",
            "retcode": retcode,
            "data": {"error": message},
            "message": message,
            "wording": message,
            "echo": echo,
        }

    # ------------------------------------------------------------------ 分发入口
    async def dispatch(
        self, action: str | None, params: dict[str, Any] | None = None, echo: Any = None
    ) -> dict[str, Any]:
        params = params or {}

        if not action:
            return self._fail("缺少action参数", echo, 1400)

        try:
            if action in SEND_ACTIONS:
                return await self._handle_send(action, params, echo)
            if action in SELF_INFO_ACTIONS:
                return self._ok(
                    {
                        "user_id": self.bot_id,
                        "nickname": "AstrBot",
                        "sex": "unknown",
                        "age": 0,
                        "level": 0,
                        "sign": "",
                        "birthday": {"year": 0, "month": 0, "day": 0},
                        "friend_group_id": 0,
                    },
                    echo,
                )
            if action == "get_version_info":
                return self._ok(
                    {
                        "app_name": "AstrBot",
                        "app_version": "1.0.0",
                        "protocol_version": "v11",
                        "coolq_edition": "eridanus",
                        "coolq_directory": "",
                        "plugin_version": "1.0.0",
                        "plugin_build_number": "1",
                        "plugin_build_configuration": "release",
                    },
                    echo,
                )
            if action == "get_status":
                return self._ok({"online": True, "good": True, "stat": {}}, echo)
            if action == "can_send_image":
                return self._ok({"yes": True}, echo)
            if action == "can_send_record":
                return self._ok({"yes": True}, echo)

            if action in MESSAGE_ACTIONS:
                normalized = "delete_msg" if action == "recall_message" else action
                return await self._forward_to_api(self.message_api, normalized, params, echo)
            if action in GROUP_ACTIONS:
                return await self._forward_to_api(self.group_api, action, params, echo)
            if action == "get_friend_list":
                return await self._forward_to_api(self.friend_api, action, params, echo)
            if action in CLIENT_ACTIONS:
                return await self._call_client(action, params, echo)
            if action in NOOP_ACTIONS:
                return self._ok({"success": True}, echo)
        except Exception as exc:  # 兜底，避免单个动作异常打断连接
            logger.error(f"[Bot通信] 处理OneBot动作 {action} 失败: {exc}")
            return self._fail(str(exc), echo)

        # 既没有处理器也没有占位数据的动作
        if action in PLACEHOLDER_STUBS:
            return self._ok(PLACEHOLDER_STUBS[action](params, self.bot_id), echo)

        logger.debug(f"[Bot通信] 不支持的OneBot v11动作: {action}")
        return self._fail(f"不支持的动作: {action}", echo, 1404)

    # ------------------------------------------------------------------ 内部实现
    async def _forward_to_api(
        self, handler, action: str, params: dict[str, Any], echo: Any
    ) -> dict[str, Any]:
        if handler is None:
            stub = PLACEHOLDER_STUBS.get(action)
            if stub is not None:
                return self._ok(stub(params, self.bot_id), echo)
            return self._fail("API处理器未初始化", echo)
        response = await handler.handle_request(action, params)
        if response.get("status") != "ok":
            # 真实接口调用失败时退回占位数据，避免调用方直接拿到错误
            stub = PLACEHOLDER_STUBS.get(action)
            if stub is not None:
                return self._ok(stub(params, self.bot_id), echo)
        response["echo"] = echo
        if "retcode" not in response:
            response["retcode"] = 0 if response.get("status") == "ok" else 1
        return response

    async def _call_client(
        self, action: str, params: dict[str, Any], echo: Any
    ) -> dict[str, Any]:
        """优先用真实平台客户端执行，失败/未实现时退回占位数据。"""
        if self.raw_api is not None:
            try:
                response = await self.raw_api.handle_request(action, params)
                if response.get("status") == "ok":
                    response["echo"] = echo
                    response.setdefault("retcode", 0)
                    return response
            except Exception as e:
                logger.debug(f"[Bot通信] 客户端执行动作 {action} 失败: {e}")

        stub = PLACEHOLDER_STUBS.get(action)
        if stub is not None:
            return self._ok(stub(params, self.bot_id), echo)

        if action in NOOP_ACTIONS:
            return self._ok({"success": True}, echo)

        return self._fail(f"平台不支持的动作: {action}", echo, 1404)

    async def _handle_send(
        self, action: str, params: dict[str, Any], echo: Any
    ) -> dict[str, Any]:
        content = params.get("message")
        if content is None:
            return self._fail("缺少message参数", echo, 1400)

        group_id = params.get("group_id")
        user_id = params.get("user_id")
        if group_id is not None:
            message_type, target_id = "GroupMessage", str(group_id)
        elif user_id is not None:
            message_type, target_id = "FriendMessage", str(user_id)
        else:
            return self._fail("缺少group_id或user_id参数", echo, 1400)

        if not self.send_message_callback:
            return self._fail("发送消息回调未初始化", echo)

        # CQ 码字符串需要解析成消息段，否则会被当作纯文本整段发出去
        if isinstance(content, str) and not params.get("auto_escape"):
            content = cq_string_to_message_array(content)
        elif isinstance(content, dict):
            # 少数实现会把 message 写成单个消息段
            content = [content]

        message_id = await self.send_message_callback(message_type, target_id, content)
        if message_id is None:
            # 优化前：发送失败也会返回 message_id="0"，调用方误判为成功
            return self._fail("消息发送失败", echo)

        logger.debug(f"[Bot通信] OneBot动作 {action} 已发送到 {target_id}")
        return self._ok({"message_id": str(message_id)}, echo)
