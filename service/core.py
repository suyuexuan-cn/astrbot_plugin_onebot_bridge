import asyncio
import time
from typing import Any

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent

from .http_client_manager import HTTPClientManager
from .http_server_manager import HTTPServerManager
from .sse_server_manager import SSEServerManager
from .websocket_manager import WebSocketManager
from .ws_server_manager import WSServerManager


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_dict(value: Any) -> dict[str, Any] | None:
    """把平台原始事件对象转成 dict。

    aiocqhttp 的 Event 是 dict 子类，但也存在包装成对象的情况，这里都兼容。
    优化前 notice / request 类事件根本无法转发，因为没人去读 raw_message。
    """
    if isinstance(value, dict):
        return dict(value)

    for attr in ("_data", "data"):
        inner = getattr(value, attr, None)
        if isinstance(inner, dict):
            return dict(inner)

    return None


def _component_media(component: Any) -> str:
    """取组件里的媒体地址，Image/Record/Video 的字段命名并不统一。"""
    for attr in ("url", "file", "path"):
        value = getattr(component, attr, None)
        if value:
            return str(value)
    return ""


#: AstrBot 组件类名与 OneBot 端命名的差异。
#: ``RPS`` 是全大写、``AtAll`` 是 ``At`` 的子类，按类名精确匹配会落空。
_COMPONENT_ALIASES = {
    "RPS": "Rps",
    "AtAll": "At",
}


class BotCommunicationCore:
    def __init__(self, config: dict[str, Any], bot_id: str, send_message_callback, platform=None):
        self.config = config
        self.bot_id = bot_id
        self.send_message_callback = send_message_callback
        self.platform = platform

        # OneBot 事件里的 self_id 必须是机器人账号本身，而不是 AstrBot 的平台实例 ID。
        # 初始化时拿不到账号时先沿用传入值，收到第一个事件后会自动校正。
        self.self_id = str(bot_id or "")

        self.ws_manager = WebSocketManager(config, bot_id, send_message_callback)
        self.http_server_manager = HTTPServerManager(config, bot_id, send_message_callback)
        self.sse_server_manager = SSEServerManager(config, bot_id, send_message_callback)
        self.http_client_manager = HTTPClientManager(config, bot_id, send_message_callback)
        self.ws_server_manager = WSServerManager(config, bot_id, send_message_callback)

        self.debug = config.get("debug", False)

        self._message_seq = 0

        self._init_api_handlers()
        self._check_duplicate_names()

    def _check_duplicate_names(self):
        """连接名称是定向投递的键，重名会导致同一条消息被投递多次。"""
        seen: dict[str, str] = {}
        for manager in self._managers():
            owner = type(manager).__name__
            for name in manager.connections:
                if name in seen:
                    logger.warning(
                        f"[Bot通信] 连接名称重复: {name}（{seen[name]} 与 {owner}），"
                        f"同一条事件会被重复推送，建议改名"
                    )
                seen[name] = owner

    def _apply_self_id(self, value: Any):
        """把真实机器人账号同步到所有管理器和分发器。

        优化前 self_id 用的是 PlatformMetadata.id（平台实例标识），
        导致转发事件的 self_id、get_login_info.user_id 都不是 QQ 号，
        report_self_message 的自身消息判断也永远不成立。
        """
        value = str(value or "").strip()
        if not value or value == self.self_id:
            return

        self.self_id = value
        self.bot_id = value
        for manager in self._managers():
            if hasattr(manager, "bot_id"):
                manager.bot_id = value
            dispatcher = getattr(manager, "dispatcher", None)
            if dispatcher is not None:
                dispatcher.bot_id = value
        logger.debug(f"[Bot通信] 已校正机器人账号 self_id: {value}")

    def _managers(self):
        return (
            self.ws_manager,
            self.http_server_manager,
            self.sse_server_manager,
            self.http_client_manager,
            self.ws_server_manager,
        )

    def connections(self) -> dict[str, Any]:
        """所有连接的配置，按名称合并。"""
        merged: dict[str, Any] = {}
        for manager in self._managers():
            merged.update(manager.connections)
        return merged

    def push_targets(self, attr: str = "enable_force_push_event") -> set[str]:
        """收集指定开关为真的连接名。

        这类开关本来就是"按连接"配置的，优化前它们只被读进 dataclass，
        实际发送时完全不区分连接（全部广播）。
        """
        names: set[str] = set()
        for manager in self._managers():
            for name, cfg in manager.connections.items():
                if getattr(cfg, attr, True):
                    names.add(name)
        return names

    def _init_api_handlers(self):
        from ..onebot_api import FriendAPI, GroupAPI, MessageAPI, RawAPI

        message_api = MessageAPI(self.platform)
        group_api = GroupAPI(self.platform)
        friend_api = FriendAPI(self.platform)
        raw_api = RawAPI(self.platform)

        for manager in self._managers():
            setter = getattr(manager, "set_api_handlers", None)
            if callable(setter):
                setter(message_api, group_api, friend_api, raw_api)

        self.message_api = message_api
        self.group_api = group_api
        self.friend_api = friend_api
        self.raw_api = raw_api

    def set_platform(self, platform):
        self.platform = platform
        for api in (self.message_api, self.group_api, self.friend_api, self.raw_api):
            if api:
                api.set_platform(platform)

    async def initialize(self):
        if self.ws_manager.websocket_clients:
            await self.ws_manager.connect_all()
        if self.http_server_manager.http_servers:
            await self.http_server_manager.start_all()
        if self.sse_server_manager.sse_servers:
            await self.sse_server_manager.start_all()
        if self.http_client_manager.http_clients:
            await self.http_client_manager.connect_all()
        if self.ws_server_manager.ws_servers:
            await self.ws_server_manager.start_all()
        logger.debug("[Bot通信] 所有通信管理器已初始化")

    async def convert_and_forward(
        self, event: AstrMessageEvent, text: str, targets: set[str] | None = None
    ):
        if not text or not text.strip():
            logger.debug(f"[Bot通信] 忽略空白消息转发: {text}")
            return

        try:
            forward_message = self._build_forward_message(event, text, force_text=True)
            if forward_message:
                await self._dispatch_message(forward_message, targets)
        except Exception as e:
            logger.error(f"[Bot通信] 转发消息失败: {e}")

    async def forward_event(
        self,
        event: AstrMessageEvent,
        message_str: str | None = None,
        targets: set[str] | None = None,
    ):
        if not event:
            logger.debug("[Bot通信] 忽略空事件转发")
            return

        try:
            message_content = message_str or event.message_str
            # 纯图片 / 语音 / 表情消息的 message_str 是空串，
            # 只看文本会把这类消息整条丢掉，需要再看一眼消息链。
            if not (message_content or "").strip() and not self._has_rich_content(event):
                logger.debug("[Bot通信] 忽略空白消息转发")
                return

            forward_message = self._build_forward_message(event, message_str)
            if forward_message:
                await self._dispatch_message(forward_message, targets)
        except Exception as e:
            logger.error(f"[Bot通信] 转发事件失败: {type(e).__name__}: {e}", exc_info=True)

    @staticmethod
    def _has_rich_content(event: AstrMessageEvent) -> bool:
        """消息链里是否有实际内容（图片、语音等没有文本部分）。"""
        getter = getattr(event, "get_messages", None)
        if not callable(getter):
            return False
        try:
            return bool(getter())
        except Exception as e:
            logger.debug(f"[Bot通信] 读取消息链失败: {e}")
            return False

    async def forward_raw_event(
        self, event: AstrMessageEvent, targets: set[str] | None = None
    ) -> bool:
        """转发 notice / request 类原始事件。

        OneBot v11 的事件模型包含 message / notice / request / meta_event 四类，
        优化前这里只实现了 message —— 群成员变动、禁言、撤回、戳一戳、
        加好友/加群请求全部丢失。
        """
        raw_obj = getattr(getattr(event, "message_obj", None), "raw_message", None)
        if raw_obj is None:
            return False

        # dict 子类（aiocqhttp.Event）可以直接判类型，避免每条普通消息都做字典拷贝
        if isinstance(raw_obj, dict):
            post_type = raw_obj.get("post_type")
            if post_type not in ("notice", "request"):
                return False
            payload = dict(raw_obj)
        else:
            raw = _as_dict(raw_obj)
            if not raw:
                return False
            post_type = raw.get("post_type")
            if post_type not in ("notice", "request"):
                return False
            payload = raw

        payload.setdefault("time", int(time.time()))

        raw_self_id = payload.get("self_id")
        if raw_self_id:
            self._apply_self_id(raw_self_id)
        payload.setdefault("self_id", self.self_id or self.bot_id)

        if self.debug:
            logger.debug(f"[Bot通信] 转发 {post_type} 事件: {payload}")

        await self._dispatch_message(payload, targets)
        return True

    async def _dispatch_message(
        self, message: dict[str, Any], targets: set[str] | None = None
    ):
        """把事件推送到各连接。

        ``targets`` 为 ``None`` 表示全部连接；否则只推给命中的连接。
        优化前这里无条件广播到所有管理器，导致按连接配置的转发规则形同虚设，
        配多个连接时同一条消息会被重复投递。
        """
        tasks = [manager.send_message(message, targets) for manager in self._managers()]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        total_success = sum(r for r in results if isinstance(r, int) and r > 0)
        if self.debug:
            errors = [r for r in results if isinstance(r, BaseException)]
            logger.debug(
                f"[Bot通信] 消息已推送到 {total_success} 个目标"
                + (f"，{len(errors)} 个目标异常" if errors else "")
            )
        return total_success

    def _next_message_id(self, fallback: Any = None) -> Any:
        if fallback not in (None, ""):
            return fallback
        self._message_seq += 1
        return self._message_seq

    def _build_forward_message(
        self,
        event: AstrMessageEvent,
        message_str: str | None = None,
        force_text: bool = False,
    ) -> dict[str, Any] | None:
        try:
            group_id = event.get_group_id()
            message_type = "group" if group_id else "private"

            # 事件里带着真实的机器人账号，顺手校正全局 self_id
            event_self_id = getattr(getattr(event, "message_obj", None), "self_id", None)
            if event_self_id:
                self._apply_self_id(event_self_id)

            raw_message = message_str or event.message_str

            if force_text:
                onebot_message = [{"type": "text", "data": {"text": raw_message}}]
            else:
                onebot_message = self._convert_message_chain(event.get_messages())

            forward_message = {
                "post_type": "message",
                "message_type": message_type,
                "user_id": _to_int(event.get_sender_id()),
                "self_id": self.self_id or self.bot_id,
                "message": onebot_message,
                "raw_message": raw_message,
                "sub_type": "normal",
                # 优化前 message_id 可能是空字符串，调用方拿去做撤回/引用时必然失败
                "message_id": self._next_message_id(
                    getattr(event.message_obj, "message_id", None)
                ),
                "time": int(time.time()),
            }

            if message_type == "group":
                forward_message["group_id"] = _to_int(group_id)

            forward_message["sender"] = self._get_sender_info(event)

            return forward_message

        except Exception as e:
            logger.error(f"[Bot通信] 构建转发消息失败: {e}")
            return None

    def _convert_message_chain(self, message_chain: list[Any]) -> list[dict[str, Any]]:
        messages: list[dict[str, Any]] = []

        for component in message_chain or []:
            try:
                segment = self._convert_component(component)
            except Exception as e:
                # 单个组件转换失败不应影响整条消息转发
                logger.error(
                    f"[Bot通信] 转换消息组件 {type(component).__name__} 失败: {e}"
                )
                segment = None

            if segment:
                messages.append(segment)

        if not messages:
            messages.append({"type": "text", "data": {"text": ""}})

        return messages

    def _convert_component(self, component: Any) -> dict[str, Any] | None:
        component_type = type(component).__name__
        component_type = _COMPONENT_ALIASES.get(component_type, component_type)

        if component_type == "Plain":
            return {"type": "text", "data": {"text": component.text}}
        if component_type == "Image":
            media = _component_media(component)
            return {"type": "image", "data": {"url": media, "file": media}}
        if component_type == "At":
            return {"type": "at", "data": {"qq": str(component.qq)}}
        if component_type == "Face":
            return {"type": "face", "data": {"id": component.id}}
        if component_type == "Record":
            media = _component_media(component)
            return {"type": "record", "data": {"url": media, "file": media}}
        if component_type == "Video":
            media = _component_media(component)
            return {"type": "video", "data": {"url": media, "file": media}}
        if component_type == "Reply":
            return {"type": "reply", "data": {"id": component.id}}
        if component_type == "File":
            return {
                "type": "file",
                "data": {
                    "name": getattr(component, "name", "") or "",
                    # 不用 _component_media：File.file 是属性，异步上下文里会尝试同步下载
                    "url": getattr(component, "url", "") or "",
                    "file": getattr(component, "file_", "") or "",
                },
            }
        if component_type == "Poke":
            return {
                "type": "poke",
                "data": {
                    # 实际字段是 _type / id，poke_type 只是构造参数名
                    "type": str(getattr(component, "_type", "") or "126"),
                    "id": str(
                        getattr(component, "id", None)
                        or getattr(component, "qq", None)
                        or ""
                    ),
                },
            }
        if component_type == "Dice":
            return {"type": "dice", "data": {"result": getattr(component, "result", 1)}}
        if component_type == "Rps":
            return {"type": "rps", "data": {"result": getattr(component, "result", 0)}}
        if component_type == "Location":
            return {
                "type": "location",
                "data": {
                    "lat": getattr(component, "lat", 0),
                    "lon": getattr(component, "lon", 0),
                    "title": getattr(component, "title", ""),
                    "content": getattr(component, "content", ""),
                },
            }
        if component_type == "Json":
            return {"type": "json", "data": {"data": getattr(component, "data", "")}}
        if component_type == "Xml":
            return {"type": "xml", "data": {"data": getattr(component, "data", "")}}
        if component_type == "Forward":
            return {"type": "forward", "data": {"id": getattr(component, "id", "")}}
        if component_type == "Node":
            return {
                "type": "node",
                "data": {
                    # Node 的真实字段是 uin / name / content(list)
                    "user_id": _to_int(getattr(component, "uin", 0)),
                    "nickname": getattr(component, "name", "") or "",
                    "content": self._convert_message_chain(
                        getattr(component, "content", None)
                    ),
                },
            }
        if component_type == "Nodes":
            return {
                "type": "forward",
                "data": {
                    "messages": [
                        self._convert_component(node)
                        for node in (getattr(component, "nodes", None) or [])
                    ]
                },
            }
        if component_type == "Share":
            return {
                "type": "share",
                "data": {
                    "url": getattr(component, "url", ""),
                    "title": getattr(component, "title", ""),
                    "content": getattr(component, "content", ""),
                    "image": getattr(component, "image", ""),
                },
            }
        if component_type == "Music":
            return {
                "type": "music",
                "data": {
                    "type": str(getattr(component, "_type", "") or "custom"),
                    "id": str(getattr(component, "id", "") or ""),
                    "url": getattr(component, "url", ""),
                    "audio": getattr(component, "audio", ""),
                    "title": getattr(component, "title", ""),
                    "content": getattr(component, "content", ""),
                    "image": getattr(component, "image", ""),
                },
            }
        if component_type == "Contact":
            return {
                "type": "contact",
                "data": {
                    "type": str(getattr(component, "_type", "") or "qq"),
                    "id": str(getattr(component, "id", "") or ""),
                },
            }

        text = getattr(component, "text", None)
        return {"type": "text", "data": {"text": str(text if text is not None else component)}}

    def _get_sender_info(self, event: AstrMessageEvent) -> dict[str, Any]:
        sender_info = {
            "user_id": _to_int(event.get_sender_id()),
            "nickname": event.get_sender_name() or "未知用户",
            "card": "",
            "sex": "unknown",
            "age": 0,
            "area": "",
            "level": 0,
            "role": "member",
            "title": "",
        }

        try:
            sender = getattr(event.message_obj, "sender", None)
            if sender is not None:
                nickname = getattr(sender, "nickname", None)
                if nickname:
                    sender_info["nickname"] = nickname
                card = getattr(sender, "card", None)
                if card:
                    sender_info["card"] = card
                role = getattr(sender, "role", None)
                if role:
                    sender_info["role"] = role
        except Exception as e:
            logger.debug(f"[Bot通信] 解析发送者信息失败，使用默认值: {e}")

        return sender_info

    async def close(self):
        try:
            await asyncio.gather(
                self.ws_manager.close(),
                self.http_server_manager.close(),
                self.sse_server_manager.close(),
                self.http_client_manager.close(),
                self.ws_server_manager.close(),
                return_exceptions=True,
            )
            logger.debug("[Bot通信] Bot通信核心服务已关闭")
        except Exception as e:
            logger.error(f"[Bot通信] 关闭Bot通信核心服务失败: {e}")
