from typing import Any

import astrbot.api.message_components as Comp
from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star, register

from .service import BotCommunicationCore

PLUGIN_VERSION = "1.1.7"

#: 支持的网络连接模板类型
SUPPORTED_TEMPLATE_KEYS = (
    "WebSocket 客户端",
    "HTTP 服务器",
    "HTTP SSE 服务器",
    "HTTP 客户端",
    "WebSocket 服务器",
)

#: 支持按连接独立配置转发规则的模板类型
FORWARD_RULE_TEMPLATE_KEYS = ("WebSocket 客户端", "HTTP 客户端", "WebSocket 服务器")


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


@register("astrbot_plugin_onebot_bridge", "苏月晅", "OneBot 桥接插件，支持将消息转发到副 Bot 并支持 OneBot v11 协议", PLUGIN_VERSION, "https://github.com/yuexuan6699/astrbot_plugin_onebot_bridge")
class BotCommunicationPlugin(Star):
    FORWARD_TYPE_FULL = "full"
    FORWARD_TYPE_STRIP = "strip"
    FORWARD_TYPE_NONE = None

    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.bot_comm: BotCommunicationCore | None = None
        self._enabled = False

        self.bot_forward_rules = {}
        self._platform_cache = None
        self._platform_info_cache: tuple[str, str] | None = None

    def _has_valid_config(self) -> bool:
        websocket_clients = self.config.get("websocket_clients", [])

        if not websocket_clients:
            return False

        for client_config in websocket_clients:
            if isinstance(client_config, dict):
                template_key = client_config.get("__template_key")
                if template_key in SUPPORTED_TEMPLATE_KEYS and client_config.get("enable", True):
                    return True

        return False

    def _load_bot_forward_rules(self):
        websocket_clients = self.config.get("websocket_clients", [])
        self.bot_forward_rules = {}

        for client_config in websocket_clients:
            if isinstance(client_config, dict):
                template_key = client_config.get("__template_key")
                if template_key not in FORWARD_RULE_TEMPLATE_KEYS:
                    continue

                name = client_config.get("name", "default")
                enable = client_config.get("enable", True)

                if not enable:
                    continue

                forward_rules = {
                    "enable_group_forward": client_config.get("enable_group_forward", True),
                    "enable_private_forward": client_config.get("enable_private_forward", False),
                    "command_prefix": client_config.get("command_prefix", ""),
                    "forward_fprefix": client_config.get("forward_fprefix", []),
                    "forward_keyword": client_config.get("forward_keyword", []),
                }

                if isinstance(forward_rules["forward_fprefix"], str):
                    forward_rules["forward_fprefix"] = [forward_rules["forward_fprefix"]] if forward_rules["forward_fprefix"] else []
                if isinstance(forward_rules["forward_keyword"], str):
                    forward_rules["forward_keyword"] = [forward_rules["forward_keyword"]] if forward_rules["forward_keyword"] else []

                self.bot_forward_rules[name] = forward_rules
                logger.debug(f"[Bot 通信] 已加载机器人 {name} 的转发规则：{forward_rules}")

    async def initialize(self):
        if not self._has_valid_config():
            logger.info("[Bot 通信] 未检测到有效配置，插件将不会启动")
            self._enabled = False
            return

        logger.debug("[Bot 通信] Bot 通信插件正在初始化...")

        self._load_bot_forward_rules()

        bot_id = self._get_bot_id()
        platform = self._get_platform()

        self.bot_comm = BotCommunicationCore(
            config=self.config,
            bot_id=bot_id,
            send_message_callback=self._send_message_callback,
            platform=platform
        )

        await self.bot_comm.initialize()
        self._enabled = True

        logger.debug("[Bot 通信] Bot 通信插件初始化完成")

    def _get_platform(self):
        if self._platform_cache is not None:
            return self._platform_cache
        try:
            platforms = self.context.platform_manager.get_insts()
            if platforms:
                # 同时装了多个平台时优先挑 OneBot 系的适配器，
                # 优化前直接取 platforms[0]，多平台环境下可能拿到完全无关的平台
                preferred = None
                for platform in platforms:
                    try:
                        meta = platform.meta()
                        name = str(getattr(meta, "name", "") or "").lower()
                        metadata = getattr(platform, "metadata", None)
                        adapter = str(getattr(metadata, "name", "") or "").lower()
                    except Exception as e:
                        logger.debug(f"[Bot通信] 读取平台元信息失败，跳过该平台: {e}")
                        continue
                    if (
                        "onebot" in name
                        or "aiocqhttp" in name
                        or "onebot" in adapter
                        or "aiocqhttp" in adapter
                    ):
                        preferred = platform
                        break

                self._platform_cache = preferred or platforms[0]

                # 插件初始化时平台可能还没就绪，core 里的 API 处理器会拿到 None；
                # 这里补一次同步，否则所有 OneBot API 会一直走空实现。
                if self.bot_comm is not None and getattr(self.bot_comm, "platform", None) is not self._platform_cache:
                    try:
                        self.bot_comm.set_platform(self._platform_cache)
                        logger.info("[Bot通信] 已补挂平台实例到 OneBot API 处理器")
                    except Exception as e:
                        logger.debug(f"[Bot通信] 补挂平台实例失败: {e}")

                return self._platform_cache
        except Exception as e:
            logger.debug(f"[Bot通信] 获取平台实例失败: {e}")
        return None

    @staticmethod
    def _probe_self_id(platform) -> str:
        """尽力探测机器人账号（QQ 号）。

        注意不能用 PlatformMetadata.id —— 那是平台**实例**标识，用于拼
        unified_msg_origin，把它当成 QQ 号会让转发事件的 self_id 变成一串
        平台 ID，副 Bot 侧看到的是一个不存在的账号。
        """
        candidates = []

        for attr in ("self_id", "bot_id"):
            try:
                value = getattr(platform, attr, None)
            except Exception:
                value = None
            if value:
                candidates.append(value)

        getter = getattr(platform, "get_client", None)
        if callable(getter):
            try:
                client = getter()
            except Exception:
                client = None
            if client is not None:
                for attr in ("self_id", "bot_id"):
                    try:
                        value = getattr(client, attr, None)
                    except Exception:
                        value = None
                    if value:
                        candidates.append(value)

        for value in candidates:
            text = str(value).strip()
            if text and text.isdigit():
                return text

        return str(candidates[0]).strip() if candidates else ""

    def _platform_ids(self) -> tuple[str, str]:
        """返回 (bot_id, platform_id)，结果缓存，避免每次转发都遍历平台。"""
        if self._platform_info_cache is not None:
            return self._platform_info_cache

        bot_id, platform_id = "0", "aiocqhttp"
        platform = self._get_platform()

        if platform is None:
            # 平台还没加载出来时不缓存，等就绪后重新解析
            return (bot_id, platform_id)

        metadata = getattr(platform, "metadata", None)
        meta_getter = getattr(platform, "meta", None)
        if callable(meta_getter):
            try:
                metadata = meta_getter() or metadata
            except Exception as e:
                logger.debug(f"[Bot通信] 获取平台元信息失败: {e}")

        if metadata is not None:
            platform_id = str(
                getattr(metadata, "id", None) or getattr(metadata, "name", None) or platform_id
            )

        # 账号探测不到时退回平台 ID，保证 session 仍能构造出来；
        # 真正的账号会由 core 在收到第一个事件时校正。
        bot_id = self._probe_self_id(platform) or platform_id

        self._platform_info_cache = (bot_id, platform_id)
        return self._platform_info_cache

    def _get_bot_id(self) -> str:
        return self._platform_ids()[0]

    async def _send_message_callback(self, message_type: str, target_id: str, content: Any) -> str | None:
        try:
            platform_id = self._get_platform_id()
            message_chain = self._build_message_chain(content)

            if message_type not in ("GroupMessage", "FriendMessage"):
                return None

            session_str = f"{platform_id}:{message_type}:{target_id}"
            result = await self.context.send_message(session_str, message_chain)
            # 部分版本返回 False 表示发送失败，此处如实反馈给上层
            if result is False:
                logger.warning(f"[Bot通信] 消息发送未被平台接受: {session_str}")
                return None
            return "0"
        except Exception as e:
            logger.error(f"[Bot通信] 发送消息失败: {e}")
        return None

    def _build_message_chain(self, content: Any) -> MessageChain:
        message_chain = MessageChain()

        if isinstance(content, list):
            for item in content:
                try:
                    if isinstance(item, dict):
                        self._add_dict_component(message_chain, item)
                    elif isinstance(item, str):
                        message_chain.message(item)
                except Exception as e:
                    # 单个消息段转换失败不影响其余内容发送
                    logger.warning(f"[Bot通信] 忽略无法转换的消息段: {item!r} ({e})")
        elif isinstance(content, str):
            message_chain.message(content)
        else:
            message_chain.message(str(content))

        return message_chain

    @staticmethod
    def _media_url(item_data: dict) -> str:
        for key in ("url", "file", "path"):
            value = item_data.get(key)
            if value:
                return str(value)
        return ""

    def _add_dict_component(self, message_chain: MessageChain, item: dict):
        item_type = item.get("type")
        item_data = item.get("data")
        if isinstance(item_data, str):
            item_data = {"text": item_data}
        elif not isinstance(item_data, dict):
            item_data = {}

        if item_type == "text":
            message_chain.message(item_data.get("text", ""))
        elif item_type == "image":
            url = self._media_url(item_data)
            if url:
                if url.startswith("base64://"):
                    message_chain.chain.append(Comp.Image.fromBase64(url[9:]))
                elif url.startswith(("http://", "https://")):
                    message_chain.chain.append(Comp.Image.fromURL(url))
                else:
                    message_chain.chain.append(Comp.Image(file=url))
        elif item_type == "at":
            qq = item_data.get("qq")
            if qq:
                message_chain.chain.append(Comp.At(qq=str(qq)))
        elif item_type == "face":
            message_chain.chain.append(Comp.Face(id=_to_int(item_data.get("id"), 0)))
        elif item_type == "mface":
            face_id = item_data.get("emoji_id") or item_data.get("id")
            if face_id is not None:
                message_chain.chain.append(Comp.Face(id=_to_int(face_id, 0)))
        elif item_type == "record":
            url = self._media_url(item_data)
            if url:
                message_chain.chain.append(Comp.Record(file=url))
        elif item_type == "video":
            url = self._media_url(item_data)
            if url.startswith(("http://", "https://")):
                message_chain.chain.append(Comp.Video.fromURL(url))
            elif url:
                message_chain.chain.append(Comp.Video(file=url))
        elif item_type == "reply":
            reply_id = item_data.get("id")
            if reply_id is not None:
                message_chain.chain.append(Comp.Reply(id=reply_id))
        elif item_type == "json":
            message_chain.message("[JSON消息]")
        elif item_type == "xml":
            message_chain.message("[XML卡片]")
        elif item_type == "markdown":
            content = item_data.get("content", "")
            message_chain.message(content)
        elif item_type == "location":
            lat = item_data.get("lat", 0)
            lon = item_data.get("lon", 0)
            title = item_data.get("title", "")
            message_chain.message(f"[位置] {title} ({lat}, {lon})")
        elif item_type == "contact":
            message_chain.message("[联系人]")
        elif item_type == "dice":
            message_chain.message("[骰子]")
        elif item_type == "rps":
            message_chain.message("[猜拳]")
        elif item_type == "poke":
            message_chain.message("[戳一戳]")
        elif item_type == "music":
            title = item_data.get("title", "音乐")
            message_chain.message(f"[{title}]")
        elif item_type == "file":
            name = item_data.get("name", "文件")
            message_chain.message(f"[文件] {name}")
        elif item_type == "forward":
            message_chain.message("[合并转发]")
        elif item_type == "node":
            nickname = item_data.get("nickname", "")
            content = item_data.get("content", "")
            message_chain.message(f"[转发] {nickname}: {content}")

    def _get_platform_id(self) -> str:
        return self._platform_ids()[1]

    def _check_forward_condition(
        self, message_str: str, bot_name: str | None = None
    ) -> tuple[str, str | None]:
        if bot_name and bot_name in self.bot_forward_rules:
            rules = self.bot_forward_rules[bot_name]
            forward_fprefix = rules.get("forward_fprefix", [])
            forward_keyword = rules.get("forward_keyword", [])
            command_prefix = rules.get("command_prefix", "")
        else:
            forward_fprefix = []
            forward_keyword = []
            command_prefix = ""

        if forward_fprefix:
            for prefix in forward_fprefix:
                if prefix and message_str.startswith(prefix):
                    return (self.FORWARD_TYPE_FULL, None)

        if forward_keyword:
            for keyword in forward_keyword:
                if keyword and keyword in message_str:
                    return (self.FORWARD_TYPE_FULL, None)

        if command_prefix and message_str.startswith(command_prefix):
            content = message_str[len(command_prefix):].strip()
            if content:
                return (self.FORWARD_TYPE_STRIP, content)

        return (self.FORWARD_TYPE_NONE, None)

    async def _process_forward(
        self,
        event: AstrMessageEvent,
        forward_type: str,
        content: str | None,
        targets: set[str] | None = None,
    ):
        if not self.bot_comm:
            return

        if forward_type == self.FORWARD_TYPE_FULL:
            await self.bot_comm.forward_event(event, targets=targets)
        elif forward_type == self.FORWARD_TYPE_STRIP and content:
            await self.bot_comm.convert_and_forward(event, content, targets=targets)

    async def _forward_if_matched(self, event: AstrMessageEvent, is_group: bool) -> bool:
        if not self.bot_forward_rules:
            return False

        message_str = event.message_str
        enable_key = "enable_group_forward" if is_group else "enable_private_forward"

        forwarded = False
        for bot_name, rules in self.bot_forward_rules.items():
            if not rules.get(enable_key, is_group):
                continue

            forward_type, content = self._check_forward_condition(message_str, bot_name)
            if forward_type == self.FORWARD_TYPE_NONE:
                continue

            # 只投递给命中的那条连接。优化前无论命中哪条规则都会广播给全部连接，
            # 配了多个连接时同一条消息会被重复投递多次。
            await self._process_forward(event, forward_type, content, targets={bot_name})
            forwarded = True

        return forwarded

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def handle_group_message(self, event: AstrMessageEvent):
        '''处理群消息转发'''
        if not self._enabled:
            return

        # 只在消息被转发时才停止事件传播
        if await self._forward_if_matched(event, is_group=True):
            event.stop_event()

    @filter.event_message_type(filter.EventMessageType.PRIVATE_MESSAGE)
    async def handle_private_message(self, event: AstrMessageEvent):
        '''处理私聊消息转发'''
        if not self._enabled:
            return

        # 只在消息被转发时才停止事件传播
        if await self._forward_if_matched(event, is_group=False):
            event.stop_event()

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def handle_raw_event(self, event: AstrMessageEvent):
        '''转发 notice / request 类事件

        优化前只处理了 message 事件，群成员增减、禁言、撤回、戳一戳、
        加好友/加群请求这些通知与请求事件全部丢失。
        是否投递由连接配置的 enable_force_push_event 决定。
        '''
        if not self._enabled or not self.bot_comm:
            return

        targets = self.bot_comm.push_targets("enable_force_push_event")
        if not targets:
            return

        if await self.bot_comm.forward_raw_event(event, targets):
            event.stop_event()

    async def terminate(self):
        if self.bot_comm:
            await self.bot_comm.close()
            self.bot_comm = None
        self._platform_cache = None
        self._platform_info_cache = None
        logger.debug("[Bot 通信] Bot 通信插件已终止")
