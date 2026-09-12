from typing import Optional, Any, Tuple

from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, register
from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import MessageChain
import astrbot.api.message_components as Comp

from .service import BotCommunicationCore


@register("astrbot_plugin_onebot_bridge", "苏月晅", "OneBot 桥接插件，支持将消息转发到副 Bot 并支持 OneBot v11 协议", "1.1.4", "https://github.com/yuexuan6699/astrbot_plugin_onebot_bridge")
class BotCommunicationPlugin(Star):
    FORWARD_TYPE_FULL = "full"
    FORWARD_TYPE_STRIP = "strip"
    FORWARD_TYPE_NONE = None
    
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.bot_comm: Optional[BotCommunicationCore] = None
        self._enabled = False
        
        self.bot_forward_rules = {}
    
    def _has_valid_config(self) -> bool:
        websocket_clients = self.config.get("websocket_clients", [])
        
        if not websocket_clients:
            return False
        
        valid_template_keys = [
            "WebSocket 客户端",
            "HTTP 服务器", 
            "HTTP SSE 服务器",
            "HTTP 客户端",
            "WebSocket 服务器"
        ]
        
        for client_config in websocket_clients:
            if isinstance(client_config, dict):
                template_key = client_config.get("__template_key")
                if template_key in valid_template_keys:
                    if client_config.get("enable", True):
                        return True
        
        return False
    
    def _load_bot_forward_rules(self):
        websocket_clients = self.config.get("websocket_clients", [])
        self.bot_forward_rules = {}
        
        for client_config in websocket_clients:
            if isinstance(client_config, dict):
                template_key = client_config.get("__template_key")
                if template_key not in ["WebSocket 客户端", "HTTP 客户端", "WebSocket 服务器"]:
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
        try:
            platforms = self.context.platform_manager.get_insts()
            if platforms:
                return platforms[0]
        except Exception as e:
            logger.debug(f"[Bot通信] 获取平台实例失败: {e}")
        return None
    
    def _get_bot_id(self) -> str:
        try:
            platforms = self.context.platform_manager.get_insts()
            if platforms:
                platform = platforms[0]
                if hasattr(platform, "metadata") and hasattr(platform.metadata, "id"):
                    return str(platform.metadata.id)
                elif hasattr(platform, "bot_id"):
                    return str(platform.bot_id)
                elif hasattr(platform, "self_id"):
                    return str(platform.self_id)
                elif hasattr(platform, "id"):
                    return str(platform.id)
        except Exception as e:
            logger.error(f"[Bot通信] 获取Bot ID失败: {e}")
        return "0"
    
    async def _send_message_callback(self, message_type: str, target_id: str, content: Any) -> Optional[str]:
        try:
            platform_id = self._get_platform_id()
            message_chain = self._build_message_chain(content)
            
            if message_type == "GroupMessage":
                session_str = f"{platform_id}:GroupMessage:{target_id}"
                await self.context.send_message(session_str, message_chain)
                return "0"
            elif message_type == "FriendMessage":
                session_str = f"{platform_id}:FriendMessage:{target_id}"
                await self.context.send_message(session_str, message_chain)
                return "0"
        except Exception as e:
            logger.error(f"[Bot通信] 发送消息失败: {e}")
        return None
    
    def _build_message_chain(self, content: Any) -> MessageChain:
        message_chain = MessageChain()
        
        if isinstance(content, list):
            for item in content:
                if isinstance(item, dict):
                    self._add_dict_component(message_chain, item)
                elif isinstance(item, str):
                    message_chain.message(item)
        elif isinstance(content, str):
            message_chain.message(content)
        else:
            message_chain.message(str(content))
        
        return message_chain
    
    def _add_dict_component(self, message_chain: MessageChain, item: dict):
        item_type = item.get("type")
        item_data = item.get("data", {})
        
        if item_type == "text":
            message_chain.message(item_data.get("text", ""))
        elif item_type == "image":
            url = item_data.get("url") or item_data.get("file")
            if url:
                if url.startswith("base64://"):
                    message_chain.chain.append(Comp.Image.fromBase64(url[9:]))
                elif url.startswith("http://") or url.startswith("https://"):
                    message_chain.chain.append(Comp.Image.fromURL(url))
                else:
                    message_chain.chain.append(Comp.Image(file=url))
        elif item_type == "at":
            qq = item_data.get("qq")
            if qq:
                message_chain.chain.append(Comp.At(qq=str(qq)))
        elif item_type == "face":
            message_chain.chain.append(Comp.Face(id=item_data.get("id", 0)))
        elif item_type == "mface":
            message_chain.chain.append(Comp.Face(id=int(item_data.get("emoji_id", 0))))
        elif item_type == "record":
            url = item_data.get("url") or item_data.get("file")
            if url:
                message_chain.chain.append(Comp.Record(file=url))
        elif item_type == "video":
            url = item_data.get("url") or item_data.get("file")
            if url:
                message_chain.chain.append(Comp.Video.fromURL(url))
        elif item_type == "reply":
            message_chain.chain.append(Comp.Reply(id=item_data.get("id")))
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
        try:
            platforms = self.context.platform_manager.get_insts()
            if platforms:
                platform = platforms[0]
                meta = platform.meta()
                if hasattr(meta, "id"):
                    return meta.id
                elif hasattr(meta, "name"):
                    return meta.name
        except Exception as e:
            logger.debug(f"[Bot通信] 获取平台ID失败: {e}")
        return "aiocqhttp"
    
    def _check_forward_condition(self, message_str: str, bot_name: str = None) -> Tuple[str, Optional[str]]:
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
    
    async def _process_forward(self, event: AstrMessageEvent, forward_type: str, content: Optional[str]):
        if not self.bot_comm:
            return
        
        if forward_type == self.FORWARD_TYPE_FULL:
            await self.bot_comm.forward_event(event)
        elif forward_type == self.FORWARD_TYPE_STRIP and content:
            await self.bot_comm.convert_and_forward(event, content)
    
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def handle_group_message(self, event: AstrMessageEvent):
        '''处理群消息转发'''
        if not self._enabled:
            return
        
        message_str = event.message_str
        forwarded = False
        
        if self.bot_forward_rules:
            for bot_name, rules in self.bot_forward_rules.items():
                if not rules.get("enable_group_forward", True):
                    continue
                
                forward_type, content = self._check_forward_condition(message_str, bot_name)
                if forward_type != self.FORWARD_TYPE_NONE:
                    await self._process_forward(event, forward_type, content)
                    forwarded = True
        
        # 只在消息被转发时才停止事件传播
        if forwarded:
            event.stop_event()
    
    @filter.event_message_type(filter.EventMessageType.PRIVATE_MESSAGE)
    async def handle_private_message(self, event: AstrMessageEvent):
        '''处理私聊消息转发'''
        if not self._enabled:
            return
        
        message_str = event.message_str
        forwarded = False
        
        if self.bot_forward_rules:
            for bot_name, rules in self.bot_forward_rules.items():
                if not rules.get("enable_private_forward", False):
                    continue
                
                forward_type, content = self._check_forward_condition(message_str, bot_name)
                if forward_type != self.FORWARD_TYPE_NONE:
                    await self._process_forward(event, forward_type, content)
                    forwarded = True
        
        # 只在消息被转发时才停止事件传播
        if forwarded:
            event.stop_event()
    
    async def terminate(self):
        if self.bot_comm:
            await self.bot_comm.close()
        logger.debug("[Bot 通信] Bot 通信插件已终止")
