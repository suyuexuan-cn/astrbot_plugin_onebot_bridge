import asyncio
import random
import json
import time
import websockets
import websockets.exceptions
from typing import Dict, Any
from dataclasses import dataclass

from astrbot.api import logger


@dataclass
class WebSocketClientConfig:
    enable: bool = True
    name: str = "default"
    uri: str = "ws://localhost:2536/OneBotv11"
    token: str = ""
    report_self_message: bool = False
    enable_force_push_event: bool = True
    message_post_format: str = "array"
    debug: bool = False
    heart_interval: int = 30000
    ping_interval: int = 10
    ping_timeout: int = 20
    connect_timeout: int = 10
    close_timeout: int = 5
    max_reconnect_attempts: int = 0

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "WebSocketClientConfig":
        return cls(
            enable=config_dict.get("enable", True),
            name=config_dict.get("name", "default"),
            uri=config_dict.get("uri", "ws://localhost:2536/OneBotv11"),
            token=config_dict.get("token", ""),
            report_self_message=config_dict.get("report_self_message", False),
            enable_force_push_event=config_dict.get("enable_force_push_event", True),
            message_post_format=config_dict.get("message_post_format", "array"),
            debug=config_dict.get("debug", False),
            heart_interval=config_dict.get("heart_interval", 30000),
            ping_interval=config_dict.get("ping_interval", 10),
            ping_timeout=config_dict.get("ping_timeout", 20),
            connect_timeout=config_dict.get("connect_timeout", 10),
            close_timeout=config_dict.get("close_timeout", 5),
            max_reconnect_attempts=config_dict.get("max_reconnect_attempts", 0),
        )


class WebSocketManager:
    def __init__(self, config: Dict[str, Any], bot_id: str, send_message_callback):
        self.config = config
        self.bot_id = bot_id
        self.send_message_callback = send_message_callback
        
        self.websocket_clients: Dict[str, WebSocketClientConfig] = {}
        self._load_websocket_clients_config()
        
        self.websockets: Dict[str, websockets.client.WebSocketClientProtocol] = {}
        self.connected: Dict[str, bool] = {}
        self.reconnect_tasks: Dict[str, asyncio.Task] = {}
        
        self.initial_connect = False
        self._monitor_task = None
        self._connecting: Dict[str, bool] = {}
        
        self.message_api = None
        self.group_api = None
        self.friend_api = None
    
    def set_api_handlers(self, message_api, group_api, friend_api):
        self.message_api = message_api
        self.group_api = group_api
        self.friend_api = friend_api
        
    def _load_websocket_clients_config(self):
        websocket_clients = self.config.get("websocket_clients", [])
        
        if websocket_clients:
            logger.debug("[Bot通信] 正在加载WebSocket客户端配置")
            for i, client_config in enumerate(websocket_clients):
                if isinstance(client_config, dict):
                    template_key = client_config.get("__template_key")
                    if template_key != "WebSocket 客户端":
                        logger.debug(f"[Bot通信] 跳过非WebSocket客户端配置: {template_key}")
                        continue
                    name = client_config.get("name", f"client_{i}")
                    self.websocket_clients[name] = WebSocketClientConfig.from_dict(client_config)
                    logger.info(f"[Bot通信] 已加载客户端配置: {name} -> {self.websocket_clients[name].uri}")
    
    async def _monitor_connections(self):
        while True:
            try:
                for name, client in self.websocket_clients.items():
                    if client.enable:
                        if not self.connected.get(name, False):
                            logger.debug(f"[Bot通信] WebSocket客户端 {name} ({client.uri}) 未连接，尝试重连")
                            await self.reconnect(name)
                await asyncio.sleep(30)
            except Exception as e:
                logger.error(f"[Bot通信] 连接监控任务异常: {e}")
                await asyncio.sleep(10)
    
    async def connect_to_bot(self, client_name: str) -> bool:
        client = self.websocket_clients.get(client_name)
        if not client:
            logger.error(f"[Bot通信] WebSocket客户端 {client_name} 不存在")
            return False
        
        if not client.enable:
            logger.debug(f"[Bot通信] WebSocket客户端 {client_name} 已禁用，跳过连接")
            return False
        
        if self._connecting.get(client_name, False):
            logger.debug(f"[Bot通信] WebSocket客户端 {client_name} 正在连接中，跳过重复连接")
            return False
        
        self._connecting[client_name] = True
        
        try:
            if self.connected.get(client_name, False) and self.websockets.get(client_name):
                try:
                    await self.websockets[client_name].close(code=1000, reason="Normal closure")
                    logger.debug(f"[Bot通信] 已关闭旧连接: {client_name} ({client.uri})")
                except Exception as e:
                    logger.error(f"[Bot通信] 关闭WebSocket客户端连接失败 {client_name} ({client.uri}): {e}")
                self.connected[client_name] = False
                self.websockets.pop(client_name, None)
            
            reconnect_count = 0
            
            while client.max_reconnect_attempts == 0 or reconnect_count < client.max_reconnect_attempts:
                try:
                    logger.debug(f"[Bot通信] 正在连接WebSocket客户端: {client_name} ({client.uri}) (尝试 {reconnect_count + 1})")
                    
                    websocket = await asyncio.wait_for(
                        websockets.connect(
                            client.uri,
                            ping_interval=client.ping_interval,
                            ping_timeout=client.ping_timeout,
                            close_timeout=client.close_timeout,
                            max_size=None
                        ),
                        timeout=client.connect_timeout
                    )
                    
                    self.websockets[client_name] = websocket
                    self.connected[client_name] = True
                    logger.debug(f"[Bot通信] 成功连接到WebSocket客户端: {client_name} ({client.uri})")
                    
                    asyncio.create_task(self.receive_messages(client_name))
                    
                    if client.heart_interval > 0:
                        asyncio.create_task(self._send_heartbeat(client_name))
                    
                    await self.send_connection_event(client_name)
                    
                    return True
                except asyncio.TimeoutError:
                    logger.error(f"[Bot通信] 连接WebSocket客户端超时 {client_name} ({client.uri})")
                except websockets.exceptions.InvalidURI as e:
                    logger.error(f"[Bot通信] WebSocket客户端URI无效 {client_name} ({client.uri}): {e}")
                    break
                except websockets.exceptions.InvalidHandshake as e:
                    logger.error(f"[Bot通信] WebSocket客户端握手失败 {client_name} ({client.uri}): {e}")
                except ConnectionRefusedError as e:
                    logger.error(f"[Bot通信] WebSocket客户端连接被拒绝 {client_name} ({client.uri}): {e}")
                except EOFError as e:
                    logger.error(f"[Bot通信] WebSocket客户端连接中断 {client_name} ({client.uri}): {e}")
                except Exception as e:
                    logger.error(f"[Bot通信] 连接WebSocket客户端失败 {client_name} ({client.uri}): {type(e).__name__}: {e}")
                
                self.connected[client_name] = False
                self.websockets.pop(client_name, None)
                
                base_delay = 5
                jitter = random.uniform(0, 1)
                reconnect_delay = min(base_delay * (2 ** reconnect_count) + jitter, 60)
                
                logger.debug(f"[Bot通信] {reconnect_delay:.1f}秒后重试连接WebSocket客户端 {client_name} ({client.uri})")
                await asyncio.sleep(reconnect_delay)
                reconnect_count += 1
            
            if client.max_reconnect_attempts > 0 and reconnect_count >= client.max_reconnect_attempts:
                logger.error(f"[Bot通信] 超过最大重连次数({client.max_reconnect_attempts})，停止尝试连接WebSocket客户端: {client_name} ({client.uri})")
                self.connected[client_name] = False
            
            return False
        finally:
            self._connecting.pop(client_name, None)
    
    async def _send_heartbeat(self, client_name: str):
        client = self.websocket_clients.get(client_name)
        if not client:
            return
        
        try:
            while self.connected.get(client_name, False) and self.websockets.get(client_name):
                await asyncio.sleep(client.heart_interval / 1000)
                
                try:
                    heartbeat_event = {
                        "post_type": "meta_event",
                        "meta_event_type": "heartbeat",
                        "status": "ok",
                        "interval": client.heart_interval,
                        "time": int(time.time()),
                        "self_id": self.bot_id
                    }
                    
                    if self.websockets.get(client_name):
                        await self.websockets[client_name].send(json.dumps(heartbeat_event))
                        if client.debug:
                            logger.debug(f"[Bot通信] 已发送心跳到WebSocket客户端: {client_name} ({client.uri})")
                except Exception as e:
                    logger.error(f"[Bot通信] 发送心跳失败 {client_name} ({client.uri}): {e}")
                    break
        except asyncio.CancelledError:
            logger.debug(f"[Bot通信] 心跳任务已取消: {client_name} ({client.uri})")
        except Exception as e:
            logger.error(f"[Bot通信] 心跳任务异常 {client_name} ({client.uri}): {e}")
    
    async def send_connection_event(self, client_name: str):
        client = self.websocket_clients.get(client_name)
        if not client:
            return
        
        try:
            connection_event = {
                "post_type": "meta_event",
                "meta_event_type": "lifecycle",
                "sub_type": "connect",
                "time": int(time.time()),
                "self_id": self.bot_id
            }
            
            if self.connected.get(client_name, False) and self.websockets.get(client_name):
                await self.websockets[client_name].send(json.dumps(connection_event))
                logger.debug(f"[Bot通信] 已发送连接事件到WebSocket客户端: {client_name} ({client.uri})")
        except Exception as e:
            logger.error(f"[Bot通信] 发送连接事件失败 {client_name} ({client.uri}): {e}")
    
    async def reconnect(self, client_name: str):
        existing = self.reconnect_tasks.get(client_name)
        if existing and not existing.done():
            return
        
        task = asyncio.create_task(self.connect_to_bot(client_name))
        
        def task_done_callback(fut):
            try:
                fut.result()
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error(f"[Bot通信] WebSocket客户端重连任务异常 {client_name}: {e}")
            if self.reconnect_tasks.get(client_name) == fut:
                self.reconnect_tasks.pop(client_name, None)
        
        task.add_done_callback(task_done_callback)
        self.reconnect_tasks[client_name] = task
    
    async def connect_all(self):
        # 后台启动连接，避免无限重连阻塞 AstrBot 插件初始化流程
        for name, client in self.websocket_clients.items():
            if client.enable:
                await self.reconnect(name)
        
        if self._monitor_task is None:
            self._monitor_task = asyncio.create_task(self._monitor_connections())
        
        return len([client for client in self.websocket_clients.values() if client.enable])
    
    async def send_message(self, message: Any) -> int:
        success_count = 0
        
        for name, client in self.websocket_clients.items():
            if not client.enable:
                continue
                
            if not self.connected.get(name, False) or not self.websockets.get(name):
                logger.debug(f"[Bot通信] WebSocket客户端 {name} ({client.uri}) 未连接，跳过发送")
                continue
            
            try:
                if isinstance(message, dict):
                    if client.message_post_format == "array":
                        if "message" in message and isinstance(message["message"], list):
                            await self.websockets[name].send(json.dumps(message, ensure_ascii=False))
                        elif "message" in message and isinstance(message["message"], str):
                            message_copy = message.copy()
                            message_copy["message"] = [{"type": "text", "data": {"text": message["message"]}}]
                            await self.websockets[name].send(json.dumps(message_copy, ensure_ascii=False))
                        else:
                            await self.websockets[name].send(json.dumps(message, ensure_ascii=False))
                    else:
                        await self.websockets[name].send(json.dumps(message, ensure_ascii=False))
                else:
                    await self.websockets[name].send(str(message))
                success_count += 1
            except websockets.exceptions.ConnectionClosed:
                logger.error(f"[Bot通信] WebSocket客户端 {name} ({client.uri}) 连接已关闭，准备重连")
                self.connected[name] = False
                await self.reconnect(name)
            except Exception as e:
                logger.error(f"[Bot通信] 消息发送失败到WebSocket客户端 {name} ({client.uri}): {e}")
                self.connected[name] = False
                await self.reconnect(name)
        
        if success_count > 0:
            logger.debug(f"[Bot通信] 消息已发送到 {success_count} 个WebSocket客户端")
        else:
            logger.warning("[Bot通信] 消息发送失败，所有WebSocket客户端未连接")
        
        return success_count
    
    async def receive_messages(self, client_name: str):
        client = self.websocket_clients.get(client_name)
        if not client:
            logger.error(f"[Bot通信] WebSocket客户端 {client_name} 不存在")
            return
        
        try:
            websocket = self.websockets.get(client_name)
            if not websocket:
                logger.error(f"[Bot通信] WebSocket客户端 {client_name} ({client.uri}) 连接不存在")
                self.connected[client_name] = False
                await self.reconnect(client_name)
                return
            
            logger.debug(f"[Bot通信] 开始接收WebSocket客户端 {client_name} ({client.uri}) 的消息")
            while self.connected.get(client_name, False) and websocket == self.websockets.get(client_name):
                try:
                    message = await websocket.recv()
                    
                    try:
                        data = json.loads(message)
                        logger.debug(f"[Bot通信] 收到WebSocket客户端 {client_name} ({client.uri}) 的消息: {data}")
                        
                        await self.handle_message(data, client_name)
                    except json.JSONDecodeError:
                        logger.error(f"[Bot通信] 无效的JSON消息来自WebSocket客户端 {client_name} ({client.uri}): {message}")
                    except Exception as e:
                        logger.error(f"[Bot通信] 解析消息失败: {e}")
                except websockets.exceptions.ConnectionClosedOK:
                    logger.debug(f"[Bot通信] WebSocket客户端 {client_name} ({client.uri}) 连接已正常关闭")
                    break
                except websockets.exceptions.ConnectionClosedError as e:
                    logger.error(f"[Bot通信] WebSocket客户端 {client_name} ({client.uri}) 连接异常关闭: {e}")
                    break
                except websockets.exceptions.ConnectionClosed:
                    logger.debug(f"[Bot通信] WebSocket客户端 {client_name} ({client.uri}) 连接已关闭")
                    break
                except Exception as e:
                    logger.error(f"[Bot通信] 接收WebSocket客户端 {client_name} ({client.uri}) 消息时发生异常: {e}")
                    await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"[Bot通信] 接收WebSocket客户端 {client_name} ({client.uri}) 消息主循环失败: {e}")
        finally:
            self.connected[client_name] = False
            if client_name in self.websockets:
                del self.websockets[client_name]
            logger.debug(f"[Bot通信] 停止接收WebSocket客户端 {client_name} ({client.uri}) 的消息")
            
            await self.reconnect(client_name)
    
    async def handle_message(self, message: Dict[str, Any], client_name: str):
        client = self.websocket_clients.get(client_name)
        if not client:
            return
            
        try:
            logger.debug(f"[Bot通信] 处理来自WebSocket客户端 {client_name} ({client.uri}) 的消息: {message}")
            
            if message.get("action"):
                await self._handle_onebot_request(message, client_name)
            else:
                logger.debug(f"[Bot通信] 未处理的消息格式: {message}")
        except Exception as e:
            logger.error(f"[Bot通信] 处理来自WebSocket客户端 {client_name} ({client.uri}) 的消息失败: {e}")
    
    async def _handle_onebot_request(self, message: Dict[str, Any], client_name: str):
        client = self.websocket_clients.get(client_name)
        if not client:
            return
            
        action = message.get("action")
        params = message.get("params", {})
        echo = message.get("echo")
        
        response: Dict[str, Any] = {
            "status": "ok",
            "data": {},
            "echo": echo
        }
        
        try:
            if action in ["send_msg", "send_group_msg", "send_private_msg"]:
                await self._handle_send_message(action, params, response, client_name)
            elif action == "get_self_info" or action == "get_login_info":
                response["data"] = {
                    "user_id": self.bot_id,
                    "nickname": "AstrBot",
                    "sex": "unknown",
                    "age": 0,
                    "level": 0,
                    "sign": "",
                    "birthday": {
                        "year": 0,
                        "month": 0,
                        "day": 0
                    },
                    "friend_group_id": 0
                }
            elif action == "get_version_info":
                response["data"] = {
                    "app_name": "AstrBot",
                    "app_version": "1.0.0",
                    "protocol_version": "v11",
                    "coolq_edition": "eridanus",
                    "coolq_directory": "",
                    "plugin_version": "1.0.0",
                    "plugin_build_number": "1",
                    "plugin_build_configuration": "release"
                }
            elif action == "get_friend_list":
                if self.friend_api:
                    response = await self.friend_api.handle_request(action, params)
                    response["echo"] = echo
                else:
                    response["data"] = []
            elif action == "get_group_list":
                if self.group_api:
                    response = await self.group_api.handle_request(action, params)
                    response["echo"] = echo
                else:
                    response["data"] = []
            elif action == "get_group_info":
                if "group_id" in params:
                    response["data"] = {
                        "group_id": params["group_id"],
                        "group_name": "未知群聊",
                        "member_count": 0,
                        "max_member_count": 0
                    }
            elif action == "get_group_member_info":
                if self.group_api:
                    response = await self.group_api.handle_request(action, params)
                    response["echo"] = echo
                elif "group_id" in params and "user_id" in params:
                    response["data"] = {
                        "user_id": params["user_id"],
                        "nickname": "未知用户",
                        "card": "",
                        "role": "member"
                    }
            elif action == "get_group_member_list":
                if self.group_api:
                    response = await self.group_api.handle_request(action, params)
                    response["echo"] = echo
                elif "group_id" in params:
                    response["data"] = []
            elif action == "get_stranger_info":
                if "user_id" in params:
                    response["data"] = {
                        "user_id": params["user_id"],
                        "nickname": "未知用户"
                    }
            elif action == "recall_message" or action == "delete_msg":
                if self.message_api:
                    response = await self.message_api.handle_request("delete_msg", params)
                    response["echo"] = echo
                else:
                    response["data"] = {"success": True}
            elif action == "set_group_ban":
                response["data"] = {"success": True}
            elif action == "set_group_whole_ban":
                response["data"] = {"success": True}
            elif action == "set_group_kick":
                response["data"] = {"success": True}
            elif action == "set_group_card":
                response["data"] = {"success": True}
            elif action == "set_group_admin":
                response["data"] = {"success": True}
            elif action == "send_like":
                response["data"] = {"success": True}
            elif action == "delete_friend":
                response["data"] = {"success": True}
            elif action == "set_group_name":
                response["data"] = {"success": True}
            elif action == "set_group_leave":
                response["data"] = {"success": True}
            elif action == "set_group_special_title":
                response["data"] = {"success": True}
            elif action == "get_record":
                if "file" in params:
                    response["data"] = {"url": params["file"]}
            elif action == "get_image":
                if "file" in params:
                    response["data"] = {"url": params["file"]}
            elif action == "get_msg":
                if self.message_api:
                    response = await self.message_api.handle_request(action, params)
                    response["echo"] = echo
                elif "message_id" in params:
                    response["data"] = {
                        "message_id": params["message_id"],
                        "message": [],
                        "raw_message": "",
                        "sender": {"user_id": 0, "nickname": "未知用户"}
                    }
            elif action == "get_cookies":
                response["data"] = {"cookies": ""}
            elif action == "get_csrf_token":
                response["data"] = {"token": 123456789}
            elif action == "get_guild_list":
                response["data"] = []
            elif action == "get_online_clients":
                response["data"] = {
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
                            "guid": "1234567890"
                        }
                    ]
                }
            elif action == "get_guild_service_profile":
                response["data"] = {
                    "guild_service_profile": {
                        "user_id": self.bot_id,
                        "nickname": "AstrBot",
                        "tiny_id": str(self.bot_id),
                        "avatar_url": f"https://q.qlogo.cn/g?b=qq&s=0&nk={self.bot_id}",
                        "guild_count": 0,
                        "joined_guild_count": 0
                    }
                }
            elif action == "_set_model_show":
                response["data"] = {
                    "model": params.get("model", ""),
                    "model_show": params.get("model_show", "")
                }
            elif action == "get_group_honor_info":
                if "group_id" in params:
                    response["data"] = {
                        "group_id": params["group_id"],
                        "current_talkative": None,
                        "talkative_list": [],
                        "performer_list": [],
                        "legend_list": [],
                        "strong_newbie_list": [],
                        "emotion_list": []
                    }
            elif action == "get_essence_msg_list":
                if "group_id" in params:
                    response["data"] = {"messages": []}
            elif action == "set_essence_msg":
                response["data"] = {"success": True}
            elif action == "delete_essence_msg":
                response["data"] = {"success": True}
            elif action == "send_group_sign":
                response["data"] = {"success": True}
            elif action == "get_forward_msg":
                if self.message_api:
                    response = await self.message_api.handle_request(action, params)
                    response["echo"] = echo
                elif "message_id" in params:
                    response["data"] = {"messages": []}
            elif action == "send_group_forward_msg":
                if self.message_api:
                    response = await self.message_api.handle_request(action, params)
                    response["echo"] = echo
                elif "group_id" in params and "messages" in params:
                    response["data"] = {"message_id": "success"}
            elif action == "send_private_forward_msg":
                if self.message_api:
                    response = await self.message_api.handle_request(action, params)
                    response["echo"] = echo
                elif "user_id" in params and "messages" in params:
                    response["data"] = {"message_id": "success"}
            elif action == "get_friend_msg_history":
                if "user_id" in params:
                    response["data"] = {"messages": []}
            elif action == "get_group_msg_history":
                if "group_id" in params:
                    response["data"] = {"messages": []}
            elif action == "download_file":
                if "url" in params:
                    response["data"] = {"file": params["url"]}
            elif action == "upload_private_file":
                response["data"] = {"file_id": "0"}
            elif action == "upload_group_file":
                response["data"] = {"file_id": "0"}
            elif action == "delete_group_file":
                response["data"] = {"success": True}
            elif action == "create_group_file_folder":
                response["data"] = {"folder_id": "0"}
            elif action == "get_group_file_system_info":
                if "group_id" in params:
                    response["data"] = {"total_space": 10737418240, "used_space": 0}
            elif action == "get_group_files_by_folder":
                if "group_id" in params:
                    response["data"] = {"files": [], "folders": []}
            elif action == "get_group_root_files":
                if "group_id" in params:
                    response["data"] = {"files": [], "folders": []}
            elif action == "get_group_file_url":
                response["data"] = {"url": ""}
            elif action == "set_friend_add_request":
                response["data"] = {"success": True}
            elif action == "set_group_add_request":
                response["data"] = {"success": True}
            elif action == "set_qq_profile":
                response["data"] = {"success": True}
            elif action == "set_qq_avatar":
                response["data"] = {"success": True}
            elif action == "get_guild_meta_by_guest":
                if "guild_id" in params:
                    response["data"] = {
                        "guild_id": params["guild_id"],
                        "guild_name": "未知频道"
                    }
            elif action == "get_guild_channel_list":
                if "guild_id" in params:
                    response["data"] = []
            elif action == "get_guild_member_list":
                if "guild_id" in params:
                    response["data"] = {"members": [], "finished": True, "next_token": ""}
            elif action == "get_guild_member_profile":
                if "guild_id" in params and "user_id" in params:
                    response["data"] = {
                        "tiny_id": str(params["user_id"]),
                        "nickname": "未知用户"
                    }
            else:
                logger.debug(f"[Bot通信] 不支持的OneBot v11动作: {action}")
                response["status"] = "failed"
                response["data"] = {"error": f"不支持的动作: {action}"}
        
        except Exception as e:
            logger.error(f"[Bot通信] 处理OneBot v11请求失败: {e}")
            response["status"] = "failed"
            if "data" not in response or response["data"] is None:
                response["data"] = {"error": str(e)}
        
        if "retcode" not in response:
            response["retcode"] = 0 if response["status"] == "ok" else 1
        
        if echo is not None and self.connected.get(client_name, False):
            try:
                await self.websockets[client_name].send(json.dumps(response))
                logger.debug(f"[Bot通信] 已发送响应到WebSocket客户端 {client_name} ({client.uri}): {response}")
            except Exception as e:
                logger.error(f"[Bot通信] 发送响应失败: {e}")
    
    async def _handle_send_message(self, action: str, params: Dict[str, Any], response: Dict[str, Any], client_name: str):
        try:
            if action in ["send_msg", "send_group_msg", "send_private_msg"]:
                content = params.get("message")
                if content is None:
                    response["status"] = "failed"
                    response["data"] = {"error": "缺少message参数"}
                    return
                
                if "group_id" in params:
                    group_id = str(params["group_id"])
                    if self.send_message_callback:
                        msg_id = await self.send_message_callback("GroupMessage", group_id, content)
                        response["data"] = {"message_id": msg_id or "0"}
                        logger.debug(f"[Bot通信] 已发送OneBot v11消息到群组 {group_id}")
                elif "user_id" in params:
                    user_id = str(params["user_id"])
                    if self.send_message_callback:
                        msg_id = await self.send_message_callback("FriendMessage", user_id, content)
                        response["data"] = {"message_id": msg_id or "0"}
                        logger.debug(f"[Bot通信] 已发送OneBot v11消息到用户 {user_id}")
                else:
                    response["status"] = "failed"
                    response["data"] = {"error": "缺少group_id或user_id参数"}
        except Exception as e:
            logger.error(f"[Bot通信] 处理发送消息失败: {e}")
            response["status"] = "failed"
            response["data"] = {"error": str(e)}
    
    async def close(self):
        logger.debug("[Bot通信] 开始关闭WebSocket管理器...")
        
        if self._monitor_task and not self._monitor_task.done():
            try:
                self._monitor_task.cancel()
                await asyncio.wait_for(self._monitor_task, timeout=5)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            except Exception as e:
                logger.error(f"[Bot通信] 取消监控任务失败: {e}")
        
        close_tasks = []
        for name, client in self.websocket_clients.items():
            if self.websockets.get(name):
                close_tasks.append(self._safe_close(name))
        
        if close_tasks:
            await asyncio.gather(*close_tasks, return_exceptions=True)
        
        for name, task in list(self.reconnect_tasks.items()):
            if not task.done():
                try:
                    task.cancel()
                    logger.debug(f"[Bot通信] 已取消WebSocket客户端 {name} 的重连任务")
                except Exception:
                    pass
        
        self.connected = {}
        self.websockets = {}
        self.reconnect_tasks = {}
        
        logger.debug("[Bot通信] WebSocket管理器已关闭")
    
    async def _safe_close(self, client_name: str):
        client = self.websocket_clients.get(client_name)
        if not client:
            return
            
        try:
            await asyncio.wait_for(
                self.websockets[client_name].close(code=1000, reason="Manager closing"),
                timeout=client.close_timeout
            )
            logger.debug(f"[Bot通信] 已关闭WebSocket客户端连接: {client_name} ({client.uri})")
        except Exception as e:
            logger.error(f"[Bot通信] 关闭WebSocket客户端连接失败 {client_name} ({client.uri}): {e}")
        finally:
            self.connected[client_name] = False
            if client_name in self.websockets:
                del self.websockets[client_name]
