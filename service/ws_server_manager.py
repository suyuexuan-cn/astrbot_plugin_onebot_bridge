import asyncio
import json
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import websockets
import websockets.exceptions
from astrbot.api import logger

from .onebot_actions import (
    OneBotActionDispatcher,
    message_to_cq_string,
    should_skip_self_message,
)
from .ws_compat import extract_token, get_request_path

if TYPE_CHECKING:
    from websockets.asyncio.server import ServerConnection as WebSocketServerProtocol
else:
    WebSocketServerProtocol = Any


@dataclass
class WSServerConfig:
    enable: bool = True
    name: str = "ws_server"
    host: str = "127.0.0.1"
    port: int = 3001
    path: str = "/OneBotv11"
    token: str = ""
    debug: bool = False
    report_self_message: bool = False
    enable_force_push_event: bool = True
    message_post_format: str = "array"
    heart_interval: int = 30000

    @classmethod
    def from_dict(cls, config_dict: dict[str, Any]) -> "WSServerConfig":
        return cls(
            enable=config_dict.get("enable", True),
            name=config_dict.get("name", "ws_server"),
            host=config_dict.get("host", "127.0.0.1"),
            port=config_dict.get("port", 3001),
            path=config_dict.get("path", "/OneBotv11"),
            token=config_dict.get("token", ""),
            debug=config_dict.get("debug", False),
            report_self_message=config_dict.get("report_self_message", False),
            enable_force_push_event=config_dict.get("enable_force_push_event", True),
            message_post_format=config_dict.get("message_post_format", "array"),
            heart_interval=config_dict.get("heart_interval", 30000),
        )


class WSServerManager:
    def __init__(self, config: dict[str, Any], bot_id: str, send_message_callback):
        self.config = config
        self.bot_id = bot_id
        self.send_message_callback = send_message_callback

        self.ws_servers: dict[str, WSServerConfig] = {}
        self.ws_clients: dict[str, set[WebSocketServerProtocol]] = {}
        self.servers: dict[str, Any] = {}

        self._heartbeat_tasks: dict[str, asyncio.Task] = {}
        self._closing = False

        self.dispatcher = OneBotActionDispatcher(bot_id, send_message_callback)

        self._load_ws_servers_config()

    @property
    def connections(self) -> dict[str, WSServerConfig]:
        return self.ws_servers

    def set_api_handlers(self, message_api, group_api, friend_api, raw_api=None):
        self.dispatcher.set_api_handlers(message_api, group_api, friend_api, raw_api)

    def _load_ws_servers_config(self):
        websocket_clients = self.config.get("websocket_clients", [])

        if websocket_clients:
            for i, server_config in enumerate(websocket_clients):
                if isinstance(server_config, dict):
                    template_key = server_config.get("__template_key")
                    if template_key != "WebSocket 服务器":
                        continue
                    name = server_config.get("name", f"ws_server_{i}")
                    self.ws_servers[name] = WSServerConfig.from_dict(server_config)
                    self.ws_clients[name] = set()
                    logger.info(
                        f"[Bot通信] 已加载WebSocket服务器配置: {name} -> "
                        f"{self.ws_servers[name].host}:{self.ws_servers[name].port}"
                    )

    async def start_all(self):
        tasks = [self.start_server(name) for name, server in self.ws_servers.items() if server.enable]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        success_count = sum(1 for r in results if r is True)
        total_count = len([s for s in self.ws_servers.values() if s.enable])
        logger.debug(f"[Bot通信] 已启动所有WebSocket服务器，成功 {success_count}/{total_count}")

        return success_count

    async def start_server(self, server_name: str) -> bool:
        server = self.ws_servers.get(server_name)
        if not server:
            logger.error(f"[Bot通信] WebSocket服务器 {server_name} 不存在")
            return False

        if not server.enable:
            logger.debug(f"[Bot通信] WebSocket服务器 {server_name} 已禁用，跳过启动")
            return False

        try:
            async def handler(websocket: WebSocketServerProtocol, *args):
                # 兼容 websockets 12/13 的 (ws, path) 回调签名
                await self._handle_connection(server_name, websocket)

            ws_server = await websockets.serve(
                handler, server.host, server.port, subprotocols=["onebot.v11"]
            )

            self.servers[server_name] = ws_server

            # 优化前 heart_interval 只读进配置，服务端从未发过心跳事件
            if server.heart_interval > 0:
                self._heartbeat_tasks[server_name] = asyncio.create_task(
                    self._heartbeat_loop(server_name)
                )

            logger.info(
                f"[Bot通信] WebSocket服务器 {server_name} 已启动: "
                f"ws://{server.host}:{server.port}{server.path}"
            )
            return True

        except Exception as e:
            logger.error(f"[Bot通信] 启动WebSocket服务器 {server_name} 失败: {e}")
            return False

    async def _heartbeat_loop(self, server_name: str):
        server = self.ws_servers.get(server_name)
        if not server:
            return

        interval = server.heart_interval / 1000
        try:
            while not self._closing:
                await asyncio.sleep(interval)

                event = {
                    "post_type": "meta_event",
                    "meta_event_type": "heartbeat",
                    "status": "ok",
                    "interval": server.heart_interval,
                    "self_id": self.bot_id,
                    "time": int(time.time()),
                }
                data = json.dumps(event, ensure_ascii=False)

                for client in list(self.ws_clients.get(server_name, ())):
                    try:
                        await client.send(data)
                    except Exception:
                        self.ws_clients.get(server_name, set()).discard(client)
        except asyncio.CancelledError:
            logger.debug(f"[Bot通信] WebSocket服务器 {server_name} 心跳任务已取消")
            raise
        except Exception as e:
            logger.error(f"[Bot通信] WebSocket服务器 {server_name} 心跳任务异常: {e}")

    async def _handle_connection(self, server_name: str, websocket: WebSocketServerProtocol):
        server = self.ws_servers.get(server_name)
        if not server:
            return

        if server.token:
            # 优化前仅识别 request_headers，websockets 14+ 会读不到而一律拒绝连接
            provided_token = extract_token(websocket)
            if provided_token != server.token:
                logger.warning(f"[Bot通信] WebSocket服务器 {server_name} 拒绝未授权连接")
                await websocket.close(code=1008, reason="Unauthorized")
                return

        # path 配置项优化前只是打印日志用的，实际并不校验
        if server.path:
            base = get_request_path(websocket).split("?")[0]
            if base and base != "/" and base != server.path:
                logger.warning(
                    f"[Bot通信] WebSocket服务器 {server_name} 拒绝路径不匹配的连接: {base}"
                    f"（期望 {server.path}，可在配置中调整 path 或留空）"
                )
                await websocket.close(code=1008, reason="Invalid path")
                return

        clients = self.ws_clients.setdefault(server_name, set())
        clients.add(websocket)
        client_addr = getattr(websocket, "remote_address", "unknown")
        logger.info(f"[Bot通信] WebSocket服务器 {server_name} 新连接: {client_addr}")

        try:
            await self._send_lifecycle_event(websocket, "connect")

            async for message in websocket:
                try:
                    data = json.loads(message)
                    if server.debug:
                        logger.debug(f"[Bot通信] WebSocket服务器 {server_name} 收到消息: {data}")

                    await self._handle_message(server_name, websocket, data)
                except json.JSONDecodeError:
                    logger.error(f"[Bot通信] 无效的JSON消息: {message}")
                except Exception as e:
                    logger.error(f"[Bot通信] 处理消息失败: {e}")

        except websockets.exceptions.ConnectionClosedOK:
            logger.debug(f"[Bot通信] WebSocket服务器 {server_name} 连接正常关闭: {client_addr}")
        except websockets.exceptions.ConnectionClosedError as e:
            logger.error(f"[Bot通信] WebSocket服务器 {server_name} 连接异常关闭: {client_addr}: {e}")
        except Exception as e:
            logger.error(f"[Bot通信] WebSocket服务器 {server_name} 连接异常: {client_addr}: {e}")
        finally:
            # close() 会清空 ws_clients，这里必须容错，否则 finally 里抛 KeyError
            self.ws_clients.get(server_name, set()).discard(websocket)
            logger.info(f"[Bot通信] WebSocket服务器 {server_name} 连接断开: {client_addr}")

    async def _send_lifecycle_event(self, websocket: WebSocketServerProtocol, sub_type: str):
        event = {
            "post_type": "meta_event",
            "meta_event_type": "lifecycle",
            "sub_type": sub_type,
            "self_id": self.bot_id,
            "time": int(time.time()),
        }
        await websocket.send(json.dumps(event, ensure_ascii=False))

    async def _handle_message(
        self, server_name: str, websocket: WebSocketServerProtocol, data: dict[str, Any]
    ):
        action = data.get("action")
        params = data.get("params") or {}
        echo = data.get("echo")

        response = await self.dispatcher.dispatch(action, params, echo)
        await websocket.send(json.dumps(response, ensure_ascii=False))

    async def send_message(self, message: Any, targets: set[str] | None = None) -> int:
        success_count = 0

        for server_name, clients in self.ws_clients.items():
            if targets is not None and server_name not in targets:
                continue

            server = self.ws_servers.get(server_name)
            if server is not None and not server.enable:
                continue
            if should_skip_self_message(server, message, self.bot_id):
                continue

            for client in list(clients):
                try:
                    if isinstance(message, dict):
                        payload = message
                        if server and server.message_post_format == "string" and isinstance(
                            payload.get("message"), (list, tuple)
                        ):
                            payload = dict(payload)
                            payload["message"] = message_to_cq_string(payload["message"])
                            payload["raw_message"] = payload.get("raw_message") or payload["message"]
                        data = json.dumps(payload, ensure_ascii=False)
                    else:
                        data = str(message)

                    await client.send(data)
                    success_count += 1

                    if server and server.debug:
                        logger.debug(f"[Bot通信] WebSocket服务器 {server_name} 消息发送成功")
                except Exception as e:
                    logger.error(f"[Bot通信] WebSocket服务器发送消息失败: {e}")
                    clients.discard(client)

        return success_count

    async def close(self):
        logger.debug("[Bot通信] 开始关闭WebSocket服务器管理器...")
        self._closing = True

        heartbeat_tasks = []
        for task in list(self._heartbeat_tasks.values()):
            if not task.done():
                task.cancel()
                heartbeat_tasks.append(task)
        self._heartbeat_tasks = {}
        if heartbeat_tasks:
            await asyncio.gather(*heartbeat_tasks, return_exceptions=True)

        for clients in self.ws_clients.values():
            for client in list(clients):
                try:
                    await client.close(code=1000, reason="Server shutting down")
                except Exception as e:
                    logger.debug(f"[Bot通信] 关闭WebSocket服务器连接失败: {e}")

        for name, server in self.servers.items():
            try:
                server.close()
                await server.wait_closed()
                logger.debug(f"[Bot通信] 已关闭WebSocket服务器: {name}")
            except Exception as e:
                logger.error(f"[Bot通信] 关闭WebSocket服务器 {name} 失败: {e}")

        self.servers = {}
        self.ws_clients = {}

        logger.debug("[Bot通信] WebSocket服务器管理器已关闭")
