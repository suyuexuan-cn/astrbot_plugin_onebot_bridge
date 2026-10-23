import asyncio
import json
import random
import time
from dataclasses import dataclass
from typing import Any

import websockets
import websockets.exceptions
from astrbot.api import logger

from .onebot_actions import (
    OneBotActionDispatcher,
    message_to_cq_string,
    should_skip_self_message,
)
from .ws_compat import auth_header_kwargs


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
    def from_dict(cls, config_dict: dict[str, Any]) -> "WebSocketClientConfig":
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
    def __init__(self, config: dict[str, Any], bot_id: str, send_message_callback):
        self.config = config
        self.bot_id = bot_id
        self.send_message_callback = send_message_callback

        self.websocket_clients: dict[str, WebSocketClientConfig] = {}
        self._load_websocket_clients_config()

        self.websockets: dict[str, Any] = {}
        self.connected: dict[str, bool] = {}
        self.reconnect_tasks: dict[str, asyncio.Task] = {}

        self.initial_connect = False
        self._monitor_task = None
        self._connecting: dict[str, bool] = {}
        self._is_closing = False
        self._gave_up: set[str] = set()

        # 每条连接的后台任务，重连/关闭时必须先取消，否则心跳任务会不断累积
        self._receive_tasks: dict[str, asyncio.Task] = {}
        self._heartbeat_tasks: dict[str, asyncio.Task] = {}

        self.dispatcher = OneBotActionDispatcher(bot_id, send_message_callback)

    @property
    def connections(self) -> dict[str, WebSocketClientConfig]:
        return self.websocket_clients

    def set_api_handlers(self, message_api, group_api, friend_api, raw_api=None):
        self.dispatcher.set_api_handlers(message_api, group_api, friend_api, raw_api)

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
                    logger.info(
                        f"[Bot通信] 已加载客户端配置: {name} -> {self.websocket_clients[name].uri}"
                    )

    async def _monitor_connections(self):
        while True:
            try:
                for name, client in self.websocket_clients.items():
                    if not client.enable or self.connected.get(name, False):
                        continue
                    if name in self._gave_up:
                        # 已达 max_reconnect_attempts，不再无限重试
                        continue
                    logger.debug(
                        f"[Bot通信] WebSocket客户端 {name} ({client.uri}) 未连接，尝试重连"
                    )
                    await self.reconnect(name)
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"[Bot通信] 连接监控任务异常: {e}")
                await asyncio.sleep(10)

    def _cancel_task(self, tasks: dict[str, asyncio.Task], name: str):
        task = tasks.pop(name, None)
        if task and not task.done():
            task.cancel()

    async def _stop_client_tasks(self, client_name: str):
        """取消客户端的所有后台任务，避免重连后出现重复的心跳/接收循环。"""
        self._cancel_task(self._receive_tasks, client_name)
        self._cancel_task(self._heartbeat_tasks, client_name)

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
                    logger.error(
                        f"[Bot通信] 关闭WebSocket客户端连接失败 {client_name} ({client.uri}): {e}"
                    )
                self.connected[client_name] = False
                self.websockets.pop(client_name, None)

            await self._stop_client_tasks(client_name)

            reconnect_count = 0

            while (
                client.max_reconnect_attempts == 0
                or reconnect_count < client.max_reconnect_attempts
            ):
                try:
                    logger.debug(
                        f"[Bot通信] 正在连接WebSocket客户端: {client_name} ({client.uri}) "
                        f"(尝试 {reconnect_count + 1})"
                    )

                    connect_kwargs: dict[str, Any] = {
                        "ping_interval": client.ping_interval,
                        "ping_timeout": client.ping_timeout,
                        "close_timeout": client.close_timeout,
                        "max_size": None,
                    }
                    # 优化前：配置里的 token 从未下发，副 Bot 开启鉴权时握手必然失败
                    connect_kwargs.update(auth_header_kwargs(client.token))

                    websocket = await asyncio.wait_for(
                        websockets.connect(client.uri, **connect_kwargs),
                        timeout=client.connect_timeout,
                    )

                    self.websockets[client_name] = websocket
                    self.connected[client_name] = True
                    self._gave_up.discard(client_name)
                    logger.info(
                        f"[Bot通信] 成功连接到WebSocket客户端: {client_name} ({client.uri})"
                    )

                    self._receive_tasks[client_name] = asyncio.create_task(
                        self.receive_messages(client_name)
                    )

                    if client.heart_interval > 0:
                        self._heartbeat_tasks[client_name] = asyncio.create_task(
                            self._send_heartbeat(client_name)
                        )

                    await self.send_connection_event(client_name)

                    return True
                except asyncio.TimeoutError:
                    logger.error(
                        f"[Bot通信] 连接WebSocket客户端超时 {client_name} ({client.uri})"
                    )
                except websockets.exceptions.InvalidURI as e:
                    logger.error(
                        f"[Bot通信] WebSocket客户端URI无效 {client_name} ({client.uri}): {e}"
                    )
                    break
                except websockets.exceptions.InvalidHandshake as e:
                    logger.error(
                        f"[Bot通信] WebSocket客户端握手失败 {client_name} ({client.uri}): {e}"
                    )
                except ConnectionRefusedError as e:
                    logger.error(
                        f"[Bot通信] WebSocket客户端连接被拒绝 {client_name} ({client.uri}): {e}"
                    )
                except EOFError as e:
                    logger.error(
                        f"[Bot通信] WebSocket客户端连接中断 {client_name} ({client.uri}): {e}"
                    )
                except Exception as e:
                    logger.error(
                        f"[Bot通信] 连接WebSocket客户端失败 {client_name} ({client.uri}): "
                        f"{type(e).__name__}: {e}"
                    )

                self.connected[client_name] = False
                self.websockets.pop(client_name, None)

                base_delay = 5
                jitter = random.uniform(0, 1)
                reconnect_delay = min(base_delay * (2**reconnect_count) + jitter, 60)

                logger.debug(
                    f"[Bot通信] {reconnect_delay:.1f}秒后重试连接WebSocket客户端 "
                    f"{client_name} ({client.uri})"
                )
                await asyncio.sleep(reconnect_delay)
                reconnect_count += 1

            if client.max_reconnect_attempts > 0 and reconnect_count >= client.max_reconnect_attempts:
                logger.error(
                    f"[Bot通信] 超过最大重连次数({client.max_reconnect_attempts})，"
                    f"停止尝试连接WebSocket客户端: {client_name} ({client.uri})"
                )
                self.connected[client_name] = False
                # 优化前该配置只影响当前这次重试，30 秒后的监控任务仍会无限重连
                self._gave_up.add(client_name)

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
                        "self_id": self.bot_id,
                    }

                    websocket = self.websockets.get(client_name)
                    if websocket:
                        await websocket.send(
                            json.dumps(heartbeat_event, ensure_ascii=False)
                        )
                        if client.debug:
                            logger.debug(
                                f"[Bot通信] 已发送心跳到WebSocket客户端: {client_name} ({client.uri})"
                            )
                except Exception as e:
                    logger.error(
                        f"[Bot通信] 发送心跳失败 {client_name} ({client.uri}): {e}"
                    )
                    break
        except asyncio.CancelledError:
            logger.debug(f"[Bot通信] 心跳任务已取消: {client_name} ({client.uri})")
            raise
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
                "self_id": self.bot_id,
            }

            websocket = self.websockets.get(client_name)
            if self.connected.get(client_name, False) and websocket:
                await websocket.send(json.dumps(connection_event, ensure_ascii=False))
                logger.debug(
                    f"[Bot通信] 已发送连接事件到WebSocket客户端: {client_name} ({client.uri})"
                )
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

        if self._monitor_task is None or self._monitor_task.done():
            self._monitor_task = asyncio.create_task(self._monitor_connections())

        return len([c for c in self.websocket_clients.values() if c.enable])

    async def send_message(self, message: Any, targets: set[str] | None = None) -> int:
        """推送事件。

        ``targets`` 为 ``None`` 时推给全部连接，否则只推给指定名称的连接 ——
        转发规则本来就按连接配置，全局广播会让消息重复投递到不该收的连接。
        """
        success_count = 0
        attempted = 0

        for name, client in self.websocket_clients.items():
            if targets is not None and name not in targets:
                continue
            if not client.enable:
                continue
            if should_skip_self_message(client, message, self.bot_id):
                continue

            websocket = self.websockets.get(name)
            if not self.connected.get(name, False) or not websocket:
                logger.debug(
                    f"[Bot通信] WebSocket客户端 {name} ({client.uri}) 未连接，跳过发送"
                )
                continue

            attempted += 1

            try:
                if isinstance(message, dict):
                    payload = dict(message)
                    if client.message_post_format == "string" and isinstance(
                        payload.get("message"), (list, tuple)
                    ):
                        # 优化前该配置项被忽略，始终发送数组格式
                        payload["message"] = message_to_cq_string(payload["message"])
                        payload["raw_message"] = payload.get("raw_message") or payload["message"]
                    elif client.message_post_format == "array":
                        content = payload.get("message")
                        if isinstance(content, str):
                            payload["message"] = [
                                {"type": "text", "data": {"text": content}}
                            ]
                    await websocket.send(json.dumps(payload, ensure_ascii=False))
                else:
                    await websocket.send(str(message))
                success_count += 1
            except websockets.exceptions.ConnectionClosed:
                logger.error(
                    f"[Bot通信] WebSocket客户端 {name} ({client.uri}) 连接已关闭，准备重连"
                )
                self.connected[name] = False
                await self.reconnect(name)
            except Exception as e:
                logger.error(
                    f"[Bot通信] 消息发送失败到WebSocket客户端 {name} ({client.uri}): {e}"
                )
                self.connected[name] = False
                await self.reconnect(name)

        if success_count > 0:
            logger.debug(f"[Bot通信] 消息已发送到 {success_count} 个WebSocket客户端")
        elif attempted > 0:
            # 只有确实尝试过发送才告警，否则只用 HTTP/SSE 的场景每条消息都会刷这条警告
            logger.warning("[Bot通信] 消息发送失败，WebSocket客户端连接不可用")

        return success_count

    async def receive_messages(self, client_name: str):
        client = self.websocket_clients.get(client_name)
        if not client:
            logger.error(f"[Bot通信] WebSocket客户端 {client_name} 不存在")
            return

        websocket = self.websockets.get(client_name)
        if not websocket:
            logger.error(
                f"[Bot通信] WebSocket客户端 {client_name} ({client.uri}) 连接不存在"
            )
            self.connected[client_name] = False
            await self.reconnect(client_name)
            return

        try:
            logger.debug(
                f"[Bot通信] 开始接收WebSocket客户端 {client_name} ({client.uri}) 的消息"
            )
            while self.connected.get(client_name, False) and websocket == self.websockets.get(
                client_name
            ):
                try:
                    message = await websocket.recv()

                    try:
                        data = json.loads(message)
                        logger.debug(
                            f"[Bot通信] 收到WebSocket客户端 {client_name} ({client.uri}) 的消息: {data}"
                        )
                        await self.handle_message(data, client_name)
                    except json.JSONDecodeError:
                        logger.error(
                            f"[Bot通信] 无效的JSON消息来自WebSocket客户端 {client_name} "
                            f"({client.uri}): {message}"
                        )
                    except Exception as e:
                        logger.error(f"[Bot通信] 解析消息失败: {e}")
                except websockets.exceptions.ConnectionClosedOK:
                    logger.debug(
                        f"[Bot通信] WebSocket客户端 {client_name} ({client.uri}) 连接已正常关闭"
                    )
                    break
                except websockets.exceptions.ConnectionClosedError as e:
                    logger.error(
                        f"[Bot通信] WebSocket客户端 {client_name} ({client.uri}) 连接异常关闭: {e}"
                    )
                    break
                except websockets.exceptions.ConnectionClosed:
                    logger.debug(
                        f"[Bot通信] WebSocket客户端 {client_name} ({client.uri}) 连接已关闭"
                    )
                    break
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(
                        f"[Bot通信] 接收WebSocket客户端 {client_name} ({client.uri}) "
                        f"消息时发生异常: {e}"
                    )
                    await asyncio.sleep(1)
        except asyncio.CancelledError:
            logger.debug(
                f"[Bot通信] 接收任务已取消: {client_name} ({client.uri})"
            )
            raise
        except Exception as e:
            logger.error(
                f"[Bot通信] 接收WebSocket客户端 {client_name} ({client.uri}) 消息主循环失败: {e}"
            )
        finally:
            # 只有当前连接仍是这条时才重置状态，避免覆盖新连接
            if self.websockets.get(client_name) is websocket:
                self.connected[client_name] = False
                self.websockets.pop(client_name, None)
                logger.debug(
                    f"[Bot通信] 停止接收WebSocket客户端 {client_name} ({client.uri}) 的消息"
                )
                if not self._is_closing:
                    await self.reconnect(client_name)

    async def handle_message(self, message: dict[str, Any], client_name: str):
        client = self.websocket_clients.get(client_name)
        if not client:
            return

        try:
            logger.debug(
                f"[Bot通信] 处理来自WebSocket客户端 {client_name} ({client.uri}) 的消息: {message}"
            )

            if message.get("action"):
                await self._handle_onebot_request(message, client_name)
            else:
                logger.debug(f"[Bot通信] 未处理的消息格式: {message}")
        except Exception as e:
            logger.error(
                f"[Bot通信] 处理来自WebSocket客户端 {client_name} ({client.uri}) 的消息失败: {e}"
            )

    async def _handle_onebot_request(self, message: dict[str, Any], client_name: str):
        client = self.websocket_clients.get(client_name)
        if not client:
            return

        echo = message.get("echo")
        response = await self.dispatcher.dispatch(
            message.get("action"), message.get("params") or {}, echo
        )

        if echo is not None and self.connected.get(client_name, False):
            websocket = self.websockets.get(client_name)
            if not websocket:
                return
            try:
                # 优化前未指定 ensure_ascii，中文错误信息会被转义成 \uXXXX
                await websocket.send(json.dumps(response, ensure_ascii=False))
                logger.debug(
                    f"[Bot通信] 已发送响应到WebSocket客户端 {client_name} ({client.uri}): {response}"
                )
            except Exception as e:
                logger.error(f"[Bot通信] 发送响应失败: {e}")

    async def close(self):
        logger.debug("[Bot通信] 开始关闭WebSocket管理器...")
        self._is_closing = True

        if self._monitor_task and not self._monitor_task.done():
            try:
                self._monitor_task.cancel()
                await asyncio.wait_for(self._monitor_task, timeout=5)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            except Exception as e:
                logger.error(f"[Bot通信] 取消监控任务失败: {e}")

        for name in list(self.websocket_clients):
            await self._stop_client_tasks(name)

        close_tasks = [
            self._safe_close(name)
            for name in self.websocket_clients
            if self.websockets.get(name)
        ]

        if close_tasks:
            await asyncio.gather(*close_tasks, return_exceptions=True)

        for name, task in list(self.reconnect_tasks.items()):
            if not task.done():
                try:
                    task.cancel()
                    logger.debug(f"[Bot通信] 已取消WebSocket客户端 {name} 的重连任务")
                except Exception as e:
                    logger.debug(f"[Bot通信] 取消重连任务失败 {name}: {e}")

        self.connected = {}
        self.websockets = {}
        self.reconnect_tasks = {}
        self._receive_tasks = {}
        self._heartbeat_tasks = {}

        logger.debug("[Bot通信] WebSocket管理器已关闭")

    async def _safe_close(self, client_name: str):
        client = self.websocket_clients.get(client_name)
        if not client:
            return

        try:
            await asyncio.wait_for(
                self.websockets[client_name].close(code=1000, reason="Manager closing"),
                timeout=client.close_timeout,
            )
            logger.debug(
                f"[Bot通信] 已关闭WebSocket客户端连接: {client_name} ({client.uri})"
            )
        except Exception as e:
            logger.debug(
                f"[Bot通信] 关闭WebSocket客户端连接异常 {client_name} ({client.uri}): {e}"
            )
