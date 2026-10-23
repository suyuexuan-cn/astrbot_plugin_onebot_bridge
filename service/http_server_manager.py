import asyncio
import json
import time
from dataclasses import dataclass
from functools import partial
from typing import Any

from aiohttp import WSMsgType, web
from astrbot.api import logger

from .onebot_actions import (
    OneBotActionDispatcher,
    message_to_cq_string,
    should_skip_self_message,
)


@dataclass
class HTTPServerConfig:
    enable: bool = True
    name: str = "http_server"
    host: str = "127.0.0.1"
    port: int = 3000
    token: str = ""
    debug: bool = False
    enable_cors: bool = True
    enable_websocket: bool = False
    report_self_message: bool = False
    enable_force_push_event: bool = True
    message_post_format: str = "array"

    @classmethod
    def from_dict(cls, config_dict: dict[str, Any]) -> "HTTPServerConfig":
        return cls(
            enable=config_dict.get("enable", True),
            name=config_dict.get("name", "http_server"),
            host=config_dict.get("host", "127.0.0.1"),
            port=config_dict.get("port", 3000),
            token=config_dict.get("token", ""),
            debug=config_dict.get("debug", False),
            enable_cors=config_dict.get("enable_cors", True),
            enable_websocket=config_dict.get("enable_websocket", False),
            report_self_message=config_dict.get("report_self_message", False),
            enable_force_push_event=config_dict.get("enable_force_push_event", True),
            message_post_format=config_dict.get("message_post_format", "array"),
        )


class HTTPServerManager:
    def __init__(self, config: dict[str, Any], bot_id: str, send_message_callback):
        self.config = config
        self.bot_id = bot_id
        self.send_message_callback = send_message_callback

        self.http_servers: dict[str, HTTPServerConfig] = {}
        self.runners: dict[str, web.AppRunner] = {}
        self.sites: dict[str, web.TCPSite] = {}

        # enable_websocket 打开后，同端口的反向 WebSocket 连接
        self.ws_clients: dict[str, set[web.WebSocketResponse]] = {}

        self.dispatcher = OneBotActionDispatcher(bot_id, send_message_callback)

        self._load_http_servers_config()

    @property
    def connections(self) -> dict[str, HTTPServerConfig]:
        return self.http_servers

    def set_api_handlers(self, message_api, group_api, friend_api, raw_api=None):
        self.dispatcher.set_api_handlers(message_api, group_api, friend_api, raw_api)

    def _load_http_servers_config(self):
        websocket_clients = self.config.get("websocket_clients", [])

        if websocket_clients:
            for i, server_config in enumerate(websocket_clients):
                if isinstance(server_config, dict):
                    template_key = server_config.get("__template_key")
                    if template_key != "HTTP 服务器":
                        continue
                    name = server_config.get("name", f"http_server_{i}")
                    self.http_servers[name] = HTTPServerConfig.from_dict(server_config)
                    self.ws_clients[name] = set()
                    logger.info(
                        f"[Bot通信] 已加载HTTP服务器配置: {name} -> "
                        f"{self.http_servers[name].host}:{self.http_servers[name].port}"
                    )

    async def start_all(self):
        tasks = [self.start_server(name) for name, server in self.http_servers.items() if server.enable]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        success_count = sum(1 for r in results if r is True)
        total_count = len([s for s in self.http_servers.values() if s.enable])
        logger.debug(f"[Bot通信] 已启动所有HTTP服务器，成功 {success_count}/{total_count}")

        return success_count

    async def start_server(self, server_name: str) -> bool:
        server = self.http_servers.get(server_name)
        if not server:
            logger.error(f"[Bot通信] HTTP服务器 {server_name} 不存在")
            return False

        if not server.enable:
            logger.debug(f"[Bot通信] HTTP服务器 {server_name} 已禁用，跳过启动")
            return False

        try:
            app = web.Application()

            if server.enable_cors:
                app.middlewares.append(self._cors_middleware)

            if server.token:
                app.middlewares.append(self._create_auth_middleware(server.token))

            app.router.add_post('/onebot/v11', self._handle_onebot_request)
            app.router.add_post('/api', self._handle_onebot_request)
            app.router.add_get('/health', self._handle_health_check)

            # 优化前 enable_websocket 读了配置但没有任何实现
            if server.enable_websocket:
                ws_handler = partial(self._handle_ws_connection, server_name)
                app.router.add_get('/ws', ws_handler)
                app.router.add_get('/onebot/v11/ws', ws_handler)

            runner = web.AppRunner(app)
            await runner.setup()

            site = web.TCPSite(runner, server.host, server.port)
            await site.start()

            self.runners[server_name] = runner
            self.sites[server_name] = site

            suffix = " (含 WebSocket 端点 /ws)" if server.enable_websocket else ""
            logger.info(
                f"[Bot通信] HTTP服务器 {server_name} 已启动: "
                f"http://{server.host}:{server.port}{suffix}"
            )
            return True

        except Exception as e:
            logger.error(f"[Bot通信] 启动HTTP服务器 {server_name} 失败: {e}")
            return False

    @web.middleware
    async def _cors_middleware(self, request: web.Request, handler):
        if request.method == "OPTIONS":
            response = web.Response()
        else:
            response = await handler(request)

        # WebSocket 响应在握手阶段已发送，之后不能再次写入响应头
        if isinstance(response, web.WebSocketResponse):
            return response

        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
        response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
        return response

    def _create_auth_middleware(self, token: str):
        @web.middleware
        async def auth_middleware(request: web.Request, handler):
            # 预检请求不带鉴权头，直接放行交给 CORS 中间件处理
            if request.method == "OPTIONS":
                return await handler(request)

            auth_header = request.headers.get('Authorization', '')
            if auth_header.startswith('Bearer '):
                provided_token = auth_header[7:]
            else:
                provided_token = request.headers.get('access_token', '')

            if provided_token != token:
                return web.json_response(
                    {'status': 'failed', 'retcode': 1403, 'data': None}, status=403
                )

            return await handler(request)
        return auth_middleware

    async def _handle_health_check(self, request: web.Request) -> web.Response:
        return web.json_response({'status': 'ok', 'self_id': self.bot_id})

    async def _handle_ws_connection(self, server_name: str, request: web.Request) -> web.WebSocketResponse:
        server = self.http_servers.get(server_name)
        if server is None or not server.enable_websocket:
            raise web.HTTPNotFound()

        ws = web.WebSocketResponse(heartbeat=30, max_msg_size=0)
        await ws.prepare(request)

        clients = self.ws_clients.setdefault(server_name, set())
        clients.add(ws)
        logger.info(f"[Bot通信] HTTP服务器 {server_name} 新WebSocket连接: {request.remote}")

        try:
            await ws.send_str(
                json.dumps(
                    {
                        "post_type": "meta_event",
                        "meta_event_type": "lifecycle",
                        "sub_type": "connect",
                        "self_id": self.bot_id,
                        "time": int(time.time()),
                    },
                    ensure_ascii=False,
                )
            )

            async for msg in ws:
                if msg.type != WSMsgType.TEXT:
                    continue
                try:
                    data = json.loads(msg.data)
                except json.JSONDecodeError:
                    logger.error(f"[Bot通信] HTTP服务器 {server_name} 收到无效JSON: {msg.data}")
                    continue

                if server.debug:
                    logger.debug(f"[Bot通信] HTTP服务器 {server_name} 收到WS消息: {data}")

                response = await self.dispatcher.dispatch(
                    data.get("action"), data.get("params") or {}, data.get("echo")
                )
                await ws.send_str(json.dumps(response, ensure_ascii=False))

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"[Bot通信] HTTP服务器 {server_name} WebSocket连接异常: {e}")
        finally:
            clients.discard(ws)
            logger.info(f"[Bot通信] HTTP服务器 {server_name} WebSocket连接断开: {request.remote}")

        return ws

    async def _handle_onebot_request(self, request: web.Request) -> web.Response:
        try:
            data = await request.json()
        except json.JSONDecodeError:
            return web.json_response({'status': 'failed', 'retcode': 1400, 'data': None}, status=400)
        except Exception as e:
            logger.error(f"[Bot通信] 解析HTTP请求失败: {e}")
            return web.json_response(
                {'status': 'failed', 'retcode': 1400, 'data': {'error': str(e)}}, status=400
            )

        try:
            if not isinstance(data, dict):
                return web.json_response(
                    {'status': 'failed', 'retcode': 1400, 'data': None}, status=400
                )

            response = await self.dispatcher.dispatch(
                data.get('action'), data.get('params') or {}, data.get('echo')
            )
            return web.json_response(response)
        except Exception as e:
            logger.error(f"[Bot通信] 处理HTTP请求失败: {e}")
            return web.json_response(
                {'status': 'failed', 'retcode': 1500, 'data': {'error': str(e)}}, status=500
            )

    async def send_message(self, message: Any, targets: set[str] | None = None) -> int:
        """纯 HTTP 服务器无法主动推送；开启 enable_websocket 时走 WebSocket 连接。"""
        success_count = 0

        for server_name, clients in self.ws_clients.items():
            if targets is not None and server_name not in targets:
                continue

            server = self.http_servers.get(server_name)
            if server is None or not server.enable_websocket:
                continue
            if not clients:
                continue
            if should_skip_self_message(server, message, self.bot_id):
                continue

            if isinstance(message, dict):
                payload = message
                if isinstance(payload.get("message"), (list, tuple)) and (
                    server.message_post_format == "string"
                ):
                    payload = dict(payload)
                    payload["message"] = message_to_cq_string(payload["message"])
                    payload["raw_message"] = payload.get("raw_message") or payload["message"]
                data = json.dumps(payload, ensure_ascii=False)
            else:
                data = str(message)

            for client in list(clients):
                try:
                    await client.send_str(data)
                    success_count += 1
                except Exception as e:
                    logger.error(f"[Bot通信] HTTP服务器 {server_name} 推送失败: {e}")
                    clients.discard(client)

        return success_count

    async def close(self):
        logger.debug("[Bot通信] 开始关闭HTTP服务器管理器...")

        for clients in self.ws_clients.values():
            for client in list(clients):
                try:
                    await client.close()
                except Exception as e:
                    logger.debug(f"[Bot通信] 关闭HTTP服务器WebSocket连接失败: {e}")

        for name, site in self.sites.items():
            try:
                await site.stop()
                logger.debug(f"[Bot通信] 已停止HTTP服务器: {name}")
            except Exception as e:
                logger.error(f"[Bot通信] 停止HTTP服务器 {name} 失败: {e}")

        for name, runner in self.runners.items():
            try:
                await runner.cleanup()
            except Exception as e:
                logger.error(f"[Bot通信] 清理HTTP服务器 {name} 失败: {e}")

        self.runners = {}
        self.sites = {}
        self.ws_clients = {}

        logger.debug("[Bot通信] HTTP服务器管理器已关闭")
