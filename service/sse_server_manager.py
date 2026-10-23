import asyncio
import json
from dataclasses import dataclass
from functools import partial
from typing import Any

from aiohttp import web
from astrbot.api import logger

from .onebot_actions import (
    OneBotActionDispatcher,
    message_to_cq_string,
    should_skip_self_message,
)


@dataclass
class SSEServerConfig:
    enable: bool = True
    name: str = "sse_server"
    host: str = "127.0.0.1"
    port: int = 3001
    token: str = ""
    debug: bool = False
    enable_cors: bool = True
    message_post_format: str = "array"
    report_self_message: bool = False
    enable_force_push_event: bool = True

    @classmethod
    def from_dict(cls, config_dict: dict[str, Any]) -> "SSEServerConfig":
        return cls(
            enable=config_dict.get("enable", True),
            name=config_dict.get("name", "sse_server"),
            host=config_dict.get("host", "127.0.0.1"),
            port=config_dict.get("port", 3001),
            token=config_dict.get("token", ""),
            debug=config_dict.get("debug", False),
            enable_cors=config_dict.get("enable_cors", True),
            message_post_format=config_dict.get("message_post_format", "array"),
            report_self_message=config_dict.get("report_self_message", False),
            enable_force_push_event=config_dict.get("enable_force_push_event", True),
        )


class SSEServerManager:
    def __init__(self, config: dict[str, Any], bot_id: str, send_message_callback):
        self.config = config
        self.bot_id = bot_id
        self.send_message_callback = send_message_callback

        self.sse_servers: dict[str, SSEServerConfig] = {}
        self.runners: dict[str, web.AppRunner] = {}
        self.sites: dict[str, web.TCPSite] = {}
        self.sse_clients: dict[str, set[web.StreamResponse]] = {}

        self.dispatcher = OneBotActionDispatcher(bot_id, send_message_callback)

        self._load_sse_servers_config()

    @property
    def connections(self) -> dict[str, SSEServerConfig]:
        return self.sse_servers

    def set_api_handlers(self, message_api, group_api, friend_api, raw_api=None):
        self.dispatcher.set_api_handlers(message_api, group_api, friend_api, raw_api)

    def _load_sse_servers_config(self):
        websocket_clients = self.config.get("websocket_clients", [])

        if websocket_clients:
            for i, server_config in enumerate(websocket_clients):
                if isinstance(server_config, dict):
                    template_key = server_config.get("__template_key")
                    if template_key != "HTTP SSE 服务器":
                        continue
                    name = server_config.get("name", f"sse_server_{i}")
                    self.sse_servers[name] = SSEServerConfig.from_dict(server_config)
                    self.sse_clients[name] = set()
                    logger.info(
                        f"[Bot通信] 已加载SSE服务器配置: {name} -> "
                        f"{self.sse_servers[name].host}:{self.sse_servers[name].port}"
                    )

    async def start_all(self):
        tasks = [self.start_server(name) for name, server in self.sse_servers.items() if server.enable]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        success_count = sum(1 for r in results if r is True)
        total_count = len([s for s in self.sse_servers.values() if s.enable])
        logger.debug(f"[Bot通信] 已启动所有SSE服务器，成功 {success_count}/{total_count}")

        return success_count

    async def start_server(self, server_name: str) -> bool:
        server = self.sse_servers.get(server_name)
        if not server:
            logger.error(f"[Bot通信] SSE服务器 {server_name} 不存在")
            return False

        if not server.enable:
            logger.debug(f"[Bot通信] SSE服务器 {server_name} 已禁用，跳过启动")
            return False

        try:
            app = web.Application()

            if server.enable_cors:
                app.middlewares.append(self._cors_middleware)

            if server.token:
                app.middlewares.append(self._create_auth_middleware(server.token))

            # 优化前靠请求端口反查服务器名，多实例/反向代理下会挂错客户端集合；
            # 这里在注册路由时就把 server_name 绑定进去。
            sse_handler = partial(self._handle_sse_connection, server_name)
            app.router.add_get('/sse', sse_handler)
            app.router.add_get('/events', sse_handler)
            app.router.add_post('/onebot/v11', self._handle_onebot_request)
            app.router.add_get('/health', self._handle_health_check)

            runner = web.AppRunner(app)
            await runner.setup()

            site = web.TCPSite(runner, server.host, server.port)
            await site.start()

            self.runners[server_name] = runner
            self.sites[server_name] = site

            logger.info(f"[Bot通信] SSE服务器 {server_name} 已启动: http://{server.host}:{server.port}")
            return True

        except Exception as e:
            logger.error(f"[Bot通信] 启动SSE服务器 {server_name} 失败: {e}")
            return False

    @web.middleware
    async def _cors_middleware(self, request: web.Request, handler):
        if request.method == "OPTIONS":
            response = web.Response()
        else:
            response = await handler(request)

        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
        response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
        return response

    def _create_auth_middleware(self, token: str):
        @web.middleware
        async def auth_middleware(request: web.Request, handler):
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

    async def _handle_sse_connection(self, server_name: str, request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse()
        response.content_type = 'text/event-stream'
        response.headers['Cache-Control'] = 'no-cache'
        response.headers['Connection'] = 'keep-alive'
        response.headers['Access-Control-Allow-Origin'] = '*'

        await response.prepare(request)

        clients = self.sse_clients.get(server_name)
        if clients is None:
            clients = self.sse_clients.setdefault(server_name, set())

        clients.add(response)
        logger.info(f"[Bot通信] SSE服务器 {server_name} 新连接: {request.remote}")

        try:
            payload = json.dumps(
                {"status": "connected", "self_id": self.bot_id}, ensure_ascii=False
            )
            await response.write(f'data: {payload}\n\n'.encode())

            while True:
                await asyncio.sleep(30)
                await response.write(b': heartbeat\n\n')
        except asyncio.CancelledError:
            # 取消信号必须向上抛，否则关闭时连接会一直挂在这里
            raise
        except (ConnectionResetError, BrokenPipeError):
            pass
        except Exception as e:
            logger.debug(f"[Bot通信] SSE服务器 {server_name} 连接中断: {e}")
        finally:
            clients.discard(response)
            logger.info(f"[Bot通信] SSE服务器 {server_name} 连接断开: {request.remote}")

        return response

    async def _handle_onebot_request(self, request: web.Request) -> web.Response:
        try:
            data = await request.json()
        except json.JSONDecodeError:
            return web.json_response({'status': 'failed', 'retcode': 1400, 'data': None}, status=400)
        except Exception as e:
            logger.error(f"[Bot通信] 解析SSE HTTP请求失败: {e}")
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
            logger.error(f"[Bot通信] 处理SSE HTTP请求失败: {e}")
            return web.json_response(
                {'status': 'failed', 'retcode': 1500, 'data': {'error': str(e)}}, status=500
            )

    async def send_message(self, message: Any, targets: set[str] | None = None) -> int:
        success_count = 0

        for server_name, clients in self.sse_clients.items():
            if targets is not None and server_name not in targets:
                continue

            server = self.sse_servers.get(server_name)
            if server is not None and not server.enable:
                continue
            if not clients:
                continue
            if should_skip_self_message(server, message, self.bot_id):
                continue

            if isinstance(message, dict):
                payload = message
                # 优化前 message_post_format 读了配置但从未生效
                if isinstance(payload.get("message"), (list, tuple)) and (
                    server and server.message_post_format == "string"
                ):
                    payload = dict(payload)
                    payload["message"] = message_to_cq_string(payload["message"])
                    payload["raw_message"] = payload.get("raw_message") or payload["message"]
                data = json.dumps(payload, ensure_ascii=False)
            else:
                data = str(message)

            for client in list(clients):
                try:
                    await client.write(f'data: {data}\n\n'.encode())
                    success_count += 1
                except Exception as e:
                    logger.error(f"[Bot通信] SSE服务器 {server_name} 发送消息失败: {e}")
                    clients.discard(client)

        return success_count

    async def close(self):
        logger.debug("[Bot通信] 开始关闭SSE服务器管理器...")

        for clients in self.sse_clients.values():
            for client in list(clients):
                try:
                    await client.write(b'data: {"status": "closing"}\n\n')
                except Exception as e:
                    logger.debug(f"[Bot通信] 关闭SSE连接时写入失败: {e}")

        for name, site in self.sites.items():
            try:
                await site.stop()
                logger.debug(f"[Bot通信] 已停止SSE服务器: {name}")
            except Exception as e:
                logger.error(f"[Bot通信] 停止SSE服务器 {name} 失败: {e}")

        for name, runner in self.runners.items():
            try:
                await runner.cleanup()
            except Exception as e:
                logger.error(f"[Bot通信] 清理SSE服务器 {name} 失败: {e}")

        self.runners = {}
        self.sites = {}
        self.sse_clients = {}

        logger.debug("[Bot通信] SSE服务器管理器已关闭")
