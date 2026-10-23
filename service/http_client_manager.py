import asyncio
from dataclasses import dataclass
from typing import Any

import aiohttp
from astrbot.api import logger

from .onebot_actions import message_to_cq_string, should_skip_self_message


def _normalize_endpoint(endpoint: str) -> str:
    """aiohttp 在设置 base_url 后要求相对路径以 ``/`` 开头，否则直接抛错。"""
    endpoint = (endpoint or "").strip() or "/onebot/v11"
    if endpoint.startswith(("http://", "https://")):
        return endpoint
    return endpoint if endpoint.startswith("/") else "/" + endpoint


@dataclass
class HTTPClientConfig:
    enable: bool = True
    name: str = "http_client"
    base_url: str = "http://localhost:3000"
    token: str = ""
    debug: bool = False
    report_self_message: bool = False
    enable_force_push_event: bool = True
    message_post_format: str = "array"
    timeout: int = 30
    endpoint: str = "/onebot/v11"

    @classmethod
    def from_dict(cls, config_dict: dict[str, Any]) -> "HTTPClientConfig":
        return cls(
            enable=config_dict.get("enable", True),
            name=config_dict.get("name", "http_client"),
            base_url=config_dict.get("base_url", "http://localhost:3000"),
            token=config_dict.get("token", ""),
            debug=config_dict.get("debug", False),
            report_self_message=config_dict.get("report_self_message", False),
            enable_force_push_event=config_dict.get("enable_force_push_event", True),
            message_post_format=config_dict.get("message_post_format", "array"),
            timeout=config_dict.get("timeout", 30),
            endpoint=config_dict.get("endpoint", "/onebot/v11"),
        )


class HTTPClientManager:
    def __init__(self, config: dict[str, Any], bot_id: str, send_message_callback):
        self.config = config
        self.bot_id = bot_id
        self.send_message_callback = send_message_callback

        self.http_clients: dict[str, HTTPClientConfig] = {}
        self.sessions: dict[str, aiohttp.ClientSession] = {}

        self._load_http_clients_config()

    @property
    def connections(self) -> dict[str, HTTPClientConfig]:
        return self.http_clients

    def _load_http_clients_config(self):
        websocket_clients = self.config.get("websocket_clients", [])

        if websocket_clients:
            for i, client_config in enumerate(websocket_clients):
                if isinstance(client_config, dict):
                    template_key = client_config.get("__template_key")
                    if template_key != "HTTP 客户端":
                        continue
                    name = client_config.get("name", f"http_client_{i}")
                    self.http_clients[name] = HTTPClientConfig.from_dict(client_config)
                    logger.info(
                        f"[Bot通信] 已加载HTTP客户端配置: {name} -> "
                        f"{self.http_clients[name].base_url}"
                    )

    async def connect_all(self):
        tasks = [self.init_client(name) for name, client in self.http_clients.items() if client.enable]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        success_count = sum(1 for r in results if r is True)
        total_count = len([c for c in self.http_clients.values() if c.enable])
        logger.debug(f"[Bot通信] 已初始化所有HTTP客户端，成功 {success_count}/{total_count}")

        return success_count

    async def init_client(self, client_name: str) -> bool:
        client = self.http_clients.get(client_name)
        if not client:
            logger.error(f"[Bot通信] HTTP客户端 {client_name} 不存在")
            return False

        if not client.enable:
            logger.debug(f"[Bot通信] HTTP客户端 {client_name} 已禁用，跳过初始化")
            return False

        # 重复初始化时先关闭旧会话，避免连接池泄漏
        old_session = self.sessions.pop(client_name, None)
        if old_session and not old_session.closed:
            try:
                await old_session.close()
            except Exception as e:
                logger.debug(f"[Bot通信] 关闭HTTP客户端旧会话失败 {client_name}: {e}")

        try:
            timeout = aiohttp.ClientTimeout(total=client.timeout)
            headers = {"Content-Type": "application/json"}

            if client.token:
                headers["Authorization"] = f"Bearer {client.token}"

            base_url = (client.base_url or "").strip()
            if base_url and not base_url.startswith(("http://", "https://")):
                # 漏写协议时 aiohttp 会直接抛异常，这里补上默认 http
                base_url = "http://" + base_url
                logger.debug(f"[Bot通信] HTTP客户端 {client_name} 补全 base_url: {base_url}")

            session = aiohttp.ClientSession(
                base_url=base_url,
                headers=headers,
                timeout=timeout,
            )

            self.sessions[client_name] = session

            try:
                async with session.get('/health') as resp:
                    if resp.status == 200:
                        logger.info(f"[Bot通信] HTTP客户端 {client_name} 已连接: {client.base_url}")
                    else:
                        # 大多数 OneBot v11 HTTP 实现没有 /health，404 属正常情况
                        logger.debug(
                            f"[Bot通信] HTTP客户端 {client_name} 健康检查返回 HTTP {resp.status}"
                        )
            except Exception as e:
                logger.debug(f"[Bot通信] HTTP客户端 {client_name} 未响应健康检查: {e}")

            return True

        except Exception as e:
            logger.error(f"[Bot通信] 初始化HTTP客户端 {client_name} 失败: {e}")
            return False

    async def send_message(self, message: Any, targets: set[str] | None = None) -> int:
        success_count = 0

        for name, client in self.http_clients.items():
            if targets is not None and name not in targets:
                continue
            if not client.enable:
                continue
            if should_skip_self_message(client, message, self.bot_id):
                continue

            session = self.sessions.get(name)
            if not session or session.closed:
                continue

            try:
                if isinstance(message, dict):
                    data = dict(message)
                    if client.message_post_format == "string" and isinstance(
                        data.get("message"), (list, tuple)
                    ):
                        data["message"] = message_to_cq_string(data["message"])
                else:
                    data = {
                        "post_type": "message",
                        "message": message_to_cq_string(message)
                        if client.message_post_format == "string"
                        else str(message),
                    }

                async with session.post(_normalize_endpoint(client.endpoint), json=data) as resp:
                    if 200 <= resp.status < 300:
                        success_count += 1
                        if client.debug:
                            logger.debug(f"[Bot通信] HTTP客户端 {name} 消息发送成功")
                    else:
                        logger.error(
                            f"[Bot通信] HTTP客户端 {name} 消息发送失败: HTTP {resp.status}"
                        )

            except asyncio.TimeoutError:
                logger.error(f"[Bot通信] HTTP客户端 {name} 请求超时")
            except aiohttp.ClientError as e:
                logger.error(f"[Bot通信] HTTP客户端 {name} 发送失败: {e}")
            except Exception as e:
                logger.error(f"[Bot通信] HTTP客户端 {name} 发送异常: {e}")

        return success_count

    async def close(self):
        logger.debug("[Bot通信] 开始关闭HTTP客户端管理器...")

        for name, session in list(self.sessions.items()):
            try:
                await session.close()
                logger.debug(f"[Bot通信] 已关闭HTTP客户端: {name}")
            except Exception as e:
                logger.error(f"[Bot通信] 关闭HTTP客户端 {name} 失败: {e}")

        self.sessions = {}

        logger.debug("[Bot通信] HTTP客户端管理器已关闭")
