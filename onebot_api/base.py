from typing import Any

from astrbot.api import logger


class ClientUnavailable(RuntimeError):
    """平台客户端尚未就绪。"""


class _FallbackClient:
    """平台客户端不可用时的占位实现。

    仅用于平台尚未就绪（例如 AstrBot 刚启动、aiocqhttp 还未连上）时保证
    OneBot 接口不会直接抛异常：列表类接口返回空列表，其余动作明确报不可用，
    由上层退回到占位数据，而不是返回一个"成功但内容为空"的响应。
    """

    async def call_action(self, action, **params):
        if action in ("get_friend_list", "get_group_list", "get_group_member_list"):
            return []
        raise ClientUnavailable(f"平台客户端不可用，无法执行 {action}")


class OneBotAPIHandler:
    def __init__(self, platform=None):
        self.platform = platform
        self._client = None

    def set_platform(self, platform):
        self.platform = platform
        self._client = None

    def _resolve_platform_client(self):
        """尽力从平台实例上取到真实的 OneBot 客户端。

        优化前这里直接构造了一个空实现并返回，导致 get_group_list /
        get_friend_list / get_msg / delete_msg 等动作全部返回空数据 ——
        调用方看到的是 status=ok，但内容永远是空的。
        """
        platform = self.platform
        if platform is None:
            return None

        getter = getattr(platform, "get_client", None)
        if callable(getter):
            try:
                client = getter()
            except Exception as e:
                logger.debug(f"[OneBot API] 调用平台 get_client 失败: {e}")
                client = None
            if client is not None:
                return client

        for attr in ("bot", "client"):
            client = getattr(platform, attr, None)
            if client is not None and (
                hasattr(client, "call_action") or hasattr(client, "api")
            ):
                return client

        return None

    def _get_client(self):
        # 占位实现不缓存，下次调用时重新尝试获取真实客户端
        if self._client is not None and not isinstance(self._client, _FallbackClient):
            return self._client

        client = self._resolve_platform_client()
        if client is not None:
            self._client = client
            logger.debug(f"[OneBot API] 已获取平台客户端: {type(client).__name__}")
            return self._client

        self._client = _FallbackClient()
        logger.debug("[OneBot API] 平台客户端不可用，使用占位实现")
        return self._client

    async def call_api(self, action: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        client = self._get_client()

        try:
            if hasattr(client, 'api') and hasattr(client.api, 'call_action'):
                logger.debug(f"[OneBot API] 调用 client.api.call_action: {action}")
                result = await client.api.call_action(action, **(params or {}))
                return self._success_response(result)
            elif hasattr(client, 'call_action'):
                logger.debug(f"[OneBot API] 调用 client.call_action: {action}")
                result = await client.call_action(action, **(params or {}))
                return self._success_response(result)
            elif hasattr(client, 'call_api'):
                logger.debug(f"[OneBot API] 调用 client.call_api: {action}")
                result = await client.call_api(action, params)
                return self._success_response(result)
            else:
                logger.warning(f"[OneBot API] 客户端不支持API调用: {type(client)}, 可用属性: {dir(client)}")
                return self._error_response(1401, "客户端不支持API调用")
        except ClientUnavailable as e:
            logger.debug(f"[OneBot API] {e}")
            return self._error_response(1403, str(e))
        except Exception as e:
            logger.error(f"[OneBot API] 调用API失败 {action}: {e}")
            return self._error_response(1400, str(e))

    def _success_response(self, data: Any = None) -> dict[str, Any]:
        return {
            "status": "ok",
            "retcode": 0,
            "data": data if data is not None else {},
            "message": "",
            "wording": "",
            "stream": "normal-action"
        }

    def _error_response(self, retcode: int, message: str) -> dict[str, Any]:
        return {
            "status": "failed",
            "retcode": retcode,
            "data": None,
            "message": message,
            "wording": message,
            "stream": "normal-action"
        }

    async def handle_request(self, action: str, params: dict[str, Any]) -> dict[str, Any]:
        return self._error_response(1400, f"未实现的API: {action}")


class RawAPI(OneBotAPIHandler):
    """把任意动作原样透传给平台客户端。

    用于群管理、请求处理、获取陌生人信息等没有专门封装的 OneBot 动作。
    """

    async def handle_request(self, action: str, params: dict[str, Any]) -> dict[str, Any]:
        return await self.call_api(action, params)

