"""websockets 版本兼容工具。

requirements 中声明的是 ``websockets>=12.0``，而 12/13 与 14+ 的实现差异较大：

- 12/13：legacy 实现，自定义请求头参数为 ``extra_headers``，
  服务端通过 ``connection.request_headers`` 读取头部、``connection.path`` 读取路径。
- 14+：asyncio 实现（成为默认），自定义请求头参数为 ``additional_headers``，
  服务端通过 ``connection.request.headers`` 读取头部、``connection.request.path`` 读取路径。

本模块把这层差异收敛到一处，避免各管理器里写满 ``hasattr`` 判断。
"""

from typing import Any
from urllib.parse import parse_qs

import websockets
import websockets.exceptions


def _detect_major_version() -> int:
    version = str(getattr(websockets, "__version__", ""))
    head = version.split(".", 1)[0]
    try:
        return int(head)
    except ValueError:
        # 无法识别时按新实现（14+）处理，避免误用已被移除的参数
        return 14


MAJOR_VERSION = _detect_major_version()
IS_NEW_IMPL = MAJOR_VERSION >= 14

#: ``websockets.connect`` 中用于附加自定义请求头的参数名
HEADER_KWARG = "additional_headers" if IS_NEW_IMPL else "extra_headers"


def auth_header_kwargs(token: str) -> dict[str, Any]:
    """构造携带访问令牌的连接参数，token 为空时返回空字典。"""
    if not token:
        return {}
    return {HEADER_KWARG: {"Authorization": f"Bearer {token}"}}


def _headers_of(connection: Any) -> Any:
    request = getattr(connection, "request", None)
    headers = getattr(request, "headers", None)
    if headers is not None and hasattr(headers, "get"):
        return headers
    headers = getattr(connection, "request_headers", None)
    if headers is not None and hasattr(headers, "get"):
        return headers
    return None


def get_request_header(connection: Any, name: str) -> str:
    """跨版本读取握手请求头。"""
    headers = _headers_of(connection)
    if headers is None:
        return ""
    value = headers.get(name, "")
    return value or ""


def get_request_path(connection: Any) -> str:
    """跨版本读取握手请求路径（含查询串）。"""
    request = getattr(connection, "request", None)
    path = getattr(request, "path", None)
    if path:
        return path
    return getattr(connection, "path", "") or ""


def extract_token(connection: Any) -> str:
    """从握手信息中提取访问令牌。

    依次尝试 ``Authorization: Bearer``、``access_token`` 请求头、
    以及 URL 查询参数 ``?access_token=``（OneBot v11 常见三种方式）。
    """
    authorization = get_request_header(connection, "Authorization")
    if authorization[:7].lower() == "bearer ":
        return authorization[7:].strip()

    header_token = get_request_header(connection, "access_token")
    if header_token:
        return header_token

    path = get_request_path(connection)
    if "?" in path:
        query = parse_qs(path.split("?", 1)[1])
        values = query.get("access_token")
        if values:
            return values[0]
    return ""
