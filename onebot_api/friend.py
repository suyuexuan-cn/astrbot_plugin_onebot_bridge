from typing import Any

from .base import OneBotAPIHandler


class FriendAPI(OneBotAPIHandler):
    async def handle_request(self, action: str, params: dict[str, Any]) -> dict[str, Any]:
        if action == "get_friend_list":
            return await self.get_friend_list(params)
        else:
            return self._error_response(1400, f"未实现的好友API: {action}")

    async def get_friend_list(self, params: dict[str, Any]) -> dict[str, Any]:
        call_params = {}
        if params.get("no_cache") is not None:
            call_params["no_cache"] = params["no_cache"]

        result = await self.call_api("get_friend_list", call_params)
        return result
