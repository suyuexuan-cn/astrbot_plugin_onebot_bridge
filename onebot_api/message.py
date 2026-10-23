from typing import Any

from .base import OneBotAPIHandler


class MessageAPI(OneBotAPIHandler):
    async def handle_request(self, action: str, params: dict[str, Any]) -> dict[str, Any]:
        if action == "get_msg":
            return await self.get_msg(params)
        elif action == "delete_msg":
            return await self.delete_msg(params)
        elif action == "get_forward_msg":
            return await self.get_forward_msg(params)
        elif action == "send_group_forward_msg":
            return await self.send_group_forward_msg(params)
        elif action == "send_private_forward_msg":
            return await self.send_private_forward_msg(params)
        else:
            return self._error_response(1400, f"未实现的消息API: {action}")

    async def get_msg(self, params: dict[str, Any]) -> dict[str, Any]:
        message_id = params.get("message_id")
        if message_id is None:
            return self._error_response(1400, "缺少message_id参数")

        return await self.call_api("get_msg", {"message_id": message_id})

    async def delete_msg(self, params: dict[str, Any]) -> dict[str, Any]:
        message_id = params.get("message_id")
        if message_id is None:
            return self._error_response(1400, "缺少message_id参数")

        result = await self.call_api("delete_msg", {"message_id": message_id})
        return result

    async def get_forward_msg(self, params: dict[str, Any]) -> dict[str, Any]:
        message_id = params.get("message_id") or params.get("id")
        if message_id is None:
            return self._error_response(1400, "缺少message_id参数")

        result = await self.call_api("get_forward_msg", {"message_id": message_id})
        return result

    async def send_group_forward_msg(self, params: dict[str, Any]) -> dict[str, Any]:
        group_id = params.get("group_id")
        messages = params.get("messages")

        if group_id is None:
            return self._error_response(1400, "缺少group_id参数")
        if messages is None:
            return self._error_response(1400, "缺少messages参数")

        call_params = {
            "group_id": group_id,
            "messages": messages
        }

        if params.get("news"):
            call_params["news"] = params["news"]
        if params.get("summary"):
            call_params["summary"] = params["summary"]
        if params.get("prompt"):
            call_params["prompt"] = params["prompt"]
        if params.get("source"):
            call_params["source"] = params["source"]

        result = await self.call_api("send_group_forward_msg", call_params)
        return result

    async def send_private_forward_msg(self, params: dict[str, Any]) -> dict[str, Any]:
        user_id = params.get("user_id")
        messages = params.get("messages")

        if user_id is None:
            return self._error_response(1400, "缺少user_id参数")
        if messages is None:
            return self._error_response(1400, "缺少messages参数")

        call_params = {
            "user_id": user_id,
            "messages": messages
        }

        if params.get("news"):
            call_params["news"] = params["news"]
        if params.get("summary"):
            call_params["summary"] = params["summary"]
        if params.get("prompt"):
            call_params["prompt"] = params["prompt"]
        if params.get("source"):
            call_params["source"] = params["source"]

        result = await self.call_api("send_private_forward_msg", call_params)
        return result
