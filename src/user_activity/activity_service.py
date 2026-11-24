import asyncio
from datetime import datetime, timezone

from fastapi import WebSocket, WebSocketDisconnect, status

from schemas.user_activity import UpdateUserActivitySchema
from security.interfaces import JWTAuthManagerInterface
from exceptions import TokenExpiredError, InvalidTokenError
from user_activity.interfaces import (
    UserActivityServiceInterface,
    UserActivityWebSocketManagerInterface,
    UserActivityRepoInterface
)


class UserActivityService(UserActivityServiceInterface):
    """Coordinates authentication and lifecycle handling for user activity WebSocket connections."""

    def __init__(
            self,
            manager: UserActivityWebSocketManagerInterface,
            activity_repo: UserActivityRepoInterface,
            jwt_auth_manager: JWTAuthManagerInterface
    ) -> None:
        """
        Initialize the service with its collaborating components.

        Args:
            manager (UserActivityWebSocketManagerInterface): Manager responsible for tracking
                active WebSocket connections and broadcasting events.
            activity_repo (UserActivityRepoInterface): Repository used to persist user activity state.
            jwt_auth_manager (JWTAuthManagerInterface): Manager used to decode and validate access tokens.
        """
        self.activity_ws_manager = manager
        self.activity_repo = activity_repo
        self.jwt_auth_manager = jwt_auth_manager

    async def process(self, websocket: WebSocket) -> None:
        """
        Handle the full lifecycle of a single user activity WebSocket connection.

        Accepts the connection, authenticates the user via a token sent as the
        first message, registers the connection, broadcasts online status,
        and cleans up (marking the user offline) once the connection closes.

        Args:
            websocket (WebSocket): The incoming WebSocket connection.

        Returns:
            None
        """
        await websocket.accept()
        try:
            message = await asyncio.wait_for(websocket.receive_json(), timeout=30)
            token = message.get("token") if isinstance(message, dict) else None
            if not isinstance(token, str) or not token:
                raise InvalidTokenError
            user_id = self.jwt_auth_manager.decode_access_token(token).get("user_id")
            if type(user_id) is not int:
                raise InvalidTokenError
        except WebSocketDisconnect:
            return
        except (asyncio.TimeoutError, ValueError, TokenExpiredError, InvalidTokenError):
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return

        manager = self.activity_ws_manager
        registered = False
        try:
            async with manager.presence_lock:
                try:
                    activity = await self.activity_repo.update_user_activity(
                        UpdateUserActivitySchema(
                            user_id=user_id, status="online", connected_at=datetime.now(timezone.utc)
                        )
                    )
                except LookupError:
                    await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
                    return

                registered = True
                await manager.connect(websocket, activity)
                users = await self.activity_repo.get_all_active_users()
                await manager.send_personal_message(
                    {"event": "online_users", "users": [user.model_dump(mode="json") for user in users]},
                    user_id,
                )

            while True:
                try:
                    await websocket.receive_json()
                except ValueError:
                    continue
        except WebSocketDisconnect:
            pass
        finally:
            if registered:
                async with manager.presence_lock:
                    try:
                        await manager.disconnect(user_id, websocket)
                    finally:
                        if not manager.active_connections.get(user_id):
                            await self.activity_repo.update_user_activity(
                                UpdateUserActivitySchema(
                                    user_id=user_id,
                                    status="offline",
                                    disconnected_at=datetime.now(timezone.utc),
                                )
                            )
