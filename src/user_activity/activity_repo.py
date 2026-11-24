from typing import List

from pymongo.asynchronous.database import AsyncDatabase

from schemas.user_activity import UserBaseDTO, UpdateUserActivitySchema, UserActivitySchema
from user_activity.interfaces import UserActivityRepoInterface


class UserActivityRepository(UserActivityRepoInterface):
    """MongoDB-backend implementation of the user activity repository."""

    def __init__(self, db: AsyncDatabase) -> None:
        """
        Initialize the repository with a MongoDB database instance.

        Args:
            db (AsyncDatabase): The PyMongo async database instance.
        """
        self.db = db

    async def create_user_activity(self, user_data: UserBaseDTO) -> None:
        """
        Create or reset a user's activity record with an initial "offline" status.

        Args:
            user_data (UserBaseDTO): Basic user data (id and email) to seed the activity record.

        Returns:
            None
        """
        await self.db["user_activity"].update_one(
            {"user_id": user_data.id},
            {"$set": {"email": user_data.email, "status": "offline"}},
            upsert=True,
        )

    async def update_user_activity(
            self,
            user_data: UpdateUserActivitySchema
    ) -> UserActivitySchema:
        """
        Update an existing activity record and return the resulting document.

        Args:
            user_data (UpdateUserActivitySchema): Fields to update on the user's activity record.

        Returns:
            UserActivitySchema: The updated activity record.
        """
        collection = self.db["user_activity"]
        query = {"user_id": user_data.user_id}
        await collection.update_one(
            query, {"$set": user_data.model_dump(exclude_none=True, exclude={"user_id"})}
        )
        document = await collection.find_one(query, {"_id": 0})
        if document is None:
            raise LookupError(f"No activity record for user {user_data.user_id}")
        return UserActivitySchema.model_validate(document)

    async def get_all_active_users(self) -> List[UserActivitySchema]:
        """
        Retrieve all users that currently have an "online" status.

        Returns:
            List[UserActivitySchema]: A list of activity records for all online users.
        """
        cursor = self.db["user_activity"].find({"status": "online"}, {"_id": 0})
        return [UserActivitySchema.model_validate(document) async for document in cursor]
