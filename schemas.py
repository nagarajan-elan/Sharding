from pydantic import BaseModel, Field
import uuid


class User(BaseModel):
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    name: str = ""
