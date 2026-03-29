from fastapi import FastAPI
from hash_ring import ShardRouter
from schemas import User
from sqlalchemy_schemas import UserDB

app = FastAPI()
shard_router = ShardRouter()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.post("/users/")
def create_user(user: User):
    shard_router.create_record(UserDB, user.dict())
    return {"id": user.id}


@app.get("/users/{user_id}")
def get_user(user_id: str):
    user = shard_router.get_record(UserDB, user_id)
    return user


@app.put("/users/{user_id}/")
def update_user(user_id: str, user: User):
    shard_router.update_record(UserDB, user_id, user)
    return user


@app.delete("/users/{user_id}/")
def delete_user(user_id: str):
    is_deleted = shard_router.delete_record(UserDB, user_id)
    return {"status": is_deleted}
