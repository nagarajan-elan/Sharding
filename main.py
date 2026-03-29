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
    shard_router.create_record(user)
    return {"id": user.id}


@app.get("/users")
def list_users():
    users = shard_router.list_records(UserDB)
    return users


@app.get("/users/{user_id}")
def get_user(user_id: str):
    user = shard_router.get_record(UserDB, user_id)
    return user


@app.put("/users/{user_id}/")
def update_user(user: User):
    shard_router.update_record(UserDB, **user)
    return {"id": user.id}


@app.delete("/users/{user_id}/")
def delete_user(user_id: str):
    is_deleted = shard_router.delete_record(UserDB, user_id)
    return {"status": is_deleted}
