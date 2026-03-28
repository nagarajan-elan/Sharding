from fastapi import FastAPI

from schemas import User

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.post("/users/")
def read_item(user: User):
    return user.dict()

@app.get("/users/{user_id}")
def read_item(user_id: int):
    return {"user_id": user_id}
