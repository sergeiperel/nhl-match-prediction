from fastapi import FastAPI
from redis import Redis

app = FastAPI()
client = Redis(host="db")


@app.get("/health")
def health():
    return {"message": "Healthy!"}


@app.get("/")
def test():
    return {"message": "Hello, World!"}


@app.get("/api/ok")
def get_api():
    return {"API": "OK!"}


@app.get("/api/redis/put{x}")
def put_redis(x: str):
    client.set(x, "1")
    return {"message": "OK!"}
