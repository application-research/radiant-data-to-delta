import sqlite3

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from main import upload_to_delta


def create_database():
    sqlite3.connect('radiant_to_delta.db')


def create_app():
    app = FastAPI()
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    return app


app = create_app()


@app.get("/")
def read_root():
    return {"Hello": "World"}


# upload a file endpoint with a miner and estuary_api_key
@app.post("/upload")
def upload_file(miner: str, estuary_api_key: str):
    return upload_to_delta(miner, estuary_api_key)

