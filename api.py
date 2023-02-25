import sqlite3

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.param_functions import Path

from main import upload_to_delta

def create_database():
    # Establish connection to sqlite server
    sqlite3.connect('radiant_to_delta.db')
    create_table()

# Create the table for the Delta information
def create_table():
    connection = sqlite3.connect('radiant_to_delta.db')
    cursor = connection.cursor()

    # (Content ID: Integer, Name of File: String, Size of File: String, CID: String)
    cursor.execute("""CREATE TABLE delta_info (id int,name text,size int,cid text)""")

    connection.commit()
    connection.close()

def add_to_database(id:int, name:str, size:int, cid:str):
    """
    Save all Delta uploads to Database
    :param id: Content ID of the Delta Upload
    :param name: Name of the file
    :param size: Size of the file being added
    :param cid: Content Identifier of the Delta Upload
    """
    # Establish connection to database
    connection = sqlite3.connect('radiant_to_delta.db')

    cursor = connection.cursor()
    # Insert information from Delta Upload
    cursor.executemany("INSERT INTO delta_info VALUES (?,?,?,?)",[(id,name,size,cid)])
    connection.commit()
    connection.close()

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
    return {"Hello": "Delta!"}


# upload a file endpoint with a miner and estuary_api_key
@app.post("/upload")
def upload_file(miner: str, estuary_api_key: str):
    return upload_to_delta(miner, estuary_api_key)

@app.get("/search/{id}")
def search_database(id: Path(...)):
    """
    Search Function to query through the database
    :param id: Content ID to query through the database
    :return: Fetched Result from the SQLite Database
    """

    # Establish Connection
    connection = sqlite3.connect('radiant_to_delta.db')
    cursor = connection.cursor()

    # Select the row where the ID is:
    cursor.execute("SELECT * FROM delta_info WHERE id = ?",(id,))
    result = cursor.fetchone()
    connection.close()

    # If the result is there, return the result, else return an error statement
    if result:
        # Return the result at this endpoint
        return {"data": result}
    else:
        return {"error": "ID not found inside the Radiant Data"}


