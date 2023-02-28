import sqlite3
import time

from fastapi import FastAPI, File, UploadFile, Request
from fastapi.middleware.cors import CORSMiddleware
import requests
from fastapi.params import Form
from fastapi.responses import HTMLResponse

from jinja2 import Environment, FileSystemLoader

# Database functions that allows adding to database and fetching all data
from database_functions import create_database,create_table,add_to_database,get_data

# Connect to database
connection = sqlite3.connect('radiant_to_delta.db', check_same_thread=False)


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

# Call the Delta Stats endpoint to get the content information and add it to the database
# TODO: NEED TO TRY IT USING REAL API
def delta_stats_lookup(content_id:str, key:str):
    headers = {
        'Authorization': f'Bearer {key}',
    }

    try:
        response = requests.get(f'http://shuttle-4-bs2.estuary.tech:1414/api/v1/stats/content/{content_id}', headers=headers)

        print(response.text)

        content = response.json()['Content']

        print(content)

        # TODO: untested, need to try it with a working api...
        add_to_database(content['ID'],content['name'],content['size'],content['cid'],content['created_at'],content['updated_at'],content['status'],content['last_message'],key)

        return content

    except:
        return {"error":"Not Found"}
def call_delta_api(miner:str, key:str,file_name:str, contents:bytes):
    """
    Call the Delta API, return the content_id
    :param miner: User's Miner
    :param key: User's Estuary API Key
    :param file_name: User's Uploaded Filename
    :param contents: User's Uploaded File Bytes
    :return:
    """
    url = "http://shuttle-4-bs1.estuary.tech:1414/api/v1/deal/content"

    payload = {
        "metadata": {
            "miner:": f"{miner}",
            "connection_mode": "e2e",
            "remove_unsealed_copies": "true",
            "skip_ipni_announce": "true",
        }
    }
    files = [
        ('data',
         (file_name, contents, 'application/octet-stream'))
    ]
    headers = {
        'Authorization': f'Bearer {key}'
    }

    response = requests.request("POST", url, headers=headers, data=payload, files=files)

    print(response.json())
    return response.json().get("content_id")

@app.get("/", response_class=HTMLResponse)
def read_root():
    """
    Renders the index.html webpage, should have an 'index.html' file in the same directory as this file
    Requests all the data from the database and renders it onto the table in the index.html file

    :return: Rendered index.html file
    """
    # Get the data from the database
    data = get_data()

    # Render the HTML template with the data
    env = Environment(loader=FileSystemLoader('.'))
    template = env.get_template('index.html')
    html_content = template.render(data=data)

    # Return the HTML page
    return html_content


# TODO: Figure out the API Call in order to add to database
@app.post("/upload_files")
async def upload_files(miner: str = Form(...),key:str = Form(...),file: UploadFile = File(...)):
    """
    Uploads files to Delta and adds it to a Database.

    Calls the Delta upload endpoint to get the content_id
    Calls the Delta Stats endpoint to get the content data
    Updates the database

    :param miner: User's Miner
    :param key: User's Estuary Key
    :param file: User's uploaded file
    :return: True or False
    """
    # Read the contents into bytes to send to Delta
    contents = await file.read()

    # Post API function
    content_id = call_delta_api(miner,key,file.filename,contents)
    response_of_stats = delta_stats_lookup(content_id,key)

    # Test function to add to database
    # TODO: Delete this in PROD
    add_to_database(21335,
                    file.filename,
                    file.size,
                    "bafybeihiecc3nltivel4mckhsuzzmf42rcuzcrb357enha7bvbimohxewy",
                    "2023-02-25T02:20:54.221897Z",
                    "2023-02-25T02:20:57.354899Z",
                    "deal-proposal-failed",
                    "connecting to f0123456: lotus error: failed to load miner actor state: actor code is not miner: account",
                    "EST8070044f-d361-43d4-a0f7-afa90693029fARY")

    if response_of_stats != {"error":"Not Found"}:
        return {"success": True}

    else:
        return {"success": False}


@app.get("/upload", response_class=HTMLResponse)
async def upload():
    """
    Renders the upload.html webpage, should have an 'upload.html' file in the same directory as this file
    :return: Rendered html webpage
    """
    # Render the HTML template with the data
    env = Environment(loader=FileSystemLoader('.'))
    template = env.get_template('upload.html')
    # html_content = template.render()
    return template.render()

@app.get("/search/file/{name}")
def search_database_by_name(name:str):
    """
    Search the database using filename returns all instances of the name

    :param name: filename
    :return: All usages of the name in the database
    """
    # Connect to Database
    connection = sqlite3.connect('radiant_to_delta.db')
    cursor = connection.cursor()
    # Select all the rows that have the filename
    cursor.execute("SELECT * FROM delta_info WHERE name = ?",(name,))
    result = cursor.fetchall()
    connection.close()

    # If the result is there, return the result, else return an error statement
    if result:
        # Return the result at this endpoint
        return {"data": result}
    else:
        return {"error": "ID not found inside the Radiant Data"}

@app.get("/search/id/{id}")
def search_database_by_id(id):
    """
    Search Function to search for the Delta ID, returns one instance of the unique ID from the Database

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

# TODO: UNTESTED...
@app.get("/update_id/{id}/{estuary_key}")
async def update_id(id,estuary_key):
    """
    Updates the database with the new updated_date, status, and status_message

    Calls the Delta Stats endpoint to get the new values of 'status, status_message, and updated_date'
    Update the database with the new values

    :param id: File's ID
    :param estuary_key: User's Estuary API Key
    :return: True or False
    """
    connection = sqlite3.connect('radiant_to_delta.db', check_same_thread=False)

    try:
        # Call the Delta stats endpoint and get the content, extract status, status_message, and updated_date
        # ['content'] if it does not exist, it will panic and automatically go to the catch
        data = delta_stats_lookup(id,estuary_key)['content']

        status = data['status']
        status_message = data['status_message']
        updated_info = data['updated_date']
        cursor = connection.cursor()

        # Update the database with such values
        cursor.executemany('UPDATE delta_info SET status = ?, status_message = ?, updated_info = ? WHERE id = ?',(status, status_message,updated_info,id))
        return {"success": True}

    except:
        return {"success": False}