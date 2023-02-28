import sqlite3
def create_database():

    # Establish connection to sqlite server
    sqlite3.connect('radiant_to_delta.db')
    create_table()

# Create the table for the Delta information
def create_table():
    connection = sqlite3.connect('radiant_to_delta.db', check_same_thread=False)
    cursor = connection.cursor()

    # (Content ID: Integer, Name of File: String, Size of File: String, CID: String)
    cursor.execute("""CREATE TABLE delta_info (
        id int,
        name text,
        size int,
        cid text,
        created_date text,
        updated_date text,
        status text,
        status_message text,
        estuary_key text)""")

    connection.commit()
    connection.close()

def add_to_database(id:int, name:str, size:int, cid:str,created_date:str,updated_date:str,status:str,status_message:str,estuary_key:str):
    """
    Save all Delta uploads to Database
    :param id: Content ID of the Delta Upload
    :param name: Name of the file
    :param size: Size of the file being added
    :param cid: Content Identifier of the Delta Upload
    """
    # Establish connection to database
    connection = sqlite3.connect('radiant_to_delta.db', check_same_thread=False)

    cursor = connection.cursor()
    # Insert information from Delta Upload
    cursor.executemany("INSERT INTO delta_info VALUES (?,?,?,?,?,?,?,?,?)",[(id,name,size,cid,created_date,updated_date,status,status_message,estuary_key)])
    connection.commit()
    connection.close()


def get_data():
    connection = sqlite3.connect('radiant_to_delta.db', check_same_thread=False)
    cursor = connection.execute('SELECT * FROM delta_info')
    data = cursor.fetchall()
    return data


# create_database()
# create_table()
# add_to_database(1,"s",123,"fdihfo","fidpajf",'gaj','dfa','afadd','asdofh')
