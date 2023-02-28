import http.client
import mimetypes
import os
import ssl
import datetime
import shutil
import sys
from codecs import encode
from radiant_mlhub import Dataset
import threading
from apscheduler.schedulers.blocking import BlockingScheduler

context = ssl.create_default_context()
context.check_hostname = False
context.verify_mode = ssl.CERT_NONE

# non ssl connection
conn = http.client.HTTPConnection(
    host='shuttle-4-bs2.estuary.tech',
    port=1414,
)


# function to upload to delta and accepts a file path
def upload_to_delta(file_path, miner, estuary_api_key):
    # get filename
    filename = file_path.split('/')[-1]

    dataList = []
    boundary = 'wL36Yn8afVp8Ag7AmP8qZ0SA4n1v9T'
    dataList.append(encode('--' + boundary))
    dataList.append(encode('Content-Disposition: form-data; name=data; filename={0}'.format(filename)))

    fileType = mimetypes.guess_type(file_path)[0] or 'application/octet-stream'
    dataList.append(encode('Content-Type: {}'.format(fileType)))
    dataList.append(encode(''))

    with open(file_path, 'rb') as f:
        dataList.append(f.read())
    dataList.append(encode('--' + boundary))
    dataList.append(encode('Content-Disposition: form-data; name=metadata;'))

    dataList.append(encode('Content-Type: {}'.format('text/plain')))
    dataList.append(encode(''))
    dataList.append(encode(
        "{\"miner\":\"" + miner + "\",\"connection_mode\":\"e2e\",\"remove_unsealed_copies\":true,\"skip_ipni_announce\": true}"))
    dataList.append(encode('--' + boundary + '--'))
    dataList.append(encode(''))
    body = b'\r\n'.join(dataList)
    payload = body
    headers = {
        'Authorization': 'Bearer ' + estuary_api_key,
        'Content-type': 'multipart/form-data; boundary={}'.format(boundary)
    }
    conn.request("POST", "/api/v1/deal/content", payload, headers)
    res = conn.getresponse()
    data = res.read()
    print(data.decode("utf-8"))


# function to get all the files in the directory
def get_all_files(directory):
    # initializing empty file paths list
    file_paths = []

    # crawling through directory and subdirectories
    for root, directories, files in os.walk(directory):
        for filename in files:
            # join the two strings in order to form the full filepath.
            filepath = os.path.join(root, filename)
            file_paths.append(filepath)

    # returning all file paths
    return file_paths


def process_data_set(dataset, location="./all_datasets/"):
    dataset.download(location)
    batch_files()


def batch_files(location="./all_datasets/", output_location="./all_output/"):
    # get all files and upload to delta with a given SP
    files = get_all_files(location)

    batch_size = 3 * 1024 * 1024 * 1024 # 5GB
    batches = []
    batch = []
    batch_size_so_far = 0

    for file in files:
        file_size = os.path.getsize(file)
        if batch_size_so_far + file_size > batch_size:
            batches.append(batch)
            batch = []
            batch_size_so_far = 0
        batch.append(file)
        batch_size_so_far += file_size
    batches.append(batch)

    # add counter
    counter = 0
    for batch in batches:
        location = output_location + "car_" + datetime.datetime.now().strftime("%Y%m%d%H%M%S") + str(counter) + "/"
        counter += 1
        os.mkdir(location)
        batch_temp_folder = location
        # copy files to the batch temp folder
        for file in batch:
            shutil.copy(file, batch_temp_folder)


miner = sys.argv[1]
estuary_api_key = sys.argv[2]
download_only = sys.argv[3]
length_from_download = sys.argv[4]
length_to_download = sys.argv[5]
batch_all_files = sys.argv[6]
push_to_delta = sys.argv[7]
datasets = Dataset.list()

print("miner: " + miner)
print("estuary_api_key: " + estuary_api_key)
print("Dataset.__sizeof__(): " + datasets.__sizeof__().__str__())
print("length_from_download: " + length_from_download)
print("length_to_download: " + length_to_download)

if length_to_download == "":
    length_to_download = datasets.__sizeof__()

# convert lenght to download to int
length_from_download = int(length_from_download)
length_to_download = int(length_to_download)

if download_only == "true":
    threads = []
    scheduler = BlockingScheduler()
    for dataset in datasets[length_from_download:length_to_download]:
        scheduler.add_job(process_data_set, 'interval', args=(dataset,), seconds=1)
    scheduler.start()

if batch_all_files == "true":
    batch_files()

if push_to_delta == "true":
    files = get_all_files("./all_output/")
    for file in files:
        upload_to_delta(file, miner, estuary_api_key)
