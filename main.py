import http.client
import mimetypes
import os
import ssl
import sys
from codecs import encode
from radiant_mlhub import Dataset
import threading

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


def process_data_set(dataset):
    dataset.download()

    # get all files and upload to delta with a given SP
    files = get_all_files("./" + dataset.id)
    for file in files:
        upload_to_delta(file, miner, estuary_api_key)


miner = sys.argv[1]
estuary_api_key = sys.argv[2]
datasets = Dataset.list()

print("miner: " + miner)
print("estuary_api_key: " + estuary_api_key)
print("Dataset.__sizeof__(): " + datasets.__sizeof__().__str__())

length = datasets.__sizeof__()
threads = []
for dataset in datasets[0:length]:
    t = threading.Thread(target=process_data_set, args=(dataset,))
    threads.append(t)
    t.start()
