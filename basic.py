import requests


url = "http://shuttle-4-bs1.estuary.tech:1414/api/v1/deal/content"

payload=  {
 "metadata":{
  "miner:": "f01907556",
  "connection_mode":"e2e",
  "remove_unsealed_copies":"true",
  "skip_ipni_announce": "true",
 }
}
files=[
  ('data',('baga6ea4seaqhfvwbdypebhffobtxjyp4gunwgwy2ydanlvbe6uizm5hlccxqmeq.car',open('/Users/alvinreyes/Downloads/baga6ea4seaqhfvwbdypebhffobtxjyp4gunwgwy2ydanlvbe6uizm5hlccxqmeq.car','rb'),'application/octet-stream'))
]
headers = {
  'Authorization': 'Bearer EST-JUSTANEXAMPLE-ARY'
}

response = requests.request("POST", url, headers=headers, data=payload, files=files)

print(response.text)
