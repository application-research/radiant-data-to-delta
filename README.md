# Radiant MLHub Data to Delta

A simple script to download datasets from Radiant MLHub and uploads all of them to Delta.

## Flow
1 - Pull all datasets from Radiant
2 - Download and save it on the host PC / server
3 - Upload all extracted and decompressed content to Delta

This will run multiple threads on the host server. 

## Installation
```bash
pip install radiant_mlhub
```

## Usage
Initialize your Radiant API key
```
mlhub configure
API Key: Enter your API key here...
```

Run the script
```bash
python main.py <miner> <estuary_api_key>
```

# Author
Outercore Engineering Team.
