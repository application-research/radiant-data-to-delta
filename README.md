# Radiant MLHub Data to Delta

A simple script to download datasets from Radiant MLHub and uploads all of them to Delta.

Note: This will run multiple threads on the host server. 

## Flow
- Pull all datasets from Radiant
- Download and save it on the host PC / server
- Upload all extracted and decompressed content to Delta

Each upload will return a delta `content_id` which can be monitored using the following delta endpoint.
```
curl --location --request GET 'http://shuttle-4-bs2.estuary.tech:1414/api/v1/stats/content/<content_id>' \
--header 'Authorization: Bearer [ESTUARY_API_KEY]'
```

Returns the following result from delta

```
{
    "piece_commitment": [
        {
            "ID": 20884,
            "cid": "bafybeihiecc3nltivel4mckhsuzzmf42rcuzcrb357enha7bvbimohxewy",
            "piece": "baga6ea4seaqbstd72jvvpdtylm5zlf4lv7eflrlbrou46nledefeacp2k2hjogy",
            "size": 3824,
            "padded_piece_size": 4096,
            "unnpadded_piece_size": 4064,
            "status": "open",
            "last_message": "",
            "created_at": "2023-02-25T02:20:55.195719Z",
            "updated_at": "2023-02-25T02:20:55.195719Z"
        }
    ],
    "content": {
        "ID": 21395,
        "name": "nasa_marine_debris_labels_20181124_155715_1049_16765-29692-16.json",
        "size": 3716,
        "cid": "bafybeihiecc3nltivel4mckhsuzzmf42rcuzcrb357enha7bvbimohxewy",
        "piece_commitment_id": 20884,
        "status": "deal-proposal-failed",
        "connection_mode": "e2e",
        "last_message": "connecting to f0123456: lotus error: failed to load miner actor state: actor code is not miner: account",
        "created_at": "2023-02-25T02:20:54.221897Z",
        "updated_at": "2023-02-25T02:20:57.354899Z"
    },
    "deal_proposals": [
        {
            "ID": 19266,
            "content": 21395,
            "label": "bafybeihiecc3nltivel4mckhsuzzmf42rcuzcrb357enha7bvbimohxewy",
            "duration": 1494720,
            "remove_unsealed_copy": true,
            "skip_ipni_announce": true,
            "created_at": "2023-02-25T02:20:54.777549Z",
            "updated_at": "2023-02-25T02:20:54.777549Z"
        }
    ],
    "deals": null
}
```

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

## Download the files first
```bash
python main.py <miner> <estuary_api_key> all true false false
```

## Group the files
```bash
python main.py <miner> <estuary_api_key> all false true false
```

## Push the batch files to delta
```bash
python main.py <miner> <estuary_api_key> all false false true
```

# Author
Outercore Engineering Team.
