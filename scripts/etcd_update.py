import etcd3
import json

client = etcd3.client(host='localhost', port=2379)

config_data = {
    "state": "idle",
    "shards": [0]
}

# Store entire JSON in one key
client.put("/myapp/config/db", json.dumps(config_data))
    
# Retrieve and parse
value, _ = client.get("/myapp/config/db")
config = json.loads(value.decode())
print(f"Updated config: {config}")