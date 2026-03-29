# Sharding

Start Application
```
docker compose up -d
python scripts/etcd_update.py
uvicorn main:app --host 0.0.0.0 --port 8000
```

Teardown
```
# Stop the fastapi app
docker compose down
```