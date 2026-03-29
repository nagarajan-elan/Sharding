# Database Sharding
refer: https://en.wikipedia.org/wiki/Shard_(database_architecture)

Start Application
```
docker compose up -d
python scripts/etcd_update.py
uvicorn main:app --host 0.0.0.0 --port 8000
```

To update shard config and run migration
```
python scripts/etcd_update.py
python scripts/migration.py
```

Teardown
```
# Stop the fastapi app
docker compose down
```
