# Sharding

```
docker run -d \
  --name my-postgres \
  -e POSTGRES_USER=myuser \
  -e POSTGRES_PASSWORD=mypassword \
  -e POSTGRES_DB=mydatabase0 \
  -p 5432:5432 \
  postgres:15
```

```
docker exec -it 7636438c9218 psql -U myuser -d mydatabase0 -c "
CREATE TABLE users (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL DEFAULT ''
);
"
```

```
docker exec -it my-postgres psql -U myuser -c "CREATE DATABASE mydatabase1;"
docker exec -it my-postgres psql -U myuser -d mydatabase1 -c "
CREATE TABLE users (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL DEFAULT ''
);
"
```

```
docker exec -it my-postgres psql -U myuser -c "CREATE DATABASE mydatabase2;"
docker exec -it my-postgres psql -U myuser -d mydatabase2 -c "
CREATE TABLE users (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL DEFAULT ''
);
"

```
