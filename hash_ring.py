import hashlib
import bisect
import etcd3
import json
import logging
from typing import Any, Dict, Optional, Type
from constants import DATABASE_SHARDS
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

logger = logging.getLogger(__name__)

# Configuration constants
DEFAULT_REPLICA_NODES = 10
ETCD_DB_CONFIG_KEY = "/myapp/config/db"
MIGRATION_STATE_MIGRATING = "migrating"
MIGRATION_STATE_IDLE = "idle"


class ConsistentHashRing:
    def __init__(self, shards: list[int], nodes: int):
        self._engine_cache: Dict[int, Any] = {}
        self.shard_map: Dict[int, int] = {}
        self.ring: list[int] = []

        for shard_id in shards:
            for i in range(nodes):
                key = f"{shard_id}-{i}"
                h = self._hash(key)
                self.ring.append(h)
                self.shard_map[h] = shard_id

        self.ring.sort()

    def _hash(self, key: str) -> int:
        if not isinstance(key, str):
            key = str(key)
        return int(hashlib.sha256(key.encode()).hexdigest(), 16)

    def get_engine(self, shard_id: int) -> Any:
        if shard_id not in self._engine_cache:
            self._engine_cache[shard_id] = create_engine(DATABASE_SHARDS[shard_id])
        return self._engine_cache[shard_id]

    def get_shard_id(self, key: str) -> int:
        k = self._hash(key)
        clockwise_node_index = bisect.bisect_right(self.ring, k)
        if clockwise_node_index == len(self.ring):
            clockwise_node_index = 0

        ring_key = self.ring[clockwise_node_index]
        shard_id = self.shard_map[ring_key]
        print(f"{key}: {shard_id}")
        logger.debug(f"Selected shard {shard_id} for key {key}")
        return shard_id

    def get_session(self, key: str) -> Session:
        shard_id = self.get_shard_id(key)
        engine = self.get_engine(shard_id)
        SessionLocal = sessionmaker(bind=engine)
        return SessionLocal()


class ShardRouter:
    def __init__(
        self,
        etcd_host: str = "localhost",
        etcd_port: int = 2379,
        replica_nodes: int = DEFAULT_REPLICA_NODES,
    ):
        self.ring: Optional[ConsistentHashRing] = None
        self.old_ring: Optional[ConsistentHashRing] = None
        self.migration_state: str = MIGRATION_STATE_IDLE
        self.replica_nodes = replica_nodes
        self.etcd_client = etcd3.client(host=etcd_host, port=etcd_port)
        self.setup()
        self.etcd_client.add_watch_callback(ETCD_DB_CONFIG_KEY, self.config_watch)

    def config_watch(self, response) -> None:
        """Handle configuration changes from etcd."""
        try:
            for event in response.events:
                value = json.loads(event.value.decode())
                self.migration_state = value["state"]
                active_shards = value["shards"]
                self.old_ring = self.ring
                self.ring = ConsistentHashRing(active_shards, self.replica_nodes)
                logger.info(
                    f"DB config updated: migration_state={self.migration_state}, shards={active_shards}"
                )
                break
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.error(f"Failed to parse config from etcd: {e}")

    def setup(self) -> None:
        """Initialize ring from etcd configuration."""
        try:
            value, _ = self.etcd_client.get(ETCD_DB_CONFIG_KEY)
            config = json.loads(value.decode())
            self.migration_state = config["state"]
            active_shards = config["shards"]
            self.ring = ConsistentHashRing(active_shards, self.replica_nodes)
            logger.info(f"Initialized ring with shards {active_shards}")
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.error(f"Failed to initialize ring from etcd: {e}")
            raise

    def _is_migrating(self) -> bool:
        """Check if system is in migration state."""
        return (
            self.migration_state == MIGRATION_STATE_MIGRATING
            and self.old_ring is not None
        )

    def create_record(self, model: Type, data: Dict[str, Any]) -> Any:
        """Create a record in the shard ring (and old ring during migration)."""
        record_id = data["id"]

        if self._is_migrating() and self.old_ring.get_shard_id(
            record_id
        ) != self.ring.get_shard_id(record_id):
            try:
                session_old = self.old_ring.get_session(record_id)
                try:
                    record_old = model(**data)
                    session_old.add(record_old)
                    session_old.commit()
                    session_old.refresh(record_old)
                finally:
                    session_old.close()
            except Exception as e:
                logger.error(f"Failed to create record in old ring: {e}")

        session = self.ring.get_session(record_id)
        try:
            record = model(**data)
            session.add(record)
            session.commit()
            session.refresh(record)
            return record
        finally:
            session.close()

    def get_record(self, model: Type, record_id: Any) -> Optional[Any]:
        """Get a record from the shard ring (fallback to old ring during migration)."""
        record_id = str(record_id)

        session = self.ring.get_session(record_id)
        try:
            record = session.query(model).filter(model.id == record_id).first()
            if record:
                return record
        finally:
            session.close()

        if self._is_migrating():
            session_old = self.old_ring.get_session(record_id)
            try:
                record = session_old.query(model).filter(model.id == record_id).first()
                if record:
                    return record
            finally:
                session_old.close()

        return None

    def update_record(self, model: Type, record_id: Any, record_data: Any) -> None:
        """Update a record in the shard ring (and old ring during migration)."""
        record_id = str(record_id)
        update_dict = record_data.model_dump(exclude_unset=True)

        if self._is_migrating() and self.old_ring.get_shard_id(
            record_id
        ) != self.ring.get_shard_id(record_id):
            session_old = self.old_ring.get_session(record_id)
            try:
                rows = (
                    session_old.query(model)
                    .filter(model.id == record_id)
                    .update(update_dict, synchronize_session=False)
                )
                if rows > 0:
                    session_old.commit()
            except Exception as e:
                logger.error(f"Failed to update record in old ring: {e}")
            finally:
                session_old.close()

        session = self.ring.get_session(record_id)
        try:
            rows = (
                session.query(model)
                .filter(model.id == record_id)
                .update(update_dict, synchronize_session=False)
            )

            if rows == 0:
                # Record doesn't exist, create it
                update_dict["id"] = record_id
                new_record = model(**update_dict)
                session.add(new_record)

            session.commit()
        finally:
            session.close()

    def delete_record(self, model: Type, record_id: Any) -> bool:
        """Delete a record from the shard ring (and old ring during migration)."""
        record_id = str(record_id)

        if self._is_migrating():
            session_old = self.old_ring.get_session(record_id)
            try:
                record = session_old.query(model).filter(model.id == record_id).first()
                if record:
                    session_old.delete(record)
                    session_old.commit()
            except Exception as e:
                logger.error(f"Failed to delete record from old ring: {e}")
            finally:
                session_old.close()

        session = self.ring.get_session(record_id)
        try:
            record = session.query(model).filter(model.id == record_id).first()
            if not record:
                return False
            session.delete(record)
            session.commit()
            return True
        finally:
            session.close()
