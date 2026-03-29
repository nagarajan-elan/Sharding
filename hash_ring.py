import hashlib
import bisect
from constants import DATABASE_SHARDS
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class ConsistentHashRing:
    def __init__(self, shards, nodes):
        self._engine_cache = {}
        self.shard_map = {}
        self.ring = []

        for shard_id in shards:
            for i in range(nodes):
                key = f"{shard_id}-{i}"
                h = self._hash(key)
                self.ring.append(h)
                self.shard_map[h] = shard_id

        self.ring.sort()

    def _hash(self, key: str) -> int:
        return int(hashlib.sha256(key.encode()).hexdigest(), 16)

    def get_engine(self, shard_id: int):
        if shard_id not in self._engine_cache:
            self._engine_cache[shard_id] = create_engine(DATABASE_SHARDS[shard_id])
        return self._engine_cache[shard_id]

    def get_shard_id(self, key: str) -> int:
        k = self._hash(key)
        clockwise_node_index = bisect.bisect_right(self.ring, k)
        if clockwise_node_index == len(self.ring):
            clockwise_node_index = 0

        ring_key = self.ring[clockwise_node_index]
        return self.shard_map[ring_key]

    def get_session(self, key: str):
        shard_id = 0
        # shard_id = self.get_shard_id(key)
        engine = self.get_engine(shard_id)
        Session = sessionmaker(bind=engine)
        return Session()


class ShardRouter:
    def __init__(
        self,
    ):
        self.ring: ConsistentHashRing = None
        self.old_ring: ConsistentHashRing = None
        self.state = None  # migrating, idle
        self.setup()

    def setup(self):
        # handle adding or removing shard logic
        self.ring = ConsistentHashRing([0], 10)

    def create_record(self, model, data):
        try:
            session = self.ring.get_session(data["id"])
            record = model(**data)
            session.add(record)
            session.commit()
            session.refresh(record)  # get DB-generated values
            return record
        finally:
            session.close()

    def get_record(self, model, id):
        try:
            session = self.ring.get_session(id)
            return session.query(model).filter(model.id == id).first()
        finally:
            session.close()

    def update_record(self, model, id, record):
        try:
            session = self.ring.get_session(id)
            record = record.model_dump(exclude_unset=True)
            rows = (
                session.query(model)
                .filter(model.id == id)
                .update(record, synchronize_session=False)
            )

            if rows == 0:
                raise ValueError("Matching record not found")

            session.commit()
        finally:
            session.close()

    def delete_record(self, model, id):
        try:
            session = self.ring.get_session(id)
            record = session.query(model).filter(model.id == id).first()
            if not record:
                return False
            session.delete(record)
            session.commit()
            return True
        finally:
            session.close()
