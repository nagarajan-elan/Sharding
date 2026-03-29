#!/usr/bin/env python3
"""
Database Shard Migration Script
Migrates records from wrong shards to correct shards based on new hash ring topology.
"""

import sys
import json
import logging
from typing import Dict, List
from pathlib import Path

# Add parent directory to path to import project modules
sys.path.insert(0, str(Path(__file__).parent.parent))

import etcd3
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker, Session

from constants import DATABASE_SHARDS
from hash_ring import ConsistentHashRing
from sqlalchemy_schemas import Base, UserDB

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
ETCD_DB_CONFIG_KEY = "/myapp/config/db"
MODELS_TO_MIGRATE = [UserDB]  # Add other models here as needed


class ShardMigrator:
    """Handles migration of records between shards."""
    
    def __init__(self, dry_run: bool = True):
        self.dry_run = dry_run
        self.etcd_client = etcd3.client(host="localhost", port=2379)
        self.shard_engines: Dict[int, any] = {}
        self.shard_sessions: Dict[int, Session] = {}
        self.migration_stats = {
            'total_records': 0,
            'misplaced_records': 0,
            'migrated_records': 0,
            'failed_records': 0,
            'errors': []
        }
        
        # Initialize engines for all shards
        self._init_engines()
    
    def _init_engines(self) -> None:
        """Initialize database engines for all shards."""
        logger.info(f"Initializing {len(DATABASE_SHARDS)} shard engines")
        for shard_id, connection_string in DATABASE_SHARDS.items():
            try:
                self.shard_engines[shard_id] = create_engine(connection_string)
                logger.info(f"✓ Engine initialized for shard {shard_id}")
            except Exception as e:
                logger.error(f"✗ Failed to initialize engine for shard {shard_id}: {e}")
                raise
    
    def _get_session(self, shard_id: int) -> Session:
        """Get or create a session for a shard."""
        if shard_id not in self.shard_sessions:
            engine = self.shard_engines[shard_id]
            SessionLocal = sessionmaker(bind=engine)
            self.shard_sessions[shard_id] = SessionLocal()
        return self.shard_sessions[shard_id]
    
    def _close_sessions(self) -> None:
        """Close all active sessions."""
        for session in self.shard_sessions.values():
            session.close()
        self.shard_sessions.clear()
    
    def get_current_config(self) -> Dict:
        """Get the current shard configuration from etcd."""
        try:
            value, _ = self.etcd_client.get(ETCD_DB_CONFIG_KEY)
            if value:
                config = json.loads(value.decode())
                logger.info(f"Current config from etcd: {config}")
                return config
            else:
                logger.error("No configuration found in etcd")
                raise ValueError("No configuration found in etcd")
        except Exception as e:
            logger.error(f"Failed to get config from etcd: {e}")
            raise
    
    def get_all_records(self, model, current_shard_id: int) -> List[any]:
        """Get all records from a specific shard."""
        session = self._get_session(current_shard_id)
        try:
            records = session.query(model).all()
            return records
        except Exception as e:
            logger.error(f"Failed to query records from shard {current_shard_id}: {e}")
            return []
    
    def migrate_records(self) -> None:
        """
        Migrate misplaced records to their correct shards.
        """
        logger.info("="*80)
        logger.info("STARTING SHARD MIGRATION")
        logger.info(f"Dry Run Mode: {self.dry_run}")
        logger.info("="*80)
        
        try:
            config = self.get_current_config()
            active_shards = config.get("shards", [])
            new_ring = ConsistentHashRing(active_shards, 10)
            
            logger.info(f"Active shards: {active_shards}")
            
            # Process each model
            for model in MODELS_TO_MIGRATE:
                logger.info(f"\nProcessing model: {model.__name__}")
                self._migrate_model(model, new_ring, active_shards)
            
            self._print_migration_report()
        
        except Exception as e:
            logger.error(f"Migration failed: {e}", exc_info=True)
            raise
        
        finally:
            self._close_sessions()
    
    def _migrate_model(self, model, new_ring: ConsistentHashRing, active_shards: List[int]) -> None:
        """Migrate records for a specific model."""
        misplaced_in_shard: Dict[int, List] = {}
        
        # Scan all shards for records
        for current_shard_id in DATABASE_SHARDS.keys():
            logger.info(f"  Scanning shard {current_shard_id}...")
            records = self.get_all_records(model, current_shard_id)
            self.migration_stats['total_records'] += len(records)
            
            for record in records:
                record_id = str(record.id)
                correct_shard_id = new_ring.get_shard_id(record_id)
                
                # Check if record is in wrong shard
                if current_shard_id != correct_shard_id:
                    self.migration_stats['misplaced_records'] += 1
                    logger.warning(
                        f"    MISPLACED: {model.__name__} id={record_id[:8]}... "
                        f"(in shard {current_shard_id}, should be in {correct_shard_id})"
                    )
                    
                    if correct_shard_id not in misplaced_in_shard:
                        misplaced_in_shard[correct_shard_id] = []
                    misplaced_in_shard[correct_shard_id].append((record, current_shard_id))
        
        # Migrate misplaced records
        if misplaced_in_shard:
            logger.info(f"  Found {self.migration_stats['misplaced_records']} misplaced records")
            self._move_records(model, misplaced_in_shard)
        else:
            logger.info(f"  ✓ All records in {model.__name__} are in correct shards")
    
    def _move_records(self, model, misplaced_by_shard: Dict[int, List]) -> None:
        """Move misplaced records to their correct shards."""
        for target_shard_id, records_with_source in misplaced_by_shard.items():
            for record, source_shard_id in records_with_source:
                try:
                    record_id = str(record.id)
                    
                    # Read record details
                    source_session = self._get_session(source_shard_id)
                    db_record = (
                        source_session.query(model)
                        .filter(model.id == record.id)
                        .first()
                    )
                    
                    if not db_record:
                        logger.warning(f"    Record {record_id[:8]}... not found in shard {source_shard_id}")
                        continue
                    
                    # Extract record data
                    record_data = self._record_to_dict(db_record)
                    
                    if not self.dry_run:
                        # Write to correct shard
                        target_session = self._get_session(target_shard_id)
                        new_record = model(**record_data)
                        target_session.add(new_record)
                        target_session.flush()
                        
                        # Delete from wrong shard
                        source_session.delete(db_record)
                        
                        # Commit both transactions
                        target_session.commit()
                        source_session.commit()
                        
                        self.migration_stats['migrated_records'] += 1
                        logger.info(
                            f"    ✓ Migrated {model.__name__} id={record_id[:8]}... "
                            f"from shard {source_shard_id} to {target_shard_id}"
                        )
                    else:
                        logger.info(
                            f"    [DRY RUN] Would migrate {model.__name__} id={record_id[:8]}... "
                            f"from shard {source_shard_id} to {target_shard_id}"
                        )
                        self.migration_stats['migrated_records'] += 1
                
                except Exception as e:
                    self.migration_stats['failed_records'] += 1
                    self.migration_stats['errors'].append(
                        f"Failed to migrate record {record.id}: {str(e)}"
                    )
                    logger.error(
                        f"    ✗ Failed to migrate {model.__name__} id={record.id}: {e}",
                        exc_info=True
                    )
    
    def _record_to_dict(self, record) -> Dict:
        """Convert a SQLAlchemy record to a dictionary."""
        mapper = inspect(type(record))
        return {c.key: getattr(record, c.key) for c in mapper.columns}
    
    def verify_migration(self) -> None:
        """Verify that all records are in correct shards."""
        logger.info("\n" + "="*80)
        logger.info("VERIFYING MIGRATION")
        logger.info("="*80)
        
        try:
            config = self.get_current_config()
            active_shards = config.get("shards", [])
            new_ring = ConsistentHashRing(active_shards, 10)
            
            misplaced_count = 0
            
            for model in MODELS_TO_MIGRATE:
                logger.info(f"\nVerifying {model.__name__}...")
                
                for shard_id in DATABASE_SHARDS.keys():
                    records = self.get_all_records(model, shard_id)
                    
                    for record in records:
                        record_id = str(record.id)
                        correct_shard = new_ring.get_shard_id(record_id)
                        
                        if shard_id != correct_shard:
                            misplaced_count += 1
                            logger.error(
                                f"  STILL MISPLACED: {model.__name__} id={record_id[:8]}... "
                                f"in shard {shard_id}, should be in {correct_shard}"
                            )
            
            if misplaced_count == 0:
                logger.info("✓ Verification successful! All records are in correct shards.")
            else:
                logger.warning(f"✗ Found {misplaced_count} records still in wrong shards!")
        
        except Exception as e:
            logger.error(f"Verification failed: {e}", exc_info=True)
        
        finally:
            self._close_sessions()
    
    def _print_migration_report(self) -> None:
        """Print a detailed migration report."""
        logger.info("\n" + "="*80)
        logger.info("MIGRATION REPORT")
        logger.info("="*80)
        logger.info(f"Total Records Scanned: {self.migration_stats['total_records']}")
        logger.info(f"Misplaced Records Found: {self.migration_stats['misplaced_records']}")
        logger.info(f"Records Migrated: {self.migration_stats['migrated_records']}")
        logger.info(f"Failed Migrations: {self.migration_stats['failed_records']}")
        
        if self.migration_stats['errors']:
            logger.info("\nErrors:")
            for error in self.migration_stats['errors']:
                logger.error(f"  - {error}")
        
        logger.info("="*80 + "\n")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Migrate records from wrong shards to correct shards"
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        default=False,
        help="Execute migration (default: dry run)"
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        default=False,
        help="Only verify migration without migrating"
    )
    
    args = parser.parse_args()
    
    migrator = ShardMigrator(dry_run=not args.execute)
    
    try:
        if args.verify_only:
            migrator.verify_migration()
        else:
            migrator.migrate_records()
            if args.execute:
                migrator.verify_migration()
    
    except KeyboardInterrupt:
        logger.info("Migration cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
