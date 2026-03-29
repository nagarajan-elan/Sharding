#!/usr/bin/env python3
"""
CRUD Operations Load Tester
Tests the FastAPI sharding app with continuous CRUD operations and records timing metrics.
"""

import requests
import time
import uuid
import statistics
from datetime import datetime
from typing import Dict, List
import signal
import sys

# Configuration
API_BASE_URL = "http://localhost:8000"
BATCH_SIZE = 1  # Number of CRUD operations per cycle

class TimingStats:
    """Tracks timing statistics for different operation types."""
    
    def __init__(self):
        self.operations: Dict[str, List[float]] = {
            "create": [],
            "read": [],
            "update": [],
            "delete": []
        }
        self.total_cycles = 0
        self.total_errors = 0
        self.error_log: List[str] = []
    
    def record(self, operation: str, elapsed_time: float) -> None:
        """Record a timing for an operation."""
        if operation in self.operations:
            self.operations[operation].append(elapsed_time)
    
    def record_error(self, operation: str, error: str) -> None:
        """Record an error."""
        self.total_errors += 1
        self.error_log.append(f"[{operation}] {error}")
    
    def add_cycle(self) -> None:
        """Increment cycle counter."""
        self.total_cycles += 1
    
    def get_stats(self, operation: str) -> Dict:
        """Get statistics for a specific operation."""
        times = self.operations[operation]
        if not times:
            return {}
        
        return {
            "count": len(times),
            "min": min(times),
            "max": max(times),
            "avg": statistics.mean(times),
            "median": statistics.median(times),
            "stdev": statistics.stdev(times) if len(times) > 1 else 0
        }
    
    def print_report(self) -> None:
        """Print a detailed timing report."""
        print("\n" + "="*80)
        print("CRUD OPERATIONS TIMING REPORT")
        print("="*80)
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Total Cycles: {self.total_cycles}")
        print(f"Total Errors: {self.total_errors}")
        print("-"*80)
        
        for operation in ["create", "read", "update", "delete"]:
            stats = self.get_stats(operation)
            if stats:
                print(f"\n{operation.upper()} Operations: {stats['count']} calls")
                print(f"  Min:    {stats['min']*1000:.2f}ms")
                print(f"  Max:    {stats['max']*1000:.2f}ms")
                print(f"  Avg:    {stats['avg']*1000:.2f}ms")
                print(f"  Median: {stats['median']*1000:.2f}ms")
                print(f"  StDev:  {stats['stdev']*1000:.2f}ms")
        
        if self.error_log:
            print("\n" + "-"*80)
            print("RECENT ERRORS (last 10):")
            for error in self.error_log[-10:]:
                print(f"  {error}")
        
        print("="*80 + "\n")


class CRUDTester:
    """Performs CRUD operations on the API."""
    
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.stats = TimingStats()
        self.active_users: List[str] = []
    
    def create_user(self) -> tuple[bool, str]:
        """Create a new user and return (success, user_id)."""
        user_id = str(uuid.uuid4())
        user_data = {
            "id": user_id,
            "name": f"TestUser_{user_id[:8]}"
        }
        
        start_time = time.perf_counter()
        st = time.time()
        try:
            response = requests.post(
                f"{self.base_url}/users/",
                json=user_data,
                timeout=10
            )
            print(time.time() - st)
            elapsed = time.perf_counter() - start_time
            
            if response.status_code == 200:
                self.stats.record("create", elapsed)
                self.active_users.append(user_id)
                return True, user_id
            else:
                self.stats.record_error("create", f"Status {response.status_code}: {response.text[:100]}")
                return False, ""
        
        except requests.RequestException as e:
            elapsed = time.perf_counter() - start_time
            self.stats.record_error("create", str(e)[:100])
            return False, ""
    
    def read_user(self, user_id: str) -> bool:
        """Read a user by ID."""
        start_time = time.perf_counter()
        try:
            response = requests.get(
                f"{self.base_url}/users/{user_id}",
                timeout=10
            )
            elapsed = time.perf_counter() - start_time
            
            if response.status_code == 200:
                self.stats.record("read", elapsed)
                return True
            else:
                self.stats.record_error("read", f"Status {response.status_code}")
                return False
        
        except requests.RequestException as e:
            elapsed = time.perf_counter() - start_time
            self.stats.record_error("read", str(e)[:100])
            return False
    
    def update_user(self, user_id: str) -> bool:
        """Update a user."""
        update_data = {
            "id": user_id,
            "name": f"UpdatedUser_{user_id[:8]}_{int(time.time())}"
        }
        
        start_time = time.perf_counter()
        try:
            response = requests.put(
                f"{self.base_url}/users/{user_id}/",
                json=update_data,
                timeout=10
            )
            elapsed = time.perf_counter() - start_time
            
            if response.status_code == 200:
                self.stats.record("update", elapsed)
                return True
            else:
                self.stats.record_error("update", f"Status {response.status_code}")
                return False
        
        except requests.RequestException as e:
            elapsed = time.perf_counter() - start_time
            self.stats.record_error("update", str(e)[:100])
            return False
    
    def delete_user(self, user_id: str) -> bool:
        """Delete a user."""
        start_time = time.perf_counter()
        try:
            response = requests.delete(
                f"{self.base_url}/users/{user_id}/",
                timeout=10
            )
            elapsed = time.perf_counter() - start_time
            
            if response.status_code == 200:
                self.stats.record("delete", elapsed)
                if user_id in self.active_users:
                    self.active_users.remove(user_id)
                return True
            else:
                self.stats.record_error("delete", f"Status {response.status_code}")
                return False
        
        except requests.RequestException as e:
            elapsed = time.perf_counter() - start_time
            self.stats.record_error("delete", str(e)[:100])
            return False
    
    def run_crud_cycle(self) -> None:
        """Run a complete CRUD cycle."""
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Starting CRUD Cycle #{self.stats.total_cycles + 1}")
        
        # CREATE
        for i in range(BATCH_SIZE):
            success, user_id = self.create_user()
            if success:
                print(f"  ✓ CREATE: {user_id[:8]}...")
            else:
                print(f"  ✗ CREATE failed")
        
        # READ
        for user_id in self.active_users[:BATCH_SIZE]:
            success = self.read_user(user_id)
            if success:
                print(f"  ✓ READ: {user_id[:8]}...")
            else:
                print(f"  ✗ READ failed: {user_id[:8]}...")
        
        # UPDATE
        for user_id in self.active_users[:BATCH_SIZE]:
            success = self.update_user(user_id)
            if success:
                print(f"  ✓ UPDATE: {user_id[:8]}...")
            else:
                print(f"  ✗ UPDATE failed: {user_id[:8]}...")
        
        # DELETE
        users_to_delete = self.active_users[:BATCH_SIZE]
        for user_id in users_to_delete:
            success = self.delete_user(user_id)
            if success:
                print(f"  ✓ DELETE: {user_id[:8]}...")
            else:
                print(f"  ✗ DELETE failed: {user_id[:8]}...")
        
        self.stats.add_cycle()
        print(f"  Cycle #{self.stats.total_cycles} complete")
    
    def run_loop(self, cycles: int = None) -> None:
        """Run CRUD cycles indefinitely or for specified number of cycles."""
        cycle_count = 0
        
        def signal_handler(sig, frame):
            print("\n\nInterrupt received. Generating final report...")
            self.stats.print_report()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        
        print(f"Starting CRUD Load Test (Press Ctrl+C to stop)")
        print(f"API Base URL: {self.base_url}")
        print(f"Batch Size: {BATCH_SIZE} operations per cycle")
        print(f"Target Cycles: {cycles if cycles else 'Unlimited (until Ctrl+C)'}")
        
        try:
            while cycles is None or cycle_count < cycles:
                self.run_crud_cycle()
                cycle_count += 1
                
                # Print intermediate report every 5 cycles
                if self.stats.total_cycles % 5 == 0:
                    self.stats.print_report()
                
                # Small delay between cycles
                time.sleep(0.1)
        
        except KeyboardInterrupt:
            print("\n\nInterrupt received. Generating final report...")
            self.stats.print_report()
            sys.exit(0)


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="CRUD Load Tester for Sharding App"
    )
    parser.add_argument(
        "--url",
        default=API_BASE_URL,
        help=f"API base URL (default: {API_BASE_URL})"
    )
    parser.add_argument(
        "--cycles",
        type=int,
        default=None,
        help="Number of cycles to run (default: unlimited, press Ctrl+C to stop)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5,
        help=f"Number of operations per cycle (default: {5})"
    )
    
    args = parser.parse_args()
    
    tester = CRUDTester(args.url)
    # global BATCH_SIZE
    BATCH_SIZE = args.batch_size
    
    try:
        tester.run_loop(cycles=args.cycles)
    except KeyboardInterrupt:
        print("\n\nInterrupt received. Generating final report...")
        tester.stats.print_report()
        sys.exit(0)


if __name__ == "__main__":
    main()
