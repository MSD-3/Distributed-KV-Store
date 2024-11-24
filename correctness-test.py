import socket
import threading
import time
import random
import concurrent.futures
import logging
import unittest
from datetime import datetime
from typing import Optional, Tuple, List
import json
from contextlib import contextmanager
import os

class KVStoreLogger:
    def __init__(self, test_name="general", iteration=0):
        self.filename = f"logs/kvstore_communication_{test_name}_iteration_{iteration}.log"
        # Create logs directory if it doesn't exist
        os.makedirs("logs", exist_ok=True)
        # Clear the file at start
        with open(self.filename, 'w') as f:
            f.write(f"=== KV Store Communication Log - {test_name} - Iteration {iteration} - Started at {datetime.now()} ===\n\n")
    
    def log_operation(self, node_id: int, operation: str, request: str, response: str, duration_ms: float, success: bool):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        log_entry = {
            "timestamp": timestamp,
            "node_id": node_id,
            "operation": operation,
            "request": request,
            "response": response,
            "duration_ms": duration_ms,
            "success": success
        }
        
        with open(self.filename, 'a') as f:
            f.write(f"\n{'='*80}\n")
            f.write(f"Timestamp: {timestamp}\n")
            f.write(f"Node: {node_id} (Port {8081 + node_id})\n")
            f.write(f"Operation: {operation}\n")
            f.write(f"Request: {request}\n")
            f.write(f"Response: {response}\n")
            f.write(f"Duration: {duration_ms:.2f}ms\n")
            f.write(f"Success: {success}\n")
            f.write(f"{'='*80}\n")

class KVStoreClient:
    def __init__(self, host: str = '127.0.0.1', base_port: int = 8081, test_name="general", iteration=0):
        self.host = host
        self.base_port = base_port
        self.current_node = 0
        self.logger = KVStoreLogger(test_name, iteration)

    @contextmanager
    def _timed_operation(self, node_id: int, operation: str, request: str):
        start_time = time.time()
        response_container = [None] 
        try:
            yield response_container
            duration = (time.time() - start_time) * 1000
            success = True
        except Exception as e:
            duration = (time.time() - start_time) * 1000
            response_container[0] = str(e)
            success = False
            raise
        finally:
            self.logger.log_operation(
                node_id=node_id,
                operation=operation,
                request=request,
                response=str(response_container[0]),
                duration_ms=duration,
                success=success
            )

    def get(self, key: str, node_id: Optional[int] = None) -> Tuple[bool, str]:
        if node_id is None:
            node_id = self.current_node
        
        request = f"GET:{key}"
        with self._timed_operation(node_id, "GET", request) as response_container:
            try:
                sock = self.connect_to_node(node_id)
                sock.send(request.encode())
                
                response = sock.recv(4096).decode()
                response_container[0] = response
                sock.close()
                
                return True, response
            except Exception as e:
                response_container[0] = str(e)
                logging.error(f"GET operation failed: {e}")
                return False, str(e)

    def put(self, key: str, value: str, node_id: Optional[int] = None) -> bool:
        if node_id is None:
            node_id = self.current_node
        
        request = f"SET:{key}:{value}"
        with self._timed_operation(node_id, "PUT", request) as response_container:
            try:
                sock = self.connect_to_node(node_id)
                sock.send(request.encode())
                
                response = sock.recv(4096).decode()
                response_container[0] = response
                sock.close()
                
                return response == "ACK"
            except Exception as e:
                response_container[0] = str(e) 
                logging.error(f"PUT operation failed: {e}")
                return False

    def connect_to_node(self, node_id: int) -> socket.socket:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        try:
            sock.connect((self.host, self.base_port + node_id))
            return sock
        except Exception as e:
            logging.error(f"Failed to connect to node {node_id}: {e}")
            raise

    def use_node(self, node_id: int) -> None:
        self.current_node = node_id
        self.logger.log_operation(
            node_id=node_id,
            operation="USE_NODE",
            request=f"Switching to node {node_id}",
            response="N/A",
            duration_ms=0,
            success=True
        )

class KVStoreLoadTest(unittest.TestCase):
    def setUp(self):
        self.test_name = self._testMethodName
        self.iteration = getattr(self, '_iteration', 0)
        self.client = KVStoreClient(test_name=self.test_name, iteration=self.iteration)
        self.num_nodes = 10
        time.sleep(1)


    def test_concurrent_operations(self, num_operations=1000, num_threads=50):
        """Test concurrent PUT and GET operations"""
        print(f"\nRunning concurrent test (iteration {self.iteration}) with {num_operations} operations across {num_threads} threads")
        
        # Calculate operations per thread
        ops_per_thread = max(1, num_operations // num_threads)
        remaining_ops = num_operations % num_threads
        
        def worker_task(thread_id):
            """Execute a series of random PUT and GET operations"""
            operations_to_perform = ops_per_thread + (1 if thread_id < remaining_ops else 0)
            
            for op_num in range(operations_to_perform):
                operation_type = random.choice(['PUT', 'GET'])
                key = f"concurrent_key_{thread_id}_{op_num}"
                
                try:
                    if operation_type == 'PUT':
                        value = f"concurrent_value_{thread_id}_{op_num}"
                        success = self.client.put(key, value)
                        self.assertTrue(success, f"PUT operation failed for key {key}")
                    else:
                        success, value = self.client.get(key)
                except Exception as e:
                    self.fail(f"Thread {thread_id} operation {op_num} failed: {str(e)}")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(worker_task, thread_id) for thread_id in range(num_threads)]
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    self.fail(f"Concurrent operation failed: {str(e)}")

    def test_basic_operations(self):
        """Test basic PUT and GET operations"""
        print(f"\nRunning basic operations test (iteration {self.iteration})")
        test_key = f"test_basic_key_{random.randint(1, 1000)}"
        test_value = f"test_basic_value_{random.randint(1, 1000)}"
        
        success = self.client.put(test_key, test_value)
        self.assertTrue(success)
        
        success, value = self.client.get(test_key)
        self.assertTrue(success)
        self.assertEqual(value, test_value)

    def test_multi_node_consistency(self):
        """Test data consistency across multiple nodes"""
        print(f"\nRunning multi-node consistency test (iteration {self.iteration})")
        test_key = f"consistency_test_key_{random.randint(1, 1000)}"
        test_value = f"consistency_test_value_{random.randint(1, 1000)}"
        
        # Put data through node 0
        self.client.use_node(0)
        success = self.client.put(test_key, test_value)
        self.assertTrue(success)
        
        # Try to read from different nodes
        for node in range(min(5, self.num_nodes)):
            self.client.use_node(node)
            success, value = self.client.get(test_key)
            self.assertTrue(success)
            self.assertEqual(value, test_value, f"Inconsistent value read from node {node}")
            time.sleep(0.1)

    def test_cache_coherence(self):
        """Test cache coherence across nodes"""
        print(f"\nRunning cache coherence test (iteration {self.iteration})")
        test_key = f"cache_test_key_{random.randint(1, 1000)}"
        initial_value = f"initial_value_{random.randint(1, 1000)}"
        updated_value = f"updated_value_{random.randint(1, 1000)}"
        
        # Put initial value through node 0
        self.client.use_node(0)
        success = self.client.put(test_key, initial_value)
        self.assertTrue(success)
        time.sleep(0.1)
        
        # Read from node 1 to potentially cache the value
        self.client.use_node(1)
        success, value = self.client.get(test_key)
        self.assertTrue(success)
        self.assertEqual(value, initial_value)
        
        # Update value through node 2
        self.client.use_node(2)
        success = self.client.put(test_key, updated_value)
        self.assertTrue(success)
        time.sleep(0.1)  
        
        # Read again from node 1 
        self.client.use_node(1)
        success, value = self.client.get(test_key)
        self.assertTrue(success)
        self.assertEqual(value, updated_value, 
                        f"Cache coherence failure: expected {updated_value} but got {value}")
        

        


def run_configurable_tests(num_iterations=1, num_operations=1000, num_threads=50, selected_tests=None):
    """
    Run specified tests multiple times with configurable parameters
    """
    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)

    # Create a summary log file
    with open("logs/kvstore_test_summary.log", "w") as f:
        f.write(f"KV Store Test Summary - {datetime.now()}\n")
        f.write(f"Parameters:\n")
        f.write(f"- Iterations: {num_iterations}\n")
        f.write(f"- Operations per concurrent test: {num_operations}\n")
        f.write(f"- Threads per concurrent test: {num_threads}\n")
        f.write(f"- Selected tests: {selected_tests if selected_tests else 'All'}\n\n")

    all_tests = {
        'basic': 'test_basic_operations',
        'concurrent': 'test_concurrent_operations',
        'consistency': 'test_multi_node_consistency',
        'cache': 'test_cache_coherence'
    }

    if not selected_tests:
        selected_tests = list(all_tests.keys())

    suite = unittest.TestSuite()
    
    for iteration in range(num_iterations):
        for test_key in selected_tests:
            if test_key in all_tests:
                test_name = all_tests[test_key]
                test = KVStoreLoadTest(test_name)
                # Set iteration number for logging purposes
                setattr(test, '_iteration', iteration)
                if test_key == 'concurrent':
                    setattr(test, '_testMethodArgs', (num_operations, num_threads))
                suite.addTest(test)

    print(f"\nStarting test run with {num_iterations} iterations...")
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # Write summary
    with open("logs/kvstore_test_summary.log", "a") as f:
        f.write("\nTest Summary:\n")
        f.write(f"Tests Run: {result.testsRun}\n")
        f.write(f"Tests Passed: {result.testsRun - len(result.failures) - len(result.errors)}\n")
        f.write(f"Tests Failed: {len(result.failures)}\n")
        f.write(f"Test Errors: {len(result.errors)}\n")
        
        if result.failures:
            f.write("\nFailures:\n")
            for failure in result.failures:
                f.write(f"{failure[0]}: {failure[1]}\n")
        
        if result.errors:
            f.write("\nErrors:\n")
            for error in result.errors:
                f.write(f"{error[0]}: {error[1]}\n")
        
        # Write log file locations
        f.write("\nLog File Locations:\n")
        for test_key in selected_tests:
            for i in range(num_iterations):
                f.write(f"{test_key} test iteration {i}: logs/kvstore_communication_{all_tests[test_key]}_iteration_{i}.log\n")

if __name__ == '__main__':
    NUM_ITERATIONS = 3
    NUM_OPERATIONS = 100
    NUM_THREADS = 50
    
    SELECTED_TESTS = [
        'basic',
        'concurrent',
        'consistency',
        'cache'
    ]
    
    run_configurable_tests(
        num_iterations=NUM_ITERATIONS,
        num_operations=NUM_OPERATIONS,
        num_threads=NUM_THREADS,
        selected_tests=SELECTED_TESTS
    )