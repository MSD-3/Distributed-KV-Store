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
class KVStoreThroughputTest(unittest.TestCase):
    def setUp(self):
        self.test_name = self._testMethodName
        self.iteration = getattr(self, '_iteration', 0)
        self.client = KVStoreClient(test_name=self.test_name, iteration=self.iteration)
        self.num_nodes = 10
        time.sleep(1)

    def test_throughput(self, num_operations=16000, num_threads=50):
        """Test system throughput with concurrent operations"""
        print(f"\nRunning throughput test (iteration {self.iteration})")
        print(f"Parameters: {num_operations} operations across {num_threads} threads")

        # Statistics tracking with atomic counters
        self.put_times = []
        self.get_times = []
        self.operation_lock = threading.Lock()
        self.total_puts = 0
        self.total_gets = 0
        self.successful_puts = 0
        self.successful_gets = 0
        
        # Calculate operations per thread - divide by 2 since each "operation" is actually a PUT+GET pair
        ops_per_thread = max(1, (num_operations // 2) // num_threads)
        remaining_ops = (num_operations // 2) % num_threads

        def worker_task(thread_id):
            """Worker task that performs PUT and GET operations"""
            operations_to_perform = ops_per_thread + (1 if thread_id < remaining_ops else 0)
            
            local_put_times = []
            local_get_times = []
            local_puts = 0
            local_gets = 0
            local_successful_puts = 0
            local_successful_gets = 0
            
            for op_num in range(operations_to_perform):
                key = f"throughput_key_{thread_id}_{op_num}"
                value = f"throughput_value_{thread_id}_{op_num}"
                
                # Perform PUT operation
                start_time = time.time()
                success = self.client.put(key, value)
                end_time = time.time()
                local_puts += 1
                if success:
                    local_successful_puts += 1
                    local_put_times.append(end_time - start_time)
                
                # Perform GET operation
                start_time = time.time()
                success, _ = self.client.get(key)
                end_time = time.time()
                local_gets += 1
                if success:
                    local_successful_gets += 1
                    local_get_times.append(end_time - start_time)

            # Aggregate local measurements
            with self.operation_lock:
                self.put_times.extend(local_put_times)
                self.get_times.extend(local_get_times)
                self.total_puts += local_puts
                self.total_gets += local_gets
                self.successful_puts += local_successful_puts
                self.successful_gets += local_successful_gets
        # Record overall test start time
        test_start_time = time.time()

        # Run concurrent operations
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(worker_task, thread_id) for thread_id in range(num_threads)]
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    self.fail(f"Throughput test failed: {str(e)}")

        # Calculate test duration
        test_duration = time.time() - test_start_time

        # Calculate throughput metrics
        total_attempted = self.total_puts + self.total_gets
        total_successful = self.successful_puts + self.successful_gets
        overall_throughput = total_successful / test_duration

        # Calculate latency metrics
        avg_put_latency = sum(self.put_times) / len(self.put_times) if self.put_times else 0
        avg_get_latency = sum(self.get_times) / len(self.get_times) if self.get_times else 0
        
        # Calculate individual operation throughput
        put_throughput = self.successful_puts / test_duration
        get_throughput = self.successful_gets / test_duration

        # Calculate success rates
        put_success_rate = (self.successful_puts / self.total_puts * 100) if self.total_puts else 0
        get_success_rate = (self.successful_gets / self.total_gets * 100) if self.total_gets else 0

        # Log results
        results = {
            "total_attempted_operations": total_attempted,
            "total_successful_operations": total_successful,
            "test_duration_seconds": test_duration,
            "overall_throughput_ops_per_sec": overall_throughput,
            "put_operations": {
                "attempted": self.total_puts,
                "successful": self.successful_puts,
                "throughput_ops_per_sec": put_throughput,
                "success_rate_percent": put_success_rate,
                "avg_latency_ms": avg_put_latency * 1000
            },
            "get_operations": {
                "attempted": self.total_gets,
                "successful": self.successful_gets,
                "throughput_ops_per_sec": get_throughput,
                "success_rate_percent": get_success_rate,
                "avg_latency_ms": avg_get_latency * 1000
            }
        }

        # Log results to file
        with open(f"logs/throughput_results_iteration_{self.iteration}.json", 'w') as f:
            json.dump(results, f, indent=4)

        # Print results
        print("\nThroughput Test Results:")
        print(f"Total Attempted Operations: {total_attempted}")
        print(f"Total Successful Operations: {total_successful}")
        print(f"Test Duration: {test_duration:.2f} seconds")
        print("\nPUT Operations:")
        print(f"- Attempted: {self.total_puts}")
        print(f"- Successful: {self.successful_puts}")
        print(f"- Throughput: {put_throughput:.2f} ops/sec")
        print(f"- Success Rate: {put_success_rate:.1f}%")
        print(f"- Average Latency: {avg_put_latency * 1000:.2f} ms")
        print("\nGET Operations:")
        print(f"- Attempted: {self.total_gets}")
        print(f"- Successful: {self.successful_gets}")
        print(f"- Throughput: {get_throughput:.2f} ops/sec")
        print(f"- Success Rate: {get_success_rate:.1f}%")
        print(f"- Average Latency: {avg_get_latency * 1000:.2f} ms")

def run_throughput_tests(num_iterations=3, num_operations=1000, num_threads=50):
    """Run throughput tests with specified parameters"""
    os.makedirs("logs", exist_ok=True)

    # Create summary log file
    with open("logs/throughput_test_summary.log", "w") as f:
        f.write(f"KV Store Throughput Test Summary - {datetime.now()}\n")
        f.write(f"Parameters:\n")
        f.write(f"- Iterations: {num_iterations}\n")
        f.write(f"- Operations per test: {num_operations}\n")
        f.write(f"- Threads per test: {num_threads}\n\n")

    suite = unittest.TestSuite()
    
    for iteration in range(num_iterations):
        test = KVStoreThroughputTest('test_throughput')
        setattr(test, '_iteration', iteration)
        setattr(test, '_testMethodArgs', (num_operations, num_threads))
        suite.addTest(test)

    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)

if __name__ == '__main__':

    NUM_ITERATIONS = 1
    NUM_OPERATIONS = 8000 
    NUM_THREADS = 50
    
    run_throughput_tests(
        num_iterations=NUM_ITERATIONS,
        num_operations=NUM_OPERATIONS,
        num_threads=NUM_THREADS
    )