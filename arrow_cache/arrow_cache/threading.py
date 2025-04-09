"""
Threading module for Arrow Cache

Provides classes and functions for parallel processing, thread pools,
and asynchronous operations.
"""
import os
import queue
import threading
import logging
import time
import concurrent.futures
from typing import Dict, List, Set, Optional, Any, Callable, Tuple, Union, TypeVar, Generic
import traceback

logger = logging.getLogger(__name__)

# Type variables for generic task functions
T = TypeVar('T')  # Task input type
R = TypeVar('R')  # Task result type


class ThreadPoolManager:
    """
    Manages thread pools for parallel processing
    """
    def __init__(self, config: Any):
        """
        Initialize the thread pool manager
        
        Args:
            config: ArrowCache configuration
        """
        self.config = config
        self.thread_count = config["thread_count"]
        self.background_threads = config["background_threads"]
        self._main_executor = None
        self._background_executor = None
        self.lock = threading.RLock()
        
    @property
    def main_executor(self) -> concurrent.futures.ThreadPoolExecutor:
        """Get or create the main thread pool executor"""
        with self.lock:
            if self._main_executor is None:
                self._main_executor = concurrent.futures.ThreadPoolExecutor(
                    max_workers=self.thread_count,
                    thread_name_prefix="arrow_cache_main"
                )
            return self._main_executor
    
    @property
    def background_executor(self) -> concurrent.futures.ThreadPoolExecutor:
        """Get or create the background thread pool executor"""
        with self.lock:
            if self._background_executor is None:
                self._background_executor = concurrent.futures.ThreadPoolExecutor(
                    max_workers=self.background_threads,
                    thread_name_prefix="arrow_cache_bg"
                )
            return self._background_executor
    
    def map(self, fn: Callable[[T], R], items: List[T], timeout: Optional[float] = None) -> List[R]:
        """
        Execute a function on each item in parallel
        
        Args:
            fn: Function to execute
            items: List of items to process
            timeout: Maximum time to wait for completion (None for no timeout)
            
        Returns:
            List of results in the same order as items
        """
        if not items:
            return []
            
        with self.lock:
            try:
                return list(self.main_executor.map(fn, items, timeout=timeout))
            except concurrent.futures.TimeoutError:
                logger.warning(f"Timeout exceeded in ThreadPoolManager.map after {timeout} seconds")
                raise
    
    def submit(self, fn: Callable[..., R], *args, **kwargs) -> concurrent.futures.Future:
        """
        Submit a task to the main thread pool
        
        Args:
            fn: Function to execute
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function
            
        Returns:
            Future representing the task
        """
        return self.main_executor.submit(fn, *args, **kwargs)
    
    def submit_background(self, fn: Callable[..., Any], *args, **kwargs) -> concurrent.futures.Future:
        """
        Submit a task to the background thread pool
        
        Args:
            fn: Function to execute
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function
            
        Returns:
            Future representing the task
        """
        return self.background_executor.submit(fn, *args, **kwargs)
    
    def shutdown(self, wait: bool = True) -> None:
        """
        Shutdown the thread pools
        
        Args:
            wait: Whether to wait for tasks to complete
        """
        with self.lock:
            if self._main_executor is not None:
                self._main_executor.shutdown(wait=wait)
                self._main_executor = None
                
            if self._background_executor is not None:
                self._background_executor.shutdown(wait=wait)
                self._background_executor = None


class AsyncTaskManager(Generic[T, R]):
    """
    Manager for asynchronous background tasks
    """
    def __init__(self, thread_pool: ThreadPoolManager):
        """
        Initialize the async task manager
        
        Args:
            thread_pool: Thread pool manager to use
        """
        self.thread_pool = thread_pool
        self.tasks = {}  # task_id -> Future
        self.results = {}  # task_id -> result
        self.callbacks = {}  # task_id -> callback
        self.lock = threading.RLock()
        self.next_task_id = 0
        
    def submit(
        self,
        fn: Callable[..., R],
        callback: Optional[Callable[[str, R], None]] = None,
        *args,
        **kwargs
    ) -> str:
        """
        Submit an asynchronous task
        
        Args:
            fn: Function to execute
            callback: Optional callback function called with (task_id, result) when done
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function
            
        Returns:
            Task ID
        """
        with self.lock:
            task_id = str(self.next_task_id)
            self.next_task_id += 1
            
            # Create a wrapper function that stores the result and calls the callback
            def task_wrapper(*args, **kwargs):
                try:
                    result = fn(*args, **kwargs)
                    with self.lock:
                        self.results[task_id] = result
                    if callback:
                        try:
                            callback(task_id, result)
                        except Exception as e:
                            logger.error(f"Error in task callback: {e}")
                    return result
                except Exception as e:
                    logger.error(f"Error in async task {task_id}: {e}")
                    logger.debug(traceback.format_exc())
                    with self.lock:
                        self.results[task_id] = None
                    if callback:
                        try:
                            callback(task_id, None)
                        except Exception as e:
                            logger.error(f"Error in task error callback: {e}")
                    raise
            
            # Submit the task to the background thread pool
            future = self.thread_pool.submit_background(task_wrapper, *args, **kwargs)
            self.tasks[task_id] = future
            if callback:
                self.callbacks[task_id] = callback
                
            return task_id
    
    def get_result(self, task_id: str, timeout: Optional[float] = None) -> Optional[R]:
        """
        Get the result of an asynchronous task
        
        Args:
            task_id: Task ID
            timeout: Maximum time to wait for completion (None for no timeout)
            
        Returns:
            Task result or None if the task failed or doesn't exist
        """
        with self.lock:
            # Check if we already have the result
            if task_id in self.results:
                return self.results[task_id]
                
            # Check if the task exists
            if task_id not in self.tasks:
                return None
                
            # Wait for the task to complete
            future = self.tasks[task_id]
            
        try:
            result = future.result(timeout=timeout)
            return result
        except concurrent.futures.TimeoutError:
            logger.warning(f"Timeout waiting for task {task_id}")
            return None
        except Exception as e:
            logger.error(f"Error getting result for task {task_id}: {e}")
            return None
    
    def is_done(self, task_id: str) -> bool:
        """
        Check if an asynchronous task is done
        
        Args:
            task_id: Task ID
            
        Returns:
            True if the task is done or doesn't exist, False otherwise
        """
        with self.lock:
            if task_id in self.results:
                return True
                
            if task_id not in self.tasks:
                return True
                
            return self.tasks[task_id].done()
    
    def cancel(self, task_id: str) -> bool:
        """
        Cancel an asynchronous task
        
        Args:
            task_id: Task ID
            
        Returns:
            True if the task was cancelled, False otherwise
        """
        with self.lock:
            if task_id not in self.tasks:
                return False
                
            future = self.tasks[task_id]
            if future.done():
                return False
                
            return future.cancel()
    
    def cleanup(self) -> None:
        """Clean up completed tasks to free memory"""
        with self.lock:
            done_tasks = [
                task_id for task_id, future in self.tasks.items()
                if future.done()
            ]
            
            for task_id in done_tasks:
                try:
                    # Get the result to handle any exceptions
                    if task_id not in self.results:
                        self.results[task_id] = self.tasks[task_id].result()
                except Exception:
                    # Task failed, but we still want to clean it up
                    self.results[task_id] = None
                
                # Remove the future
                del self.tasks[task_id]
                
                # Remove the callback
                if task_id in self.callbacks:
                    del self.callbacks[task_id]
    
    def get_active_count(self) -> int:
        """
        Get the number of active tasks
        
        Returns:
            Number of active tasks
        """
        with self.lock:
            return sum(1 for future in self.tasks.values() if not future.done())
    
    def get_all_results(self) -> Dict[str, Optional[R]]:
        """
        Get all task results
        
        Returns:
            Dictionary mapping task IDs to results
        """
        with self.lock:
            return self.results.copy()
    
    def shutdown(self) -> None:
        """Shutdown the task manager and cancel all tasks"""
        with self.lock:
            for task_id, future in self.tasks.items():
                if not future.done():
                    future.cancel()
            
            self.tasks.clear()
            # Don't clear results - they might still be needed


class BackgroundProcessingQueue:
    """
    Queue for background processing tasks
    """
    def __init__(
        self,
        thread_pool: ThreadPoolManager,
        worker_count: int = 1,
        max_queue_size: int = 1000
    ):
        """
        Initialize the background processing queue
        
        Args:
            thread_pool: Thread pool manager to use
            worker_count: Number of worker threads
            max_queue_size: Maximum queue size
        """
        self.thread_pool = thread_pool
        self.worker_count = worker_count
        self.queue = queue.Queue(maxsize=max_queue_size)
        self.workers = []
        self.running = False
        self.lock = threading.RLock()
        self.stop_event = threading.Event()
        
    def start(self) -> None:
        """Start the worker threads"""
        with self.lock:
            if self.running:
                return
                
            self.running = True
            self.stop_event.clear()
            
            for i in range(self.worker_count):
                worker = threading.Thread(
                    target=self._worker_loop,
                    name=f"bg_worker_{i}",
                    daemon=True
                )
                worker.start()
                self.workers.append(worker)
    
    def stop(self, wait: bool = True) -> None:
        """
        Stop the worker threads
        
        Args:
            wait: Whether to wait for the queue to be empty
        """
        with self.lock:
            if not self.running:
                return
                
            self.running = False
            self.stop_event.set()
            
            if wait:
                for worker in self.workers:
                    worker.join()
                    
            self.workers = []
    
    def _worker_loop(self) -> None:
        """Worker thread loop"""
        while not self.stop_event.is_set():
            try:
                # Get task from queue with timeout
                try:
                    task, args, kwargs = self.queue.get(timeout=0.1)
                except queue.Empty:
                    continue
                    
                # Execute task
                try:
                    task(*args, **kwargs)
                except Exception as e:
                    logger.error(f"Error in background task: {e}")
                    logger.debug(traceback.format_exc())
                finally:
                    self.queue.task_done()
            except Exception as e:
                logger.error(f"Error in worker loop: {e}")
                logger.debug(traceback.format_exc())
    
    def add_task(self, task: Callable, *args, **kwargs) -> None:
        """
        Add a task to the queue
        
        Args:
            task: Function to execute
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function
        """
        if not self.running:
            self.start()
            
        try:
            self.queue.put((task, args, kwargs), block=False)
        except queue.Full:
            logger.warning("Background processing queue is full, task dropped")
    
    def is_empty(self) -> bool:
        """
        Check if the queue is empty
        
        Returns:
            True if the queue is empty, False otherwise
        """
        return self.queue.empty()
    
    def get_queue_size(self) -> int:
        """
        Get the current queue size
        
        Returns:
            Current queue size
        """
        return self.queue.qsize()
    
    def wait_completion(self, timeout: Optional[float] = None) -> bool:
        """
        Wait for all tasks to complete
        
        Args:
            timeout: Maximum time to wait (None for no timeout)
            
        Returns:
            True if all tasks completed, False if timeout occurred
        """
        try:
            self.queue.join()
            return True
        except (KeyboardInterrupt, SystemExit):
            return False


def parallel_map(
    fn: Callable[[T], R],
    items: List[T],
    max_workers: Optional[int] = None,
    chunk_size: int = 1,
    timeout: Optional[float] = None
) -> List[R]:
    """
    Process items in parallel using a thread pool
    
    Args:
        fn: Function to execute on each item
        items: List of items to process
        max_workers: Maximum number of worker threads (None for default)
        chunk_size: Number of items per chunk
        timeout: Maximum time to wait for completion (None for no timeout)
        
    Returns:
        List of results in the same order as items
    """
    if not items:
        return []
        
    if max_workers is None:
        max_workers = min(32, os.cpu_count() + 4)
        
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        return list(executor.map(fn, items, chunksize=chunk_size, timeout=timeout)) 