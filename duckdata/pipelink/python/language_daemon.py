import os
import sys
import time
import socket
import threading
import subprocess
import json
import logging
import tempfile
import atexit
import signal
import yaml
from pathlib import Path
import uuid
from queue import Queue, Empty

logger = logging.getLogger(__name__)

class LanguageDaemonPool:
    """
    Enhanced pool of language daemons for efficient node execution in pipelines.
    Maintains pre-started language daemons and includes health checking and keep-alive mechanisms.
    """
    
    def __init__(self, min_daemons=1, max_daemons=4, socket_dir=None, working_dir=None):
        """
        Initialize daemon pool
        
        Args:
            min_daemons: Minimum number of daemons to maintain per language
            max_daemons: Maximum number of daemons to allow per language
            socket_dir: Directory for socket files (defaults to temp dir)
            working_dir: Base working directory for daemons
        """
        self.min_daemons = min_daemons
        self.max_daemons = max_daemons
        self.working_dir = working_dir or os.getcwd()
        
        # Use a shorter socket path to avoid "AF_UNIX path too long" errors on macOS
        if socket_dir:
            self.socket_dir = socket_dir
        else:
            # Create a short socket directory in /tmp
            self.socket_dir = "/tmp/pipelink_sockets"
            os.makedirs(self.socket_dir, exist_ok=True)
        
        # Initialize daemon pools for each language
        self.daemon_pools = {
            'python': [],
            'r': [],
            'julia': []
        }
        
        # Locks for thread safety
        self.pool_locks = {
            'python': threading.Lock(),
            'r': threading.Lock(),
            'julia': threading.Lock()
        }
        
        # Track daemon health and last usage time
        self.daemon_health = {}  # daemon_id -> health status (bool)
        self.last_used = {}      # daemon_id -> timestamp
        
        # Start background threads
        self._health_check_thread = threading.Thread(target=self._health_check_worker)
        self._health_check_thread.daemon = True
        self._health_check_thread.start()
        
        self._keep_alive_thread = threading.Thread(target=self._keep_alive_worker)
        self._keep_alive_thread.daemon = True
        self._keep_alive_thread.start()
        
        # Initialize minimum number of daemons for each language
        self._initialize_pools()
        
        # Register cleanup handler
        atexit.register(self.shutdown_all)
        signal.signal(signal.SIGTERM, lambda sig, frame: self.shutdown_all())
        signal.signal(signal.SIGINT, lambda sig, frame: self.shutdown_all())
    
    def _initialize_pools(self):
        """Initialize daemon pools with minimum number of daemons for each language"""
        for language in self.daemon_pools:
            with self.pool_locks[language]:
                # Only initialize if language is available
                if self._is_language_available(language):
                    while len(self.daemon_pools[language]) < self.min_daemons:
                        daemon = self._create_daemon(language)
                        if daemon:
                            self.daemon_pools[language].append(daemon)
                            self.last_used[daemon.socket_path] = time.time()
                            self.daemon_health[daemon.socket_path] = True
                            logger.info(f"Initialized {language} daemon in pool: {daemon.socket_path}")
                        else:
                            logger.warning(f"Failed to create {language} daemon during initialization")
                            break
                else:
                    logger.warning(f"Language '{language}' is not available on this system")

    def _is_language_available(self, language):
        """Check if the language executable is available on the system"""
        if language == 'python':
            return True  # Python is always available since we're running in it
        
        # Check if R is available
        if language == 'r':
            try:
                result = subprocess.run(['which', 'Rscript'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                return result.returncode == 0
            except:
                return False
        
        # Check if Julia is available
        if language == 'julia':
            try:
                result = subprocess.run(['which', 'julia'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                return result.returncode == 0
            except:
                return False
        
        return False
    
    def _create_daemon(self, language):
        """Create a new daemon for the specified language"""
        try:
            # Generate unique ID for daemon
            daemon_id = str(uuid.uuid4())[:8]
            daemon = LanguageDaemon(language, self.working_dir, self.socket_dir, daemon_id=daemon_id)
            daemon.start()
            return daemon
        except Exception as e:
            logger.error(f"Failed to create {language} daemon: {e}")
            return None
    
    def _health_check_worker(self):
        """Background thread to periodically check daemon health"""
        while True:
            try:
                # Iterate through all daemons and check health
                for language, pool in self.daemon_pools.items():
                    with self.pool_locks[language]:
                        # Create list of daemons that need to be replaced
                        to_replace = []
                        
                        for i, daemon in enumerate(pool):
                            # Check if daemon is healthy
                            is_healthy = self._check_daemon_health(daemon)
                            self.daemon_health[daemon.socket_path] = is_healthy
                            
                            if not is_healthy:
                                logger.warning(f"{language} daemon {daemon.socket_path} is unhealthy, marking for replacement")
                                to_replace.append(i)
                        
                        # Replace unhealthy daemons
                        for i in reversed(to_replace):
                            try:
                                daemon = pool[i]
                                daemon.shutdown()
                            except Exception as e:
                                logger.warning(f"Error shutting down unhealthy daemon: {e}")
                            
                            # Create replacement daemon if below max
                            if len(pool) <= self.max_daemons:
                                new_daemon = self._create_daemon(language)
                                if new_daemon:
                                    pool[i] = new_daemon
                                    self.last_used[new_daemon.socket_path] = time.time()
                                    self.daemon_health[new_daemon.socket_path] = True
                                    logger.info(f"Replaced unhealthy {language} daemon")
                                else:
                                    # If we can't create a replacement, remove this one
                                    pool.pop(i)
                                    logger.warning(f"Failed to replace unhealthy {language} daemon")
            except Exception as e:
                logger.error(f"Error in health check thread: {e}")
            
            # Sleep before next check
            time.sleep(30)  # Check health every 30 seconds
    
    def _keep_alive_worker(self):
        """Background thread to send keep-alive messages to idle daemons"""
        while True:
            try:
                # Current time
                current_time = time.time()
                
                # Iterate through all daemons
                for language, pool in self.daemon_pools.items():
                    with self.pool_locks[language]:
                        for daemon in pool:
                            # Only send keep-alive to healthy daemons that have been idle
                            if self.daemon_health.get(daemon.socket_path, False):
                                last_used = self.last_used.get(daemon.socket_path, 0)
                                # If daemon has been idle for more than 5 minutes
                                if current_time - last_used > 300:
                                    # Send keep-alive message
                                    if self._send_keep_alive(daemon):
                                        self.last_used[daemon.socket_path] = current_time
                                        logger.debug(f"Sent keep-alive to {language} daemon: {daemon.socket_path}")
            except Exception as e:
                logger.error(f"Error in keep-alive thread: {e}")
            
            # Sleep before next cycle
            time.sleep(60)  # Send keep-alives every minute
    
    def _check_daemon_health(self, daemon):
        """Check if a daemon is healthy by sending a ping request"""
        try:
            # Connect to the daemon's socket
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.settimeout(3)  # 3 second timeout for health check
            
            try:
                sock.connect(daemon.socket_path)
                
                # Send ping request
                request = {'action': 'ping'}
                request_data = json.dumps(request).encode('utf-8')
                sock.sendall(len(request_data).to_bytes(4, byteorder='big'))
                sock.sendall(request_data)
                
                # Wait for response
                response_size_data = sock.recv(4)
                response_size = int.from_bytes(response_size_data, byteorder='big')
                
                response_data = b''
                while len(response_data) < response_size:
                    chunk = sock.recv(min(4096, response_size - len(response_data)))
                    if not chunk:
                        break
                    response_data += chunk
                
                # Parse response
                response = json.loads(response_data.decode('utf-8'))
                
                return response.get('status') == 'ok'
                
            except Exception as e:
                logger.debug(f"Health check failed for daemon: {e}")
                return False
            finally:
                sock.close()
        except Exception:
            return False
    
    def _send_keep_alive(self, daemon):
        """Send keep-alive message to daemon to prevent timeout"""
        try:
            # Connect to the daemon's socket
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.settimeout(3)  # 3 second timeout for keep-alive
            
            try:
                sock.connect(daemon.socket_path)
                
                # Send keep-alive request
                request = {'action': 'keep_alive'}
                request_data = json.dumps(request).encode('utf-8')
                sock.sendall(len(request_data).to_bytes(4, byteorder='big'))
                sock.sendall(request_data)
                
                # Wait for response
                response_size_data = sock.recv(4)
                response_size = int.from_bytes(response_size_data, byteorder='big')
                
                response_data = b''
                while len(response_data) < response_size:
                    chunk = sock.recv(min(4096, response_size - len(response_data)))
                    if not chunk:
                        break
                    response_data += chunk
                
                # Parse response
                response = json.loads(response_data.decode('utf-8'))
                
                return response.get('status') == 'ok'
                
            except Exception:
                return False
            finally:
                sock.close()
        except Exception:
            return False
    
    def get_daemon(self, language):
        """Get an available daemon for the specified language"""
        with self.pool_locks[language]:
            # Find a healthy daemon in the pool
            for daemon in self.daemon_pools[language]:
                if self.daemon_health.get(daemon.socket_path, False):
                    # Update last used time
                    self.last_used[daemon.socket_path] = time.time()
                    return daemon
            
            # If no healthy daemon is found, create a new one
            if len(self.daemon_pools[language]) < self.max_daemons:
                daemon = self._create_daemon(language)
                if daemon:
                    self.daemon_pools[language].append(daemon)
                    self.daemon_health[daemon.socket_path] = True
                    self.last_used[daemon.socket_path] = time.time()
                    logger.info(f"Created new {language} daemon on demand: {daemon.socket_path}")
                    return daemon
            
            # If we're at capacity, try to find any daemon to use
            if self.daemon_pools[language]:
                daemon = self.daemon_pools[language][0]
                # Try to restart it
                try:
                    daemon.shutdown()
                except:
                    pass
                
                # Create a new one in its place
                new_daemon = self._create_daemon(language)
                if new_daemon:
                    self.daemon_pools[language][0] = new_daemon
                    self.daemon_health[new_daemon.socket_path] = True
                    self.last_used[new_daemon.socket_path] = time.time()
                    logger.info(f"Replaced daemon at pool capacity: {new_daemon.socket_path}")
                    return new_daemon
            
            logger.error(f"No available {language} daemon and cannot create more")
            return None
    
    def execute_node(self, node_config, pipeline_config, working_dir, meta_path):
        """Execute a node using a language daemon from the pool"""
        language = node_config['language']
        
        # Skip for data nodes and unsupported languages
        if language not in ['python', 'r', 'julia']:
            raise ValueError(f"Language '{language}' not supported for daemon execution")
        
        # Get an available daemon
        daemon = self.get_daemon(language)
        if not daemon:
            raise RuntimeError(f"No available {language} daemon")
        
        # Execute task via daemon
        script_path = os.path.join(working_dir, node_config['script'])
        return daemon.execute(script_path, meta_path, working_dir)
    
    def shutdown_all(self):
        """Shutdown all running daemons"""
        logger.info("Shutting down all language daemons")
        for language, pool in self.daemon_pools.items():
            with self.pool_locks[language]:
                for daemon in pool:
                    try:
                        daemon.shutdown()
                    except Exception as e:
                        logger.warning(f"Error shutting down {language} daemon: {e}")
                self.daemon_pools[language] = []

class LanguageDaemonManager:
    """
    Manager for language daemons that keep language processes running
    to avoid startup overhead in cross-language pipelines.
    """
    
    def __init__(self, working_dir=None, socket_dir=None):
        """
        Initialize daemon manager
        
        Args:
            working_dir: Base working directory for daemons
            socket_dir: Directory for socket files (defaults to temp dir)
        """
        self.working_dir = working_dir or os.getcwd()
        
        # Use a shorter socket path to avoid "AF_UNIX path too long" errors on macOS
        if socket_dir:
            self.socket_dir = socket_dir
        else:
            # Create a short socket directory in /tmp
            self.socket_dir = "/tmp/pipelink_sockets"
            os.makedirs(self.socket_dir, exist_ok=True)
            
        self.daemons = {}
        self.process_lock = threading.Lock()
        
        # Register cleanup handler
        atexit.register(self.shutdown_all)
        signal.signal(signal.SIGTERM, lambda sig, frame: self.shutdown_all())
        signal.signal(signal.SIGINT, lambda sig, frame: self.shutdown_all())
    
    def get_daemon(self, language):
        """Get or create a daemon for the specified language"""
        with self.process_lock:
            if language not in self.daemons:
                # Create and start a new daemon
                self.daemons[language] = LanguageDaemon(language, self.working_dir, self.socket_dir)
                self.daemons[language].start()
            
            return self.daemons[language]
    
    def execute_node(self, node_config, pipeline_config, working_dir, meta_path):
        """
        Execute a node using the appropriate language daemon
        
        Args:
            node_config: Node configuration
            pipeline_config: Pipeline configuration 
            working_dir: Working directory
            meta_path: Path to metadata file
            
        Returns:
            Tuple of (success, stdout, stderr)
        """
        language = node_config['language']
        
        # Skip for data nodes and unsupported languages
        if language not in ['python', 'r', 'julia']:
            raise ValueError(f"Language '{language}' not supported for daemon execution")
            
        try:
            # Get daemon for this language
            daemon = self.get_daemon(language)
            
            # Execute task via daemon
            script_path = os.path.join(working_dir, node_config['script'])
            return daemon.execute(script_path, meta_path, working_dir)
        except Exception as e:
            # If we can't connect to the daemon, raise the error to allow fallback
            logger.warning(f"Error executing with {language} daemon: {e}")
            raise
    
    def shutdown_all(self):
        """Shutdown all running daemons"""
        with self.process_lock:
            for language, daemon in list(self.daemons.items()):
                try:
                    daemon.shutdown()
                except Exception as e:
                    logger.warning(f"Error shutting down {language} daemon: {e}")
                    
            self.daemons = {}

class LanguageDaemon:
    """
    A daemon process for a specific language that stays running
    to avoid startup overhead in cross-language pipelines.
    """
    
    def __init__(self, language, working_dir, socket_dir, daemon_id=None):
        """
        Initialize a language daemon
        
        Args:
            language: Language for this daemon ('python', 'r', 'julia')
            working_dir: Base working directory
            socket_dir: Directory for socket files
            daemon_id: Optional unique identifier for this daemon
        """
        self.language = language
        self.working_dir = working_dir
        self.socket_dir = socket_dir
        self.process = None
        
        # Create shorter socket name to avoid path length issues
        daemon_id = daemon_id or str(uuid.uuid4())[:8]
        self.socket_path = os.path.join(socket_dir, f"pl_{language}_{daemon_id}.sock")
        self.running = False
        self.daemon_script = self._get_daemon_script()
    
    def _get_daemon_script(self):
        """Get path to the daemon script for this language"""
        scripts_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'daemon_scripts')
        
        if self.language == 'python':
            return os.path.join(scripts_dir, 'python_daemon.py')
        elif self.language == 'r':
            return os.path.join(scripts_dir, 'r_daemon.R')
        elif self.language == 'julia':
            return os.path.join(scripts_dir, 'julia_daemon.jl')
        else:
            raise ValueError(f"Unsupported language: {self.language}")
    
    def _try_tmux_for_r(self):
        """Try to use tmux as a fallback for R daemon"""
        try:
            # Check if tmux is available
            result = subprocess.run(
                ["which", "tmux"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            if result.returncode != 0:
                logger.warning("tmux not found, cannot use fallback method")
                return False
            
            # Create a unique session name
            session_name = f"pipelink_r_{os.path.basename(self.socket_path)}"
            
            # Start R in a tmux session
            cmd = [
                "tmux", "new-session", "-d", "-s", session_name,
                f"R --vanilla"
            ]
            
            subprocess.run(cmd, check=True)
            logger.info(f"Started R in tmux session '{session_name}'")
            
            # Create the socket file to indicate the daemon is running
            with open(self.socket_path, 'w') as f:
                f.write(session_name)
            
            return True
        except Exception as e:
            logger.error(f"Failed to start R using tmux: {e}")
            return False

    def start(self):
        """Start the daemon process"""
        logger.info(f"Starting {self.language} daemon")
        
        # Create daemon script if needed
        if not os.path.exists(self.daemon_script):
            if self.language == 'python':
                self._create_python_daemon_script()
            elif self.language == 'r':
                self._create_r_daemon_script()
            elif self.language == 'julia':
                self._create_julia_daemon_script()
            else:
                raise ValueError(f"Unsupported language: {self.language}")
        
        # Start daemon process
        if self.language == 'python':
            self.process = subprocess.Popen(
                [sys.executable, self.daemon_script, self.socket_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=self.working_dir
            )
        elif self.language == 'r':
            self.process = subprocess.Popen(
                ['Rscript', self.daemon_script, self.socket_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=self.working_dir
            )
        elif self.language == 'julia':
            self.process = subprocess.Popen(
                ['julia', self.daemon_script, self.socket_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=self.working_dir
            )
        
        # Wait for socket file to appear
        max_wait = 30  # 30 seconds
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            if os.path.exists(self.socket_path):
                self.running = True
                break
            time.sleep(0.1)
            
            # Check if process is still running
            if self.process.poll() is not None:
                stdout, stderr = self.process.communicate()
                raise RuntimeError(f"Daemon process failed to start: {stderr}")
        
        if not self.running:
            raise TimeoutError(f"Timeout waiting for {self.language} daemon to start")
        
        # For R, we need to verify that Rserve is actually running
        if self.language == 'r':
            try:
                import pyRserve
                # Read the port from the socket file
                rserve_port = 6311  # default port
                try:
                    with open(self.socket_path, 'r') as f:
                        socket_content = f.read().strip()
                        if socket_content.isdigit():
                            rserve_port = int(socket_content)
                            logger.info(f"Using Rserve on port {rserve_port}")
                            self.rserve_port = rserve_port
                            self.using_rserve = True
                        elif socket_content == "tmux":
                            logger.info("Using tmux for R daemon")
                            if self._try_tmux_for_r():
                                self.using_rserve = False
                                return
                            else:
                                raise RuntimeError("Failed to start R daemon with tmux")
                        else:
                            # Try the default port
                            try:
                                conn = pyRserve.connect(host='localhost', port=6311)
                                conn.close()
                                logger.info("Successfully connected to Rserve on default port")
                                self.using_rserve = True
                            except Exception as e:
                                logger.warning(f"Failed to connect to Rserve on default port: {e}")
                                # Try tmux as a fallback
                                if self._try_tmux_for_r():
                                    logger.info("Successfully started R using tmux")
                                    self.using_rserve = False
                                else:
                                    raise RuntimeError("Failed to start R daemon with any method")
                except Exception as e:
                    logger.warning(f"Failed to read Rserve port from socket file: {e}")
                    # Try the default port
                    try:
                        conn = pyRserve.connect(host='localhost', port=6311)
                        conn.close()
                        logger.info("Successfully connected to Rserve on default port")
                        self.using_rserve = True
                    except Exception as e:
                        logger.warning(f"Failed to connect to Rserve on default port: {e}")
                        # Try tmux as a fallback
                        if self._try_tmux_for_r():
                            logger.info("Successfully started R using tmux")
                            self.using_rserve = False
                        else:
                            raise RuntimeError("Failed to start R daemon with any method")
            except ImportError:
                logger.warning("pyRserve not installed, cannot verify if Rserve is running")
                # Try tmux as a fallback
                if self._try_tmux_for_r():
                    logger.info("Successfully started R using tmux")
                    self.using_rserve = False
                else:
                    logger.warning("Unable to start R daemon with any method")
        
        logger.info(f"{self.language} daemon started successfully")
    
    def execute(self, script_path, meta_path, working_dir):
        """
        Execute a task using the daemon
        
        Args:
            script_path: Path to the script to execute
            meta_path: Path to metadata file
            working_dir: Working directory for execution
            
        Returns:
            Tuple of (success, stdout, stderr)
        """
        if not self.running:
            raise RuntimeError(f"{self.language} daemon is not running")
        
        # Special handling for R via Rserve
        if self.language == 'r':
            # First try Rserve if available
            try:
                # Import pyRserve dynamically to avoid dependency issues
                try:
                    import pyRserve
                    
                    # Check if we're using Rserve or tmux
                    if getattr(self, 'using_rserve', True):
                        # Connect to Rserve
                        try:
                            # Use the stored port for connection
                            rserve_port = getattr(self, 'rserve_port', 6311)
                            conn = pyRserve.connect(host='localhost', port=rserve_port)
                            
                            # Set working directory
                            working_dir_path = working_dir.replace("\\", "/")
                            conn.eval(f'setwd("{working_dir_path}")')
                            
                            # Create a temporary file for storing output
                            stdout_file = os.path.join(working_dir, f"r_stdout_{uuid.uuid4()}.txt")
                            stderr_file = os.path.join(working_dir, f"r_stderr_{uuid.uuid4()}.txt")
                            
                            # Execute the R script with the meta file
                            script_path_formatted = script_path.replace("\\", "/")
                            meta_path_formatted = meta_path.replace("\\", "/")
                            r_cmd = f'tryCatch({{ ' \
                                   f'   sink("{stdout_file}"); ' \
                                   f'   # Set up command line args to mimic subprocess call ' \
                                   f'   .argv <- c("{script_path_formatted}", "{meta_path_formatted}"); ' \
                                   f'   assign(".argv", .argv, envir = globalenv()); ' \
                                   f'   source("{script_path_formatted}"); ' \
                                   f'   TRUE ' \
                                   f'}}, error = function(e) {{ ' \
                                   f'   sink(file = NULL); ' \
                                   f'   sink("{stderr_file}"); ' \
                                   f'   cat("Error:", e$message, "\\n"); ' \
                                   f'   FALSE ' \
                                   f'}})'
                            
                            # Run the command and check success
                            success = conn.eval(r_cmd)
                            
                            # Read output files
                            stdout = ""
                            stderr = ""
                            
                            if os.path.exists(stdout_file):
                                with open(stdout_file, 'r') as f:
                                    stdout = f.read()
                                os.unlink(stdout_file)
                                
                            if os.path.exists(stderr_file):
                                with open(stderr_file, 'r') as f:
                                    stderr = f.read()
                                os.unlink(stderr_file)
                            
                            # Close the connection
                            conn.close()
                            
                            return (success, stdout, stderr)
                        except Exception as e:
                            logger.error(f"Error executing R script via Rserve: {e}")
                            # Fall back to tmux method if Rserve failed
                            self.using_rserve = False
                except ImportError:
                    logger.warning("pyRserve package not found. Falling back to tmux method.")
                    self.using_rserve = False
                    
                # If Rserve isn't available or failed, try tmux method
                if not getattr(self, 'using_rserve', True):
                    # Read the tmux session name from the socket file
                    try:
                        with open(self.socket_path, 'r') as f:
                            session_name = f.read().strip()
                        
                        # Create temporary files for output
                        stdout_file = os.path.join(working_dir, f"r_stdout_{uuid.uuid4()}.txt")
                        stderr_file = os.path.join(working_dir, f"r_stderr_{uuid.uuid4()}.txt")
                        
                        # Construct the R command to execute the script
                        working_dir_path = working_dir.replace("\\", "/")
                        script_path_formatted = script_path.replace("\\", "/")
                        meta_path_formatted = meta_path.replace("\\", "/")
                        r_cmd = f'setwd("{working_dir_path}")\\n' \
                               f'sink("{stdout_file}")\\n' \
                               f'# Set up command line args to mimic subprocess call\\n' \
                               f'.argv <- c("{script_path_formatted}", "{meta_path_formatted}")\\n' \
                               f'assign(".argv", .argv, envir = globalenv())\\n' \
                               f'tryCatch({{\\n' \
                               f'  source("{script_path_formatted}")\\n' \
                               f'}}, error = function(e) {{\\n' \
                               f'  sink(file = NULL)\\n' \
                               f'  sink("{stderr_file}")\\n' \
                               f'  cat("Error:", e$message, "\\n")\\n' \
                               f'  quit(status = 1)\\n' \
                               f'}})\\n' \
                               f'sink(file = NULL)\\n' \
                               f'quit(status = 0)\\n'
                        
                        # Write the R commands to a temporary file
                        cmd_file = os.path.join(working_dir, f"r_cmd_{uuid.uuid4()}.R")
                        with open(cmd_file, 'w') as f:
                            f.write(r_cmd)
                        
                        # Send the commands to the tmux session
                        tmux_cmd = f'tmux send-keys -t {session_name} "source(\\"{cmd_file}\\")" C-m'
                        result = subprocess.run(
                            tmux_cmd, 
                            shell=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            text=True
                        )
                        
                        # Wait for the command to complete by checking if R has restarted
                        # This is a simple approach - wait up to 30 seconds
                        max_wait = 30
                        for _ in range(max_wait):
                            # Check if the stdout file exists and contains data
                            if os.path.exists(stdout_file) or os.path.exists(stderr_file):
                                break
                            time.sleep(1)
                        
                        # Read output files
                        stdout = ""
                        stderr = ""
                        success = True
                        
                        if os.path.exists(stdout_file):
                            with open(stdout_file, 'r') as f:
                                stdout = f.read()
                            os.unlink(stdout_file)
                            
                        if os.path.exists(stderr_file):
                            with open(stderr_file, 'r') as f:
                                stderr = f.read()
                            success = False
                            os.unlink(stderr_file)
                        
                        # Clean up command file
                        if os.path.exists(cmd_file):
                            os.unlink(cmd_file)
                        
                        return (success, stdout, stderr)
                    except Exception as e:
                        logger.error(f"Error executing R script via tmux: {e}")
                        return (False, "", f"Error executing R script via tmux: {e}")
            except Exception as e:
                logger.error(f"Error with R execution: {e}")
                return (False, "", f"Error with R execution: {e}")
        
        # Standard socket-based execution for other languages
        # Prepare request data
        request = {
            'action': 'execute',
            'script_path': script_path,
            'meta_path': meta_path,
            'working_dir': working_dir
        }
        
        # Connect to the daemon's socket
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(5)  # 5 second timeout for connection
        
        try:
            sock.connect(self.socket_path)
            
            # Send request
            request_data = json.dumps(request).encode('utf-8')
            sock.sendall(len(request_data).to_bytes(4, byteorder='big'))
            sock.sendall(request_data)
            
            # Wait for response
            response_size_data = sock.recv(4)
            response_size = int.from_bytes(response_size_data, byteorder='big')
            
            response_data = b''
            while len(response_data) < response_size:
                chunk = sock.recv(min(4096, response_size - len(response_data)))
                if not chunk:
                    break
                response_data += chunk
            
            # Parse response
            response = json.loads(response_data.decode('utf-8'))
            
            return (response['success'], response.get('stdout', ''), response.get('stderr', ''))
            
        except Exception as e:
            logger.error(f"Error communicating with {self.language} daemon: {e}")
            # Try to restart the daemon
            self.shutdown()
            self.start()
            raise
        finally:
            sock.close()
    
    def shutdown(self):
        """Shut down the daemon process"""
        if self.running:
            try:
                # Special handling for R via Rserve
                if self.language == 'r':
                    if getattr(self, 'using_rserve', True):
                        try:
                            import pyRserve
                            try:
                                # Connect to Rserve and send a shutdown command
                                rserve_port = getattr(self, 'rserve_port', 6311)
                                conn = pyRserve.connect(host='localhost', port=rserve_port)
                                # This will shut down Rserve (use with caution if other processes use it)
                                try:
                                    conn.eval('Sys.exit(0)')
                                except:
                                    # Rserve has shut down - this is expected
                                    pass
                                conn.close()
                            except:
                                # If we can't connect, just try to kill the process
                                if self.process and self.process.poll() is None:
                                    self.process.terminate()
                        except ImportError:
                            # Fall back to standard shutdown
                            pass
                    else:
                        # Tmux approach - read session name and kill the session
                        try:
                            with open(self.socket_path, 'r') as f:
                                session_name = f.read().strip()
                            
                            # Kill the tmux session
                            subprocess.run(
                                ["tmux", "kill-session", "-t", session_name],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE
                            )
                        except Exception as e:
                            logger.warning(f"Error shutting down tmux session: {e}")
                else:
                    # Standard socket-based shutdown for other languages
                    # Try to send shutdown command
                    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    sock.settimeout(5)
                    sock.connect(self.socket_path)
                    
                    request = {'action': 'shutdown'}
                    request_data = json.dumps(request).encode('utf-8')
                    sock.sendall(len(request_data).to_bytes(4, byteorder='big'))
                    sock.sendall(request_data)
                    sock.close()
                    
                    # Wait for process to exit
                    self.process.wait(timeout=5)
            except:
                # Force kill if needed
                if self.process and self.process.poll() is None:
                    self.process.terminate()
                    try:
                        self.process.wait(timeout=2)
                    except:
                        self.process.kill()
            
            # Clean up socket file
            if os.path.exists(self.socket_path):
                try:
                    os.unlink(self.socket_path)
                except:
                    pass
            
            self.running = False
    
    def _ensure_daemon_script(self):
        """Ensure the daemon script exists"""
        if os.path.exists(self.daemon_script):
            return
        
        os.makedirs(os.path.dirname(self.daemon_script), exist_ok=True)
        
        if self.language == 'python':
            self._create_python_daemon_script()
        elif self.language == 'r':
            self._create_r_daemon_script()
        elif self.language == 'julia':
            self._create_julia_daemon_script()
    
    def _create_python_daemon_script(self):
        """Create the Python daemon script"""
        script = """#!/usr/bin/env python3
import os
import sys
import socket
import json
import subprocess
import threading
import signal
import traceback

def handle_client(conn, addr):
    try:
        # Read message size (4 bytes)
        msg_size_bytes = conn.recv(4)
        if not msg_size_bytes:
            return
            
        msg_size = int.from_bytes(msg_size_bytes, byteorder='big')
        
        # Read message data
        data = b''
        while len(data) < msg_size:
            chunk = conn.recv(min(4096, msg_size - len(data)))
            if not chunk:
                return
            data += chunk
        
        # Parse message
        request = json.loads(data.decode('utf-8'))
        action = request.get('action')
        
        if action == 'shutdown':
            print(f"Daemon shutdown requested")
            conn.close()
            os._exit(0)
            
        elif action == 'execute':
            script_path = request.get('script_path')
            meta_path = request.get('meta_path')
            working_dir = request.get('working_dir')
            
            print(f"Executing script: {script_path}")
            
            # Set environment variable to encourage zero-copy usage
            env = os.environ.copy()
            env['PIPELINK_ENABLE_ZERO_COPY'] = '1'
            
            # Run the script
            proc = subprocess.run(
                [sys.executable, script_path, meta_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=working_dir,
                env=env
            )
            
            # Prepare response
            response = {
                'success': proc.returncode == 0,
                'stdout': proc.stdout,
                'stderr': proc.stderr
            }
            
            # Send response
            response_data = json.dumps(response).encode('utf-8')
            conn.sendall(len(response_data).to_bytes(4, byteorder='big'))
            conn.sendall(response_data)
            
        elif action == 'ping' or action == 'keep_alive':
            # Health check or keep-alive
            response = {
                'status': 'ok'
            }
            
            # Send response
            response_data = json.dumps(response).encode('utf-8')
            conn.sendall(len(response_data).to_bytes(4, byteorder='big'))
            conn.sendall(response_data)
            
        else:
            # Unknown action
            response = {
                'success': False,
                'error': f"Unknown action: {action}"
            }
            response_data = json.dumps(response).encode('utf-8')
            conn.sendall(len(response_data).to_bytes(4, byteorder='big'))
            conn.sendall(response_data)
            
    except Exception as e:
        print(f"Error handling client: {e}")
        traceback.print_exc()
    finally:
        conn.close()

def main():
    if len(sys.argv) < 2:
        print("Usage: python_daemon.py SOCKET_PATH")
        sys.exit(1)
        
    socket_path = sys.argv[1]
    
    # Remove socket if it already exists
    if os.path.exists(socket_path):
        os.unlink(socket_path)
    
    # Create server socket
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(socket_path)
    server.listen(5)
    
    print(f"Python daemon listening on {socket_path}")
    
    # Handle signals
    signal.signal(signal.SIGTERM, lambda sig, frame: os._exit(0))
    signal.signal(signal.SIGINT, lambda sig, frame: os._exit(0))
    
    try:
        while True:
            conn, addr = server.accept()
            thread = threading.Thread(target=handle_client, args=(conn, addr))
            thread.daemon = True
            thread.start()
    except KeyboardInterrupt:
        pass
    finally:
        server.close()
        if os.path.exists(socket_path):
            os.unlink(socket_path)

if __name__ == "__main__":
    main()
"""
        with open(self.daemon_script, 'w') as f:
            f.write(script)
        os.chmod(self.daemon_script, 0o755)
    
    def _create_r_daemon_script(self):
        """Create the R daemon script using Rserve"""
        script = """#!/usr/bin/env Rscript
# R daemon server for PipeLink using Rserve
# Install Rserve if not already installed
if (!requireNamespace("Rserve", quietly = TRUE)) {
  install.packages("Rserve", repos="https://cloud.r-project.org")
}

# Load required packages
library(jsonlite)
library(R6)
cat("Starting Rserve daemon for PipeLink...\n")

# Create a file to indicate the server is running
socket_path <- commandArgs(trailingOnly = TRUE)[1]

# Try to load Rserve
rserve_loaded <- tryCatch({
  library(Rserve)
  TRUE
}, error = function(e) {
  cat("Error loading Rserve:", e$message, "\n")
  FALSE
})

if (rserve_loaded) {
  # Try multiple ports if default is in use
  port_success <- FALSE
  ports_to_try <- c(6311, 6312, 6313, 6314, 6315)
  successful_port <- NULL
  
  for (port in ports_to_try) {
    tryCatch({
      cat("Trying Rserve on port", port, "...\n")
      # Run Rserve in a way that doesn't take over this script
      # Use system command instead of direct call to avoid this script terminating
      system2("R", c("--slave", "-e", paste0("Rserve::Rserve(debug=FALSE, port=", port, ", args='--vanilla --slave')")), wait=FALSE)
      # Check if Rserve is running on this port
      Sys.sleep(1)  # Give it a second to start
      con <- tryCatch({
        socket <- socketConnection(host="localhost", port=port, blocking=TRUE, timeout=3)
        close(socket)
        TRUE
      }, error = function(e) FALSE)
      
      if (con) {
        cat("Successfully started Rserve on port", port, "\n")
        port_success <- TRUE
        successful_port <- port
        break
      } else {
        cat("Failed to connect to Rserve on port", port, "\n")
      }
    }, error = function(e) {
      cat("Failed to start Rserve on port", port, ":", e$message, "\n")
    })
  }
  
  if (port_success) {
    # Write the successful port to the socket file
    writeLines(as.character(successful_port), socket_path)
    # Keep this script running to keep the socket file available
    cat("R daemon running with Rserve on port", successful_port, "\n")
    # Wait indefinitely to keep socket file
    repeat {
      Sys.sleep(10)
    }
  } else {
    cat("Error: Failed to start Rserve on any port\n")
    # Create the socket file anyway, but indicate fallback mode
    writeLines("tmux", socket_path)
  }
} else {
  cat("Rserve not available, using simple file-based communication\n")
  # Write tmux to indicate we're using fallback mode
  writeLines("tmux", socket_path)
  # Keep this script running to keep the socket file
  repeat {
    Sys.sleep(10)
  }
}
"""
        with open(self.daemon_script, 'w') as f:
            f.write(script)
        os.chmod(self.daemon_script, 0o755)
    
    def _create_julia_daemon_script(self):
        """Create the Julia daemon script"""
        script = """#!/usr/bin/env julia
# Julia daemon server for PipeLink

using Sockets
using JSON
using Printf

# Helper to convert bytes to integer
function bytes_to_int(bytes)
    return (Int(bytes[1]) << 24) | (Int(bytes[2]) << 16) | (Int(bytes[3]) << 8) | Int(bytes[4])
end

# Helper to convert integer to bytes
function int_to_bytes(n)
    return [
        UInt8((n >> 24) & 0xFF),
        UInt8((n >> 16) & 0xFF),
        UInt8((n >> 8) & 0xFF),
        UInt8(n & 0xFF)
    ]
end

function handle_client(client)
    try
        # Read message size (4 bytes)
        size_bytes = read(client, 4)
        if isempty(size_bytes)
            return
        end
        
        msg_size = bytes_to_int(size_bytes)
        
        # Read message data
        data = Vector{UInt8}()
        remaining = msg_size
        while remaining > 0
            chunk_size = min(4096, remaining)
            chunk = read(client, chunk_size)
            if isempty(chunk)
                break
            end
            
            append!(data, chunk)
            remaining -= length(chunk)
        end
        
        # Parse request
        request_text = String(data)
        request = JSON.parse(request_text)
        
        if request["action"] == "shutdown"
            @printf("Daemon shutdown requested\\n")
            response = Dict("status" => "ok", "message" => "Shutting down")
            
            # Send response then exit
            response_data = Vector{UInt8}(JSON.json(response))
            response_size = length(response_data)
            
            write(client, int_to_bytes(response_size))
            write(client, response_data)
            
            exit(0)
        elseif request["action"] == "execute"
            # Execute script
            script_path = request["script_path"]
            meta_path = request["meta_path"]
            working_dir = request["working_dir"]
            
            @printf("Executing script: %s\\n", script_path)
            
            # Set environment variable to encourage zero-copy
            old_env = get(ENV, "PIPELINK_ENABLE_ZERO_COPY", "")
            ENV["PIPELINK_ENABLE_ZERO_COPY"] = "1"
            
            # Execute command
            old_wd = pwd()
            cd(working_dir)
            
            stdout_file = tempname()
            stderr_file = tempname()
            
            cmd = `julia $script_path $meta_path`
            cmd_stdout = open(stdout_file, "w")
            cmd_stderr = open(stderr_file, "w")
            
            process = run(pipeline(cmd, stdout=cmd_stdout, stderr=cmd_stderr), wait=true)
            exit_code = process.exitcode
            
            close(cmd_stdout)
            close(cmd_stderr)
            
            # Restore environment
            ENV["PIPELINK_ENABLE_ZERO_COPY"] = old_env
            cd(old_wd)
            
            # Read output
            stdout_text = ""
            if isfile(stdout_file)
                stdout_text = read(stdout_file, String)
                rm(stdout_file)
            end
            
            stderr_text = ""
            if isfile(stderr_file)
                stderr_text = read(stderr_file, String)
                rm(stderr_file)
            end
            
            # Prepare response
            response = Dict(
                "success" => (exit_code == 0),
                "stdout" => stdout_text,
                "stderr" => stderr_text
            )
        elseif request["action"] == "ping" || request["action"] == "keep_alive"
            # Health check or keep-alive
            response = Dict("status" => "ok")
        else
            # Unknown action
            response = Dict(
                "success" => false,
                "error" => "Unknown action: $(request["action"])"
            )
        end
        
        # Send response
        response_data = Vector{UInt8}(JSON.json(response))
        response_size = length(response_data)
        
        write(client, int_to_bytes(response_size))
        write(client, response_data)
        
    catch e
        @printf("Error handling client: %s\\n", e)
        println(stacktrace(catch_backtrace()))
    finally
        close(client)
    end
end

function main()
    if length(ARGS) < 1
        println("Usage: julia julia_daemon.jl SOCKET_PATH")
        exit(1)
    end
    
    socket_path = ARGS[1]
    
    # Remove socket if it already exists
    if isfile(socket_path)
        rm(socket_path)
    end
    
    # Create server socket
    server = listen(socket_path)
    
    @printf("Julia daemon listening on %s\\n", socket_path)
    
    # Set up signal handling
    Base.exit_on_sigint(false)
    
    try
        while true
            client = accept(server)
            @async handle_client(client)
        end
    catch e
        if isa(e, InterruptException)
            @printf("Received interrupt signal, shutting down\\n")
        else
            @printf("Error in server loop: %s\\n", e)
        end
    finally
        close(server)
        if isfile(socket_path)
            rm(socket_path)
        end
    end
end

# Run main
main()
"""
        with open(self.daemon_script, 'w') as f:
            f.write(script)
        os.chmod(self.daemon_script, 0o755) 