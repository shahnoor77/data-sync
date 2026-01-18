"""
Debezium embedded engine with JPype bridge
Captures CDC events from source database transaction log
"""

import jpype
import jpype.imports
from jpype.types import *
import json
import queue
import threading
from typing import Dict, Any, Callable
from pathlib import Path


class DebeziumEngine:
    """
    Embedded Debezium engine using JPype
    Captures database changes via CDC
    """
    
    def __init__(
        self,
        properties: Dict[str, str],
        event_handler: Callable[[Dict[str, Any]], None],
        logger
    ):
        """
        Initialize Debezium engine
        
        Args:
            properties: Debezium connector properties
            event_handler: Callback function for handling events
            logger: Logger instance
        """
        self.properties = properties
        self.event_handler = event_handler
        self.logger = logger
        self.event_queue = queue.Queue(maxsize=1000)
        self.running = False
        
        # Initialize JVM
        self._init_jvm()
        
        # Create engine
        self.engine = None
        self.consumer = None
    
    def _init_jvm(self):
        """Initialize Java Virtual Machine"""
        if jpype.isJVMStarted():
            self.logger.info("JVM already started")
            return
        
        # Find JAR files - In Docker, ensure this matches your volume/path
        lib_dir = Path("/app/debezium/lib")
        if not lib_dir.exists():
            lib_dir = Path("debezium/lib") # Fallback for local dev
            
        jar_files = list(lib_dir.glob("*.jar"))
        
        if not jar_files:
            raise FileNotFoundError(
                f"No JAR files found in {lib_dir} directory. "
                "Ensure JARs are downloaded and paths are correct."
            )
        
        # Build classpath using Linux separator (:) for Docker
        classpath = ":".join([str(jar.absolute()) for jar in jar_files])
        
        self.logger.info(f"Starting JVM with {len(jar_files)} JAR files")
        
        # Start JVM with memory limits and string conversion enabled
        jpype.startJVM(
            jpype.getDefaultJVMPath(),
            f"-Djava.class.path={classpath}",
            "-Xmx512m",
            convertStrings=True
        )
        
        self.logger.info("✓ JVM started successfully")
    
    def start(self):
        """Start the Debezium engine"""
        if self.running:
            self.logger.warning("Engine is already running")
            return
        
        self.logger.info("Starting Debezium engine...")
        
        # Import Java classes
        from java.util import Properties
        
        # Accessing EmbeddedEngine via JPackage to avoid 'io' namespace collision
        debezium_embedded = jpype.JPackage("io").debezium.embedded
        EmbeddedEngine = debezium_embedded.EmbeddedEngine
        
        # Create Java Properties
        props = Properties()
        for key, value in self.properties.items():
            props.setProperty(str(key), str(value))
        
        # Create change consumer
        self.consumer = self._create_consumer()
        
        # Build engine
        engine_builder = EmbeddedEngine.create()
        engine_builder = engine_builder.using(props)
        engine_builder = engine_builder.notifying(self.consumer)
        
        self.engine = engine_builder.build()
        
        # Start engine in separate thread
        self.running = True
        engine_thread = threading.Thread(
            target=self._run_engine,
            daemon=True,
            name="DebeziumEngine"
        )
        engine_thread.start()
        
        # Start event processing thread
        processing_thread = threading.Thread(
            target=self._process_events,
            daemon=True,
            name="EventProcessor"
        )
        processing_thread.start()
        
        self.logger.info("✓ Debezium engine started")

    def _create_consumer(self):
        """Create Java ChangeConsumer implementation"""
        try:
            # Access the inner interface correctly using the $ separator
            # This bypasses the Python 'io' package conflict
            ChangeConsumer = jpype.JClass("io.debezium.engine.DebeziumEngine$ChangeConsumer")
            
            # Define the implementation
            @jpype.JImplements(ChangeConsumer)
            class PythonChangeConsumer:
                def __init__(self, event_queue, logger):
                    self.event_queue = event_queue
                    self.logger = logger
                
                @jpype.JOverride
                def handleBatch(self, records, committer):
                    # CRITICAL: Attach this Java-callback thread to the JVM
                    jpype.attachThreadToJVM()
                    try:
                        # records is a java.util.List
                        for i in range(records.size()):
                            record = records.get(i)
                            # Convert Java string to Python string
                            value = str(record.value())
                            
                            if value:
                                try:
                                    # Non-blocking put with timeout
                                    self.event_queue.put(value, timeout=1.0)
                                except queue.Full:
                                    self.logger.warning("Event queue is full, dropping event")
                        
                        # Confirm batch completion to Debezium engine
                        committer.markBatchFinished()
                        
                    except Exception as e:
                        self.logger.error(f"Error in handleBatch: {e}", exc_info=True)
            
            # Return an instance of our bridged class
            return PythonChangeConsumer(self.event_queue, self.logger)
            
        except Exception as e:
            self.logger.error(f"Failed to load Debezium ChangeConsumer: {e}")
            raise

    def _run_engine(self):
        """Run the Debezium engine (called in separate thread)"""
        # CRITICAL: Attach this specific Python thread to the JVM
        jpype.attachThreadToJVM()
        try:
            self.logger.info("Debezium engine thread started and attached to JVM")
            self.engine.run()
        except Exception as e:
            self.logger.error(f"Debezium engine error: {e}", exc_info=True)
            self.running = False
    
    def _process_events(self):
        """Process events from the queue (called in separate thread)"""
        self.logger.info("Event processing thread started")
        
        while self.running:
            try:
                # Get event from queue with timeout
                event_json = self.event_queue.get(timeout=1.0)
                
                # Parse JSON
                event = json.loads(event_json)
                
                # Call event handler
                try:
                    self.event_handler(event)
                except Exception as e:
                    self.logger.error(
                        f"Error in event handler: {e}",
                        exc_info=True
                    )
                
                # Mark as done
                self.event_queue.task_done()
                
            except queue.Empty:
                continue
            except Exception as e:
                self.logger.error(f"Error processing event: {e}", exc_info=True)
    
    def stop(self):
        """Stop the Debezium engine"""
        self.logger.info("Stopping Debezium engine...")
        self.running = False
        
        if self.engine:
            try:
                self.engine.close()
            except Exception as e:
                self.logger.error(f"Error stopping engine: {e}")
        
        self.logger.info("✓ Debezium engine stopped")
    
    def get_queue_size(self) -> int:
        """Get current queue size"""
        return self.event_queue.qsize()