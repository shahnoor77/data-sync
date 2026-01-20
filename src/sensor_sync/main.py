"""
Level 3: Multi-Instance Parallelism (System Scaling)
Main application entry point with process factory for parallel instances
- Process Factory: Launch parallel processes using multiprocessing
- Topic Isolation: Process 1 (sensors/live/#), Process 2 (sensors/db/#)
- Collision Prevention: Unique client_id per process
"""

import sys
import signal
import time
import multiprocessing
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.sensor_sync.config.settings import Settings
from src.sensor_sync.core.change_detector import ChangeDetector
from src.sensor_sync.core.event_processor import EventProcessor
from src.sensor_sync.core.state_manager import StateManager
from src.sensor_sync.core.dead_letter_queue import DeadLetterQueue
from src.sensor_sync.mqtt.emqx_publisher import EMQXPublisher
from src.sensor_sync.mqtt.emqx_subscriber import EMQXSubscriber
from src.sensor_sync.utils.logger import StructuredLogger
from src.sensor_sync.utils.crypto import CryptoManager
from src.sensor_sync.utils.metrics import MetricsCollector


class PublisherApplication:
    """
    Publisher: Detects changes -> Processes -> Publishes to MQTT
    Uses pure polling-based change detection (no Debezium)
    """
    
    def __init__(self, config_file: str = "config/config.yaml"):
        """Initialize publisher application"""
        print("=" * 70)
        print("  SENSOR SYNC PUBLISHER - Pure MQTT Data Sync")
        print("  (Debezium-Free, Native Database Polling)")
        print("=" * 70)
        
        # Load configuration
        print("\n[1/8] Loading configuration...")
        self.settings = Settings(config_file)
        print("✓ Configuration loaded")
        
        # Setup logger
        print("\n[2/8] Setting up logging...")
        self.logger = StructuredLogger(
            name="sensor_sync_publisher",
            log_dir="logs",
            level=self.settings.get('application.log_level', 'INFO')
        )
        print("✓ Logging configured")
        
        # Setup metrics
        print("\n[3/8] Setting up metrics...")
        self.metrics = MetricsCollector(
            enable_prometheus=self.settings.get('application.enable_metrics', True),
            port=self.settings.get('application.metrics_port', 9090)
        )
        print("✓ Metrics configured")
        
        # Setup crypto
        print("\n[4/8] Setting up encryption...")
        self.crypto = CryptoManager(
            encryption_key=self.settings.get('security.encryption_key'),
            signing_key=self.settings.get('security.signing_key')
        )
        print("✓ Encryption configured")
        
        # Setup state manager
        print("\n[5/8] Setting up state manager...")
        self.state_manager = StateManager(self.logger, self.metrics)
        print("✓ State manager ready")
        
        # Setup DLQ
        print("\n[6/8] Setting up dead letter queue...")
        self.dlq = DeadLetterQueue(
            storage_dir="data/dlq",
            max_retries=self.settings.get('application.retry_attempts', 3),
            logger=self.logger
        )
        print("✓ DLQ ready")
        
        # Setup event processor
        print("\n[7/8] Setting up event processor...")
        self.event_processor = EventProcessor(
            crypto_manager=self.crypto,
            sensitive_fields=self.settings.get_sensitive_fields(),
            logger=self.logger,
            metrics=self.metrics
        )
        print("✓ Event processor ready")
        
        # Setup MQTT publisher (EMQX)
        print("\n[8/8] Setting up EMQX publisher...")
        self.mqtt_publisher = EMQXPublisher(
            config=self.settings.get_mqtt_config(),
            state_manager=self.state_manager,
            dlq=self.dlq,
            logger=self.logger,
            metrics=self.metrics
        )
        print("✓ MQTT publisher ready")
        
        # Change detector (pure Python polling - replaces Debezium)
        self.change_detector = None
        
        self.running = False
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        print("\n✓ Publisher initialized successfully!")
        print("=" * 70)
    
    def _handle_change(self, raw_event: dict):
        """Handle database change event from ChangeDetector"""
        try:
            self.metrics.increment_counter('events_captured')
            
            # Process event
            processed_event = self.event_processor.process_event(raw_event)
            
            if processed_event:
                # Publish to MQTT
                success = self.mqtt_publisher.publish_event(processed_event)
                
                if not success:
                    self.logger.error("Failed to publish event")
                    try:
                        self.dlq.add_message(
                            event_id=processed_event.get('message', {}).get('event_id', 'unknown'),
                            message_type='PUBLISH_FAILURE',
                            payload=processed_event,
                            error="MQTT publish failed"
                        )
                    except Exception as dlq_error:
                        self.logger.error(f"DLQ operation failed: {dlq_error}")
            
        except Exception as e:
            self.logger.error(f"Error handling change: {e}", exc_info=True)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        self.logger.info(f"Received signal {signum}, shutting down...")
        self.stop()
        sys.exit(0)
    
    def start(self):
        """Start the publisher"""
        try:
            self.logger.info("=" * 70)
            self.logger.info("Starting Publisher")
            self.logger.info("=" * 70)
            
            # Connect MQTT publisher
            self.mqtt_publisher.connect()
            
            # Start change detector
            self.logger.info("Starting change detector...")
            self.change_detector = ChangeDetector(
                db_config=self.settings.get_source_db_config(),
                monitored_tables=self.settings.get('source_database.monitored_tables'),
                poll_interval=self.settings.get('change_detection.poll_interval_seconds', 5),
                batch_size=self.settings.get('change_detection.batch_size', 100),
                event_handler=self._handle_change,
                logger=self.logger
            )
            self.change_detector.start()
            
            self.running = True
            
            self.logger.info("✓ Publisher started successfully!")
            self.logger.info("Polling for database changes...")
            
            # Keep running
            while self.running:
                time.sleep(1)
                
                # Periodic tasks
                if int(time.time()) % 300 == 0:  # Every 5 minutes
                    self.dlq.cleanup_old_messages()
                    self.state_manager.cleanup_old_events()
                
        except Exception as e:
            self.logger.error(f"Error starting publisher: {e}", exc_info=True)
            self.stop()
            raise
    
    def stop(self):
        """Stop the publisher"""
        if not self.running:
            return
        
        self.logger.info("Stopping publisher...")
        self.running = False
        
        if self.change_detector:
            self.change_detector.stop()
        
        if self.mqtt_publisher:
            self.mqtt_publisher.disconnect()
        
        stats = self.state_manager.get_statistics()
        self.logger.info("=" * 70)
        self.logger.info("Final Statistics:")
        self.logger.info(f"  Total Events: {stats['total_events']}")
        self.logger.info(f"  Completed: {stats['completed_events']}")
        self.logger.info(f"  Failed: {stats['failed_events']}")
        self.logger.info("=" * 70)
        
        self.logger.info("✓ Publisher stopped")


class SubscriberApplication:
    """
    Level 2: High-Throughput Subscriber with Internal Decoupling
    Level 3: Multi-Instance support with topic isolation
    """
    
    def __init__(self, config_file: str = "config/config.yaml", instance_id: str = None, topic_filter: str = None):
        """Initialize subscriber application with Level 3 multi-instance support"""
        self.instance_id = instance_id or os.getenv('INSTANCE_ID', 'subscriber_001')
        self.topic_filter = topic_filter or "sensors/data/#"  # Default topic
        
        print("=" * 70)
        print(f"  LEVEL 2+3: HIGH-THROUGHPUT SUBSCRIBER - {self.instance_id}")
        print(f"  Topic Filter: {self.topic_filter}")
        print(f"  Features: Internal Decoupling, MySQL Persistence, Quantum Processing")
        print("=" * 70)
        
        # Load configuration
        print("\n[1/6] Loading configuration...")
        self.settings = Settings(config_file)
        print("✓ Configuration loaded")
        
        # Setup logger with instance ID
        print("\n[2/6] Setting up logging...")
        self.logger = StructuredLogger(
            name=f"sensor_sync_{self.instance_id}",
            log_dir="logs",
            level=self.settings.get('application.log_level', 'INFO')
        )
        print("✓ Logging configured")
        
        # Setup metrics
        print("\n[3/6] Setting up metrics...")
        self.metrics = MetricsCollector(
            enable_prometheus=False  # Disable for subscriber
        )
        print("✓ Metrics configured")
        
        # Setup crypto
        print("\n[4/6] Setting up encryption...")
        self.crypto = CryptoManager(
            encryption_key=self.settings.get('security.encryption_key'),
            signing_key=self.settings.get('security.signing_key')
        )
        print("✓ Encryption configured")
        
        # Setup DLQ
        print("\n[5/6] Setting up dead letter queue...")
        self.dlq = DeadLetterQueue(
            storage_dir="data/dlq",
            max_retries=self.settings.get('application.retry_attempts', 3),
            logger=self.logger
        )
        print("✓ DLQ ready")
        
        # Setup MQTT subscriber with Level 3 unique client_id
        print("\n[6/6] Setting up Level 2+3 EMQX subscriber...")
        mqtt_config = self.settings.get_mqtt_config()
        
        # Level 3: Collision Prevention - Unique client_id per process
        mqtt_config['client_id_subscriber'] = f"{self.instance_id}_{os.getpid()}"
        mqtt_config['topic_prefix'] = self.topic_filter.replace('/#', '')
        
        self.mqtt_subscriber = EMQXSubscriber(
            mqtt_config=mqtt_config,
            target_db_config=self.settings.get_target_db_config(),
            crypto_manager=self.crypto,
            dlq=self.dlq,
            logger=self.logger,
            metrics=self.metrics
        )
        print("✓ Level 2+3 MQTT subscriber ready")
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        print(f"\n✓ Level 2+3 Subscriber initialized: {self.instance_id}")
        print("=" * 70)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        self.logger.info(f"Received signal {signum}, shutting down...")
        self.stop()
        sys.exit(0)
    
    def start(self):
        """Start the Level 2+3 subscriber"""
        try:
            self.logger.info("=" * 70)
            self.logger.info(f"Starting Level 2+3 Subscriber: {self.instance_id}")
            self.logger.info(f"Topic Filter: {self.topic_filter}")
            self.logger.info("=" * 70)
            
            # Connect to MQTT broker
            self.logger.info("Connecting to MQTT broker...")
            self.mqtt_subscriber.connect()
            
            self.logger.info(f"✓ Level 2+3 Subscriber started: {self.instance_id}")
            self.logger.info("Processing CDC events with internal decoupling...")
            
            # Run subscriber (blocking)
            self.mqtt_subscriber.run()
            
        except KeyboardInterrupt:
            self.logger.info("Keyboard interrupt received")
            self.stop()
        except Exception as e:
            self.logger.error(f"Error starting subscriber: {e}", exc_info=True)
            self.stop()
            raise
    
    def stop(self):
        """Stop the Level 2+3 subscriber"""
        self.logger.info(f"Stopping Level 2+3 Subscriber: {self.instance_id}...")
        
        if self.mqtt_subscriber:
            self.mqtt_subscriber.disconnect()
        
        self.logger.info(f"✓ Level 2+3 Subscriber stopped: {self.instance_id}")


def run_subscriber_process(instance_id: str, topic_filter: str):
    """
    Level 3: Process Factory function for running subscriber instances
    Each process gets unique client_id and topic isolation
    """
    try:
        print(f"Level 3: Starting subscriber process {instance_id} for {topic_filter}")
        
        # Set environment variables for this process
        os.environ['INSTANCE_ID'] = instance_id
        
        # Create and start subscriber
        app = SubscriberApplication(instance_id=instance_id, topic_filter=topic_filter)
        app.start()
        
    except Exception as e:
        print(f"Level 3: Subscriber process {instance_id} failed: {e}")
        raise


def start_multi_instance_subscribers():
    """
    Level 3: Multi-Instance Parallelism with "Quiet Start"
    Launch parallel subscriber processes with staggered start to prevent thundering herd
    """
    print("=" * 70)
    print("  LEVEL 3: MULTI-INSTANCE PARALLELISM WITH QUIET START")
    print("  Process Factory with Topic Isolation + Staggered Launch")
    print("=" * 70)
    
    # Level 3: Topic Isolation Configuration
    subscriber_configs = [
        {
            'instance_id': 'sub_live_01',
            'topic_filter': 'sensors/live/#',  # Real-time stream
            'description': 'Real-time sensor data processor',
            'stagger_delay': 0  # First instance starts immediately
        },
        {
            'instance_id': 'sub_db_01', 
            'topic_filter': 'sensors/db/#',    # Batch database sync
            'description': 'Batch database sync processor',
            'stagger_delay': 2  # Second instance waits 2 seconds
        }
    ]
    
    processes = []
    
    try:
        # Launch parallel processes with staggered start
        for i, config in enumerate(subscriber_configs):
            print(f"\nLevel 3: Launching {config['instance_id']} - {config['description']}")
            print(f"Topic: {config['topic_filter']}")
            
            # Quiet Start: Staggered launch to prevent thundering herd
            if config['stagger_delay'] > 0:
                print(f"Quiet Start: Waiting {config['stagger_delay']}s to prevent collision...")
                time.sleep(config['stagger_delay'])
            
            process = multiprocessing.Process(
                target=run_subscriber_process,
                args=(config['instance_id'], config['topic_filter']),
                name=config['instance_id']
            )
            process.start()
            processes.append(process)
            
            print(f"✓ Process {config['instance_id']} started (PID: {process.pid})")
        
        print(f"\n✓ Level 3: All {len(processes)} subscriber processes launched with Quiet Start")
        print("Collision Prevention: Each process has unique client_id + staggered launch")
        print("Topic Isolation: Real-time vs Batch processing")
        
        # Wait for all processes
        for process in processes:
            process.join()
            
    except KeyboardInterrupt:
        print("\nLevel 3: Shutting down all subscriber processes...")
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
        print("✓ Level 3: All processes stopped")
        
    except Exception as e:
        print(f"Level 3: Error in multi-instance setup: {e}")
        for process in processes:
            if process.is_alive():
                process.terminate()
        raise


def main():
    """
    Level 3: Main entry point with Process Factory
    Supports both single instance and multi-instance parallelism
    """
    import os
    
    # Check service mode from environment
    service_mode = os.getenv('SERVICE_MODE', 'publisher').lower()
    multi_instance = os.getenv('MULTI_INSTANCE', 'false').lower() == 'true'
    
    if service_mode == 'publisher':
        # Run publisher (CDC -> MQTT)
        app = PublisherApplication()
        app.start()
        
    elif service_mode == 'subscriber':
        if multi_instance:
            # Level 3: Multi-Instance Parallelism
            start_multi_instance_subscribers()
        else:
            # Single instance subscriber
            instance_id = os.getenv('INSTANCE_ID', 'subscriber_001')
            topic_filter = os.getenv('TOPIC_FILTER', 'sensors/data/#')
            app = SubscriberApplication(instance_id=instance_id, topic_filter=topic_filter)
            app.start()
            
    else:
        print(f"Unknown service mode: {service_mode}")
        print("Set SERVICE_MODE environment variable to 'publisher' or 'subscriber'")
        print("For Level 3 multi-instance: Set MULTI_INSTANCE=true")
        sys.exit(1)


if __name__ == "__main__":
    main()