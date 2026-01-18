-- Create CDC user
CREATE USER IF NOT EXISTS 'debezium_user'@'%' IDENTIFIED BY 'debezium123';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT 
ON *.* TO 'debezium_user'@'%';
FLUSH PRIVILEGES;

-- Create sensor tables
CREATE TABLE IF NOT EXISTS sensor_readings (
    id INT AUTO_INCREMENT PRIMARY KEY,
    device_id VARCHAR(100) NOT NULL,
    sensor_type VARCHAR(50) NOT NULL,
    value DECIMAL(10, 2) NOT NULL,
    unit VARCHAR(20),
    location VARCHAR(200),
    metadata JSON,
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_device_id (device_id),
    INDEX idx_timestamp (timestamp)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS temperature_sensors (
    id INT AUTO_INCREMENT PRIMARY KEY,
    device_id VARCHAR(100) NOT NULL,
    temperature DECIMAL(5, 2) NOT NULL,
    humidity DECIMAL(5, 2),
    location VARCHAR(200),
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_device_id (device_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS pressure_sensors (
    id INT AUTO_INCREMENT PRIMARY KEY,
    device_id VARCHAR(100) NOT NULL,
    pressure DECIMAL(8, 2) NOT NULL,
    altitude DECIMAL(8, 2),
    location VARCHAR(200),
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_device_id (device_id)
) ENGINE=InnoDB;

-- Insert sample data
INSERT INTO sensor_readings (device_id, sensor_type, value, unit, location, metadata) VALUES
('SENSOR001', 'temperature', 23.5, 'celsius', 'Room A', '{"floor": 1, "building": "Main"}'),
('SENSOR002', 'temperature', 24.1, 'celsius', 'Room B', '{"floor": 2, "building": "Main"}'),
('SENSOR003', 'pressure', 101.3, 'kPa', 'Tank 1', '{"capacity": 1000, "type": "water"}');

INSERT INTO temperature_sensors (device_id, temperature, humidity, location) VALUES
('TEMP001', 22.5, 60.2, 'Server Room'),
('TEMP002', 25.3, 55.8, 'Office Floor 1');

INSERT INTO pressure_sensors (device_id, pressure, altitude, location) VALUES
('PRESS001', 101.325, 0.0, 'Ground Level'),
('PRESS002', 89.875, 1000.0, 'Roof Tank');
