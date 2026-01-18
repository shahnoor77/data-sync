-- Create replication user for Debezium
CREATE ROLE debezium_user WITH REPLICATION LOGIN PASSWORD 'debezium123';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium_user;
GRANT USAGE ON SCHEMA public TO debezium_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium_user;

-- Create publication for CDC
CREATE PUBLICATION sensor_publication FOR ALL TABLES;

-- Create sensor tables
CREATE TABLE IF NOT EXISTS sensor_readings (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(100) NOT NULL,
    sensor_type VARCHAR(50) NOT NULL,
    value DECIMAL(10, 2) NOT NULL,
    unit VARCHAR(20),
    location VARCHAR(200),
    metadata JSONB,
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS temperature_sensors (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(100) NOT NULL,
    temperature DECIMAL(5, 2) NOT NULL,
    humidity DECIMAL(5, 2),
    location VARCHAR(200),
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pressure_sensors (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(100) NOT NULL,
    pressure DECIMAL(8, 2) NOT NULL,
    altitude DECIMAL(8, 2),
    location VARCHAR(200),
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes
CREATE INDEX idx_sensor_readings_device_id ON sensor_readings(device_id);
CREATE INDEX idx_sensor_readings_timestamp ON sensor_readings(timestamp);
CREATE INDEX idx_temperature_device_id ON temperature_sensors(device_id);
CREATE INDEX idx_pressure_device_id ON pressure_sensors(device_id);

-- Insert sample data
INSERT INTO sensor_readings (device_id, sensor_type, value, unit, location, metadata) VALUES
('SENSOR001', 'temperature', 23.5, 'celsius', 'Room A', '{"floor": 1, "building": "Main"}'),
('SENSOR002', 'temperature', 24.1, 'celsius', 'Room B', '{"floor": 2, "building": "Main"}'),
('SENSOR003', 'pressure', 101.3, 'kPa', 'Tank 1', '{"capacity": 1000, "type": "water"}'),
('SENSOR004', 'humidity', 65.5, 'percent', 'Warehouse', '{"zone": "A", "section": 3}');

INSERT INTO temperature_sensors (device_id, temperature, humidity, location) VALUES
('TEMP001', 22.5, 60.2, 'Server Room'),
('TEMP002', 25.3, 55.8, 'Office Floor 1'),
('TEMP003', 21.8, 62.1, 'Cold Storage');

INSERT INTO pressure_sensors (device_id, pressure, altitude, location) VALUES
('PRESS001', 101.325, 0.0, 'Ground Level'),
('PRESS002', 89.875, 1000.0, 'Roof Tank'),
('PRESS003', 95.461, 500.0, 'Mid Level Tank');