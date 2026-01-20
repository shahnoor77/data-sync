-- ============================================================================
-- SENSOR SYNC - SOURCE DATABASE INITIALIZATION
-- Optimized for: Python Incremental Polling, .NET compatibility, and Future CDC
-- ============================================================================

-- 1. SECURITY & REPLICATION SETUP
-- Create a dedicated replication user (useful for future Debezium/WAL access)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'replication_user') THEN
        CREATE ROLE replication_user WITH REPLICATION LOGIN PASSWORD 'prod_pass_123';
    END IF;
END $$;

GRANT SELECT ON ALL TABLES IN SCHEMA public TO replication_user;
GRANT USAGE ON SCHEMA public TO replication_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO replication_user;

-- 2. AUTOMATION: UPDATED_AT TRIGGER FUNCTION
-- This ensures that whenever a row is UPDATED, the timestamp changes automatically.
-- This is the "secret sauce" that allows your Python Publisher to find changed data.
CREATE OR REPLACE FUNCTION sync_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- 3. CORE DATA TABLES
-- We use 'updated_at' as the primary bookmark for the Python Publisher.
-- We use 'TIMESTAMP WITH TIME ZONE' to prevent time-drift issues in production.

-- Table: Generic Sensor Readings
CREATE TABLE IF NOT EXISTS sensor_readings (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(100) NOT NULL,
    sensor_type VARCHAR(50) NOT NULL,
    value DECIMAL(12, 4) NOT NULL,
    unit VARCHAR(20),
    location VARCHAR(200),
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Table: Temperature Sensors (Highly targeted for thermal monitoring)
CREATE TABLE IF NOT EXISTS temperature_sensors (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(100) NOT NULL,
    temperature DECIMAL(5, 2) NOT NULL,
    humidity DECIMAL(5, 2),
    location VARCHAR(200),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Table: Pressure Sensors (Optimized for high-precision pressure data)
CREATE TABLE IF NOT EXISTS pressure_sensors (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(100) NOT NULL,
    pressure DECIMAL(10, 2) NOT NULL,
    altitude DECIMAL(10, 2),
    location VARCHAR(200),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 4. PERFORMANCE INDEXES
-- CRITICAL: Without these, the Publisher will perform "Sequential Scans" 
-- which will crash your database CPU during a 200k record/sec test.
CREATE INDEX IF NOT EXISTS idx_readings_updated_at ON sensor_readings(updated_at);
CREATE INDEX IF NOT EXISTS idx_readings_device_id ON sensor_readings(device_id);

CREATE INDEX IF NOT EXISTS idx_temp_updated_at ON temperature_sensors(updated_at);
CREATE INDEX IF NOT EXISTS idx_temp_device_id ON temperature_sensors(device_id);

CREATE INDEX IF NOT EXISTS idx_press_updated_at ON pressure_sensors(updated_at);
CREATE INDEX IF NOT EXISTS idx_press_device_id ON pressure_sensors(device_id);

-- 5. TRIGGER ACTIVATION
-- Automatically update the 'updated_at' column on every UPDATE statement.
DROP TRIGGER IF EXISTS trg_readings_updated_at ON sensor_readings;
CREATE TRIGGER trg_readings_updated_at BEFORE UPDATE ON sensor_readings 
FOR EACH ROW EXECUTE PROCEDURE sync_updated_at_column();

DROP TRIGGER IF EXISTS trg_temp_updated_at ON temperature_sensors;
CREATE TRIGGER trg_temp_updated_at BEFORE UPDATE ON temperature_sensors 
FOR EACH ROW EXECUTE PROCEDURE sync_updated_at_column();

DROP TRIGGER IF EXISTS trg_press_updated_at ON pressure_sensors;
CREATE TRIGGER trg_press_updated_at BEFORE UPDATE ON pressure_sensors 
FOR EACH ROW EXECUTE PROCEDURE sync_updated_at_column();

-- 6. CDC / REPLICATION SLOT (Future-proofing)
-- Creates a publication so that you can enable Debezium or .NET CDC later 
-- without any schema changes.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'sensor_sync_pub') THEN
        CREATE PUBLICATION sensor_sync_pub FOR ALL TABLES;
    END IF;
END $$;

-- 7. SEED DATA (For initial validation)
INSERT INTO temperature_sensors (device_id, temperature, humidity, location) VALUES
('TEMP-PROD-01', 22.5, 45.0, 'Data Center A'),
('TEMP-PROD-02', 21.8, 48.2, 'Data Center B');

INSERT INTO pressure_sensors (device_id, pressure, altitude, location) VALUES
('PRESS-PROD-01', 1013.25, 0.0, 'Ground Station'),
('PRESS-PROD-02', 980.50, 250.0, 'Elevated Tank');