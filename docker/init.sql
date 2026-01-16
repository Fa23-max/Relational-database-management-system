-- PostgreSQL initialization script for Mini RDBMS
-- This script sets up the database with the required configuration

-- Create the main database (already created by POSTGRES_DB env var)
-- Additional setup can be added here if needed

-- Create extensions if needed
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create sample schema for demonstration (optional)
-- This can be used for testing PostgreSQL connectivity

-- Sample users table
CREATE TABLE IF NOT EXISTS sample_users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sample orders table
CREATE TABLE IF NOT EXISTS sample_orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES sample_users(id),
    amount DECIMAL(10,2) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data
INSERT INTO sample_users (username, email) VALUES 
('john_doe', 'john@example.com'),
('jane_smith', 'jane@example.com'),
('bob_wilson', 'bob@example.com')
ON CONFLICT (username) DO NOTHING;

INSERT INTO sample_orders (user_id, amount, description) VALUES 
(1, 99.99, 'Laptop purchase'),
(2, 49.99, 'Mouse purchase'),
(1, 29.99, 'Keyboard purchase')
ON CONFLICT DO NOTHING;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;
