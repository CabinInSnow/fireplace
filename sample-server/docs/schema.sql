-- Drop existing tables
DROP TABLE IF EXISTS inventory CASCADE;
DROP TABLE IF EXISTS heroes CASCADE;
DROP TABLE IF EXISTS users CASCADE;
DROP TABLE IF EXISTS characters CASCADE;
DROP TABLE IF EXISTS guilds CASCADE;
DROP TABLE IF EXISTS guild_members CASCADE;

-- Users table
CREATE TABLE users (
    id BIGINT PRIMARY KEY,
    display_id VARCHAR(12) NOT NULL UNIQUE,
    nickname VARCHAR(50) NOT NULL,
    device_id VARCHAR(255) NOT NULL,
    login_type INT NOT NULL,
    login_id VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(login_type, login_id)
);

-- Heroes table
CREATE TABLE heroes (
    id BIGINT PRIMARY KEY,
    uid BIGINT NOT NULL,
    hero_type VARCHAR(50) NOT NULL,
    level INT NOT NULL DEFAULT 1,
    exp INT NOT NULL DEFAULT 0
);
CREATE INDEX idx_heroes_uid ON heroes(uid);

-- Inventory table
CREATE TABLE inventory (
    id BIGINT PRIMARY KEY,
    uid BIGINT NOT NULL,
    item_name VARCHAR(100) NOT NULL,
    quantity INT NOT NULL DEFAULT 0,
    UNIQUE(uid, item_name)
);
CREATE INDEX idx_inventory_uid ON inventory(uid);
