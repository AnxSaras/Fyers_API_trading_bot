-- Create Database
CREATE DATABASE IF NOT EXISTS Historical_data_2024;

-- Use Database
USE Historical_data_2024;

-- Table structure for table daily_price
DROP TABLE IF EXISTS daily_price;
CREATE TABLE daily_price (
  id int NOT NULL AUTO_INCREMENT,
  data_vendor_id int DEFAULT NULL,
  symbol_id int DEFAULT NULL,
  stock_name varchar(100) DEFAULT NULL,
  price_date date DEFAULT NULL,
  created_date datetime DEFAULT CURRENT_TIMESTAMP,
  last_updated_date datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  open_price decimal(10,2) DEFAULT NULL,
  high_price decimal(10,2) DEFAULT NULL,
  low_price decimal(10,2) DEFAULT NULL,
  close_price decimal(10,2) DEFAULT NULL,
  volume bigint DEFAULT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY symbol_id (symbol_id,price_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table structure for table data_vendor
DROP TABLE IF EXISTS data_vendor;
CREATE TABLE data_vendor (
  id int NOT NULL AUTO_INCREMENT,
  name varchar(255) NOT NULL,
  website_url varchar(255) DEFAULT NULL,
  created_date datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_updated_date datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Insert initial data_vendor
INSERT INTO data_vendor VALUES (1,'Fyers API','https://www.fyers.in',NOW(),NOW());

-- Table structure for table exchange
DROP TABLE IF EXISTS exchange;
CREATE TABLE exchange (
  id int NOT NULL AUTO_INCREMENT,
  abbrev varchar(32) NOT NULL,
  name varchar(255) NOT NULL,
  city varchar(255) DEFAULT NULL,
  country varchar(255) DEFAULT NULL,
  currency varchar(64) DEFAULT NULL,
  timezone_offset time DEFAULT NULL,
  created_date datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_updated_date datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Insert initial exchange data
INSERT INTO exchange VALUES (1,'NSE','National Stock Exchange','Mumbai','India','INR','05:30:00',NOW(),NOW());

-- Table structure for table symbol
DROP TABLE IF EXISTS symbol;
CREATE TABLE symbol (
  id int NOT NULL AUTO_INCREMENT,
  exchange_id int NOT NULL,
  ticker varchar(32) NOT NULL,
  name varchar(255) NOT NULL,
  sector varchar(255) DEFAULT NULL,
  industry varchar(255) DEFAULT NULL,
  isin varchar(32) DEFAULT NULL,
  created_date datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_updated_date datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY ticker (ticker),
  KEY exchange_id (exchange_id),
  CONSTRAINT symbol_ibfk_1 FOREIGN KEY (exchange_id) REFERENCES exchange (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Insert NIFTY 50 stock symbols
--- INSERT INTO symbol (id, exchange_id, ticker, name, sector, industry, isin, created_date, last_updated_date) VALUES


--- SELECT * FROM symbol;
SELECT * FROM daily_price;