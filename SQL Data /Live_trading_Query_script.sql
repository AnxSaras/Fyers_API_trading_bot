CREATE DATABASE IF NOT EXISTS Live_trading;

-- Use Database
USE Live_trading;

-- Table structure for table trade_log
DROP TABLE IF EXISTS trade_log;
CREATE TABLE log_trade (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    side ENUM('BUY', 'SELL') NOT NULL,
    qty INT NOT NULL,
    entry_price DECIMAL(10,2) NOT NULL,
    order_id VARCHAR(50) UNIQUE NOT NULL,
    trade_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE Live_trading.symbol AS 
SELECT * FROM Historical_data_2024.symbol;

SELECT * FROM symbol;
SELECT * FROM log_trade;