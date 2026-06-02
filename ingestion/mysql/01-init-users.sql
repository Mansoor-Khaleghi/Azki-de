-- =====================================================================
--  MySQL init: create the "production" users table, load the seed CSV,
--  and grant CDC privileges so Debezium can stream changes (Part 1 bonus).
--  Runs automatically on first container start (docker-entrypoint-initdb.d).
-- =====================================================================

USE azki;

CREATE TABLE IF NOT EXISTS users (
    user_id     INT UNSIGNED NOT NULL PRIMARY KEY,
    signup_date DATE         NOT NULL,
    city        VARCHAR(64)  NOT NULL,
    device_type VARCHAR(32)  NOT NULL,
    -- audit columns a real OLTP table would carry; useful for CDC ordering
    updated_at  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
                             ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Server-side bulk load from the file mounted into secure_file_priv.
LOAD DATA INFILE '/var/lib/mysql-files/users.csv'
INTO TABLE users
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(user_id, @signup_date, city, device_type)
SET signup_date = STR_TO_DATE(@signup_date, '%Y-%m-%d');

-- ─── CDC privileges for Debezium (Kafka Connect MySQL source) ───
GRANT SELECT, RELOAD, SHOW DATABASES,
      REPLICATION SLAVE, REPLICATION CLIENT
  ON *.* TO 'azki'@'%';
FLUSH PRIVILEGES;

SELECT CONCAT('users loaded: ', COUNT(*)) AS load_check FROM users;
