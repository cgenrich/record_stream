CREATE TABLE transactions (
  id INTEGER auto_increment PRIMARY KEY,
  name VARCHAR(30),
  description VARCHAR(50),
  amount INTEGER NOT NULL,
  curr_ts TIMESTAMP DEFAULT NOW()
);