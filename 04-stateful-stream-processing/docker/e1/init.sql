CREATE SCHEMA bde_h6;

CREATE TABLE bde_h6.visits (
  userId INTEGER NOT NULL,
  eventTime TIMESTAMP NOT NULL,
  page VARCHAR(20) NOT NULL,
  browserKey VARCHAR(20) NOT NULL,
  browserVersion VARCHAR(20) NOT NULL,
  PRIMARY KEY (userId, eventTime)
);
