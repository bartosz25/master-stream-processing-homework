CREATE SCHEMA bde_schema;


CREATE TYPE device_types AS ENUM ('tablet', 'smartphone', 'pc');

CREATE TABLE bde_schema.devices (
  device_type device_types NOT NULL,
  full_name VARCHAR(100) NOT NULL,
  PRIMARY KEY (device_type)
);


INSERT INTO bde_schema.devices VALUES
('tablet', 'Any tablet'),
  ('smartphone', 'Any smartphone'),
  ('pc', 'A PC or a MacBook');
