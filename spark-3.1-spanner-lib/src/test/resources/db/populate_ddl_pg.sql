CREATE TABLE composite_table (
  id int NOT NULL,
  charvCol character varying(1024),
  textCol text,
  varcharCol varchar(1024),
  boolCol bool,
  booleanCol boolean,
  bigintCol bigint,
  int8Col int8,
  intCol int,
  doubleCol double precision,
  floatCol float8,
  byteCol bytea,
  dateCol date,
  numericCol numeric,
  decimalCol decimal,
  timeWithZoneCol timestamp with time zone,
  timestampCol timestamptz,
  jsonCol jsonb,
  PRIMARY KEY(id)
);

CREATE TABLE integration_composite_table (
  id int NOT NULL,
  charvCol character varying(1024),
  textCol text,
  varcharCol varchar(1024),
  boolCol bool,
  booleanCol boolean,
  bigintCol bigint,
  int8Col int8,
  intCol int,
  doubleCol double precision,
  floatCol float8,
  byteCol bytea,
  dateCol date,
  numericCol numeric,
  decimalCol decimal,
  timeWithZoneCol timestamp with time zone,
  timestampCol timestamptz,
  jsonCol jsonb,
  PRIMARY KEY(id)
);

CREATE TABLE numeric_table (
  id int NOT NULL,
  numericCol numeric,
  PRIMARY KEY(id)
);

CREATE TABLE array_table (
  id int NOT NULL,
  charvArray character varying(1024)[],
  boolArray bool[3],
  bigintArray bigint[],
  doubleArray double precision[],
  byteArray bytea[],
  dateArray date[],
  numericArray numeric[],
  timestampArray timestamptz[],
  jsonArray jsonb[],
  PRIMARY KEY(id)
);

CREATE TABLE Shakespeare (
  id int,
  word character varying(1024),
  word_count int,
  corpus character varying(1024),
  corpus_date int,
  PRIMARY KEY(id)
);

CREATE TABLE string_table (
  id bigint NOT NULL,
  charvCol character varying(1024),
  textCol text,
  varcharCol varchar(1024),
  smallCol character varying(1),
  PRIMARY KEY(id)
);

CREATE TABLE write_test_table (
  long_col BIGINT,
  string_col TEXT,
  bool_col BOOLEAN,
  double_col DOUBLE PRECISION,
  timestamp_col TIMESTAMPTZ,
  date_col DATE,
  bytes_col BYTEA,
  numeric_col NUMERIC,
  PRIMARY KEY(long_col)
);

CREATE TABLE write_array_test_table (
  long_col BIGINT,
  string_col TEXT,
  bool_col BOOLEAN,
  double_col DOUBLE PRECISION,
  timestamp_col TIMESTAMPTZ,
  date_col DATE,
  bytes_col BYTEA,
  numeric_col NUMERIC,
  long_array BIGINT ARRAY,
  str_array TEXT ARRAY,
  boolean_array BOOLEAN ARRAY,
  double_array DOUBLE PRECISION ARRAY,
  timestamp_array TIMESTAMPTZ ARRAY,
  date_array DATE ARRAY,
  binary_array BYTEA ARRAY,
  numeric_array NUMERIC ARRAY,
  PRIMARY KEY(long_col)
);

CREATE TABLE schema_test_table (
  id BIGINT PRIMARY KEY,
  name VARCHAR,
  value DOUBLE PRECISION
);
