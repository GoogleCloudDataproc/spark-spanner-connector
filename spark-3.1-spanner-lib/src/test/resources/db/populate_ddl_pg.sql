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
