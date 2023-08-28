CREATE TABLE game_items (
  itemUUID STRING(36) NOT NULL,
  item_name STRING(MAX) NOT NULL,
  item_value NUMERIC NOT NULL,
  available_time TIMESTAMP NOT NULL,
  duration INT64,
) PRIMARY KEY(itemUUID);

CREATE TABLE games (
  gameUUID STRING(36) NOT NULL,
  players ARRAY<STRING(36)> NOT NULL,
  winner STRING(36),
  created TIMESTAMP,
  finished TIMESTAMP,
) PRIMARY KEY(gameUUID);

CREATE TABLE players (
  playerUUID STRING(36) NOT NULL,
  player_name STRING(64) NOT NULL,
  email STRING(MAX) NOT NULL,
  password_hash BYTES(60) NOT NULL,
  created TIMESTAMP,
  updated TIMESTAMP,
  stats JSON,
  account_balance NUMERIC NOT NULL DEFAULT (0),
  is_logged_in BOOL NOT NULL DEFAULT (FALSE),
  last_login TIMESTAMP,
  valid_email BOOL,
  current_game STRING(36),
  FOREIGN KEY(current_game) REFERENCES games(gameUUID),
) PRIMARY KEY(playerUUID);

CREATE TABLE simpleTable(
  A INT64 NOT NULL,
  B STRING(100),
  C FLOAT64
) PRIMARY KEY(A);

CREATE TABLE ATable(
  A INT64 NOT NULL,
  B STRING(100),
  C BYTES(MAX),
  D TIMESTAMP,
  E NUMERIC,
  F ARRAY<STRING(MAX)>
) PRIMARY KEY(A);

CREATE TABLE compositeTable (
  id INT64 NOT NULL,
  A ARRAY<INT64>,
  B ARRAY<STRING(20)>,
  C STRING(30),
  D NUMERIC,
  E DATE,
  F TIMESTAMP,
  G JSON,
  H BOOL,
) PRIMARY KEY(id);
