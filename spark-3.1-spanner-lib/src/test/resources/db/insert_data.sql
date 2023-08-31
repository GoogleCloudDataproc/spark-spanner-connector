DELETE FROM ATable WHERE 1=1;

INSERT INTO
    ATable(A, B, C, D, E)
VALUES
    (1,  "2",  NULL, TIMESTAMP("2023-08-22 12:22:00+00"), 1000.282),
    (10, "20", NULL, TIMESTAMP("2023-08-22 12:23:00+00"), 10000.282),
    (30, "30", NULL, TIMESTAMP("2023-08-22 12:24:00+00"), 30000.282);

DELETE FROM simpleTable WHERE 1=1;

INSERT INTO
    simpleTable(A, B, C)
VALUES
    (1, "1", 2.5),
    (2, "2", 5.0);

DELETE FROM players WHERE 1=1;
DELETE FROM games WHERE 1=1;
INSERT INTO
    games(gameUUID, players, winner, created, finished, max_date)
VALUES
    ("g1", ["p1", "p2", "p3"], "T1", TIMESTAMP("2023-08-26 12:22:00+00"), TIMESTAMP("2023-08-26 12:22:00+00"), DATE("2023-12-31")),
    ("g2", ["p4", "p5", "p6"], "T2", TIMESTAMP("2023-08-26 12:22:00+00"), TIMESTAMP("2023-08-26 12:22:00+00"), DATE("2023-12-31"));

DELETE FROM game_items WHERE 1=1;
INSERT INTO
    game_items(itemUUID, item_name, item_value, available_time, duration)
VALUES
    ("gi_1", "powerup", 237, TIMESTAMP("2023-08-22 12:22:00+00"), 90),
    ("gi_2", "diff", 500, TIMESTAMP("2023-08-22 12:22:00+00"), 90);

INSERT INTO
    players(playerUUID, player_name, email, password_hash, created, updated, stats, account_balance, is_logged_in, last_login, valid_email, current_game, dob)
VALUES
    ("p1", "PLAYER 1", "p1@games.com", FROM_HEX("deadbeef"), TIMESTAMP("2023-08-26 12:22:00+00"), null, TO_JSON('{"a":"b"}'), 17517, true, TIMESTAMP("2023-08-26 12:22:00+00"), true, "g1", DATE("1999-06-06")),
    ("p2", "PLAYER 2", "p2@games.com", FROM_HEX("beefdead"), TIMESTAMP("2023-08-26 12:22:00+00"), null, TO_JSON('{"1":"2","k":291}'), 8519, false, TIMESTAMP("2023-08-26 12:22:00+00"), true, "g2", DATE("1997-12-06"));
