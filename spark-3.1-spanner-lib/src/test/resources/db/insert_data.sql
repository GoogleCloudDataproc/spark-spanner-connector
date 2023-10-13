DELETE FROM ATable WHERE 1=1;

INSERT INTO
    ATable(A, B, C, D, E)
VALUES
    (1,  "2",  NULL, TIMESTAMP("2023-08-22T12:22:00Z"), 1000.282111401),
    (10, "20", NULL, TIMESTAMP("2023-08-22T12:23:00Z"), 10000.282111603),
    (30, "30", NULL, TIMESTAMP("2023-08-22T12:24:00Z"), 30000.282111805);

DELETE FROM simpleTable WHERE 1=1;

INSERT INTO
    simpleTable(A, B, C)
VALUES
    (1, "1", 2.5),
    (2, "2", 5.0),
    (3, "3", CAST("+inf" AS FLOAT64)),
    (4, "4", CAST("-inf" AS FLOAT64)),
    (5, "5", CAST("NaN" AS FLOAT64)),
    (6, "6", 100000000017.100000000017),
    (7, "7", -0.1),
    (8, "8", +0.1),
    (9, "9", -19999997.9);

DELETE FROM players WHERE 1=1;
DELETE FROM games WHERE 1=1;
INSERT INTO
    games(gameUUID, players, winner, created, finished, max_date)
VALUES
    ("g1", ["p1", "p2", "p3"], "T1", TIMESTAMP("2023-08-26T12:22:00Z"), TIMESTAMP("2023-08-26T12:22:00Z"), DATE("2023-12-31T00:00:00Z")),
    ("g2", ["p4", "p5", "p6"], "T2", TIMESTAMP("2023-08-26T12:22:00Z"), TIMESTAMP("2023-08-26T12:22:00Z"), DATE("2023-12-31T00:00:00Z"));

DELETE FROM game_items WHERE 1=1;
INSERT INTO
    game_items(itemUUID, item_name, item_value, available_time, duration)
VALUES
    ("gi_1", "powerup", 237, TIMESTAMP("2023-08-22T12:22:00Z"), 90),
    ("gi_2", "diff", 500, TIMESTAMP("2023-08-22T12:22:00Z"), 90);

INSERT INTO
    players(playerUUID, player_name, email, password_hash, created, updated, stats, account_balance, is_logged_in, last_login, valid_email, current_game, dob)
VALUES
    ("p1", "PLAYER 1", "p1@games.com", FROM_HEX("deadbeef"), TIMESTAMP("2023-08-26T12:22:00Z"), null, TO_JSON('{"a":"b"}'), 17517, true, TIMESTAMP("2023-08-26T12:22:00Z"), true, "g1", DATE("1999-06-06T00:00:00Z")),
    ("p2", "PLAYER 2", "p2@games.com", FROM_HEX("beefdead"), TIMESTAMP("2023-08-26T12:22:00Z"), null, TO_JSON('{"1":"2","k":291}'), 8519, false, TIMESTAMP("2023-08-26T12:22:00Z"), true, "g2", DATE("1997-12-06T00:00:00Z"));


DELETE FROM compositeTable WHERE 1=1;
INSERT INTO
    compositeTable(id, A, B, C, D, E, F, G, H, I, J, K)
VALUES
    (
        "id1", [10, 100, 991, 567282], ["a", "b", "c"], "foobar", 2934, DATE(2023, 1, 1),
        TIMESTAMP("2023-08-26T12:22:05Z"), true, [DATE(2023, 1, 2), DATE(2023, 12, 31)],
        [TIMESTAMP("2023-08-26T12:11:10Z"), TIMESTAMP("2023-08-27T12:11:09Z")], FROM_HEX("beefdead"),
        JSON'{"a":1, "b":2}'
    ),
    (
        "id2", [20, 200, 2991, 888885], ["A", "B", "C"], "this one", 93411, DATE(2023, 9, 23),
        TIMESTAMP("2023-09-22T12:22:05Z"), false, [DATE(2023, 9, 2), DATE(2023, 12, 31)],
        [TIMESTAMP("2023-09-22T12:11:10Z"), TIMESTAMP("2023-09-23T12:11:09Z")], FROM_HEX("deadbeef"),
        JSON'{}'
    );

DELETE FROM nullsTable WHERE 1=1;
INSERT INTO
    nullsTable(id, A, B, C, D, E, F, G, H, I, J, K, M, N, O)
VALUES
    (1, NULL, NULL, NULL, NULL, NULL, NULL, true, [NULL, DATE("2023-09-23T00:00:00Z")], NULL, [true, NULL, false], [23.67], NULL, NULL, [CAST(-99.37171 AS NUMERIC), NULL]),
    (2, [1, 2, NULL], NULL, NULL, 99.37171, NULL, NULL, NULL, [DATE("2022-10-02T00:00:00Z"), NULL], NULL, [NULL, NULL, true], [NULL, 198.1827], NULL, NULL, NULL),
    (3, [2, 3, NULL], ["a", "b", "FF", NULL], "😎🚨", NULL, NULL, TIMESTAMP("2023-09-23T12:11:09Z"), false, NULL, NULL, NULL, [-28888.8888, 0.12, NULL], NULL, NULL, [NULL, CAST(-55.7 AS NUMERIC), CAST(9.3 AS NUMERIC)]),
    (4, [NULL, 4, 57, 10], ["💡🚨", NULL, "b", "fg"], "🚨", 55.7, DATE("2023-12-31T00:00:00Z"), NULL, false, NULL, [NULL, TIMESTAMP("2023-09-23T12:11:09Z")], [true, true], [0.71], [NULL, FROM_HEX("beefdead")], [NULL, JSON'{"a":1}'], [NULL, CAST(12 AS NUMERIC)]);

DELETE FROM bytesTable WHERE 1=1;
INSERT INTO
    bytesTable(id, A)
VALUES
   (1, B"ABCDEFGHIJKLMNOPQ"),
   (2, B"abcdefghijklmnopq"),
   (3, B"1234efghijklmnopq");


DELETE FROM valueLimitsTable WHERE 1=1;
INSERT INTO
    valueLimitsTable(A, B, C, D, E)
VALUES
    (-9223372036854775808, CAST("NaN" AS FLOAT64), -9.9999999999999999999999999999999999999E+28, DATE("1700-01-01T00:00:00Z"), TIMESTAMP("9999-12-30T23:59:59.00Z")),
    (9223372036854775807, CAST("+inf" AS FLOAT64), +9.9999999999999999999999999999999999999E+28, DATE("4000-12-30T23:59:59Z"), TIMESTAMP("2222-02-22T22:22:22Z")),
    (0, CAST("-inf" AS FLOAT64), 10.389, DATE("1900-12-30T23:59:59Z"), TIMESTAMP("2023-09-28 21:59:29", "America/Los_Angeles")),
    (1, CAST("0.657818" AS FLOAT64), -10.389, DATE("2023-09-28T00:00:00Z"), TIMESTAMP("0001-01-03T00:00:01Z"));
