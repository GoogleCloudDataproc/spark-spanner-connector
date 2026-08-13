DELETE FROM ATable WHERE 1=1;

INSERT INTO
    ATable(A, B, C, D, E, F, G, H, K)
VALUES
    (1,  "2",  NULL, TIMESTAMP("2023-08-22T12:22:00Z"), NUMERIC '1000.282111401', true, -0.1, DATE("2023-12-31"), -0.1),
    (10, "20", NULL, TIMESTAMP("2023-08-22T12:23:00Z"), NUMERIC '10000.282111603', false, 0.2, DATE("2024-01-01"), null),
    (30, "30", NULL, TIMESTAMP("2023-08-22T12:24:00Z"), NUMERIC '30000.282111805', false, 0.1, DATE("2025-12-31"), -0.1);
INSERT INTO
    ATable(A, B, C, D, E, F, G, H, I, J, K)
VALUES
    (40, "40", b'xyz', TIMESTAMP('2025-01-01T12:34:56Z'), NUMERIC '123.456789123', TRUE,3.14, DATE ("2026-01-01"), ['a','b','c'], JSON '{"name":"john"}', NULL);


INSERT INTO
    ATable(A, B, C, D, E, F, G, H, I, J, K)
VALUES
    (50, NULL, NULL, NULL, NULL, NULL,NULL, NULL, NULL, NULL, NULL);

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
    ),
    (
        "id3", NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, b"deadbeef", NULL
    );

DELETE FROM nullsTable WHERE 1=1;
INSERT INTO
    nullsTable(id, A, B, C, D, E, F, G, H, I, J, K, M, N, O)
VALUES
    (1, NULL, NULL, NULL, NULL, NULL, NULL, true, [NULL, DATE("2023-09-23T00:00:00Z")], NULL, [true, NULL, false], [23.67], NULL, NULL, [CAST(-99.37171 AS NUMERIC), NULL]),
    (2, [1, 2, NULL], NULL, NULL, 99.37171, NULL, NULL, NULL, [DATE("2022-10-02T00:00:00Z"), NULL], NULL, [NULL, NULL, true], [NULL, 198.1827], NULL, NULL, NULL),
    (3, [2, 3, NULL], ["a", "b", "FF", NULL], "😎🚨", NULL, NULL, TIMESTAMP("2023-09-23T12:11:09Z"), false, NULL, NULL, NULL, [-28888.8888, 0.12, NULL], NULL, NULL, [NULL, CAST(-55.7 AS NUMERIC), CAST(9.3 AS NUMERIC)]),
    (4, [NULL, 4, 57, 10], ["💡🚨", NULL, "b", "fg"], "🚨", 55.7, DATE(2023, 12, 31), NULL, false, NULL, [NULL, TIMESTAMP("2023-09-23T12:11:09Z")], [true, true], [0.71], [NULL, FROM_HEX("beefdead")], [NULL, JSON'{"a":1}'], [NULL, CAST(12 AS NUMERIC)]),
    (5, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
    (6, [NULL, 1234], [NULL, "stringarray"], NULL, NULL, NULL, NULL, NULL, [NULL, DATE(2023, 12, 31)], [NULL, TIMESTAMP("2023-09-23T12:11:09Z")], [NULL, true], [NULL, 0.000001], [NULL, b"beefdead"], [NULL, JSON'{"a":1}'], [NULL, CAST(123456 AS NUMERIC)]),
    (7, [], [], NULL, NULL, NULL, NULL, NULL, [], [], [], [], [], [], []);


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
    (9223372036854775807, CAST("+inf" AS FLOAT64), +9.9999999999999999999999999999999999999E+28, DATE("4000-12-30T23:59:59Z"), TIMESTAMP("2222-02-22T22:22:22.999999Z")),
    (0, CAST("-inf" AS FLOAT64), 10.389, DATE("1900-12-30T23:59:59Z"), TIMESTAMP("2023-09-28 21:59:59", "America/Los_Angeles")),
    (1, CAST("0.657818" AS FLOAT64), -10.389, DATE("2023-09-28T00:00:00Z"), TIMESTAMP("0001-01-03T00:00:01Z"));

DELETE FROM ORDERS WHERE 1=1;
INSERT INTO
    ORDERS(O_ORDERKEY,
           O_CUSTKEY,
           O_ORDERSTATUS,
           O_TOTALPRICE,
           O_ORDERDATE,
           O_ORDERPRIORITY ,
           O_CLERK,
           O_SHIPPRIORITY,
           O_COMMENT)
VALUES
(1,36901,"O",173665.47,DATE("1996-01-02"),"5-LOW","Clerk#000000951",0,"nstructions sleep furiously among"),
(2,78002,"O",46929.18,DATE("1996-12-01"),"1-URGENT","Clerk#000000880",0, "foxes. pending accounts at the pending, silent asymptot"),
(3,123314,"F",193846.25,DATE("1993-10-14"),"5-LOW","Clerk#000000955",0,"sly final accounts boost. carefully regular ideas cajole carefully. depos");

DELETE FROM LINEITEM WHERE 1=1;
INSERT INTO
    LINEITEM(O_ORDERKEY,
                        L_PARTKEY,
                        L_SUPPKEY,
                        L_LINENUMBER,
                        L_QUANTITY,
                        L_EXTENDEDPRICE,
                        L_DISCOUNT,
                        L_TAX,
                        L_RETURNFLAG,
                        L_LINESTATUS,
                        L_SHIPDATE,
                        L_COMMITDATE,
                        L_RECEIPTDATE,
                        L_SHIPINSTRUCT,
                        L_SHIPMODE,
                        L_COMMENT)
VALUES
    (1,155190,7706,1,17,21168.23,0.04,0.02,"N","O",DATE("1996-03-13"),DATE("1996-02-12"),DATE("1996-03-22"),"DELIVER IN PERSON","TRUCK","egular courts above the"),
(1,67310,7311,2,36,45983.16,0.09,0.06,"N","O",DATE("1996-04-12"),DATE("1996-02-28"),DATE("1996-04-20"),"TAKE BACK RETURN","MAIL","ly final dependencies: slyly bold "),
(1,63700,3701,3,8,13309.60,0.10,0.02,"N","O",DATE("1996-01-29"),DATE("1996-03-05"),DATE("1996-01-31"),"TAKE BACK RETURN","REG AIR","riously. regular, express dep"),
(1,2132,4633,4,28,28955.64,0.09,0.06,"N","O",DATE("1996-04-21"),DATE("1996-03-30"),DATE("1996-05-16"),"NONE","AIR","lites. fluffily even de"),
(1,24027,1534,5,24,22824.48,0.10,0.04,"N","O",DATE("1996-03-30"),DATE("1996-03-14"),DATE("1996-04-01"),"NONE","FOB", "pending foxes. slyly re"),
(1,15635,638,6,32,49620.16,0.07,0.02,"N","O",DATE("1996-01-30"),DATE("1996-02-07"),DATE("1996-02-03"),"DELIVER IN PERSON","MAIL","arefully slyly ex"),
(2,106170,1191,1,38,44694.46,0.00,0.05,"N","O",DATE("1997-01-28"),DATE("1997-01-14"),DATE("1997-02-02"),"TAKE BACK RETURN","RAIL","ven requests. deposits breach a"),
(3,4297,1798,1,45,54058.05,0.06,0.00,"R","F",DATE("1994-02-02"),DATE("1994-01-04"),DATE("1994-02-23"),"NONE","AIR","ongside of the furiously brave acco"),
(3,19036,6540,2,49,46796.47,0.10,0.00,"R","F",DATE("1993-11-09"),DATE("1993-12-20"),DATE("1993-11-24"),"TAKE BACK RETURN","RAIL", "unusual accounts. eve"),
(3,128449,3474,3,27,39890.88,0.06,0.07,"A","F",DATE("1994-01-16"),DATE("1993-11-22"),DATE("1994-01-23"),"DELIVER IN PERSON","SHIP","nal foxes wake."),
(3,29380,1883,4,2,2618.76,0.01,0.06,"A","F",DATE("1993-12-04"),DATE("1994-01-07"),DATE("1994-01-01"),"NONE","TRUCK","y. fluffily pending d"),
(3,183095,650,5,28,32986.52,0.04,0.00,"R","F",DATE("1993-12-14"),DATE("1994-01-10"),DATE("1994-01-01"),"TAKE BACK RETURN","FOB","ages nag slyly pending"),
(3,62143,9662,6,26,28733.64,0.10,0.02,"A","F",DATE("1993-10-29"),DATE("1993-12-18"),DATE("1993-11-04"),"TAKE BACK RETURN","RAIL","ges sleep after the caref")
