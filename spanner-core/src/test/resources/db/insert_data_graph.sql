DELETE FROM FlexibleGraphNode WHERE TRUE;
INSERT INTO FlexibleGraphNode(id, type, properties, cluster_id)
VALUES (1, "Person", JSON '{"birthday":"1991-12-21T08:00:00Z","city":"Adelaide","country":"Australia","name":"Alex"}', 1),
       (2, "Person", JSON '{"birthday":"1980-10-31T08:00:00Z","city":"Moravia","country":"Czech_Republic","name":"Dana"}', 1),
       (3, "Person", JSON '{"birthday":"1986-12-07T08:00:00Z","city":"Kollam","country":"India","name":"Lee"}', 1),
       (7, "Account", JSON '{"create_time":"2020-01-10T14:22:20.222Z","is_blocked":false,"nick_name":"Vacation Fund"}', 1),
       (16, "Account", JSON '{"create_time":"2020-01-28T01:55:09.206Z","is_blocked":true,"nick_name":"Vacation Fund"}', 1),
       (20, "Account", JSON '{"create_time":"2020-02-18T13:44:20.655Z","is_blocked":false,"nick_name":"Rainy Day Fund"}', 1),
       (100, "Account", JSON '{"create_time":"2020-01-10T14:22:20.222Z","is_blocked":false,"nick_name":"Vacation Fund"}', 100),
       (101, "Account", JSON '{"create_time":"2020-01-28T01:55:09.206Z","is_blocked":true,"nick_name":"Vacation Fund"}',100);

DELETE FROM FlexibleGraphEdge WHERE TRUE;
INSERT INTO FlexibleGraphEdge(id, edge_type, to_id, edge_id, properties)
VALUES (1, "Owns", 7, "2020-01-10T14:22:20.222Z",
        JSON '{"create_time":"2020-01-10T14:22:20.222Z"}'),
       (2, "Owns", 20, "2020-01-28T01:55:09.206Z",
        JSON '{"create_time":"2020-01-28T01:55:09.206Z"}'),
       (3, "Owns", 16, "2020-02-18T13:44:20.655Z",
        JSON '{"create_time":"2020-02-18T13:44:20.655Z"}'),
       (7, "Transfers", 16, "2020-08-29T22:28:58.647Z",
        JSON '{"amount":300,"create_time":"2020-08-29T22:28:58.647Z","order_number":"304330008004315"}'),
       (7, "Transfers", 16, "2020-10-04T23:55:05.342Z",
        JSON '{"amount":100,"create_time":"2020-10-04T23:55:05.342Z","order_number":"304120005529714"}'),
       (16, "Transfers", 20, "2020-09-25T09:36:14.926Z",
        JSON '{"amount":300,"create_time":"2020-09-25T09:36:14.926Z","order_number":"103650009791820"}'),
       (20, "Transfers", 7, "2020-10-04T23:55:05.342Z",
        JSON '{"amount":500,"create_time":"2020-10-04T23:55:05.342Z","order_number":"304120005529714"}'),
       (20, "Transfers", 16, "2020-10-17T10:59:40.247Z",
        JSON '{"amount":200,"create_time":"2020-10-17T10:59:40.247Z","order_number":"302290001255747"}'),
       (100, "Transfers", 101, "2020-08-29T22:28:58.647Z",
        JSON '{"amount":300,"create_time":"2020-08-29T22:28:58.647Z","order_number":"304330008004315"}');

DELETE FROM ProductionCompanies WHERE TRUE;
INSERT INTO ProductionCompanies (CompanyId, CompanyName, LocationCountry, FoundedYear) VALUES (1, 'Mellow Wave', 'U.S.A.', 1993), (2, 'Rolling Stow', 'Canada', 2002), (3, 'Picky Penang', 'Malaysia', 1984), (4, 'Ice Ice', 'Poland', 2012), (5, 'Oint Is Not An Ink', 'Peru', 2000);

DELETE FROM Singers WHERE TRUE;
INSERT INTO Singers (SingerId, FirstName, LastName, BirthDate) VALUES (1, 'Cruz', 'Richards', '1970-9-3'), (2, 'Tristan', 'Smith', '1990-8-17') ,(3, 'Izumi', 'Trentor', '1991-10-2'), (4, 'Ira', 'Martin', '1991-11-9'),(5, 'Mahan', 'Lomond', '1977-1-29');

DELETE FROM Albums WHERE TRUE;
INSERT INTO Albums (SingerId, AlbumId, AlbumTitle, ReleaseDate, CompanyId) VALUES (1, 1, 'Total Junk', '2014-3-2', 1), (1, 2, 'Go Go Go', '2011-2-9', 1), (2, 3, 'Green', '2012-9-17', 2), (2, 4, 'Forever Hold Your Peace', '2010-10-15', 3), (3, 5, 'Terrified', '2008-6-7', 3), (4, 6, 'Nothing To Do With Me', '2014-4-29', 4), (5, 7, 'Play', '2013-12-21', 5);

DELETE FROM SingerFriends WHERE TRUE;
INSERT INTO SingerFriends (SingerId, FriendId) VALUES (1, 2), (1, 3), (2, 1), (2, 4), (2, 5), (3, 1), (3, 5), (4, 2), (4, 5), (5, 2), (5, 3), (5, 4);

DELETE FROM SingerContracts WHERE TRUE;
INSERT INTO SingerContracts (SingerId, CompanyId) VALUES (1, 4), (2, 2), (3, 5), (4, 1), (5, 3);