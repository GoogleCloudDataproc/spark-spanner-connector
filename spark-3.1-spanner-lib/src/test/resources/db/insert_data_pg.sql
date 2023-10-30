DELETE FROM composite_table WHERE 1=1;

INSERT INTO composite_table (id, charvcol, textcol, varcharcol, boolcol, booleancol, bigintcol, int8col, intcol, doublecol, floatcol, bytecol, datecol, numericcol, decimalcol, timewithzonecol, timestampcol, jsoncol)
VALUES (1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
  (2, 'charvcol', 'textcol', 'varcharcol', true, false, 1, -1, 0, 0.00000001, 0.00000001, 'beefdead', '1999-01-08', NUMERIC '1.23456e05', NUMERIC '9e23', '2003-04-12 04:05:06 America/Los_Angeles', '2003-04-12 05:05:06 America/Los_Angeles', '{"tags": ["multi-cuisine", "open-seating"], "rating": 4.5}');

DELETE FROM array_table WHERE 1=1;

INSERT INTO array_table (id, charvarray, boolarray, bigintarray, doublearray, bytearray, datearray, numericarray, timestamparray, jsonarray)
VALUES (1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
(2, '{NULL, "charvarray"}', '{NULL, true}', '{NULL, 1024}', '{NULL, 0.00000001}', '{NULL, "beefdead"}', '{NULL, "1999-01-08"}', '{NULL, "1.2345e05"}', '{NULL, "2003-04-12 04:05:06 America/Los_Angeles"}', ARRAY[NULL, CAST('{"tags": ["multi-cuisine", "open-seating"], "rating": 4.5}' as JSONB)]);

DELETE FROM integration_composite_table WHERE 1=1;

INSERT INTO integration_composite_table (id, charvcol, textcol, varcharcol, boolcol, booleancol, bigintcol, int8col, intcol, doublecol, floatcol, bytecol, datecol, numericcol, decimalcol, timewithzonecol, timestampcol, jsoncol)
VALUES (1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
  (2, 'charvcol', 'textcol', 'varcharcol', true, false, 1, -1, 0, 0.00000001, 0.00000001, 'beefdead', '1999-01-08', NUMERIC '1.23456e05', NUMERIC '9e23', '2003-04-12 04:05:06 America/Los_Angeles', '2003-04-12 05:05:06 America/Los_Angeles', '{"tags": ["multi-cuisine", "open-seating"], "rating": 4.5}'),
  (3, NULL, NULL, NULL, NULL, NULL, 9223372036854775807, -9223372036854775808, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
  (4, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'NaN', 'NaN', NULL, NULL, NULL, NULL, NULL, NULL, NULL),
  (5, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '9999-12-31', NULL, NULL, NULL, NULL, NULL),
  (6, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '1700-01-01', NULL, NULL, NULL, NULL, NULL),
  (7, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 99999999999999999999999999999.999999999, -99999999999999999999999999999.999999999, NULL, NULL, NULL),
  (8, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '0001-01-01 23:00:00 America/Los_Angeles', '9999-12-30 01:00:00 America/Los_Angeles', NULL),
  (9, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'NaN', 'NaN', NULL, NULL, NULL);

