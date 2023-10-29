DELETE FROM composite_table WHERE 1=1;

INSERT INTO composite_table (id, charvcol, textcol, varcharcol, boolcol, booleancol, bigintcol, int8col, intcol, doublecol, floatcol, bytecol, datecol, numericcol, decimalcol, timewithzonecol, timestampcol, jsoncol)
VALUES (1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
  (2, 'charvcol', 'textcol', 'varcharcol', true, false, 1, -1, 0, 0.00000001, 0.00000001, 'beefdead', '1999-01-08', NUMERIC '1.23456e05', NUMERIC '9e23', '2003-04-12 04:05:06 America/Los_Angeles', '2003-04-12 05:05:06 America/Los_Angeles', '{"tags": ["multi-cuisine", "open-seating"], "rating": 4.5}');
