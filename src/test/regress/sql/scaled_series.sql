--
--

SELECT * FROM scaled_series(1, 5);
SELECT * FROM scaled_series(1, 5, 1);

\set SCALE_FACTOR 2
SELECT * FROM scaled_series(1, 5);
SELECT * FROM scaled_series(1, 5, 1);

\set SCALE_FACTOR 10
SELECT * FROM scaled_series(1, 5);
SELECT * FROM scaled_series(1, 5, 1);

\set SCALE_FACTOR 2
SELECT count(*) FROM 
  (SELECT a FROM scaled_series(1, 5) a) a,
  (SELECT b FROM scaled_series(1, 5) b) b;

\set SCALE_FACTOR 1
