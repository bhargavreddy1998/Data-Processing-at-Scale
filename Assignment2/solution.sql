DROP TABLE query1;
DROP TABLE query2;
DROP TABLE query3;
DROP TABLE query4;
DROP TABLE query5;
DROP TABLE query7;
DROP TABLE query8;
DROP TABLE query9;
DROP TABLE recommendation;

CREATE TABLE query1 AS
SELECT g.name AS name, COUNT(*) AS moviecount
FROM genres g, hasagenre h
WHERE g.genreid=h.genreid
GROUP BY g.name;

create table query2 as
SELECT g.name as name, avg(r.rating) as rating 
FROM genres g, hasagenre h, ratings r
where g.genreid=h.genreid and r.movieid=h.movieid
group by g.name;

CREATE TABLE query3 AS
SELECT m.title, COUNT(*) AS countofratings
FROM ratings r, movies m
WHERE r.movieid=m.movieid
GROUP BY m.title
HAVING COUNT(*)>10;

CREATE TABLE query4 AS
SELECT m.movieid, m.title
FROM movies m, hasagenre h, genres g
WHERE m.movieid = h.movieid AND h.genreid = g.genreid AND g.name='Comedy'
ORDER BY m.movieid;

CREATE TABLE query5 AS
SELECT m.title, avg(r.rating) AS average
FROM ratings r, movies m
WHERE r.movieid=m.movieid
GROUP BY m.title;

CREATE TABLE query6 AS
SELECT avg(r.rating) AS average
FROM hasagenre h, ratings r, genres g
WHERE r.movieid=h.movieid AND g.genreid = h.genreid AND g.name='Comedy'
GROUP BY g.name;

CREATE TABLE query7 AS
SELECT avg(r.rating) AS average
FROM ratings r, hasagenre h1, hasagenre h2, genres g1, genres g2
WHERE r.movieid=h1.movieid AND r.movieid=h2.movieid AND h1.genreid=g1.genreid 
AND h2.genreid=g2.genreid AND g1.name='Comedy' AND g2.name='Romance';

CREATE TABLE query8 AS
SELECT avg(r.rating) AS average
FROM ratings r, hasagenre h, genres g
WHERE r.movieid=h.movieid AND h.genreid=g.genreid AND g.name='Romance' 
AND r.movieid NOT IN (SELECT r2.movieid FROM ratings r2, hasagenre h2, genres g2 
WHERE r2.movieid=h2.movieid AND h2.genreid=g2.genreid AND g2.name='Comedy');

-- to set the parameter run \set v1 2

CREATE TABLE query9 AS
SELECT r.movieid, r.rating
FROM ratings r
WHERE r.userid=:v1;
