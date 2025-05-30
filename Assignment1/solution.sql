DROP TABLE IF EXISTS hasagenre;
DROP TABLE IF EXISTS tags;
DROP TABLE IF EXISTS ratings;
DROP TABLE IF EXISTS genres;
DROP TABLE IF EXISTS taginfo;
DROP TABLE IF EXISTS movies;
DROP TABLE IF EXISTS users;

CREATE TABLE users(userid INTEGER PRIMARY KEY, name text NOT NULL);

CREATE TABLE movies(movieid INTEGER PRIMARY KEY, title text NOT NULL);

CREATE TABLE taginfo(tagid INTEGER PRIMARY KEY,content text NOT NULL);

CREATE TABLE genres(genreid INTEGER PRIMARY KEY, name text NOT NULL);

CREATE TABLE ratings(userid INTEGER, movieid INTEGER, rating NUMERIC CHECK(rating >= 0.0 and rating <= 5.0) NOT NULL, timestamp bigint NOT NULL, FOREIGN KEY(userid) REFERENCES users(userid), FOREIGN KEY(movieid) REFERENCES movies(movieid), PRIMARY KEY(userid, movieid));

CREATE TABLE tags(userid INTEGER, movieid INTEGER, tagid INTEGER, timestamp bigint NOT NULL, FOREIGN KEY(userid) REFERENCES users(userid), FOREIGN KEY(movieid) REFERENCES movies(movieid), FOREIGN KEY(tagid) REFERENCES taginfo(tagid), PRIMARY KEY (userid, movieid, tagid));

CREATE TABLE hasagenre(movieid INTEGER, genreid INTEGER, FOREIGN KEY(movieid) REFERENCES movies(movieid), FOREIGN KEY(genreid) REFERENCES genres(genreid), PRIMARY KEY (movieid, genreid));