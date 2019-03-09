-- Joshua Bone - UW DATA 514 - HW5 - 2019-03-04

CREATE TABLE Users
(
 username varchar(80) NOT NULL PRIMARY KEY,
 password varchar(20) NOT NULL,
 balance double precision
);

CREATE TABLE Reservations
(
 rid int NOT NULL PRIMARY KEY,
 username varchar(80) NOT NULL REFERENCES Users(username),
 day_of_month int NOT NULL,
 price double precision,
 is_paid int,
);

CREATE TABLE Itineraries
(
 reservation_id int UNIQUE NOT NULL REFERENCES Reservations(rid),
 flight_id_1 int NOT NULL REFERENCES Flights(fid),
 flight_id_2 int REFERENCES Flights(fid)
);

CREATE TABLE NextReservationId
(
	nextRid int
);
INSERT INTO NextReservationId VALUES(1);