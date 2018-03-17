CREATE TABLE PROJ (
  PNUM   VARCHAR(3) NOT NULL,
  PNAME  VARCHAR(20),
  PTYPE  CHAR(6),
  BUDGET DECIMAL(9),
  CITY   VARCHAR(15)
);

CREATE TABLE staff (
  EMPNUM  VARCHAR(3) NOT NULL,
  EMPNAME VARCHAR(20),
  GRADE   DECIMAL(4),
  CITY    VARCHAR(15)
);

INSERT INTO PROJ VALUES ('P1', 'MXSS', 'Design', 10000, 'Deale');
INSERT INTO PROJ VALUES ('P2', 'CALM', 'Code', 30000, 'Vienna');
INSERT INTO PROJ VALUES ('P3', 'SDP', 'Test', 30000, 'Tampa');
INSERT INTO PROJ VALUES ('P4', 'SDP', 'Design', 20000, 'Deale');
INSERT INTO PROJ VALUES ('P5', 'IRM', 'Test', 10000, 'Vienna');
INSERT INTO PROJ VALUES ('P6', 'PAYR', 'Design', 50000, 'Deale');
INSERT INTO PROJ VALUES ('P7', 'KMA', 'Design', 50000, 'Akron');


INSERT INTO STAFF VALUES ('E1', 'Alice', 12, 'Deale');
INSERT INTO STAFF VALUES ('E2', 'Betty', 10, 'Vienna');
INSERT INTO STAFF VALUES ('E3', 'Carmen', 13, 'Vienna');
INSERT INTO STAFF VALUES ('E4', 'Don', 12, 'Deale');
INSERT INTO STAFF VALUES ('E5', 'Ed', 13, 'Akron');
INSERT INTO STAFF VALUES ('E6', 'Joe', 13, 'Deale');
INSERT INTO STAFF VALUES ('E7', 'Fred', 13, 'Vienna');
INSERT INTO STAFF VALUES ('E8', 'Jane', 13, 'Akron');
