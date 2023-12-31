CREATE DATABASE IF NOT EXISTS challenge;
USE challenge;

CREATE TABLE IF NOT EXISTS career (
  id INT PRIMARY KEY AUTO_INCREMENT,
  career VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS course (
  id INT PRIMARY KEY AUTO_INCREMENT,
  course VARCHAR(100) NOT NULL
);


CREATE TABLE IF NOT EXISTS student (
  id INT PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(100) NOT NULL,
  enrolment VARCHAR(30) NOT NULL,
  career_id INT,
  course_id INT,
  FOREIGN KEY (career_id) REFERENCES career(id),
  FOREIGN KEY (course_id) REFERENCES course(id)
);
