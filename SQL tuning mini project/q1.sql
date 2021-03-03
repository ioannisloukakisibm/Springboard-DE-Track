USE springboardopt;

-- -------------------------------------
SET @v1 = 1612521;
SET @v2 = 1145072;
SET @v3 = 1828467;
SET @v4 = 'MGT382';
SET @v5 = 'Amber Hill';
SET @v6 = 'MGT';
SET @v7 = 'EE';			  
SET @v8 = 'MAT';

-- 1. List the name of the student with id equal to v1 (id).
SELECT name FROM Student WHERE id = @v1;

-- This query is pretty optimal. 
-- Assuming that the ask is that simple and doens't dynamically change  
-- One thing we could do is reference the ID in need directly 
-- instead of declaring a variable
-- Not sure we could be saving much here  

SELECT name FROM Student WHERE id = 1612521;
