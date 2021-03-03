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

-- 2. List the names of students with id in the range of v2 (id) to v3 (inclusive).
SELECT name FROM Student WHERE id BETWEEN @v2 AND @v3;

-- This query is pretty optimal. 
-- Assuming that the ask is that simple and doens't dynamically change  
-- One thing we could do is reference the IDs in need directly 
-- instead of declaring variables
-- We won't be saving much here either

SELECT name FROM Student WHERE id BETWEEN 1145072 AND 1828467;
