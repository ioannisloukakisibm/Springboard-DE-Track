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

-- 3. List the names of students who have taken course v4 (crsCode).
SELECT name FROM Student WHERE id IN (SELECT studId FROM Transcript WHERE crsCode = @v4);

-- The use of a subquery makes sense visually (more compact code).
-- However, the subquery will be slower than a join (inner)
SELECT a.name 
FROM 
	Student a
    inner join Transcript b on a.id = b.studId 
    WHERE b.crsCode = @v4;
