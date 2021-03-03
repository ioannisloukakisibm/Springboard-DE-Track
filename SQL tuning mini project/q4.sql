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

-- 4. List the names of students who have taken a course taught by professor v5 (name).
SELECT name FROM Student,
	(SELECT studId FROM Transcript,
		(SELECT crsCode, semester FROM Professor
			JOIN Teaching
			WHERE Professor.name = @v5 AND Professor.id = Teaching.profId) as alias1
	WHERE Transcript.crsCode = alias1.crsCode AND Transcript.semester = alias1.semester) as alias2
WHERE Student.id = alias2.studId;


-- Using subqueries makes the code looks like you are killing multiple birds with one stone
-- but every subquery is nun as many times as the parent query 
-- which means a lot of unecessary steps
-- Breat the query into pieves with CTEs or temporary tables
-- This way you run some steps once, save them in memory 
-- and don't need to run them again 
with professor_cte as 
(SELECT crsCode, semester FROM Professor
	JOIN Teaching
	WHERE Professor.name = @v5 AND Professor.id = Teaching.profId
)
,transcript_cte as 
(SELECT studId FROM Transcript, professor_cte
WHERE 
	Transcript.crsCode = professor_cte.crsCode 
	AND 
    Transcript.semester = professor_cte.semester
)

SELECT name FROM Student, transcript_cte
WHERE Student.id = transcript_cte.studId;
