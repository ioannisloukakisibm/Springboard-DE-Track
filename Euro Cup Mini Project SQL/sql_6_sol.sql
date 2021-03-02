select count(*) as games
from euro_cup_2016.match_mast
where 
	abs(substring(goal_score,1,1) - substring(goal_score,3,1)) = 1
    AND
    decided_by = 'N'