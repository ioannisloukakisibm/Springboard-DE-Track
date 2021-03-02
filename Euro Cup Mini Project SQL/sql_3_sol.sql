select match_no, play_date, cast(goal_score as signed int) as goal_score
from euro_cup_2016.match_mast
where stop1_sec = 0;