-- Every substitution contains two rows:  
-- One with the player that got in and one with the player that got out
-- We need to divide by 2 to get the actual number of substitutions
select play_stage, count(play_stage)/2 as substitutions
from 
	euro_cup_2016.player_in_out a left join euro_cup_2016.match_mast b
    on a.match_no = b.match_no
group by play_stage 
