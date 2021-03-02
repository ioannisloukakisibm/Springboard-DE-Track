select count(player_name) as number_of_captains_and_goalkeepers
from euro_cup_2016.player_mast 
where player_id in 
	(select distinct player_captain from euro_cup_2016.match_captain)
    AND posi_to_play = 'GK'
