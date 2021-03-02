select b.player_name
from 
	euro_cup_2016.player_in_out a 
    inner join euro_cup_2016.player_mast b on a.player_id = b.player_id
    
    where 
		play_half = 1
        AND
        play_schedule = 'NT'
        AND
        in_out = 'I'