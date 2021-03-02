select player_name, jersey_no
from euro_cup_2016.player_mast a
inner join euro_cup_2016.match_details b on a.team_id = b.team_id
inner join euro_cup_2016.soccer_country c on b.team_id = c.country_id

where posi_to_play = 
	(
	select distinct position_id
	from euro_cup_2016.playing_position
	where position_desc = 'Goalkeepers'
	)
AND
c.country_name = 'Germany'
AND
b.play_stage = 'G'
;
