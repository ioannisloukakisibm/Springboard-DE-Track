select *
from euro_cup_2016.player_mast a
inner join euro_cup_2016.soccer_country c on a.team_id = c.country_id

where a.playing_club = 'Liverpool' 
AND
c.country_name = 'England'
;
