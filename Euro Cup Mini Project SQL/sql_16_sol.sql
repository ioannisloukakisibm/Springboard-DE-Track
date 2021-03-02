create temporary table euro_cup_2016.raw_ref_venue_data as 
select b.referee_name, c.venue_name
from euro_cup_2016.match_mast a 
inner join euro_cup_2016.referee_mast b on a.referee_id = b.referee_id
inner join euro_cup_2016.soccer_venue c on a.venue_id = c.venue_id ;

select referee_name, venue_name, count(referee_name) as number_of_games
from euro_cup_2016.raw_ref_venue_data
group by referee_name, venue_name;
