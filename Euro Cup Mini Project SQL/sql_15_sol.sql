create temporary table euro_cup_2016.raw_booking_data2 as 
select a.*, c.referee_name
from euro_cup_2016.player_booked a
inner join euro_cup_2016.match_mast b on a.match_no = b.match_no
inner join euro_cup_2016.referee_mast c on b.referee_id = c.referee_id;

create temporary table euro_cup_2016.grouped_bookings as 
select referee_name, player_id, count(referee_name) as number_of_bookings
from euro_cup_2016.raw_booking_data2
group by referee_name, player_id;

select referee_name, count(referee_name) as number_of_players_booked
from euro_cup_2016.grouped_bookings
group by referee_name
order by number_of_players_booked desc;