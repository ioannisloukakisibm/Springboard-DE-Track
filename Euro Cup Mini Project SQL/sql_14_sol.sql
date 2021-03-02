create temporary table euro_cup_2016.raw_booking_data as 
select a.*, c.referee_name
from euro_cup_2016.player_booked a
inner join euro_cup_2016.match_mast b on a.match_no = b.match_no
inner join euro_cup_2016.referee_mast c on b.referee_id = c.referee_id;

select referee_name, count(referee_name) as number_of_bookings
from euro_cup_2016.raw_booking_data
group by referee_name
order by number_of_bookings desc