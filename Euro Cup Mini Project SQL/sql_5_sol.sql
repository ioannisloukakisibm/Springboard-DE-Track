select count(match_no) as number_of_bookings_in_st
from euro_cup_2016.player_booked
where play_schedule = 'ST'
