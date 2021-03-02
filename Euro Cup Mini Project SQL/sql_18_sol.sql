
select max(number_of_cards) as max_number_of_cards_in_one_match
from 
(
select match_no, count(*) as number_of_cards
from euro_cup_2016.player_booked
group by match_no
) a
;