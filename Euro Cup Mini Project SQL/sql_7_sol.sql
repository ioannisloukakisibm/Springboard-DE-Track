select venue_name
from euro_cup_2016.soccer_venue b
where venue_id in 
	(
	select distinct venue_id
	from euro_cup_2016.match_mast
	where decided_by = 'P'
    )




