create temporary table euro_cup_2016.grouped_penalties_per_game as
	select match_no, max(kick_no) as penalty_shots
    from euro_cup_2016.penalty_shootout
    group by match_no;


create temporary table euro_cup_2016.grouped_penalties_per_game_max as
	select max(penalty_shots) as max_penalty_shots
    from euro_cup_2016.grouped_penalties_per_game;

 
select match_no, country_name 
from 
	euro_cup_2016.match_details a
    inner join 
    euro_cup_2016.soccer_country b
    
    on a.team_id = b.country_id
    
where match_no =  
	(select match_no 
    from euro_cup_2016.grouped_penalties_per_game
    where penalty_shots =
		(select max_penalty_shots from euro_cup_2016.grouped_penalties_per_game_max)
	);
    