create temporary table euro_cup_2016.goals_per_player as 
select player_id, count(player_id) as goals
from euro_cup_2016.goal_details
group by player_id;

-- With the inner join we will get rid of players with no goals 
select country_name, posi_to_play, sum(goals) as total_goals
from euro_cup_2016.player_mast a
inner join euro_cup_2016.goals_per_player b on a.player_id = b.player_id
inner join euro_cup_2016.soccer_country c on a.team_id = c.country_id
group by country_name, posi_to_play
