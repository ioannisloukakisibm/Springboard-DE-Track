create temporary table euro_cup_2016.goals_per_player2 as 
select player_id, count(player_id) as goals
from euro_cup_2016.goal_details
group by player_id;

-- With the inner join we will get rid of players with no goals 
select player_name
from euro_cup_2016.player_mast a
inner join euro_cup_2016.goals_per_player2 b on a.player_id = b.player_id
where posi_to_play = 'DF'