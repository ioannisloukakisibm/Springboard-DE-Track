select count(case when decided_by='P' then decided_by end) as count_penalty_games
from euro_cup_2016.match_details;