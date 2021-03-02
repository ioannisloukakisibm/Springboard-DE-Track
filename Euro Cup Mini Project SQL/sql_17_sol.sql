select b.country_name, count(a.ass_ref_name) as ass_ref_count
from euro_cup_2016.asst_referee_mast a
inner join euro_cup_2016.soccer_country b on a.country_id = b.country_id
group by b.country_name
order by ass_ref_count desc
limit 1
