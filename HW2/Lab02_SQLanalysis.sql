--How many animals of each type have outcomes?
--I.e. how many cats, dogs, birds etc. Note that this question is asking about number of animals, 
--not number of outcomes, so animals with multiple outcomes should be counted only once.

select animal_type, COUNT(animal_type)
from 
	(select distinct on (animal_dim.animal_id) animal_dim.animal_id, animal_type, outcome_type, name
	from visit_fact
	join outcome_type_dim on visit_fact.outcome_type_id = outcome_type_dim.outcome_type_id
	join animal_dim on visit_fact.animal_id = animal_dim.animal_id)
group by animal_type;


--How many animals are there with more than 1 outcome?

select count(*)
from
	(select count(animal_id)
	from visit_fact
	group by animal_id
	having count(animal_id) > 1);


--What are the top 5 months for outcomes? 
--Calendar months in general, not months of a particular year. This means answer will be like April, October, etc rather than April 2013,
-- October 2018, 
select processed_dim.month, count(*)
from visit_fact
join processed_dim on visit_fact.processed_id  = processed_dim.processed_id 
group by processed_dim.month
order by count(*) desc
limit 5;


-- A "Kitten" is a "Cat" who is less than 1 year old. 
-- A "Senior cat" is a "Cat" who is over 10 years old. 
-- An "Adult" is a cat who is between 1 and 10 years old.
--What is the percentage of kittens, adults, and seniors, whose outcome is "Adopted"?


select age_category, outcome_type, perc
from
(select age_category, outcome_type,tot, sum(tot) over(partition by age_category), 
	tot/sum(tot) over(partition by age_category) * 100 as perc
from
(select age_category, outcome_type, count(*) as tot
from
(select age_days,
	case
		when age_days < 365 then 'Kitten'
		when age_days between 365 and 3650 then 'Adult'
		else 'Senior cat'
	end	as age_category, outcome_type
from
(select  extract(days from processed_ts-dob) as age_days ,dob, processed_ts, animal_type, outcome_type
	from visit_fact
	join processed_dim on visit_fact.processed_id = processed_dim.processed_id
	join animal_dim on visit_fact.animal_id = animal_dim.animal_id
	join outcome_type_dim on visit_fact.outcome_type_id = outcome_type_dim.outcome_type_id
	where animal_type ='Cat'))
group by age_category, outcome_type))
where outcome_type='Adoption'






--Conversely, among all the cats who were "Adopted", what is the percentage of kittens, adults, and seniors?


select age_category, COUNT(*) / SUM(COUNT(*)) over () * 100 percent
from
(select age_days,
	case
		when age_days < 365 then 'Kitten'
		when age_days between 365 and 3650 then 'Adult'
		else 'Senior cat'
	end	as age_category, outcome_type
from
	(select  extract(days from processed_ts-dob) as age_days ,dob, processed_ts, animal_type, outcome_type
	from visit_fact
	join processed_dim on visit_fact.processed_id = processed_dim.processed_id
	join animal_dim on visit_fact.animal_id = animal_dim.animal_id
	join outcome_type_dim on visit_fact.outcome_type_id = outcome_type_dim.outcome_type_id
	where animal_type ='Cat' and outcome_type='Adoption'))
group by age_category;



-- For each date, what is the cumulative total of outcomes up to and including this date?

select pro_date, sum(ct) over(order by pro_date) as cumulative_total
from
	(select pro_date, count(*) as ct
	 from
		(select processed_ts :: date as pro_date, animal_id
		 from visit_fact 
		 join processed_dim on visit_fact.processed_id = processed_dim.processed_id
		 )
	 group by pro_date
	 )
;


