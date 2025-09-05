create database job_analysis;

use job_analysis;

create table job_data (
	ds varchar(10),
	job_id int,
    actor_id int,
    `event` varchar(10),
    `language` varchar(10),
    time_spent int,
    org char(1)
);
    
insert into job_data
values ('2020/11/30', 21, 1001, 'skip', 'English', 15, 'A'),
		('2020/11/30', 22, 1006, 'transfer', 'Arabic', 25, 'B'),
		('2020/11/29', 23, 1003, 'decision', 'Persian', 20, 'C'),
		('2020/11/28', 23, 1005, 'transfer', 'Persian', 22, 'D'),
		('2020/11/28', 25, 1002, 'decision', 'Hindi', 11, 'B'),
		('2020/11/27', 11, 1007, 'decision', 'French', 104, 'D'),
		( '2020/11/26', 23, 1004, 'skip', 'Persian', 56, 'A'),
		('2020/11/25', 20, 1003, 'transfer', 'Italian', 45, 'C');

drop table job_data;

#jobs reviewed per hour per day

with hours_reviewed as (
	select cast(ds as date) as job_date, job_id, (time_spent/3600) as hours_spent 
	from job_data
)
select job_date, round(sum(hours_spent), 2) as total_hours, count(job_id) as jobs_per_day_per_hour
from hours_reviewed
group by job_date
order by job_date;
    
#7 day rolling average of throughput

with job_timestamp as (
select cast(ds as date) as job_date, sum(time_spent) as time_spent_per_day
from job_data
group by job_date
)
select job_date, time_spent_per_day,
round( avg(time_spent_per_day) over(order by job_date rows between 6 preceding and current row), 2) as rolling_average
from job_timestamp;

#percentage share of language over the last 30 days

select * from job_data;

with language_category as (
select `language`, count(`language`) as language_count
from job_data
group by `language`
)
select `language`, language_count,
concat(round(language_count/sum(language_count) over()*100, 1), '%') as percentage_share
from language_category
order by percentage_share desc, `language`;

#duplicate rows

select *, count(*) as duplicates 
from job_data
group by ds, job_id, actor_id, `event`, `language`, time_spent, org
having duplicates > 1;

with attibutes_aligned as (
select *, concat(ds, " ", job_id, " ", actor_id, " ", `event`, " ", `language`, " ", time_spent, " ", org) as grouped_attribute
from job_data
)
select count(grouped_attribute) as duplicates
from attibutes_aligned
group by grouped_attribute;
