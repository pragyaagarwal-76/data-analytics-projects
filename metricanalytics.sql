create database metric_spike;

use metric_spike;

create table users (
user_id int,
created_at varchar(50),
company int,
`language` varchar(25),
activated_at varchar(50),
state varchar(20)
);

show variables like 'secure_file_priv';

load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/users.csv"
into table users
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

select * from users;

alter table users add column temp_created_at datetime;
update users set temp_created_at = str_to_date(created_at, '%d-%m-%Y %H:%i');
alter table users drop column created_at;
alter table users change column temp_created_at created_at datetime;

create table events (
	user_id int,
    occured_at varchar(50),
    event_type varchar(20),
    event_name varchar(25),
    location varchar(20),
    device varchar(25),
    user_type int
);

show variables like 'secure_file_priv';

load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/events.csv"
into table events
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

select * from events;

alter table events add column temp_occurred_at datetime;
update events set temp_occurred_at = str_to_date(occured_at, '%d-%m-%Y %H:%i');
alter table events drop column occured_at;
alter table events change column temp_occurred_at occured_at datetime;

create table email_events (
	user_id int,
    occurred_at varchar(50),
    `action` varchar(50),
    user_type int
);

show variables like 'secure_file_priv';

load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/email_events.csv"
into table email_events
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

select * from email_events;

alter table email_events add column temp_occurred_at datetime;
update email_events set temp_occurred_at = str_to_date(occurred_at, '%d-%m-%Y %H:%i');
alter table email_events drop column occurred_at;
alter table email_events change column temp_occurred_at occurred_at datetime;

# weekly user engagement

select week(occured_at) as week_number, count(event_name) as impressions, count(distinct user_id) as active_users
from `events`
where event_type = 'engagement'
group by week_number
order by week_number;

# user growth for the product
with weekly_registered_users as (
select extract(year from created_at) as `Year`, week(created_at) as week_number, count(user_id) as users_registered_per_week
from users
group by `Year`, week_number
),
growth as (
select `Year`, week_number, users_registered_per_week,
users_registered_per_week - lag(users_registered_per_week,1) over() as user_growth
from weekly_registered_users
)
select `Year`, week_number, users_registered_per_week, user_growth,
round( avg(user_growth) over(rows between 1 preceding and current row), 2) as rolling_average
from growth;

# weekly retention after signup

with signed_week as (
select user_id, week(occured_at) as signup_week
from `events`
where event_type = 'signup_flow'
),
engaged_week as (
select user_id, count(distinct week(occured_at)) as weeks_active
from `events`
where event_type = 'engagement'
group by user_id
),
weeks_active_after_signup as (
select signed_week.user_id, signup_week, weeks_active as `number of weeks active after signup`
from signed_week inner join engaged_week
on signed_week.user_id = engaged_week.user_id
order by weeks_active desc
)
select `number of weeks active after signup`, count(user_id) as `number of users active`
from weeks_active_after_signup
group by `number of weeks active after signup`
order by `number of weeks active after signup` desc;

# weekly engagement per device

select week(occured_at) as week_number, device, count(distinct user_id) as active_users, count(event_name) as impressions
from `events`
where event_type = 'engagement'
group by device, week_number
order by week_number, active_users desc;

# weekly engagement with email service

select week(occurred_at) as week_number, `action`
from email_events
group by week_number, `action`
order by week_number;


create view emails_opened_table as (
select count(`action`) as number_of_emails_opened
from email_events
where `action` = 'email_open'
);

#percentage of emails clicked through out of the emails opened

with emails_through_table as (
select count(`action`) as number_of_emails_clickedthrough
from email_events
where `action` = 'email_clickthrough'
)
select concat(round(number_of_emails_clickedthrough/number_of_emails_opened * 100, 2),"%") as percentage_of_clickthroughs
from emails_opened_table cross join emails_through_table;

# percentage of re-engagement mails sent out of the emails opened

with emails_reengaged_table as (
select count(`action`) as number_of_reengagements
from email_events
where `action` = 'sent_reengagement_email'
)
select concat(round(number_of_reengagements/number_of_emails_opened * 100, 2),"%") as percentage_of_reengagements
from emails_opened_table cross join emails_reengaged_table;

# users sending weekly digest

with users_digest as (
select count(distinct user_id) as weekly_digest_users
from email_events
where `action` = 'sent_weekly_digest'
),
total_available_users as (
select count(distinct user_id) as total_users
from email_events
)
select concat(round(weekly_digest_users/total_users *100, 2),"%") as percentage_of_users_sending_weekly_digest
from users_digest cross join total_available_users;

# week in which max re-engagement emails have been sent

with weekly_reengagement as (
select week(occurred_at) as week_number, count(`action`) as number_of_reengagements
from email_events
where `action` = 'sent_reengagement_email'
group by week_number
)
select week_number, number_of_reengagements
from weekly_reengagement
having number_of_reengagements = (
select max(number_of_reengagements) from weekly_reengagement
);