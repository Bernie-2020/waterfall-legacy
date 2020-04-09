-- Annotated Waterfall Query

-- This is where we could override any values - i.e. if you wanted to override all distances to be 30 miles, etc,
-- or override our assumed acceptance rate to be greater or less than 1%
drop table if exists input_variables_temp;
create temp table input_variables_temp as (
    select NULL::int as distance1
    ,NULL::int as capacity_override
    ,0.01 as assumed_acceptance_rate
    ,true as active_event_filters
);

--watf_events
-- This table combines all the details for all of the events that we would want to pull in an individual query / job.
drop table if exists events;
create temp table events as (
    select distinct ee.id as event_id
        ,xwalk.mobilize_id
        ,ec.title as event_category
        ,ec.name as event_category_name
        ,jr.ak_title as event_title
        ,ee.city as event_city
        ,ee.zip as event_zip
        ,ee.state as event_state
        ,ee.address1
        ,ee.venue
        ,to_date(ee.starts_at,'YYYY-MM-DD') as event_date
        ,ee.status
        ,ee.campaign_id
        ,coalesce(NULLIF(jr.distance,0), input_variables_temp.distance1) as event_distance_override
        ,jr.capacity
        ,jr.combo as combo
        ,jr.event1_link as event_link
        ,jr.van_event_van_id
        ,jr.mobilize_timeslot_id
        ,jr.van_timeslot_id
        ,ee.attendee_count
        ,ee.starts_at as event_start
        ,jr.job_id as job_id
--     ActionKit events table
        from ak_bernie.events_event ee
--     ActionKit event type table
        join ak_bernie.events_campaign ec on ec.id = ee.campaign_id
--     Crosswalk table between Actionkit and Mobilize ID for the same event
        join waterfall.events_xwalk_mini xwalk on xwalk.ak_id = ee.id
--      Our 'job request' table - if pulling from bulk requests, join bulk request table here
        join waterfall.job_requests jr on jr.mobilize_id = xwalk.mobilize_id
        where extract(hour from ee.starts_at) between 6 and 21
        and ee.status='active' --turn off for sbs events only
        and ee.is_approved=true -- turn off for sbs events only
        and ee.is_private=false --turn off for sbs events only
        and ee.zip is not null
--and ee.starts_at between getdate() + interval '{date_range[0]} days' and getdate() + interval '{date_range[1]} days'
--and date(ee.starts_at) between '{date_abs[0]}' and '{date_abs[1]}'
--and ee.campaign_id in ({campaign_id_in})
--and ec.name = '{campaign_name}'
--and ee.state in ({state_in})
--and ee.state not in ({state_not_in})
--and ee.state = '{state_eq}'
--and ee.id in ({ak_id_in})
--and xwalk.mobilize_id in ({mob_id_in})
and jr.job_id in (939)
--and jr.request_id in ({req_id_in})
);

select * from events;

--watf_event_proximity
-- This portion of the query selects all users given certain support parameters, both from the voterfile as well as
-- from our ActionKit supporter lists based on distance
drop table if exists event_proximity;
create temp table event_proximity as (
    select users.*
    ,events_distance.*
    ,row_number() over (partition by users.external_id,mobilize_id order by random()) as mobilize_shift_assignment
    from (
        select u.external_id
        ,u.first_name
        ,u.last_name
        ,u.phone
        ,u.zip
        ,u.state
        from (
            select * from (
            select 'ak_' || cu.id::varchar as external_id
            ,'actionkit' as external_id_type
            ,cu.first_name
            ,cu.last_name
            ,cu.zip
            ,cu.state
            ,apc.phone
            ,row_number() over (partition by apc.phone order by cu.created_at desc) rank_recency
            from
-- all ActionKit users table
            ak_bernie.core_user cu
--   record of all contacts and IDs across various contact methods
            left join bernie_data_commons.contactcontacts_joined ccj on ccj.actionkit_id = cu.id
-- all phone numbers, limited to actionkit users
            join bernie_data_commons.apcd_ak apc on apc.actionkit_id = cu.id
-- filters for likely cell numbers only, based on previous contact, etc.
            where apc.cell_rank_for_person = 1
-- filter by field support integer
            and (support_int in(1,2) or support_int is null))rec
            where rec.rank_recency=1

            union all

            select 'dnc_' || cc.person_id::varchar as external_id
            ,'person_id' as external_id_type
            ,cc.contact_first_name as first_name
            ,cc.contact_last_name as last_name
            ,cc.voter_zip as zip
            ,cc.voter_state as state
            ,apc.phone
            ,1 as rank_recency
--  record of all contacts and IDs across various contact methods
            from bernie_data_commons.contactcontacts_joined cc
-- all phone numbers, limited to voterfile
            join bernie_data_commons.apcd_dnc apc on apc.person_id = cc.person_id
            where cc.actionkit_id is null -- make sure we're not pulling duplicate records by excluding anyone we've matched to actionkit
            and cc.person_id is not null
-- filter by field support integer
            and support_int in (1,2)
-- filters for likely cell numbers only, based on previous contact, etc.
            and apc.cell_rank_for_person = 1
            and unique_id_flag=true
            group by 1,2,3,4,5,6,7
        ) u -- combined users table
        group by 1,2,3,4,5,6
    ) users
    join (
--     This joins only the users closer to the event than the maximum distance set in the "events" table above, based on the
--      zip_proximity_new table which contains a distance between two zip codes.
        select events.*
        ,zp.zip_2
--         distance between the 2 zips
        ,zp.distance
        from events
        join bernie_data_commons.zip_proximity_new zp on zp.zip_1 = events.event_zip
    ) events_distance on events_distance.zip_2 = users.zip
 --       and events_distance.event_state = users.state
        and events_distance.distance <= events_distance.event_distance_override
    left join (
-- Remove all users who have been texted about another event by joining on cell number
            	(select right(cell,10)::bigint as phone from waterfall.t20200311_934_il_surrogatedinner)
        ) exclusions on exclusions.phone = users.phone
        where exclusions.phone is null
);

--event_proximity count
select count(*) from event_proximity;

-- adds a column ranking how close an event is to a user
drop table if exists event_proximity_ranked;
create temp table event_proximity_ranked as (
    select *
    ,row_number() over (partition by external_id order by distance) as proximity_rank
    ,row_number() over (partition by event_id order by distance) as user_rank_for_event
    from event_proximity
    where mobilize_shift_assignment = 1
);

-- adds a column randomly determining the order in which we will call users to attend the event
drop table if exists event_user_assignment_ranked;
create temp table event_user_assignment_ranked as (
    select epr.*
    ,event_assignments.proximity_rank_assignment
    from (
        select external_id
        ,min(case -- looks at whether the top 5 events near a user are full and assigns them to the closest event that still needs more texts
            when proximity_rank = 1 and user_priority <= ((capacity - attendee_count)/(select assumed_acceptance_rate from input_variables_temp)) then 1
            when proximity_rank = 2 and user_priority <= ((capacity - attendee_count)/(select assumed_acceptance_rate from input_variables_temp)) then 2
            when proximity_rank = 3 and user_priority <= ((capacity - attendee_count)/(select assumed_acceptance_rate from input_variables_temp)) then 3
            when proximity_rank = 4 and user_priority <= ((capacity - attendee_count)/(select assumed_acceptance_rate from input_variables_temp)) then 4
            when proximity_rank = 5 and user_priority <= ((capacity - attendee_count)/(select assumed_acceptance_rate from input_variables_temp)) then 5
            end
            ) as proximity_rank_assignment
        from (
            select *
            ,row_number() over (partition by event_id, proximity_rank order by distance) as user_priority
            from event_proximity_ranked
        ) closest_events
        group by 1
    ) event_assignments
    join event_proximity_ranked epr on epr.external_id = event_assignments.external_id
);

-- adds a column randomly determining the order in which we will call users to attend the event
drop table if exists event_user_assignment_modified;
create temp table event_user_assignment_modified as (
    select euar.*
    ,case
        when event_assignment_override.external_id is not null and euar.event_id = 33878 then 1
        when event_assignment_override.external_id is not null then null
        else euar.proximity_rank_assignment
        end as modified_event_assignment
    from event_user_assignment_ranked euar
    left join (
        select euar.external_id
        from event_user_assignment_ranked euar
        where euar.event_id = 33878 and user_rank_for_event < 1000
    ) event_assignment_override on event_assignment_override.external_id = euar.external_id
);

-- Verify
-- Shows for each event whether sufficient number of users have been assigned to that event
select *
,case when closest_event>=texts_needed then 1 else 0 end as sufficient_phones_closest
,case when all_users>=texts_needed then 1 else 0 end as sufficient_phones_all
from (
    select
     event_id
    ,event_title
    ,event_date --add to review that we are grabbing correct shift dates here
    ,capacity
    ,capacity - attendee_count as rsvps_needed
    ,(capacity - attendee_count)/(select assumed_acceptance_rate from input_variables_temp) as texts_needed
    ,count(case when proximity_rank=1 then external_id end) as closest_event
    ,count(external_id) as all_users
    ,count(case when proximity_rank = modified_event_assignment then external_id end) as assigned_users
    from event_user_assignment_modified
    group by 1,2,3,4,5
) x;

--watf_final_table
drop table if exists waterfall.t20200312_939_IL_dvc2;
CREATE TABLE waterfall.t20200312_939_IL_dvc2 (
    "firstname" CHARACTER VARYING(100) ENCODE zstd NULL,
    "lastname" CHARACTER VARYING(100) ENCODE zstd NULL,
    "cell" CHARACTER VARYING(15) ENCODE zstd NULL,
    "zip" CHARACTER VARYING(15) ENCODE zstd NULL,
    "external_id" CHARACTER VARYING(100) ENCODE zstd NULL,
    "state1" CHARACTER VARYING(40) ENCODE zstd NULL,
    "day1" CHARACTER VARYING(100) ENCODE zstd NULL,
    "time1" CHARACTER VARYING(100) ENCODE zstd NULL,
    "address1" CHARACTER VARYING(500) ENCODE zstd NULL,
    "venue1" CHARACTER VARYING(500) ENCODE zstd NULL,
    "city1" CHARACTER VARYING(100) ENCODE zstd NULL,
    event_title1 CHARACTER VARYING(100) ENCODE zstd NULL,
    combo_meal1 CHARACTER VARYING(500) ENCODE zstd NULL,
    "event_category1" CHARACTER VARYING(50) ENCODE zstd NULL,
    "link1" CHARACTER VARYING(500) ENCODE zstd NULL,
    job_id CHARACTER VARYING(10) ENCODE zstd NULL,
    "ak_event_id" CHARACTER VARYING(100) ENCODE zstd NULL,
    "mobilize_id" integer NULL,
    "mobilize_timeslot_id" integer NULL,
    "van_event_van_id" integer NULL,
    "van_timeslot_id" integer ENCODE zstd NULL
);

insert into waterfall.t20200312_939_IL_dvc2(
    select coalesce(initcap(case when nr.name_pattern is null then left(ea.first_name,99) end),'friend') as firstName
    ,initcap(ea.last_name) as lastName
    ,'+1' || ea.phone::varchar as cell
    ,ea.zip
    ,ea.external_id
    ,sa.state
    ,trim(to_char(ea.event_start, 'Day'))
        || ', '
        || trim(to_char(ea.event_start, 'Mon'))
        || ' '
        || trim(to_char(ea.event_start, 'DD'))::int::varchar
        as event1_date
    ,case
        when left(to_char(ea.event_start, 'HH12:MI AM'),1) = '0' then substring(to_char(ea.event_start, 'HH12:MI AM'),2)
        else to_char(ea.event_start, 'HH12:MI AM')
        end as event1_time
    ,ea.address1 as address
    ,ea.venue as venue1
    ,initcap(event_city) as event1_city
    ,ea.event_title as event_title1
    ,ea.combo as combo_meal1
    ,initcap(replace(event_category,'-',' ')) as event1_category
    ,ea.event_link
    --,'/event/' || ec.name || '/' || ee.id::varchar end as event1_link ---use for sbs events and solidarity
    ,ea.job_id
    ,ea.event_id as ak_event_id
    ,ea.mobilize_id
    ,ea.mobilize_timeslot_id
    ,ea.van_event_van_id
    ,ea.van_timeslot_id
    from event_user_assignment_modified ea
    join ak_bernie.events_campaign ec on ec.id = ea.campaign_id
-- joins state abbreviations to full state names so that state names can be referenced in the text
    join waterfall.state_abbreviation sa on ea.event_state = sa.abbreviation
-- Takes out inappropriate names (i.e slurs, etc.) that users have inputted, by joining to a table of these names.
    left join waterfall.name_replace nr on ea.first_name ilike nr.name_pattern
    where proximity_rank = modified_event_assignment
    order by random()
);

-- watf_job_tables
-- insert the name, job ID, states contained in table into the job_tables so that tables can be excluded from other pulls.
insert into waterfall.job_tables (
    select job_id,
           'waterfall.t20200312_939_IL_dvc2',
           event_state,
           getdate(),
           max(date(e.event_date))
    from events e
    group by job_id, event_state);

------------------------------------------------------------
----CHECK FOR VARIABLE UPDATES----CHECK FOR VARIABLE UPDATES
------------------------------------------------------------
-- Venues
-- Double check here to make sure that event titles and "combos" - i.e. We'll meet at venue at address are correctly
-- formatted.
select distinct event_title1, combo_meal1 from waterfall.t20200312_939_IL_dvc2;

------------------------------------------------------------
----MAKING UPDATES----MAKING UPDATES----MAKING UPDATES------
------------------------------------------------------------

-- For updating tables

--update waterfall.t20200312_939_IL_dvc2
--set venue1 = 'the Field Office'
--where venue1 ilike '%HQ%';

--update waterfall.t20200312_939_IL_dvc2
--set venue1 = 'the Field Office'
--where true;

--update waterfall.t20200312_939_IL_dvc2
--set event_title1 = 'Barnstorm'
--where true;

------------------------------------------------------------
----REVIEW NUMBERS & EXPORT-----REVIEW NUMBERS & EXPORT----
------------------------------------------------------------
-- First row
select * from waterfall.t20200312_939_IL_dvc2 limit 1;
-- Count
select count(*) from waterfall.t20200312_939_IL_dvc2;
-- Final
select * from waterfall.t20200312_939_IL_dvc2;
