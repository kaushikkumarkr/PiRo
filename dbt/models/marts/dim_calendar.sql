-- DFF Data starts Sep 14, 1989 (Week 1)
with recursive weeks as (
    select 
        1 as week_id,
        '1989-09-14'::date as start_date
    union all
    select 
        week_id + 1,
        (start_date + interval '7 days')::date
    from weeks
    where week_id < 600 -- Dataset spans ~400 weeks
),

final as (
    select
        week_id,
        start_date,
        (start_date + interval '6 days')::date as end_date,
        extract(year from start_date) as year,
        extract(month from start_date) as month,
        extract(quarter from start_date) as quarter
    from weeks
)

select * from final
