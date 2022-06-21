with
    date_spine as (
        {{ dbt_utils.date_spine(
            datepart= "day"
            , start_date= "cast('2016-09-01' as date)"
            , end_date= "date_add(current_date, interval 1 day)"
        ) }}
    )

select
    date_day
from date_spine

order by date_day
