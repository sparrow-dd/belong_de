CREATE OR REPLACE VIEW agg_perdestrian_monthly_counts_daily AS
select *
,rank() over(partition by cal_date order by daily_counts desc) as daily_count_rank
from
    (select
    cal_date
    ,cal_year
    ,cal_month
    ,cal_quarter
    ,cal_week
    ,cal_day
    ,coalesce(sensor_id,sl_sensor_id) as sensor_id
    ,coalesce(sensor_description,sensor_name) as sensor_name
    ,installation_date
    ,cancellation_date
    ,status
    ,direction_1
    ,direction_2
    ,latitude
    ,longitude
    ,location
    ,sum(case when is_in_active_period then hourly_counts else 0 end) as daily_counts
    from "pedestrian-counting-system-sy"."base_perdestrian_monthly_counts"
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
    )
;