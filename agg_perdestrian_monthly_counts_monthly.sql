CREATE OR REPLACE VIEW agg_perdestrian_monthly_counts_monthly AS
select *
,rank() over(partition by cal_year,cal_month order by monthly_counts desc) as monthly_count_rank
from
    (select
    cal_year
    ,cal_month
    ,cal_quarter
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
    ,sum(case when is_in_active_period then hourly_counts else 0 end) as monthly_counts
    from "pedestrian-counting-system-sy"."base_perdestrian_monthly_counts"
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13
)

;