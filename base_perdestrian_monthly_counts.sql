CREATE OR REPLACE VIEW base_perdestrian_monthly_counts AS

SELECT
mc.*
,sl.sensor_id as sl_sensor_id
,sl.sensor_description
,sl.sensor_name as sl_sensor_name
,sl.installation_date
,sl.status
,sl.cancellation_date
,sl.direction_1
,sl.direction_2
,sl.latitude
,sl.longitude
,sl.location
,cal.date as cal_date
,cal.year as cal_year
,cal.month as cal_month
,cal.week as cal_week
,cal.day as cal_day
,cal.quarter as cal_quarter
,case when sl.status = 'A' and mc.date_time >= sl.installation_date then True
      when sl.status = 'A' and mc.date_time < sl.installation_date then False
      when sl.status = 'R' and mc.date_time >= sl.installation_date and mc.date_time <= sl.cancellation_date then True
      when sl.status = 'R' and mc.date_time > sl.cancellation_date then False
      end as is_in_active_period
,case when sl.sensor_id is null then 'Missing sensor id in sensor locations file'
      when mc.sensor_id is null then 'Missing count data for sensor id'
      else 'Exist in both dataset'
      end as sensor_id_check
,case when sl.sensor_description = mc.sensor_name then True
      when sl.sensor_description != mc.sensor_name then False
      end as is_sensor_name_same
FROM "pedestrian-counting-system-sy"."processed_monthly_counts_sy" as mc -- 4415575
full outer join "pedestrian-counting-system-sy"."processed_sensor_locations" as sl -- 4415583
on mc.sensor_id = sl.sensor_id
full outer join "pedestrian-counting-system-sy"."calendar_date_table" as cal -- 4447269
on date_trunc('day',mc.date_time) = date_trunc('day',cal.date)
;