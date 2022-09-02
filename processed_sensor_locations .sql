CREATE OR REPLACE VIEW processed_sensor_locations AS
select *
  ,case when length(date_str) >8 then date_parse(date_str,'%d/%m/%Y')
    when length(date_str) <= 8 then date_parse(date_str,'%d/%m/%y')
    end as cancellation_date
from
    (SELECT
      sensor_id
      ,sensor_description
      ,sensor_name
      ,installation_date
      ,status
      ,direction_1
      ,direction_2
      ,latitude
      ,longitude
      ,location
      ,note
      ,case when status = 'R' then regexp_extract(note,'[0-9]{1,2}\/[0-9]{1,2}\/[0-9]{2,4}') end as date_str
   
    FROM
      "pedestrian-counting-system-sy"."processed_sensor_locations_sy"
      )
;      