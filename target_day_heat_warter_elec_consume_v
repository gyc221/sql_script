create or replace view target_day_heat_warter_elec_consume_v
as 
select 
a.station_id,
a.station_name,
a.station_type,
a.parent_id,
a.has_child,
a.year_mon,
a.day,
ifnull(b.heat_consume,d.heat_consume) as heat_consume,
ifnull(b.warter_consume,e.warter_consume/30.0) as warter_consume,
ifnull(b.elec_consume,e.elec_consume/30.0) as elec_consume
from (select t.*,ym.year_mon,d.day from temporary_year_month ym join temporary_day d join 
(
select *,
(select count(*) from temporary_heat_station where parent_id= a1.station_id) as has_child 
from temporary_heat_station a1
) t 
on t.station_type in(1,2)) a
left join target_day b on a.station_id=b.station_id and a.year_mon=b.year_mon and a.day=b.day
left join target_day_temp c on a.year_mon=c.year_mon and a.day=c.day
left join temporary_station_temp_heat_consume d on a.station_id=d.station_id and c.temp=d.temp
left join target_month_end_station e on a.station_id=e.station_id and a.year_mon=e.year_mon
