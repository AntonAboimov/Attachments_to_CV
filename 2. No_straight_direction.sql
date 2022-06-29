create view all_city as
select dep_city
  	, arr_city
from (select distinct city as dep_city from airports a) a
cross join (select distinct city as arr_city from airports a2) a2
where not dep_city = arr_city
order by dep_city, arr_city


create view flight_direction as
select
  	 a3.city as dep_city
  	 , a3.airport_code as dep_air
  	, a4.city as arr
  	, a4.airport_code as arr_air
from flights f
join airports a3 on f.departure_airport = a3.airport_code
join airports a4 on f.arrival_airport = a4.airport_code
group by dep_city, a3.airport_code, arr, a4.airport_code
order by dep_city, arr
