create view flight_direction as
select a3.city as dep_city
   	 , a3.airport_code as dep_air
  	 , a4.city as arr
  	 , a4.airport_code as arr_air
from flights f
join airports a3 on f.departure_airport = a3.airport_code
join airports a4 on f.arrival_airport = a4.airport_code
group by dep_city, a3.airport_code, arr, a4.airport_code
order by dep_city, arr


select distinct dep_city
  	 , a.airport_name as starts
  	 , a.longitude as dep_long
  	 , a.latitude as dep_lat
  	 , arr as arr_city
  	 , a2.airport_name as finish
  	 , a2.longitude as arr_long
  	 , a2.latitude as arr_lat
  	 , round(6371*acos(sind(a.latitude) * sind(a2.latitude)
  				+ cosd(a.latitude) * cosd(a2.latitude) * cosd(a.longitude - a2.longitude))
  				::numeric ,0) as "L"
  	 , ac."range"
  	 , case
  			when ac."range" - round(6371*acos(sind(a.latitude) * sind(a2.latitude)
  							+ cosd(a.latitude) * cosd(a2.latitude) * cosd(a.longitude - a2.longitude))
  							::numeric ,0) > 0 then 'Yes'
  	  		else 'No'
  	   end as "Enough_range"
from flight_direction fd
join airports a on fd.dep_air = a.airport_code
join airports a2 on fd.arr_air = a2.airport_code
join flights f on fd.dep_air = f.departure_airport and fd.arr_air = f.arrival_airport
join aircrafts ac on f.aircraft_code = ac.aircraft_code
order by dep_city, starts , arr_city, finish

