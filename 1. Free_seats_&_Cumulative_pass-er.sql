--Использую CTE, для облегчения выражения с окнонной функцией и расчетом процента в основном запросе 

with tot_seats as
(
  	select aircraft_code
         	, count (seat_no) as total_seats
  	from seats s
  	group by aircraft_code
),
        curr_seats as
(
         	select f.flight_id
               	, f.aircraft_code
               	, f.departure_airport
               	, f.actual_departure as dep_day
               	, max(boarding_no) as cur_pass
         	from flights f
         	left join ticket_flights tf on f.flight_id = tf.flight_id
         	left join boarding_passes bp on (tf.flight_id = bp.flight_id and tf.ticket_no = bp.ticket_no)
         	group by f.flight_id
         	order by flight_id
)
select                                   	
  	  flight_id
  	, cur_pass
  	, total_seats
  	, total_seats - cur_pass as empty_seats
  	, to_char(round((total_seats - cur_pass)/total_seats::numeric*100, 2), '999.9') as "empty_%"
  	, departure_airport as dep_air
  	, dep_day::timestamp
  	, sum(cur_pass) over (partition by departure_airport, date_trunc('day', dep_day)  order by dep_day) as exit_air
from curr_seats c
join tot_seats t on c.aircraft_code = t.aircraft_code
join airports a on c.departure_airport = a.airport_code
order by departure_airport, dep_day, flight_id
