select *
from (
	select store_id
       	 , date_trunc('day', payment_date)::date as sale_date
       	 , rank () over (partition by store_id order by day_count desc) as rank_max_count
       	 , day_count
       	 , rank () over (partition by store_id order by day_sum) as rank_min_sum
       	 , day_sum
 	from (
    	select *
         	 , count (payment_id) over (partition by store_id, date_trunc('day', payment_date)) as day_count
         	 , sum (amount) over (partition by store_id, date_trunc('day', payment_date)) as day_sum
    	from payment p
    	join store s2 on p.staff_id = s2.manager_staff_id
    	) t2
 	group by store_id, sale_date, day_count, day_sum
 	) t3
where rank_max_count = 1 or rank_min_sum = 1

