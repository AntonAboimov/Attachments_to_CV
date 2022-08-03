
with report_date as (select '2022-05-16'::date as report_date)
select u_user_id
	 , card_number
	 , qty_same_card
	 , card_seq
	 , fst_card_date
	 , last_card_date
	 , address_id 
	 , qty_same_address
	 , address_seq 
	 , fst_address_date
	 , last_address_date
	 , ip 
	 , qty_same_ip 
	 , ip_seq 
	 , fst_ip_date
	 , last_ip_date
from (
	select *
	 	 , case when qty_same_card is not null then dense_rank() over (partition by card_number order by fst_card_date desc) end card_seq
		 , case when qty_same_address is not null then dense_rank() over (partition by address_id order by fst_address_date desc) end address_seq
		 , case when qty_same_ip is not null then dense_rank() over (partition by ip order by fst_ip_date desc) end ip_seq
		 , max(fst_card_date) over (partition by card_number) as last_card_date
		 , max(fst_address_date) over (partition by address_id) as last_address_date
		 , max(fst_ip_date) over (partition by ip) as last_ip_date
	from (
		select *
			 , row_number() over (partition by card_number, u_user_id  order by fst_card_date) as fst_card_use
			 , row_number() over (partition by address_id, u_user_id order by fst_address_date) as fst_address_use
			 , row_number() over (partition by ip, u_user_id order by fst_ip_date) as fst_ip_use
		from (		 
			 select *
				  , case when card_group >= 5 then card_group end qty_same_card
				  , case when address_group >= 5 then address_group end qty_same_address
				  , case when ip_group >= 5 then ip_group end qty_same_ip
				  , min(payment_date) filter (where card_group >= 5) over (partition by card_number, u_user_id) as fst_card_date
				  , min(payment_date) filter (where address_group >= 5) over (partition by address_id, u_user_id) as fst_address_date
				  , min(payment_date) filter (where ip_group >= 5) over (partition by ip, u_user_id) as fst_ip_date
			from (
				select *
					 , max(card_rank) filter (where card_number is not null) over (partition by card_number) as card_group
					 , max(address_rank) filter (where address_id is not null) over (partition by address_id) as address_group
					 , max(ip_rank) filter (where ip is not null) over (partition by ip) as ip_group				  
				from (
					select u.user_id as u_user_id 
						 , *
						 , dense_rank() over (partition by card_number order by o.user_id) as card_rank
						 , dense_rank() over (partition by address_id order by o.user_id) as address_rank
						 , dense_rank() over (partition by ip order by o.user_id) as ip_rank
					from user_reg u
					left join "order" o on u.user_id  = o.user_id 
					left join purchase p on o.order_id  = p.order_id  
					left join user_log l on p.purchase_id = l.purchase_id
					) t1
				) t2	
			) t3
		) t4
	where (qty_same_card>=5 and fst_card_use = 1) or (qty_same_address>=5 and fst_address_use = 1) or (qty_same_ip>=5 and fst_ip_use = 1)
	) t5
where  last_card_date > (select report_date from report_date)
	or last_address_date > (select report_date from report_date)
	or last_ip_date > (select report_date from report_date)
order by qty_same_card desc NULLS last, card_seq, qty_same_address desc NULLS last, address_seq, qty_same_ip desc NULLS last, ip_seq


