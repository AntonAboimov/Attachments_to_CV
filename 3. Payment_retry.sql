create table payment (
payment_id bigint primary key
, user_id bigint not null
, payment_date timestamp not null
, status text not null check (status in ('fail', 'success'))
)

insert into payment values
('1', '100003', '2005-05-24 22:54:33.000', 'fail'),	('2', '100014', '2005-05-24 23:03:39.000', 'success'),	('3', '100013', '2005-05-24 23:04:41.000', 'success'),	('4', '100011', '2005-05-24 23:05:21.000', 'success'),	('5', '100002', '2005-05-24 23:08:07.000', 'success'),	('6', '100013', '2005-05-24 23:11:53.000', 'fail'),	('7', '100007', '2005-05-24 23:31:46.000', 'success'),	('8', '100010', '2005-05-24 23:31:46.000', 'fail'),	('9', '100011', '2005-05-24 23:31:46.000', 'success'),	('10', '100008', '2005-05-24 23:31:46.000', 'success'),	('11', '100002', '2005-05-30 23:31:46.000', 'fail'),	('12', '100011', '2005-05-30 23:33:46.001', 'success'),	('13', '100002', '2005-05-30 23:34:46.002', 'success'),	('14', '100009', '2005-05-30 23:34:55.003', 'success'),	('15', '100009', '2005-05-30 23:36:46.004', 'success'),	('16', '100004', '2005-05-30 23:37:46.004', 'fail'),	('17', '100001', '2005-05-30 23:38:46.004', 'success'),	('18', '100006', '2005-05-30 23:39:46.005', 'fail'),	('19', '100004', '2005-05-30 23:41:46.006', 'success'),	('20', '100000', '2005-05-30 23:42:46.007', 'success'),	('21', '100011', '2005-05-30 23:42:46.008', 'fail'),	('22', '100002', '2005-05-30 23:43:46.009', 'success'),	('23', '100014', '2005-05-30 23:45:46.010', 'success'),	('24', '100002', '2005-05-30 23:46:46.011', 'success'),	('25', '100007', '2005-05-30 23:46:46.011', 'success'),	('26', '100009', '2005-05-30 23:47:46.012', 'fail'),	('27', '100012', '2005-05-30 23:48:46.013', 'success'),	('28', '100006', '2005-05-30 23:49:46.014', 'fail'),	('29', '100011', '2005-05-30 23:50:46.015', 'success'),	('30', '100006', '2005-05-30 23:51:46.016', 'success'),	('31', '100009', '2005-05-30 23:52:46.017', 'fail'),	('32', '100011', '2005-05-30 23:53:46.018', 'success'),	('33', '100013', '2005-05-30 23:54:46.019', 'success'),	('34', '100000', '2005-05-30 23:55:46.020', 'success'),	('35', '100004', '2005-05-30 23:56:46.021', 'success'),	('36', '100007', '2005-05-30 23:57:46.022', 'fail'),	('37', '100014', '2005-05-30 23:58:46.023', 'success'),	('38', '100009', '2005-05-30 23:59:46.024', 'fail'),	('39', '100004', '2005-05-31 00:01:46.025', 'success'),	('40', '100000', '2005-05-31 00:02:46.026', 'success'),	('41', '100012', '2005-05-31 00:03:46.027', 'fail'),	('42', '100001', '2005-05-31 00:04:46.028', 'success'),	('43', '100013', '2005-05-31 00:05:46.029', 'success'),	('44', '100003', '2005-05-31 00:06:46.030', 'success'),	('45', '100010', '2005-05-31 00:07:46.031', 'success'),	('46', '100005', '2005-05-31 00:08:46.032', 'fail'),	('47', '100002', '2005-05-31 00:08:46.033', 'success'),	('48', '100005', '2005-05-31 00:08:46.034', 'fail'),	('49', '100014', '2005-05-31 00:08:46.035', 'success'),	('50', '100011', '2005-05-31 00:08:46.036', 'success')



select date
	 , count(user_id) as user
	 , sum(payment_count) as payment_count
	 , sum(user_retry) as user_retry
	 , round(100 * count(user_id) filter (where branch = 'full_fail') ::numeric  / count (user_id), 2) as "fail_user (%)" 
	 , round(100 * count(user_id) filter (where branch = 'fail-success') ::numeric  / count (user_id), 2) as "fail-success_user (%)" 
	 , round(100 * count(user_id) filter (where branch = 'full_success') ::numeric  / count (user_id), 2) as "success_user (%)" 
from (
	select *
		 , case
			 when found_fail = 'success' then 'full_success'
			 when found_success = 'fail' then 'full_fail'
			 else 'fail-success'
		   end as branch
	from (
		select date_trunc('day', payment_date)::date as "date"
			 , user_id
			 , count(payment_id) as payment_count
			 , sum(flag) as user_retry 
			 , min (status) as found_fail
			 , max (status) as found_success
		from (
			select *
				 , case
					 when status = 'fail' and next_status is null then 0
					 when status = 'success' then null
					 else 1
				   end as flag
			from (
				select *
					 , lead(status, 1) over (partition by user_id, date_trunc('day', payment_date) order by payment_date ) as next_status
				from payment p
				 ) t1
			) t2
		group by date_trunc('day', payment_date), user_id
		) t3
	) t4
group by date
order by date


