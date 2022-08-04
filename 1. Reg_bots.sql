create table reg_date (
user_id bigint primary key
, registration_date timestamp not null
, country text not null
, language text not null
)

insert into reg_date values
('1', '2005-05-24 22:54:33.000', 'Russia', 'RU'),	('2', '2005-05-24 23:03:39.000', 'China', 'CN'),	('3', '2005-05-24 23:04:41.000', 'China', 'CN'),	('4', '2005-05-24 23:05:21.000', 'China', 'CN'),	('5', '2005-05-24 23:08:07.000', 'China', 'CN'),	('6', '2005-05-24 23:11:53.000', 'China', 'CN'),	('7', '2005-05-24 23:31:46.000', 'China', 'CN'),	('8', '2005-05-24 23:31:46.000', 'China', 'CN'),	('9', '2005-05-24 23:31:46.000', 'China', 'CN'),	('10', '2005-05-24 23:31:46.000', 'China', 'CN'),	('11', '2005-05-30 23:31:46.000', 'Russia', 'RU'),	('12', '2005-05-30 23:33:46.001', 'Russia', 'RU'),	('13', '2005-05-30 23:34:46.002', 'Russia', 'RU'),	('14', '2005-05-30 23:34:55.003', 'Russia', 'RU'),	('15', '2005-05-30 23:36:46.004', 'Russia', 'RU'),	('16', '2005-05-30 23:37:46.004', 'China', 'CN'),	('17', '2005-05-30 23:38:46.004', 'Russia', 'RU'),	('18', '2005-05-30 23:39:46.005', 'Russia', 'RU'),	('19', '2005-05-30 23:41:46.006', 'Russia', 'RU'),	('20', '2005-05-30 23:42:46.007', 'Russia', 'RU'),	('21', '2005-05-30 23:42:46.008', 'Russia', 'RU'),	('22', '2005-05-30 23:43:46.009', 'Russia', 'RU'),	('23', '2005-05-30 23:45:46.010', 'Russia', 'RU'),	('24', '2005-05-30 23:46:46.011', 'Russia', 'RU'),	('25', '2005-05-30 23:46:46.011', 'USA', 'ENG'),	('26', '2005-05-30 23:47:46.012', 'Russia', 'ENG'),	('27', '2005-05-30 23:48:46.013', 'USA', 'ENG'),	('28', '2005-05-30 23:49:46.014', 'USA', 'ENG'),	('29', '2005-05-30 23:50:46.015', 'USA', 'ENG'),	('30', '2005-05-30 23:51:46.016', 'USA', 'ENG'),	('31', '2005-05-30 23:52:46.017', 'USA', 'ENG'),	('32', '2005-05-30 23:53:46.018', 'USA', 'ENG'),	('33', '2005-05-30 23:54:46.019', 'Russia', 'RU'),	('34', '2005-05-30 23:55:46.020', 'Russia', 'RU'),	('35', '2005-05-30 23:56:46.021', 'Russia', 'RU'),	('36', '2005-05-30 23:57:46.022', 'Russia', 'RU'),	('37', '2005-05-30 23:58:46.023', 'Russia', 'RU'),	('38', '2005-05-30 23:59:46.024', 'China', 'CN'),	('39', '2005-05-31 00:01:46.025', 'China', 'CN'),	('40', '2005-05-31 00:02:46.026', 'China', 'CN'),	('41', '2005-05-31 00:03:46.027', 'China', 'CN'),	('42', '2005-05-31 00:04:46.028', 'USA', 'ENG'),	('43', '2005-05-31 00:05:46.029', 'Russia', 'RU'),	('44', '2005-05-31 00:06:46.030', 'Russia', 'RU'),	('45', '2005-05-31 00:07:46.031', 'China', 'CN'),	('46', '2005-05-31 00:08:46.032', 'USA', 'ENG'),	('47', '2005-05-31 00:08:46.033', 'China', 'CN'),	('48', '2005-05-31 00:08:46.034', 'USA', 'ENG')


select *
from (
	select user_id
		 , country 
		 , language
		 , next_id
		 , registration_date
		 , seq_1fst_date
		 , count(*) over (partition by seq_1fst_date) as bot_count
	from (
		select *
			, max (seq_start_date) over (partition by country order by user_id
							rows between unbounded preceding and current row) as seq_1fst_date   
		from (
			select *
				 , case
					when(registration_date - prev_date >= '00:03:00.000') or (prev_date is null) then registration_date 
					else null
				   end as seq_start_date
			from (
				select * 
				from (
					select country
						 , language 
						 , user_id
						 , lag(user_id,  1) over w as prev_id
						 , lead(user_id,  1) over w as next_id
						 , registration_date
						 , lag(registration_date, 1) over w as prev_date
						 , lead(registration_date, 1) over w as next_date
					from reg_date
					window w as (partition by country, language order by user_id)
					order by user_id) t1
				where (next_id - user_id <= 2 or user_id - prev_id <= 2)
					and (next_date - registration_date < '00:03:00.000' or registration_date - prev_date < '00:03:00.000') 
				) t2		
			) t3		
		) t4
	) t5
where bot_count >= 5 
order by country, user_id 
	
