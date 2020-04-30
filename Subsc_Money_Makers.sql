'--- 2 or 3 consecutive months
with dates as (SELECT 
 sf.rec_crt_dt as month_dt
,lag (sf.rec_crt_dt,1) over(partition by src_sbscrbr_ky,sf.net_amt order by sf.rec_crt_dt ) as lagged_dt
,sf.src_sbscrbr_ky
,sf.net_amt
FROM bimedia.subscription_fact sf
where sf.ord_typ_nm <> 'REMOVE' ) 
--and sf.src_sbscrbr_ky = 335)

 ,dates_consecutive as
(select 
src_sbscrbr_ky,
month_dt,
lagged_dt,
months_between(cast(month_dt as timestamp),cast(lagged_dt as timestamp)) months_diff,
net_amt
from dates)

select t1.src_sbscrbr_ky,substr(t1.month_dt,1,4) as trans_yr,count(t1.consecutive_flag) as Nmbr_of_cnsctv_trans, sum(net_amt) as total_revenue_$ FROM (
select
dc.*,
case when months_diff = 1 then 'consecutive'
     when months_diff > 1 then 'non_consecutive'
     when months_diff < 1 then 'less_than_one_month'
     else null end as consecutive_flag
from dates_consecutive dc) t1
where t1.consecutive_flag = 'consecutive'
group by t1.src_sbscrbr_ky,substr(t1.month_dt,1,4)
limit 200000
