

--Compare OHD to (see below)
select substr(logical_dt,1,10) as dtm , sum(view_start_ind) as v_start, sum(view_duration_secs)/3600 as duration
from bimedia.omniture_hit_data
where substr(logical_dt,1,10)  >= '2018-10-01'
GROUP BY substr(logical_dt,1,10)

--BCV DTL
select substr(view_start_dtm,1,10) as dtm, count( distinct src_subscriber_no) as count_subs,
sum(view_cnt) as v_start, sum(view_duration_secs)/3600 as duration
from  bimedia.bond_content_views_dtl
--where src_subscriber_no is not null
group by substr(view_start_dtm,1,10)
order by substr(view_start_dtm,1,10) desc;

-------Validate BCV DTL against OHD and check for Subscriber Counts, View COunts and View duration
---OHD
select substr(logical_dt,1,10) as dtm , sum(view_start_ind) as v_start, sum(view_duration_secs)/3600 as duration,count(*) as row_cnt
from bimedia.omniture_hit_data
where substr(logical_dt,1,10)  >= '2018-10-01' and substr(logical_dt,1,10)  <= '2019-06-30'
GROUP BY substr(logical_dt,1,10)
order by dtm

;
--DTL with dups from AKAMI with multi Sbscr No
SELECT substr(view_start_dtm,1,10) as dtm, count( distinct src_subscriber_no) as count_subs,
sum(view_cnt) as v_start, sum(view_duration_secs)/3600 as duration, count(*) as row_cnt
from bimedia.bond_content_views_dtl
where
    (substr(view_start_dtm,1,10) >= '2018-10-01' and substr(view_start_dtm,1,10) <= '2019-06-30')
    and
    authentication_user_id in (
    SELECT authentication_user_id from bimedia.bond_content_views_dtl
    GROUP BY authentication_user_id
    HAVING count(DISTINCT src_subscriber_no) >1)
GROUP BY substr(view_start_dtm,1,10)
order by dtm
;

--DTL all
select substr(view_start_dtm,1,10) as dtm, count( distinct src_subscriber_no) as count_subs,
sum(view_cnt) as v_start, sum(view_duration_secs)/3600 as duration, count(*) as row_cnt
from  bimedia.bond_content_views_dtl
where (substr(view_start_dtm,1,10) >= '2018-10-01' and substr(view_start_dtm,1,10) <= '2019-06-30')
group by substr(view_start_dtm,1,10)
order by substr(view_start_dtm,1,10) ;
