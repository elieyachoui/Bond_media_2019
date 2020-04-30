'--'--Reggies: Users who register for the app but have not viewed any content.
--Non Reggies
with dim_fct as (
--1st tbl - fact
SELECT dim.*, fct.* from bimedia.subscriber_dim dim INNER JOIN bimedia.subscription_fact fct on cast(dim.subscriber_key as INT) = fct.src_sbscrbr_ky
where fct.prcng_pln_nm != '%APPLE%' and fct.ord_typ_nm <> 'REMOVE' and prd_nm not like '%TSN%' and prd_nm not like '%RDS%' and prcng_pln_nm not like '%APPLE%'
--and email like '%CHAMMAWAEL@GMAIL.COM%'
--and dim.active_subscriber = 'Y'
)
,
--second tbl - Omniture DTL
dtl_trans as (
select 
dtl.view_yr_mth,
dtl.src_subscriber_no,
dim_fct.subscriber_id

from dim_fct
left join bimedia.bond_content_views_dtl dtl
on dim_fct.subscriber_id = cast(dtl.src_subscriber_no as int)   
--and substr(cast(dim_fct.src_txtn_dt_ky as string),1,6) = concat(dtl.year,dtl.month)
--where dtl.src_subscriber_no is not null and dtl.content_id is not null ---Exclude Null Content and Subscriber IDs
)
,
Reggies_raw as (
select distinct
dtl_trans.src_subscriber_no as Omniture_subs,
dtl_trans.subscriber_id

from dtl_trans 
)

select count(distinct (subscriber_id)) as D from reggies_raw 

;
with Ohd as (
select authentication_user_id,
case
        when length( authentication_user_id ) = 24 and authentication_user_id like '5b%' then authentication_user_id
        when length(authentication_user_id) = 24 and authentication_user_id like '5c%' then authentication_user_id
        when length(authentication_user_id) = 57 then regexp_extract(authentication_user_id,'([^|]+)',1)
        else NULL
        end as um_id,
        case
        when length(authentication_user_id) = 32 then authentication_user_id
        when length(authentication_user_id) = 57 then regexp_extract(authentication_user_id,'[|]([^|]+)',1)
        else NULL
        end as profile_id,
        case when length(authentication_user_id) > 60 then authentication_user_id
        else NULL
        end as akamai_id
from bimedia.bond_content_views_dtl),

csg_mapped as (

select ohd.akamai_id,ohd.um_id, nvl(csg_um.src_subscriber_no ,csg_ak.src_subscriber_no) as subscriber_no
from Ohd

left join bimedia.csg_subscr_map csg_ak
on csg_ak.akamai_extrn_id = ohd.akamai_id

left join bimedia.csg_subscr_map csg_um
on csg_um.um_id  = ohd.um_id

)

select count(distinct(sd.subscriber_id)) from bimedia.subscriber_dim sd 
left join (select distinct subscriber_no from csg_mapped where subscriber_no is not null) s_clean 
on sd.subscriber_id = cast(s_clean.subscriber_no as int)

where s_clean.subscriber_no is not null and substr(sd.record_update_date,1,4)= '2019'
--List of subscribers from OHD that joins to CSG and at the end has a Subscription Dim record ---> Non Reggies
--Subscribers having at least 1 Omniture record
