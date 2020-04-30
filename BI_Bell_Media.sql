-- CSG vs Subscriber FACT vs Subscriber DIM
---Akamai Validate
select count(akamai_extrn_id) as count_dist from (
select akamai_extrn_id from csg_subscr_map
group by akamai_extrn_id
having count(distinct src_subscriber_no) >1
) csg;

--Akamai (with multiple Subscriber ID) validate with Subscriber dim then with Subscriber Fact to see if they have financial transactions
select count(distinct(xxx.src_sbscrbr_ky)) from (
select * from bimedia_uat.subscription_fact Fact where fact.src_sbscrbr_ky in (
                            select subscriber_key from bimedia_uat.subscriber_dim where cast(subscriber_id as string) in (
                            select  subs_final.src_subscriber_no from (select * from csg_subscr_map csg_main where csg_main.akamai_extrn_id in (
                            select akamai_extrn_id from bimedia_uat.csg_subscr_map group by akamai_extrn_id having count(src_subscriber_no) =2 )) subs_final))) xxx;

--Akamai (with multiple Subscriber ID) validate with Subscriber dim to see if they have financial transactions
select * from bimedia_uat.subscriber_dim
where cast(subscriber_id as string) in (select  subs_final.src_subscriber_no from (
                                    select * from csg_subscr_map csg_main where csg_main.akamai_extrn_id in (
                                    select akamai_extrn_id from bimedia_uat.csg_subscr_map group by akamai_extrn_id having count(src_subscriber_no) =2 )
                                    ) subs_final)

  ---Akamai (with multiple Subscriber ID) Validate with Omniture Only
  select * from bimedia_uat.omniture_hit_data
  where authentication_user_id in (select akamai_extrn_id from bimedia_uat.csg_subscr_map group by akamai_extrn_id having count(src_subscriber_no) =2 )
  limit 100;

-- Subscriber Delta Validation
--Old vs New Update CSG--- This quesry validates that the CSG table is getting updated from the source when compared to the CSG Old
select old.akamai_extrn_id,old.um_id,old.rec_crt_dtm,new.akamai_extrn_id,new.um_id,new.rec_crt_dtm
from csg_subscr_map new
inner join bimedia_uat.csg_subscr_map_old old
on old.src_subscriber_no=new.src_subscriber_no
where substr(new.rec_crt_dtm,1,10) ='2019-04-09' --The day after we reran the CSG workflow to capture the delta;

--1,023,207 on April 8 - Subscriber ID
select count(distinct(src_subscriber_no))
from csg_subscr_map;
--1,023,207
select count(src_subscriber_no)
from csg_subscr_map;

--217,342
select count(distinct(um_id))
from csg_subscr_map;

--914,266 on april 8 - Akamai ID . When validated in Prod there was 914,222, a difference of 60 akamai IDs becasue the prior implementation excludes Akamai's that appear more than once
select count(distinct(akamai_extrn_id))
from csg_subscr_map;

--1,023,207 - nondist Akamai
select count(akamai_extrn_id)
from csg_subscr_map;

--1,023,207 on April 8
select count(distinct(src_subscriber_no))
from csg_subscr_map_old;
--End of Subscriber Delta Validation

--Subscription Fact validation
---Subscription Fact validation 1,220,703 Distinct Subscribers between Fact and Dim
invalidate metadata bimedia_uat.subscription_fact;

select count(distinct(final.src_sbscrbr_ky)) from
(
select src_sbscrbr_ky
from bimedia_uat.subscription_fact s_fact
inner join (select subscriber_key from bimedia_uat.subscriber_dim) s_dim
on s_fact.src_sbscrbr_ky = s_dim.subscriber_key
) final;

select *
from bimedia_uat.subscription_fact
where prd_nm like '%STARZ';

select src_sbscrbr_ky, count(src_sbscrbr_ky) as C
from bimedia_uat.subscription_fact
group by src_sbscrbr_ky
order by C desc;

--1,224,699 for DIM
select count(distinct(subscriber_key))
from bimedia_uat.subscriber_dim;

--1,220,708 for FACT
select count(distinct(src_sbscrbr_ky))
from bimedia_uat.subscription_fact;
--End Subscription Fact validation

--Subscriber Final Validation
--Validate count of product_id, count of subscriber_key
select subscriber_key,
count(product_id) as Count_ID
--count(subscriber_key) as Count_ID
from bimedia_uat.subscriber_dim
group by subscriber_key
order by Count_ID desc
--limit 1
;
--Validate Unique product IDs (crave, crave plus, starz) Including TSN
select distinct(product_id) as ProductIDs
from bimedia_uat.subscriber_dim;

--Validate Unique update dates
select distinct record_update_date,last_action
from bimedia_uat.subscriber_dim
where substr(record_update_date,1,4)> '2016'
and last_action <> 'REMOVE' and last_action <> 'GIFT'
order by record_update_date;

--Validate Count of products belonging to 1 person using Email. Subscriber_id and Subscriber_key are Unique identifiers for products
select email, subscriber_key, count(product_id) as Prod_cnt
from bimedia_uat.subscriber_dim
group by email,subscriber_key order by Prod_cnt desc
limit 10
;
--End of Subscriber Final Validation

--Subscriber Old Validation
--Validate count of product_id, count of subscriber_key
select record_update_date,subscriber_key, count(subscriber_key) as Count_ID
from bimedia_uat.subscriber_dim_old
group by record_update_date,subscriber_key
order by Count_ID desc;

select record_update_date,subscriber_key, count(product_id) as Count_ID
from bimedia_uat.subscriber_dim_old
group by record_update_date,subscriber_key
order by Count_ID desc;

--Validate Unique product IDs (crave, crave plus, starz)
select distinct(product_id) as ProductIDs
from bimedia_uat.subscriber_dim_old;

select *
from bimedia_uat.subscriber_dim_old
limit 100;

--Subscriber Unique IDs 1,224,699
select count(subscriber_key)
from bimedia_uat.subscriber_dim
limit 100;
--End of Subscriber Old Validation
