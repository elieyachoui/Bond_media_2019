----First Video Viewed
With Final_1 as 
(select * from
        (select 
        bcvd.view_yr_mth,
        sf.ord_typ_nm,
        sf.eff_dtm,
        sd.email,
        sd.subscriber_id,
        sf.prcng_pln_nm,
        sf.prd_id,
        sf.prd_nm,
        bcvd.media_id,
        bcvd.series_nm,
        bcvd.episode_nm,
        bcvd.content_id,
        bcvd.src_brand_cd,
        bcvd.brand_cd,
        bcvd.owner_nm,
        bcvd.device_type,
        bcvd.view_start_dtm,
        row_number() over(partition by sd.subscriber_id,sf.prd_id order by bcvd.view_start_dtm asc) as content_viewed_order
        from bimedia.subscriber_dim sd
        left join (select * from bimedia.subscription_fact where ord_typ_nm = 'NEW') sf on
        sf.src_sbscrbr_ky = sd.subscriber_key
        Left join 
        (select * from bimedia.bond_content_views_dtl cvd 
        where cvd.authentication_user_id is not null and cvd.content_id is not null and google_campaign = 3 
        'where cvd.authentication_user_id is not null and cvd.content_id is not null
        )bcvd
        on cast(bcvd.src_subscriber_no as int) = sd.subscriber_id
        --and cast(concat(bcvd.year,bcvd.month,bcvd.day) as int) >= sf.src_txtn_dt_ky
        and cast(bcvd.view_start_dtm as timestamp) >= cast (sf.eff_dtm as timestamp)
        --where 
        ) final
        where final.content_viewed_order = 1
        --and final.subscriber_id = 29999357
        )
    
    select final_1.*, fvcount.product_upgrade_frequency, 
    datediff(cast(final_1.view_start_dtm as TIMESTAMP),cast(final_1.eff_dtm as TIMESTAMP)) as view_dtm_vs_upgrade_dtm
    
    from final_1
    inner join (select final_1.subscriber_id, count(distinct(final_1.eff_dtm)) as product_upgrade_frequency from final_1 group by subscriber_id) fvcount
    on cast(final_1.subscriber_id as int) =  cast(fvcount.subscriber_id as int)
    ORDER BY final_1.subscriber_id asc, final_1.eff_dtm asc
limit 50000
