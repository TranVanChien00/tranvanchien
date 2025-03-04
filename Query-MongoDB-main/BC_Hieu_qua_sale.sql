with data_order as (
                select
                    sale_order.id order_id 
                    , sale_order.summary_state state
                    ,sale_order.opportunity_type
                    , (crm_lead.create_date + interval '7 hour')::date create_date_lead
                    , (sale_order.create_date + interval '7 hour')::date create_date
                    , (sale_order.confirmed_datetime + interval '7 hour')::date confirmed_datetime
                    , res_users.id user_id 
                    , crm_group.id group_id 
                    , country_type.id country_id 
                    , product_category.id category_id
                    , case when sale_order_line.ti_gia is not null and sale_order_line.ti_gia > 1 then sale_order_line.thanh_tien_noi_dia else price_subtotal end amount 
                    
                from sale_order_line 
                    left join sale_order on sale_order.id = sale_order_line.order_id 
                    left join crm_lead on crm_lead.id = sale_order.opportunity_id 
                    left join res_users on res_users.id = sale_order.user_id  
                    left join crm_group on crm_group.id = sale_order.crm_group_id    
                    left join res_partner on sale_order.partner_id = res_partner.id 
                    left join product_category on product_category.id = sale_order.product_category_id
                    left join country_type on country_type.id = res_partner.country_type_id 
                where 
                    (res_partner.customer_type is null or res_partner.customer_type != 'wholesale')
                    and sale_order.opportunity_type in ('sale', 'resale')
					and sale_order.id = @order_id 
					and res_users.id = @user_id 
                    and crm_group.id = @group_id 
                    and country_type.id = @country_id 
                    and product_category.id = @category_id
                --limit 100 
                ),
    data_lead as (
                select
                    crm_lead.id lead_id 
                    , (crm_lead.first_date_open + interval '7 hour')::date first_date_open
                    , (crm_lead.date_open + interval '7 hour')::date date_open
                    , (select distinct crm_lead_id from crm_lead_note2 where crm_lead_note2.crm_lead_id = crm_lead.id) lead_note 
                    , crm_lead.first_user_id
                    , coalesce(res_users.id, 0) user_id
                    , coalesce(crm_group.id, 0) group_id
                    , coalesce(product_category.id, 0) category_id 
                    , coalesce(country_type.id, 0) country_id 
                from 
                    crm_lead 
                    left join crm_group on crm_group.id = crm_lead.crm_group_id 
                    left join product_category on product_category.id = crm_lead.product_category_id
                    left join res_partner on res_partner.id = crm_lead.partner_id
                    left join country_type on country_type.id = res_partner.country_type_id 
                    left join res_users on res_users.id = crm_lead.user_id
                where 
                    crm_lead.active= 'true'
                    and crm_lead.opportunity_type= 'sale'
					and crm_lead.id = @lead_id  
					and res_users.id = @user_id 
                    and crm_group.id = @group_id 
                    and country_type.id = @country_id 
                    and product_category.id = @category_id
                limit 100 
                ),
    tab_list as (            
                select distinct create_date ngay, user_id, group_id, category_id, country_id from data_order where create_date is not null 
                union 
                select distinct confirmed_datetime, user_id, group_id, category_id, country_id from data_order where confirmed_datetime is not null
                union 
                select distinct first_date_open, first_user_id, group_id, category_id, country_id from data_lead where first_date_open is not null
                union 
                select distinct date_open, user_id, group_id, category_id, country_id from data_lead where date_open is not null), 
    data_lead_new as (
                select 
                    count(distinct lead_id) so_moi
                    , count(distinct case when lead_note is not null then lead_id end) da_goi
                    , first_date_open ngay 
                    , first_user_id user_id
                    , group_id
                from data_lead
                group by first_user_id, group_id, first_date_open),
    data_lead_old as (
                select 
                count(distinct lead_id) so_cu 
                , user_id 
                , group_id
                , date_open ngay 
                from data_lead
                where 
                    user_id != first_user_id
                group by user_id, group_id, date_open), 
    tab_order as (
                select 
                    user_id 
                    , group_id
                    , create_date ngay 
                    , count(distinct order_id) don_chot 
                    , sum(amount) ds_chot 
                    , sum(case when state not in ('rfq','cancel') then amount end) ds_xn 
                from data_order
                where opportunity_type= 'sale'
                group by user_id, group_id, create_date), 
    data_resale as (
                select 
                    sum(amount) ds_resale
                    , user_id
                    , group_id
                    , confirmed_datetime ngay 
                from data_order
                where 
                    opportunity_type= 'resale'
                    and state not in ('rfq','cancel')
                group by user_id, group_id, confirmed_datetime),
    order_xn as (
                select 
                    user_id
                    , group_id
                    , confirmed_datetime ngay 
                    , sum(amount) ds_xn_xn
                    , count(distinct order_id) don_xn
                    , sum(case when state= 'completed' then amount end) ds_tc 
                    , sum(case when state!= 'confirmed' then amount end) ds_chuyen
                from data_order 
                where 
                    state not in ('rfq','cancel')
                    and opportunity_type= 'sale'
                group by user_id, group_id, confirmed_datetime), 
    order_xn_new as (
                select 
                    user_id
                    , group_id
                    , confirmed_datetime ngay 
                    , sum(amount) ds_xn_moi
                from data_order
                where 
                    state not in ('rfq','cancel')
                    and opportunity_type= 'sale'
                    and create_date_lead between (select min(ngay) from order_xn) and (select max(ngay) from order_xn)
                group by user_id, group_id, confirmed_datetime)
                
                select
                    tab_list.ngay 
					, crmf99_system.name system_name 
					, crm_group.name group_name 
					, crm_team.name team_name 
					, res_users.name user_name 
					, product_category.name category_name 
					, country_type.name country_name 
					, crmf99_system.id system_id 
					, crm_group.id group_id 
					, crm_team.id team_id 
					, res_users.id user_id 
					, product_category.id category_id 
					, country_type.id country_id 
                    , coalesce(so_moi, 0) so_nhan
                    , coalesce(don_chot, 0) don_chot
                    , coalesce(don_xn, 0) don_xn
                    , coalesce(ds_chot, 0) ds_chot
                    , coalesce(ds_xn_moi, 0) ds_xn_moi
                    , coalesce(da_goi, 0) da_goi
                    , coalesce(ds_resale, 0) ds_resale
                    , coalesce(ds_xn, 0) ds_xn
                    , coalesce(ds_xn_xn, 0) ds_xn_xn
                    , coalesce(ds_tc, 0) ds_tc
                    , coalesce(ds_chuyen, 0) ds_chuyen
                    , coalesce(so_moi, 0) so_moi
                    , coalesce(so_cu, 0) so_cu
                from 
                    tab_list
                    left join res_users on res_users.id = tab_list.user_id
                    left join crm_team on crm_team.id = res_users.sale_team_id 
                    left join crm_group on crm_group.id = tab_list.group_id 
                    left join crmf99_system on crmf99_system.id = crm_group.crmf99_system_id 
					left join product_category on product_category.id = tab_list.category_id 
					left join country_type on country_type.id = tab_list.country_id 
                    left join data_lead_new on data_lead_new.user_id = res_users.id and data_lead_new.group_id = crm_group.id and data_lead_new.ngay = tab_list.ngay
                    left join data_lead_old on data_lead_old.user_id = res_users.id and data_lead_old.group_id = crm_group.id and data_lead_old.ngay = tab_list.ngay 
                    left join tab_order on tab_order.user_id = res_users.id and tab_order.group_id = crm_group.id and tab_order.ngay = tab_list.ngay 
                    left join data_resale on data_resale.user_id = res_users.id and data_resale.group_id = crm_group.id and data_resale.ngay = tab_list.ngay 
                    left join order_xn on order_xn.user_id = res_users.id and order_xn.group_id = crm_group.id and order_xn.ngay = tab_list.ngay
                    left join order_xn_new on order_xn_new.user_id = res_users.id and order_xn_new.group_id = crm_group.id and order_xn_new.ngay = tab_list.ngay
										
 system_id, group_id, team_id, user_id, category_id, country_id 