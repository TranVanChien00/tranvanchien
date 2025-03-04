with data_rev as (
                    select
                        sale_order.id
                        , (sale_order.confirmed_datetime + interval '7 hour')::date ngay
                        , product_category.id  category_id
                        , crmf99_system.id system_id
                        , crm_group.id group_id 
                        , crm_team.id team_id 
                        , res_users.id user_id 
                        , country_type.id country_id 
                        , sale_order.opportunity_type
                        , case when sale_order_line.ti_gia is not null and sale_order_line.ti_gia > 1 then sale_order_line.thanh_tien_noi_dia else price_subtotal end doanh_so 
                    from sale_order_line 
                        left join sale_order on sale_order.id = sale_order_line.order_id 
                        left join crm_team on crm_team.id = sale_order.marketing_team_id  
                        left join res_users on res_users.id = sale_order.contact_creator_id  
                        left join crm_group on crm_group.id = sale_order.contact_creator_crm_group_id    
                        left join res_partner on sale_order.partner_id = res_partner.id 
                        left join crmf99_system on crmf99_system.id = crm_group.crmf99_system_id 
                        left join product_category on sale_order.product_category_id = product_category.id    
                        left join country_type on country_type.id = res_partner.country_type_id 
                    where 
                        sale_order.summary_state not in ('rfq','cancel') 
                        and crm_group.crm_group_type!= 'ban_buon_he_thong'
                        and (res_partner.customer_type is null or res_partner.customer_type != 'wholesale' )
                        and sale_order.confirmed_datetime = @date
                        and product_category.id = @category_id
                        and crmf99_system.id = @system_id
                        and crm_group.id = @group_id 
                        and crm_team.id = @team_id 
                        and res_users.id = @user_id 
                        and country_type.id = @country_id ),
        expense as (
                    select
                        HE.date::date ngay
                        , product_category.id  category_id
                        , crmf99_system.id system_id
                        , crm_group.id group_id 
                        , crm_team.id team_id 
                        , res_users.id user_id 
                        , country_type.id country_id
                        , HE.daily_amount chi_phi 
                    from hr_daily_expense as HE 
                        left join res_users on res_users.id = HE.user_id 
                        left join crm_team on crm_team.id = res_users.marketing_team_id
                        left join crm_group on crm_group.id = res_users.crm_group_id
                        left join crmf99_system on crmf99_system.id = crm_group.crmf99_system_id 
                        left join product_category on product_category.id = HE.product_category_id 
                        left join country_type on country_type.id = HE.country_type_id 
                    where   
                        HE.state in ('confirmed','to_confirm')
                        and HE.date = @date
                        and product_category.id = @category_id
                        and crmf99_system.id = @system_id
                        and crm_group.id = @group_id 
                        and crm_team.id = @team_id 
                        and res_users.id = @user_id 
                        and country_type.id = @country_id),
        tab as (                
                    select 
                        list.ngay
                        , 'Hệ thống nguồn' system_type
                        , 'Công ty nguồn' group_type
                        , list.system_id
                        , list.group_id
                        , list.team_id
                        , list.user_id
                        , list.category_id
                        , list.country_id
                        , coalesce( doanh_so, 0) doanh_so
                        , coalesce(chi_phi, 0) chi_phi
                    from 
                        (select distinct ngay, system_id, group_id, team_id, user_id, category_id, country_id from data_rev where opportunity_type= 'sale'
                        union 
                        select distinct ngay, system_id, group_id, team_id, user_id, category_id, country_id from expense) list 
                        left join (
                                    select 
                                        ngay, system_id, group_id, team_id, user_id, category_id, country_id
                                        , sum(doanh_so) doanh_so
                                    from data_rev
                                    where opportunity_type= 'sale'
                                    group by ngay, system_id, group_id, team_id, user_id, category_id, country_id ) tab_rev 
                                    on tab_rev.ngay = list.ngay and tab_rev.system_id = list.system_id and tab_rev.group_id = list.group_id 
                                    and tab_rev.team_id = list.team_id and tab_rev.user_id = list.user_id and tab_rev.category_id = list.category_id and tab_rev.country_id = list.country_id
                        left join (
                                    select 
                                        ngay, system_id, group_id, team_id, user_id, category_id, country_id
                                        , sum(chi_phi) chi_phi
                                    from expense
                                    group by ngay, system_id, group_id, team_id, user_id, category_id, country_id) tab_exp 
                                    on tab_exp.ngay = list.ngay and tab_exp.system_id = list.system_id and tab_exp.group_id = list.group_id 
                                    and tab_exp.team_id = list.team_id and tab_exp.user_id = list.user_id and tab_exp.category_id = list.category_id and tab_exp.country_id = list.country_id
                    union
                    select 
                        ngay
                        , 'Hệ thống Resale' system_type
                        , 'Công ty Resale' group_type
                        , system_id
                        , group_id
                        , team_id
                        , user_id
                        , category_id
                        , country_id
                        , sum(doanh_so) doanh_so
                        , 0 chi_phi
                    from data_rev
                    where opportunity_type= 'resale'
                    group by ngay, system_id, group_id, team_id, user_id, category_id, country_id) 
                    
select 
    ngay 
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
    , group_type
    , system_type
    , coalesce( doanh_so, 0) doanh_so
    , coalesce( chi_phi, 0) chi_phi
from 
    tab 
    left join crmf99_system on crmf99_system.id = tab.system_id
    left join crm_group on crm_group.id = tab.group_id 
    left join crm_team on crm_team.id = tab.team_id
    left join res_users on res_users.id = tab.user_id
    left join country_type on country_type.id = tab.country_id
    left join product_category on product_category.id = tab.category_id
                  
                  
 system_id, group_id, team_id, user_id, category_id, country_id     