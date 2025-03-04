-- Doanh số
with tab_revenue as 
(
select 
    sale_order_line.id  sale_order_line_id,
    coalesce(utm_source.id,0)  source_id,
    coalesce(utm_source.user_id,0) user_id,
    coalesce(utm_source.marketing_team_id,0) team_id,
    coalesce(sale_order.product_category_id, 0) product_category_id,
    coalesce(utm_source.crm_group_id, 0) crm_group_id,
    coalesce(utm_source.channel_id,0) channel_id,
    coalesce(res_partner.country_type_id, 0) country_id, 
    sale_order.id order_id,
    date_trunc('day', sale_order.confirmed_datetime + interval '+7 hour') date_active,
    case when sale_order_line.ti_gia is not null and sale_order_line.ti_gia > 1 then sale_order_line.thanh_tien_noi_dia else price_subtotal end doanh_so
from 
    sale_order_line   
    left join sale_order on sale_order_line.order_id = sale_order.id
    left join res_partner on sale_order.partner_id = res_partner.id
    left join utm_source on sale_order.source_id=utm_source.id
where 
    sale_order.summary_state not in ('rfq','cancel') 
    and (res_partner.customer_type is null or res_partner.customer_type != 'wholesale')
    and sale_order.opportunity_type in ('sale')
    and sale_order.confirmed_datetime = @confirmed_datetime
    and res_partner.country_type_id = @country_id
    and utm_source.channel_id = @channel_id
    and utm_source.crm_group_id = @group_id
    and sale_order.product_category_id = @category_id
    and utm_source.marketing_team_id = @team_id 
    and utm_source.user_id = @user_id 
),
tab_phone as
(
select
    res_partner.id customer_id,
    res_partner.state state,
    coalesce(utm_source.id,0)  source_id,
    coalesce(utm_source.user_id,0) user_id,
    coalesce(utm_source.marketing_team_id,0) team_id,
    coalesce(res_partner.product_category_id, 0) product_category_id,
    coalesce(utm_source.crm_group_id, 0) crm_group_id,
    coalesce(utm_source.channel_id,0) channel_id,
    coalesce(res_partner.country_type_id, 0) country_id, 
    date_trunc('day', res_partner.create_date + interval '+7 hour') date_active
from res_partner
    left join utm_source on res_partner.source_id=utm_source.id
where
    res_partner.was_closed is null or res_partner.was_closed is false 
    and res_partner.create_date = @create_date
    and res_partner.country_type_id = @country_id
    and utm_source.channel_id = @channel_id
    and utm_source.crm_group_id = @group_id
    and res_partner.product_category_id = @category_id
    and utm_source.marketing_team_id = @team_id 
    and utm_source.user_id = @user_id 
),
tab_expense as
(
select 
    HE.id ticket_id,
    date_trunc('day', HE.date) date_active,
    coalesce(utm_source.id,0) source_id,
    coalesce(utm_source.user_id,0) as user_id,
    coalesce(utm_source.marketing_team_id,0) as team_id,
    coalesce(utm_source.crm_group_id, 0) crm_group_id,
    coalesce(utm_source.channel_id,0) channel_id,
    coalesce(HE.product_category_id, 0) as product_category_id,
    coalesce(HE.country_type_id, 0) country_id, 
    HE.daily_amount chi_phi,
    HE.n_comments as fbmess_cmt,
    HE.n_messages as fbmess_mess,
    HE.n_fbcd_comments as fbcd_cmt,
    (HE.n_comments + HE.n_messages + HE.n_fbcd_comments) as total_cmt_mess
    
from hr_daily_expense as HE 
    left join utm_source on  HE.source_id = utm_source.id 
where   
    HE.state in ('confirmed','to_confirm')
    and HE.date = @date
    and HE.country_type_id = @country_id
    and utm_source.channel_id = @channel_id
    and utm_source.crm_group_id = @group_id
    and HE.product_category_id = @category_id
    and utm_source.marketing_team_id = @team_id 
    and utm_source.user_id = @user_id 
),
tab_employee as 
(
select distinct user_id, channel_id, product_category_id, team_id, crm_group_id, date_active, country_id
from
    (select distinct user_id, channel_id, product_category_id, team_id, crm_group_id, date_active, country_id from tab_revenue) a
    union
    (select distinct user_id, channel_id, product_category_id, team_id, crm_group_id, date_active, country_id from tab_phone)
    union
    (select distinct user_id, channel_id, product_category_id, team_id, crm_group_id, date_active, country_id from tab_expense)
),
tab_summary as
(
select 
    tab_employee.user_id, tab_employee.channel_id, tab_employee.product_category_id, tab_employee.team_id, tab_employee.date_active, tab_employee.crm_group_id, tab_employee.country_id,
    coalesce(tab_revenue.n_order, 0) n_order, 
    coalesce(tab_revenue.sum_amount, 0) rev_amount, 
    coalesce(tab_phone.n_phone, 0) n_phone,
    coalesce(tab_phone.n_phone_day, 0) n_phone_day,
    coalesce(tab_phone.n_phone_day_nhan, 0) n_phone_day_nhan,
    coalesce(tab_expense.sum_amount, 0) exp_amount, 
    coalesce(tab_expense.fbmess_mess, 0) fbmess_mess, 
    coalesce(tab_expense.fbmess_cmt, 0) fbmess_cmt, 
    coalesce(tab_expense.fbcd_cmt, 0) fbcd_cmt, 
    coalesce(tab_expense.total_cmt_mess, 0) total_cmt_mess
from   tab_employee
    left join 
        (
        select 
            user_id, channel_id, product_category_id, team_id, crm_group_id, date_active, country_id,
            count(distinct order_id) n_order,
            sum(doanh_so) sum_amount
        from tab_revenue
        group by user_id, channel_id, product_category_id, team_id, crm_group_id, date_active, country_id
        ) tab_revenue on tab_employee.user_id = tab_revenue.user_id and tab_employee.country_id = tab_revenue.country_id
                and tab_employee.channel_id = tab_revenue.channel_id and tab_employee.product_category_id = tab_revenue.product_category_id
                and tab_employee.team_id = tab_revenue.team_id and tab_employee.date_active = tab_revenue.date_active and tab_employee.crm_group_id = tab_revenue.crm_group_id
    left join 
        (
        select 
            user_id, channel_id, product_category_id, team_id, crm_group_id, date_active, country_id,
            count(distinct customer_id) n_phone,
            count(distinct customer_id) FILTER (WHERE date_active = date_trunc('day', current_date + interval '+7 hour') ) n_phone_day,
            count(distinct customer_id) FILTER (WHERE date_active = date_trunc('day', current_date + interval '+7 hour') and state not in('0_new_sale','1_no_sale') ) n_phone_day_nhan
        from tab_phone
        group by user_id, channel_id, product_category_id, team_id, crm_group_id, date_active, country_id
        ) tab_phone on tab_employee.user_id = tab_phone.user_id and tab_employee.country_id = tab_phone.country_id
                and tab_employee.channel_id = tab_phone.channel_id and tab_employee.product_category_id = tab_phone.product_category_id
                and tab_employee.team_id = tab_phone.team_id and tab_employee.date_active = tab_phone.date_active and tab_employee.crm_group_id = tab_phone.crm_group_id
    left join 
        (
        select 
            user_id, channel_id, product_category_id, team_id, crm_group_id, date_active, country_id,
            sum(chi_phi) sum_amount,
            sum(fbmess_cmt) fbmess_cmt,
            sum(fbmess_mess) fbmess_mess,
            sum(fbcd_cmt) fbcd_cmt,
            sum(total_cmt_mess) total_cmt_mess
        from tab_expense
        group by user_id, channel_id, product_category_id, team_id, crm_group_id, date_active, country_id
        ) tab_expense on tab_employee.user_id = tab_expense.user_id and tab_employee.country_id = tab_expense.country_id
                and tab_employee.channel_id = tab_expense.channel_id and tab_employee.product_category_id = tab_expense.product_category_id
                and tab_employee.team_id = tab_expense.team_id and tab_employee.date_active = tab_expense.date_active and tab_employee.crm_group_id = tab_expense.crm_group_id
)



select 
    date_active,
    crm_group.name group_name, 
    crmf99_system.name system_name, 
    crm_team.name team_name, 
    country_type.name country_name, 
    res_users.name user_name, 
    utm_channel.name channel_name, 
    product_category.name category_name,
    report_product_category.name report_category_name,
    crm_group.id group_id, 
    crmf99_system.id system_id, 
    crm_team.id team_id, 
    country_type.id country_id, 
    res_users.id user_id, 
    utm_channel.id channel_id, 
    product_category.id category_id,
    report_product_category.id report_category_id, 
    product_category.mkt_sale_so_mess,
    product_category.mkt_sale_gia_mess,
    product_category.mkt_sale_so_dien_thoai,
    product_category.mkt_sale_gia_so,
    product_category.mkt_sale_ti_le_sdt_per_mess,
    product_category.mkt_sale_so_don_chot,
    product_category.mkt_sale_ti_le_chot,
    product_category.mkt_sale_doanh_so,
    product_category.mkt_sale_doanh_so_per_sdt,
    product_category.mkt_sale_tong_chi_phi_qc,
    product_category.mkt_sale_phan_tram_chi_phi_qc,
    product_category.mkt_sale_don_trung_binh,
    sum(total_cmt_mess) cmt_mess,
    sum(n_phone) sdt,
    sum(n_phone_day) sdt_tao_trong_ngay, 
    sum(n_phone_day_nhan) sdt_nhan_trong_ngay, 
    sum(n_order) so_don,
    sum(rev_amount) doanh_so,
    sum(exp_amount) chi_phi,
    sum(fbmess_mess + fbmess_cmt) total_mess
from 
    tab_summary 
    left join crm_group on crm_group.id = tab_summary.crm_group_id
    left join crmf99_system on crmf99_system.id = crm_group.crmf99_system_id 
    left join product_category on product_category.id = tab_summary.product_category_id
    left join crm_team on crm_team.id = tab_summary.team_id
    left join country_type on country_type.id = tab_summary.country_id
    left join res_users on res_users.id = tab_summary.user_id
    left join utm_channel on utm_channel.id = tab_summary.channel_id
    left join report_product_category on report_product_category.id = product_category.id 
group by  
    date_active,
    crm_group.name, 
    crmf99_system.name, 
    crm_team.name, 
    country_type.name, 
    res_users.name, 
    utm_channel.name, 
    product_category.name, 
    crm_group.id, 
    crmf99_system.id, 
    crm_team.id, 
    country_type.id, 
    res_users.id, 
    utm_channel.id, 
    product_category.id,
    report_product_category.name,
    report_product_category.id,
    product_category.mkt_sale_so_mess,
    product_category.mkt_sale_gia_mess,
    product_category.mkt_sale_so_dien_thoai,
    product_category.mkt_sale_gia_so,
    product_category.mkt_sale_ti_le_sdt_per_mess,
    product_category.mkt_sale_so_don_chot,
    product_category.mkt_sale_ti_le_chot,
    product_category.mkt_sale_doanh_so,
    product_category.mkt_sale_doanh_so_per_sdt,
    product_category.mkt_sale_tong_chi_phi_qc,
    product_category.mkt_sale_phan_tram_chi_phi_qc,
    product_category.mkt_sale_don_trung_binh
  
  
  
date_active, group_id, system_id, team_id, country_id, user_id, channel_id, category_id, report_category_id,