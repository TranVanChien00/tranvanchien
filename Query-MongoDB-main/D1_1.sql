with data_raw as (
select
    summary_state 
    , date_trunc('month', sale_order.confirmed_datetime + interval '7 hour') thang
	, date_trunc('day', sale_order.confirmed_datetime + interval '7 hour') confirmed_datetime
    , res_users.name user_name 
    , crm_group.name group_name 
    , crmf99_system.name system_name 
    , crm_team.name team_name 
    , utm_channel.name channel_name 
    , country_type.name country_name 
    , product_category.name category_name  
    , user_nguon.name nguon_user_name 
    , team_nguon.name nguon_team_name  
    , group_nguon.name nguon_group_name 
    , system_nguon.name nguon_system_name 
    , chu_nhan.name chu_nhan_name 
    , ht_chu_nhan.name ht_chu_nhan_name
    , res_partner.name partner_name 
    , res_users.id user_id 
    , crm_group.id group_id 
    , crmf99_system.id system_id 
    , crm_team.id team_id 
    , utm_channel.id channel_id 
    , country_type.id country_id 
    , product_category.id category_id  
    , user_nguon.id nguon_user_id 
    , team_nguon.id nguon_team_id  
    , group_nguon.id nguon_group_id 
    , system_nguon.id nguon_system_id 
    , chu_nhan.id chu_nhan_id 
    , ht_chu_nhan.id ht_chu_nhan_id
    , res_partner.id partner_id
	, case when  crmf99_system.id != 20 and  crmf99_system.id != 32 and  crmf99_system.id != 34  then 'Butaba' else 'Đối tác' end type_system
	, case when res_partner.country_type_id = 1 or res_partner.country_type_id is null then 'Việt nam' else 'Nước ngoài' end sale_country 
	, case 
        when crm_group.crm_group_type= 'tmdt' then 'Thương mại điện tử'
        when crm_group.crm_group_type= 'ban_buon_he_thong' then 'Bán buôn tổng'
        when (res_partner.customer_type = 'wholesale' and  crm_team.sale_team_type = 'sale')  then 'Bán buôn hệ thống' 
        when crm_group.crm_group_type = 'sale' then 'Sale'
        when crm_group.crm_group_type = 'resale' then 'Resale'
        else 'Khác'
    end phan_loai_theo_cong_ty
	, case 
		when res_partner.customer_type= 'wholesale' then 'Sỉ'
		when  res_partner.customer_type= 'tmtd' then 'Thương mại điện tử'
		else 'Lẻ'
	end phan_loai_khach
	, case 
		when sale_order.opportunity_type= 'sale' then 'Sale'
		when sale_order.opportunity_type= 'resale' then 'Resale'
		when sale_order.opportunity_type is null then 'Khác'
		else sale_order.opportunity_type
	end loai_don 
	, case 
	    when sale_order.summary_state in ('confirmed','rfq')  then '4. Chưa giao vận chuyển'
        when sale_order.summary_state in ('shipping','reshipping') then '3. Đang chuyển hàng'
        when sale_order.summary_state = 'completed' then '1. Hoàn thành'
        when sale_order.summary_state in ('returning','returned') then '2. Đơn hoàn'
        else 'Không xác định' 
    end as trang_thai 
	, case when sale_order_line.ti_gia is not null and sale_order_line.ti_gia > 1 then sale_order_line.thanh_tien_noi_dia else price_subtotal end doanh_so
	, sale_order.id order_id 
    
from sale_order_line 
    left join sale_order on sale_order.id = sale_order_line.order_id 
    left join crm_team on crm_team.id = sale_order.team_id  
    left join product_product on sale_order_line.product_id = product_product.id 
    left join product_template on product_template.id = product_product.product_tmpl_id
    left join utm_source on utm_source.id = sale_order.source_id
    left join utm_channel on utm_channel.id = utm_source.channel_id
    left join res_users on res_users.id = sale_order.user_id  
    left join crm_group on crm_group.id = sale_order.crm_group_id    
    left join res_partner on sale_order.partner_id = res_partner.id 
    left join crmf99_system on crmf99_system.id = crm_group.crmf99_system_id 
    left join country_type on country_type.id = res_partner.country_type_id
    left join product_category on product_category.id = sale_order.product_category_id
    left join res_users user_nguon on user_nguon.id = utm_source.user_id 
    left join crm_team team_nguon on team_nguon.id = utm_source.marketing_team_id
    left join crm_group group_nguon on group_nguon.id = utm_source.crm_group_id 
    left join crmf99_system system_nguon on system_nguon.id = group_nguon.crmf99_system_id 
    left join crm_group chu_nhan on chu_nhan.id = product_template.crm_group_id
    left join crmf99_system ht_chu_nhan on ht_chu_nhan.id = chu_nhan.crmf99_system_id
where 
    sale_order.summary_state not in ('rfq','cancel')
    and res_users.id = @user_id 
    and crm_group.id = @group_id 
    and crmf99_system.id = @system_id 
    and crm_team.id = @team_id 
    and utm_channel.id = @channel_id 
    and country_type.id = @country_id 
    and product_category.id = @category_id  
    and user_nguon.id = @nguon_user_id 
    and team_nguon.id = @nguon_team_id  
    and group_nguon.id = @nguon_group_id 
    and system_nguon.id = @nguon_system_id 
    and chu_nhan.id = @chu_nhan_id 
    and ht_chu_nhan.id = @ht_chu_nhan_id
    and res_partner.id = @partner_id
limit 100)
		
select 
	thang
	, summary_state
	, confirmed_datetime
	, user_name
	, group_name
	, system_name
	, team_name
	, channel_name
	, country_name
	, category_name
	, nguon_user_name
	, nguon_team_name
	, nguon_group_name
	, nguon_system_name
	, chu_nhan_name
	, ht_chu_nhan_name 
	, partner_name
	, user_id
	, group_id
	, system_id
	, team_id
	, channel_id
	, country_id
	, category_id
	, nguon_user_id
	, nguon_team_id
	, nguon_group_id
	, nguon_system_id
	, chu_nhan_id
	, ht_chu_nhan_id 
	, partner_id
	, type_system
	, sale_country
	, phan_loai_theo_cong_ty
	, phan_loai_khach
	, loai_don
	, trang_thai
	, 'Tổng' tong 
	, sum(doanh_so) doanh_so 
	, count( distinct order_id) so_don 
from data_raw 
group by 
	thang
	, summary_state
	, confirmed_datetime
	, user_name
	, group_name
	, system_name
	, team_name
	, channel_name
	, country_name
	, category_name
	, nguon_user_name
	, nguon_team_name
	, nguon_group_name
	, nguon_system_name
	, chu_nhan_name
	, ht_chu_nhan_name 
	, partner_name
	, user_id
	, group_id
	, system_id
	, team_id
	, channel_id
	, country_id
	, category_id
	, nguon_user_id
	, nguon_team_id
	, nguon_group_id
	, nguon_system_id
	, chu_nhan_id
	, ht_chu_nhan_id 
	, partner_id
	, type_system
	, sale_country
	, phan_loai_theo_cong_ty
	, phan_loai_khach
	, loai_don
	, trang_thai
	
, user_id, group_id, system_id, team_id, channel_id, country_id, category_id, nguon_user_id, nguon_team_id, nguon_group_id, nguon_system_id, chu_nhan_id, ht_chu_nhan_id , partner_id