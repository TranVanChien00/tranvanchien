with data_raw as (
select
    sale_order.id, 
    sale_order.summary_state, 
    (sale_order.ngay_van_don_nhan_don + interval '7 hour')::date ngay,
    van_don.id van_don_id, 
    van_don.name van_don_name,
    res_users.name user_name,
    crm_group.name group_name, 
    crmf99_system.name system_name, 
    crm_team.name team_name, 
    country_type.name country_name, 
    product_category.name category_name, 
    res_users.id user_id,
    crm_group.id group_id, 
    crmf99_system.id system_id, 
    crm_team.id team_id, 
    country_type.id country_id, 
    product_category.id category_id, 
    case when sale_order_line.ti_gia is not null and sale_order_line.ti_gia > 1 then sale_order_line.thanh_tien_noi_dia else price_subtotal end price_subtotal
    

from sale_order_line 
    left join sale_order on sale_order.id = sale_order_line.order_id 
    left join crm_team on crm_team.id = sale_order.team_id  
    left join product_product on sale_order_line.product_id = product_product.id 
    left join res_users on res_users.id = sale_order.user_id 
    left join res_users van_don on van_don.id = sale_order.van_don_nhan_don_id
    left join crm_group on crm_group.id = sale_order.crm_group_id    
    left join res_partner on sale_order.partner_id = res_partner.id
    left join country_type on country_type.id = res_partner.country_type_id
    left join crmf99_system on crmf99_system.id = crm_group.crmf99_system_id 
    left join product_category on sale_order.product_category_id = product_category.id     
where 
    sale_order.van_don_nhan_don_id is not null 
    and sale_order.summary_state!= 'cancel'
		-- and res_users.id = @user_id
    --and crm_group.id = @group_id
    --and crmf99_system.id = @system_id 
    --and crm_team.id = @team_id 
    --and country_type.id = @country_id 
    --and product_category.id = @category_id
		--and van_don.id = @van_don_id
)
select 
    ngay
    , user_name
	, group_name
	, system_name
	, team_name
	, country_name
	, category_name
	, van_don_name
    , user_id
	, group_id
	, system_id
	, team_id
	, country_id
	, category_id
	, van_don_id
    , coalesce(sum(price_subtotal), 0) "DS Chốt"
    , coalesce(sum(case when summary_state not in ('rfq','cancel') then price_subtotal end), 0) "DS Xác nhận"
    , coalesce(sum(case when summary_state= 'completed' then price_subtotal end), 0) "DS Thành công"
from data_raw
group by 
    ngay
    , user_name
	, group_name
	, system_name
	, team_name
	, country_name
	, category_name
	, van_don_name
    , user_id
	, group_id
	, system_id
	, team_id
	, country_id
	, category_id
	, van_don_id
	
 user_id, group_id, system_id, team_id, country_id, category_id, van_don_id