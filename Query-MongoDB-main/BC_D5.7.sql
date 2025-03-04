with data_raw as (
select 
    sale_order.id order_id  
    , (sale_order.latest_done_pick_datetime + interval '7 hour')::date ngay 
    , res_users.name user_name
    , crm_group.name group_name
    , crmf99_system.name system_name 
    , crm_team.name team_name
    , country_type.name country_name 
    , product_category.name category_name 
    , stock_warehouse.name warehouse_name 
    , apif99_res_country_state.name state_name 
    , res_users.id user_id
    , crm_group.id group_id
    , crmf99_system.id system_id 
    , crm_team.id team_id
    , country_type.id country_id 
    , product_category.id category_id 
    , stock_warehouse.id warehouse_id 
    , apif99_res_country_state.id state_id 
    , sale_order.summary_state 
    , case 
            when delivery_service_name= 'vnpost2' then 'VN Post (Mới)'
            when delivery_service_name= 'viettelpost' then 'Viettel Post'
            when delivery_service_name= 'ems' then 'EMS'
            when delivery_service_name= 'vnpost' then 'VN Post (Cũ)'
            when delivery_service_name= 'jt' then 'J&T Phil'
            when delivery_service_name= 'jtvn' then 'J&T VN'
    end don_vi_van_chuyen
    , case 
        when stock_warehouse.region= 'nam' then 'Nam' 
        when stock_warehouse.region= 'bac' then 'Bắc' 
    end mien
    , case when sale_order_line.ti_gia is not null and sale_order_line.ti_gia > 1 then sale_order_line.thanh_tien_noi_dia else price_subtotal end price_subtotal
    , evaled_postage_amount tam_tinh
    , postage_amount phi_ship
    
from 
    sale_order_line
    left join sale_order on sale_order.id = sale_order_line.order_id 
    left join res_users on res_users.id = sale_order.user_id 
    left join crm_group on crm_group.id = sale_order.crm_group_id 
    left join crm_team on crm_team.id = sale_order.team_id 
    left join crmf99_system on crmf99_system.id = crm_group.crmf99_system_id
    left join res_partner on res_partner.id = sale_order.partner_id 
    left join country_type on country_type.id = res_partner.country_type_id 
    left join product_category on product_category.id = sale_order.product_category_id 
    left join stock_warehouse on sale_order.warehouse_id = stock_warehouse.id
    left join apif99_res_country_state on sale_order.state_id = apif99_res_country_state.id 
    left join apif99_delivery_order on sale_order.id = apif99_delivery_order.sale_id
where
    sale_order.summary_state not in ('rfq','cancel') 
    and sale_order.latest_done_pick_datetime is not null 
    --and sale_order.latest_done_pick_datetime = @ngay 
    --and res_users.id = @user_id
    --and crm_group.id = @group_id
    --and crmf99_system.id = @system_id 
    --and crm_team.id = @team_id
    --and country_type.id = @country_id 
    --and product_category.id = @category_id 
    --and stock_warehouse.id = @warehouse_id 
    --and apif99_res_country_state.id = @state_id
)


select 
    ngay
    , user_name
	, group_name
	, system_name
	, team_name
	, country_name
	, category_name
	, warehouse_name
	, state_name
	, user_id
	, group_id
	, system_id
	, team_id
	, country_id
	, category_id
	, warehouse_id
	, state_id
    , don_vi_van_chuyen
    , mien
    , count(distinct order_id ) so_don
    , sum(price_subtotal) doanh_so
    , sum(case when summary_state='completed' then price_subtotal end) ds_thanh_cong
    , sum(case when summary_state='returned' then price_subtotal end) ds_hoan
    , sum(tam_tinh) cuoc_tam_tinh
    , sum(phi_ship) cuoc_thuc_te
from data_raw 
group by 
    ngay
    , user_name
	, group_name
	, system_name
	, team_name
	, country_name
	, category_name
	, warehouse_name
	, state_name
	, user_id
	, group_id
	, system_id
	, team_id
	, country_id
	, category_id
	, warehouse_id
	, state_id
    , don_vi_van_chuyen
    , mien

-- user_id, group_id, system_id, team_id, country_id, category_id, warehouse_id, state_id
