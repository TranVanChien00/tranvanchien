with data_raw as 
(select 
    sale_order.id order_id 
    , sale_order.state_id
    , stock_warehouse.region khu_vuc
    , (sale_order.confirmed_datetime + interval '+7 hour + 1 second')::date ngay 
    , case when crmf99_system.id not in (32, 34) then 'Nội bộ' else 'Đối tác' end system_type 
    , case when res_partner.customer_type= 'tmtd' then 'Đơn TMĐT' else 'Đơn thường' end customer_type
    , case 
        when lydosaituyen= 'kho_uu_tien_khong_du_ton' then 'kho_uu_tien_khong_du_ton'
        when lydosaituyen= 'khong_co_kho_uu_tien_cho_tinh_thanh' then 'khong_co_kho_uu_tien_cho_tinh_thanh'
        else case when lydosuakho is null then 'ly_do_khac' else lydosuakho end 
    end lydosaituyen
    , sale_order.summary_state
    , product_category.name category_name 
    , stock_warehouse.name  warehouse_name 
    , crmf99_system.name system_name
    , crm_group.name group_name 
    , crm_team.name team_name 
    , res_users.name user_name 
    , country_type.name country_name 
    , product_category.id  category_id
    , stock_warehouse.id warehouse_id
    , crmf99_system.id system_id
    , crm_group.id group_id 
    , crm_team.id team_id 
    , res_users.id user_id 
    , country_type.id country_id 
from 
    sale_order 
    left join stock_warehouse on stock_warehouse.id = sale_order.warehouse_id
    left join report_stock_warehouse on report_stock_warehouse.id = stock_warehouse.id
    left join res_users on res_users.id = sale_order.user_id 
    left join crm_team on crm_team.id = sale_order.team_id 
    left join crm_group on crm_group.id = sale_order.crm_group_id 
    left join crmf99_system on crmf99_system.id = sale_order.crmf99_system_id 
    left join product_category on product_category.id = sale_order.product_category_id
    left join res_partner on res_partner.id = sale_order.partner_id 
    left join country_type on country_type.id = res_partner.country_type_id
where 
    sale_order.summary_state not in ('rfq','cancel') 
    and (res_partner.customer_type is null or res_partner.customer_type != 'wholesale')
    and  stock_warehouse.region is not null
    and sale_order.confirmed_datetime = @confirmed_datetime
    and product_category.id = @category_id
    and crmf99_system.id = @system_id
    and stock_warehouse.id = @warehouse_id
    and crm_group.id = @group_id
    and crm_team.id = @team_id
    and res_users.id = @user_id
    and country_type.id = @country_id 
    and res_partner.id = @partner_id 
),

test as 
(select
    data_raw.* 
    , stock_warehouse_tinh_thanh_uu_tiens.apif99_res_country_state_id 
    , case when data_raw.state_id = stock_warehouse_tinh_thanh_uu_tiens.apif99_res_country_state_id then 'True' else 'False' end test 
from 
    data_raw
    left join stock_warehouse on data_raw.warehouse_id = stock_warehouse.id 
    left join stock_warehouse_tinh_thanh_uu_tiens on stock_warehouse_tinh_thanh_uu_tiens.stock_warehouse_id = stock_warehouse.id )

select
    ngay "Ngày"
    , system_type
    , customer_type
    , category_name 
    , warehouse_name 
    , system_name
    , group_name 
    , team_name 
    , user_name 
    , country_name 
    , case when khu_vuc= 'nam' then 'Nam' when khu_vuc= 'bac' then 'Bắc' end khu_vuc
    , category_id
    , warehouse_id
    , system_id
    , group_id 
    , team_id 
    , user_id 
    , country_id 
    , count(distinct case when khu_vuc= 'bac' then data_raw.order_id end) don_bac
    , count(distinct case when khu_vuc= 'nam' then data_raw.order_id end) don_nam
    , count(distinct case when system_name= 'Nội bộ' then data_raw.order_id end) don_noi_bo
    , count(distinct case when order_true.order_id is null and khu_vuc= 'bac' then data_raw.order_id end) don_sai_bac
    , count(distinct case when order_true.order_id is null and khu_vuc= 'nam' then data_raw.order_id end) don_sai_nam
    , count(distinct case when order_true.order_id is null and system_name= 'Nội bộ' then data_raw.order_id end) don_sai_noi_bo
    , count(distinct data_raw.order_id ) tong_don
    , count(distinct case when order_true.order_id is null then data_raw.order_id end) tong_don_sai
    , count(distinct case when order_true.order_id is not null then data_raw.order_id end) tong_don_dung
    , count(distinct case when order_true.order_id is not null and summary_state= 'completed' then data_raw.order_id end) don_dung_tc
    , count(distinct case when order_true.order_id is not null and summary_state= 'returned' then data_raw.order_id end) don_dung_hoan
    , count(distinct case when order_true.order_id is not null and summary_state not in ('completed', 'returned') then data_raw.order_id end) don_dung_dang_giao
    , count(distinct case when order_true.order_id is null and summary_state= 'completed' then data_raw.order_id end) don_sai_tc
    , count(distinct case when order_true.order_id is null and summary_state= 'returned' then data_raw.order_id end) don_sai_hoan
    , count(distinct case when order_true.order_id is null and summary_state not in ('completed', 'returned') then data_raw.order_id end) don_sai_dang_giao
    , count(distinct case when order_true.order_id is null and lydosaituyen= 'do_setup_van_hanh' then data_raw.order_id end) do_setup_van_hanh
    , count(distinct case when order_true.order_id is null and lydosaituyen= 'kho_chon_lai_co_uu_dai_ve_phi_van_chuyen' then data_raw.order_id end) phi_van_chuyen
    , count(distinct case when order_true.order_id is null and lydosaituyen= 'kho_uu_tien_khong_the_van_chuyen' then data_raw.order_id end) khong_the_van_chuyen
    , count(distinct case when order_true.order_id is null and lydosaituyen= 'quan_ly_kho_chi_dinh_kho_xuat' then data_raw.order_id end) chi_dinh_kho_xuat
    , count(distinct case when order_true.order_id is null and lydosaituyen= 'kho_uu_tien_khong_du_ton' then data_raw.order_id end) khong_du_ton
    , count(distinct case when order_true.order_id is null and lydosaituyen= 'khong_co_kho_uu_tien_cho_tinh_thanh' then data_raw.order_id end) khong_co_kho_uu_tien_cho_tinh_thanh
from 
    data_raw 
    left join (select distinct order_id from test where test= 'True') order_true on data_raw.order_id = order_true.order_Id
group by 
    ngay
    , system_type
    , customer_type
    , category_name 
    , warehouse_name 
    , system_name
    , group_name 
    , team_name 
    , user_name 
    , country_name 
    , khu_vuc
    , category_id
    , warehouse_id
    , system_id
    , group_id 
    , team_id 
    , user_id 
    , country_id 
    

---category_id, warehouse_id, system_id, group_id , team_id , user_id , country_id 