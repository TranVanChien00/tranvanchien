with data_raw as (
select 
    (stock_move_line.date + interval '7 hour')::date as ngay
    , case
        when location_2.name= 'Scrap' then 'Hủy'
        when stock_move.inventory_id is not null then 'Kiểm kê'
        when location_2.name= 'Kho' or location_2.name= 'Stock' then case 
                                                            when location_1.name= 'Vendors' then 'Nhận hàng'
                                                            when stock_picking.sale_id is not null then 'Nhập hoàn'
                                                            else 'Nhập khác'
                                                        end 
        when location_2.name= 'Vendors' then 'Trả hàng'
		when stock_picking.sale_id is not null then 'Xuất bán'
		else 'Sản xuất' 
    end as phan_loai
    , product_product.id product_id 
    , location_1.warehouse_id as tu_kho
    , location_2.warehouse_id as den_kho
    , stock_move_line.qty_done
    , location_1.name as tu
    , location_2.name as den
    , stock_move.inventory_id kk_id 
from 
    stock_move_line  
    left join product_product  on product_product.id = stock_move_line.product_id 
    left join product_template pt on pt.id = product_product.product_tmpl_id 
    left join product_category  on product_category.id = pt.categ_id
    left join stock_picking  on stock_picking.id = stock_move_line.picking_id
    left join stock_location location_1 on stock_move_line.location_id = location_1.id 
    left join stock_location location_2 on stock_move_line.location_dest_id = location_2.id 
    left join stock_move on stock_move.id = stock_move_line.move_id 
where 
    stock_move_line.state= 'done'
                				---and product_category_name_level_one!= 'Vật Tư Tiêu Hao'
    and (location_1.name= 'Kho' or location_2.name= 'Kho' or location_1.name= 'Stock' or location_2.name= 'Stock')
    and stock_move_line.date between (date_trunc('month', current_date) + interval '- 12 month - 7 hour') and current_date
    and stock_move_line.date = @ngay 
    and product_product.id = @product_id 
),
tab as (
        select ngay, phan_loai, tu_kho as warehouse_id, product_id, -qty_done as so_luong
        from data_raw 
        where tu= 'Kho' or tu= 'Stock'
        union all
        select ngay, phan_loai, den_kho as warehouse_id, product_id, qty_done as so_luong
        from data_raw 
        where den= 'Kho' or den= 'Stock'),
nhap_xuat as (
select
    ngay
    , product_id
    , warehouse_id
    , sum( case when phan_loai= 'Nhận hàng' then so_luong end) as nhan_hang 
    , sum( case when phan_loai= 'Xuất bán' then so_luong end) as xuat_ban
    , sum( case when phan_loai= 'Nhập hoàn' then so_luong end) as nhap_hoan
    , sum( case when phan_loai= 'Nhập khác' then so_luong end) as nhap_khac 
    , sum( case when phan_loai= 'Kiểm kê' and so_luong<0 then so_luong end) as xuat_kk
    , sum( case when phan_loai= 'Kiểm kê' and so_luong>=0 then so_luong end) as nhap_kk
    , sum( case when phan_loai= 'Hủy' then so_luong end) as huy
    , sum( case when phan_loai= 'Sản xuất' then so_luong end) as san_xuat
    , sum( case when phan_loai= 'Trả hàng' then so_luong end) as tra_hang
from tab 
group by product_id, warehouse_id, ngay ),

ton_kho as (
select 
    product_product.id product_id
    , stock_warehouse.id as warehouse_id 
    , sum(stock_quant.quantity) as so_luong_ton 
from 
    stock_quant 
    left join stock_location on stock_location.id = stock_quant.location_id
    left join stock_warehouse on stock_warehouse.id = stock_location.warehouse_id
    left join product_product  on product_product.id = stock_quant.product_id 
    left join product_template pt on pt.id = product_product.product_tmpl_id 
    left join product_category on product_category.id = pt.categ_id
where 
    stock_location.name = 'Kho' 
    and stock_warehouse.id in (82, 97, 6, 2, 96, 4, 3, 85, 86, 81)
    and product_product.id = @product_id
    and stock_warehouse.id = @warehouse_id
 group by 
    product_product.id
    , stock_warehouse.id) 

select 
    nhap_xuat.ngay 
    , product_product.id product_id 
    , product_category.id category_id
    , stock_warehouse.id warehouse_id
    , product_product.name product_name 
    , product_category.name category_name
    , stock_warehouse.name warehouse_name
    , product_product.default_code ma_san_pham 
    , case when stock_warehouse.region= 'nam' then 'Nam' when stock_warehouse.region= 'bac' then 'Bắc' else stock_warehouse.region end mien 
    , coalesce(xuat_ban, 0) xuat_ban
    , coalesce(nhap_hoan, 0) nhap_hoan
    , coalesce(nhap_khac, 0) nhap_khac
    , coalesce(xuat_kk, 0) xuat_kk
    , coalesce(nhap_kk, 0) nhap_kk
    , coalesce(huy, 0) huy
    , coalesce(san_xuat, 0) san_xuat
    , coalesce(tra_hang, 0) tra_hang
    , coalesce(so_luong_ton, 0) so_luong_ton
    ,  coalesce(nhan_hang, 0) nhan_hang
from
    nhap_xuat 
    left join ton_kho on nhap_xuat.warehouse_id = ton_kho.warehouse_id and nhap_xuat.product_id = ton_kho.product_id
    left join product_product on product_product.id = nhap_xuat.product_id 
    left join product_template on product_template.id = product_product.product_tmpl_id
    left join product_category on product_category.id = product_template.categ_id
    left join stock_warehouse on stock_warehouse.id = nhap_xuat.warehouse_id
where 
    1=1 
    and product_product.id = @product_id
    and stock_warehouse.id = @warehouse_id
    and product_category.id = @category_id