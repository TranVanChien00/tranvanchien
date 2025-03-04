with data_order as 
(
select
			sale_order_line.product_id
			, sale_order.warehouse_id
			, latest_done_pick_datetime ngay 
			, product_uom_qty so_luong
from 
			sale_order
			left join sale_order_line on sale_order.id = sale_order_line.order_id
			left join product_product on product_product.id = sale_order_line.product_id 
			left join product_template on product_template.id = product_product.product_tmpl_id
where 
			sale_order.summary_state not in ('rfq','cancel')
			and sale_order.latest_done_pick_datetime between (date_trunc('day',current_date) + interval '-7 day -7 hour') and current_date
			and product_product.id = @product_id 
			and product_template.id = @template_id 
			and sale_order.warehouse_id = @warehouse_id
), 
purchase_order as (
select 
            purchase_order_line.product_id
            , stock_picking_type.warehouse_id
            , sum(purchase_order_line.product_qty) - sum(purchase_order_line.qty_received) product_qty 
from 
            purchase_order
            left join purchase_order_line on purchase_order.id = purchase_order_line.order_id
            left join product_product on product_product.id = purchase_order_line.product_id 
			left join product_template on product_template.id = product_product.product_tmpl_id
			left join stock_picking_type on purchase_order.picking_type_id = stock_picking_type.id 
where 
            purchase_order.state = 'purchase'
            and product_product.id = @product_id 
			and product_template.id = @template_id
			and stock_picking_type.warehouse_id = @warehouse_id
group by purchase_order_line.product_id, stock_picking_type.warehouse_id
having sum(purchase_order_line.product_qty) - sum(purchase_order_line.qty_received)> 0 
),

inventory_current as (
select 
            stock_quant.product_id
            , stock_warehouse.id warehouse_id
            , sum(stock_quant.quantity) - sum(stock_quant.reserved_quantity) as ton	
            , sum(case when stock_warehouse.id= 8 then stock_quant.quantity - stock_quant.reserved_quantity end) ton_huy 
            , sum(case when stock_warehouse.region= 'bac' then stock_quant.quantity - stock_quant.reserved_quantity end) ton_bac
            , sum(case when stock_warehouse.region= 'nam' then stock_quant.quantity - stock_quant.reserved_quantity end) ton_nam
from 
            stock_quant 
			left join stock_location on stock_quant.location_id = stock_location.id 
			left join stock_warehouse ON stock_warehouse.id = stock_location.warehouse_id
			left join product_product on product_product.id = stock_quant.product_id 
			left join product_template on product_template.id = product_product.product_tmpl_id
where           
            ( stock_location.name = 'Kho' or   stock_location.name = 'Stock')
            and stock_warehouse.id != 83 
            and product_product.id = @product_id 
			and product_template.id = @template_id
			and stock_warehouse.id = @warehouse_id
group by        
            stock_quant.product_id
            , stock_warehouse.id 
)

select 
        crmf99_system.name system_name 
        , crm_group.name group_name 
        , product_product.default_code product_code
        , product_product.name product_name
        , case when uom_uom.name = 'Units' then 'Cái' else uom_uom.name end  unit_name
        , product_category.name category_name
        , product_template.hsd_indays
        , stock_warehouse.name warehouse_name
        , crmf99_system.id system_id 
        , crm_group.id group_id 
        , product_product.id product_id 
        , product_category.id, category_id  
        , product_template.id template_id
        , stock_warehouse.id warehouse_id
        , coalesce( ton, 0) ton 
        , coalesce( ton_huy, 0) ton_huy 
        , coalesce( ton_bac, 0) ton_bac 
        , coalesce( ton_nam, 0) ton_nam 
        , coalesce( purchase_order.product_qty, 0) sl_dang_ve 
        , coalesce( tb_7day, 0) tb_7day
        , coalesce( tb_3day, 0) tb_3day
        , (current_date + (interval '1' day * product_template.hsd_indays))::date  "Date"
from 
            ( select distinct product_id, warehouse_id from inventory_current 
            union 
            select distinct product_id, warehouse_id from data_order
            union 
            select distinct product_id, warehouse_id from purchase_order) tab_product 
            left join stock_warehouse on stock_warehouse.id = tab_product.warehouse_id
			left join product_product on  product_product.id = tab_product.product_id
			left join product_template on  product_template.id = product_product.product_tmpl_id
			left join product_category on  product_template.categ_id = product_category.id
			left join crm_group on product_template.crm_group_id = crm_group.id
			left join crmf99_system on crm_group.crmf99_system_id = crmf99_system.id
			left join ir_property on ir_property.res_id = concat('product.product,', product_product.id)
			left join uom_uom on product_template.uom_id = uom_uom.id
			left join purchase_order on purchase_order.product_id = product_product.id and purchase_order.warehouse_id = stock_warehouse.id 
			left join inventory_current on inventory_current.product_id = product_product.id and inventory_current.warehouse_id = stock_warehouse.id 
			left join ( 
			            select product_id, warehouse_id, sum(so_luong)/7 tb_7day
			            from data_order
			            group by product_id, warehouse_id ) sales_7day on sales_7day.product_id = product_product.id and sales_7day.warehouse_id = stock_warehouse.id 
			left join ( 
			            select product_id, warehouse_id, sum(so_luong)/3 tb_3day
			            from data_order
			            where ngay between (date_trunc('day',current_date) + interval '-3 day -7 hour') and current_date
			            group by product_id, warehouse_id ) sales_3day on sales_3day.product_id = product_product.id and sales_3day.warehouse_id = stock_warehouse.id 


system_id , group_id , product_id , category_id, template_id,  warehouse_id