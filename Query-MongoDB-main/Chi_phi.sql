with data_raw as (
select  
   case 
        when product_product.default_code = 'CP-1653709792' then 'KOL'
        when product_product.default_code = 'CP-1653709817' then 'Nguyên liệu'
        when product_product.default_code = 'CP' then 'Chi phí'
    else 'Khác' end phan_loai
    , HE.daily_amount
    , HE.date
    , res_users.name user_name 
    , crm_group.name group_name 
    , crmf99_system.name system_name 
    , crm_team.name team_name 
    , utm_channel.name channel_name 
    , country_type.name country_name 
    , product_category.name category_name 
    , report_product_category.name report_category_name 
		, res_users.id user_id 
    , crm_group.id group_id 
    , crmf99_system.id system_id 
    , crm_team.id team_id 
    , utm_channel.id channel_id 
    , country_type.id country_id 
    , product_category.id category_id 
    , report_product_category.id report_category_id 
from hr_daily_expense as HE 
    left join res_users on res_users.id = HE.user_id 
    left join crm_team on crm_team.id = res_users.marketing_team_id
    left join crm_group on crm_group.id = res_users.crm_group_id
    left join crmf99_system on crmf99_system.id = crm_group.crmf99_system_id 
    left join utm_source on  HE.source_id = utm_source.id 
    left join utm_channel on utm_channel.id = utm_source.channel_id
    left join product_category on product_category.id = HE.product_category_id 
    left join report_product_category on report_product_category.id = product_category.id 
    left join product_product on product_product.id = HE.expense_category_id  
    left join country_type on country_type.id = HE.country_type_id
where 
		HE.state in ('confirmed','to_confirm')
limit 1  ) 
		
select 
		phan_loai 
		, date
		, user_name
		, group_name
		, system_name
		, team_name
		, channel_name
		, country_name
		, category_name
		, report_category_name
		, user_id
		, group_id
		, system_id
		, team_id
		, channel_id
		, country_id
		, category_id
		, report_category_id
		, sum(daily_amount) chi_phi 
from data_raw 
group by
		phan_loai 
		, date
		, user_name
		, group_name
		, system_name
		, team_name
		, channel_name
		, country_name
		, category_name
		, report_category_name
		, user_id
		, group_id
		, system_id
		, team_id
		, channel_id
		, country_id
		, category_id
		, report_category_id

	