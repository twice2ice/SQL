with first_cte as (
select ad_date,
campaign_name,
url_parameters,
coalesce (spend,'0') as spend,
coalesce (impressions,'0') as impressions,
coalesce (reach,'0') as reach,
coalesce (clicks,'0') as  clicks,
coalesce (leads,'0') as leads,
coalesce (value,'0') as value
from public.facebook_ads_basic_daily fabd 
full join public.facebook_campaign fc on fc.campaign_id = fabd.campaign_id
), second_cte as (
select *
from first_cte
union
select ad_date,
campaign_name,
url_parameters,
coalesce (spend,'0') as spend,
coalesce (impressions,'0') as impressions,
coalesce (reach,'0') as reach,
coalesce (clicks,'0') as clicks,
coalesce (leads,'0') as leads,
coalesce (value,'0') as value
from public.google_ads_basic_daily gabd
), third_cte as (
select
extract(month from ad_date) as ad_month,
nullif (lower(substring(url_parameters,13+ position ('utm_campaign'in url_parameters))), 'nan') as utm_campaign,
sum(spend) as spend,
sum(impressions) as impressions,
sum(clicks) as clicks,
sum(value) as value,
case
when (sum(impressions) != 0) then sum(clicks):: numeric/sum(impressions):: numeric
end  as CPR,
case when (sum(clicks) != 0) then sum(spend):: numeric/sum(clicks):: numeric
end as CPC,
case when (sum(impressions) != 0) then (sum(spend):: numeric/sum(impressions):: numeric)*1000
end as CPM,
case when (sum(spend) != 0) then (sum(value):: numeric-sum(spend))/sum(spend):: numeric 
end as ROMI
from second_cte
group by ad_date, campaign_name, url_parameters
order by ad_date
) select 
utm_campaign,
ad_month,
concat(cast(((avg(cpr) / lag(avg(cpr), 1) over (partition by utm_campaign order by ad_month))*100-100) as decimal (10,2)),'%') as CPR_difference,
concat(cast(((avg(cpc) / lag(avg(cpc), 1) over (partition by utm_campaign order by ad_month))*100-100) as decimal (10,2)),'%') as CPC_difference, 
concat(cast(((avg(cpm) / lag(avg(cpm), 1) over (partition by utm_campaign order by ad_month))*100-100) as decimal (10,2)),'%') as CPM_differnce,
concat(cast(((avg(romi) / lag(avg(romi), 1) over (partition by utm_campaign order by ad_month))*100-100) as decimal (10,2)),'%') as ROMI_difference
from third_cte
group by utm_campaign, ad_month
order by utm_campaign
