{{ config (
    alias = target.database + '_blended_forecast_performance'
)}}

{% set date_granularity_list = ['day', 'week', 'month', 'quarter', 'year'] %}
  
WITH initial_ga4_data as
  (SELECT *, {{ get_date_parts('date') }} 
  FROM {{ source('ga4_raw','granular_ecomm_salesperformance') }} ),

initial_sho_data as
  (SELECT *, order_date::date as date
  FROM {{ source('shopify_base','shopify_orders') }} ),

initial_forecast_data as
  (SELECT *, {{ get_date_parts('date') }} 
  FROM {{ source('gsheet_raw','forecast_data') }} ),

initial_ft_data as
  (SELECT *, {{ get_date_parts('date') }} 
  FROM {{ source('gsheet_raw','actual_data') }} ),
  
ga4_data as
({%- for date_granularity in date_granularity_list %}  
  SELECT '{{date_granularity}}' as date_granularity, {{date_granularity}} as date,
    first_user_source_medium::varchar as source_medium, first_user_campaign_name::varchar as campaign_name,
    COALESCE(SUM(conversions_purchase),0) as ga4_purchases, COALESCE(SUM(purchase_revenue),0) as ga4_revenue
  FROM initial_ga4_data
  GROUP BY 1,2,3,4
  {% if not loop.last %}UNION ALL
  {% endif %}
{% endfor %}),

sho_data as
({%- for date_granularity in date_granularity_list %} 
  SELECT '{{date_granularity}}' as date_granularity, {{date_granularity}} as date,
    COUNT(DISTINCT order_id) as sho_purchases
  FROM initial_sho_data
  WHERE cancelled_at IS NULL AND email !='' AND total_revenue >= 0.01 
  GROUP BY 1,2
  {% if not loop.last %}UNION ALL
  {% endif %}
{% endfor %}),

ft_data as
({%- for date_granularity in date_granularity_list %} 
  SELECT '{{date_granularity}}' as date_granularity, {{date_granularity}} as date,
    COALESCE(SUM(new_customer_purchases),0) as sho_ft_purchases
  FROM initial_ft_data
  GROUP BY 1,2
  {% if not loop.last %}UNION ALL
  {% endif %}
{% endfor %}),
  
actual_data as
    (SELECT 'Actual' as type, channel, date, date_granularity, campaign_name, COALESCE(SUM(spend),0) as spend, 
        COALESCE(SUM(paid_purchases),0) as paid_purchases, COALESCE(SUM(paid_revenue),0) as paid_revenue, 
        COALESCE(SUM(sho_purchases),0) as sho_purchases, COALESCE(SUM(sho_ft_purchases),0) as sho_ft_purchases, 
        COALESCE(SUM(ga4_purchases),0) as ga4_purchases, COALESCE(SUM(ga4_revenue),0) as ga4_revenue
    FROM
        (SELECT 'Meta' as channel, date::date, date_granularity, campaign_name::varchar,
            spend, purchases as paid_purchases, revenue as paid_revenue,
            0 as sho_purchases, 0 as sho_ft_purchases,
            0 as ga4_purchases, 0 as ga4_revenue
        FROM {{ source('reporting','facebook_ad_performance') }}
        UNION ALL
        SELECT 'Google Ads' as channel, date::date, date_granularity, campaign_name::varchar,
            spend, purchases as paid_purchases, revenue as paid_revenue,
            0 as sho_purchases, 0 as sho_ft_purchases, 
            0 as ga4_purchases, 0 as ga4_revenue
        FROM {{ source('reporting','googleads_campaign_performance') }}
        UNION ALL
        SELECT 'TikTok' as channel, date::date, date_granularity, campaign_name::varchar,
            spend, purchases as paid_purchases, revenue as paid_revenue,
            0 as sho_purchases, 0 as sho_ft_purchases, 
            0 as ga4_purchases, 0 as ga4_revenue
        FROM {{ source('reporting','tiktok_ad_performance') }}
        UNION ALL
        SELECT 'Meta' as channel, date::date, date_granularity, campaign_name::varchar,
            0 as spend, 0 as paid_purchases, 0 as paid_revenue,
            0 as sho_purchases, 0 as sho_ft_purchases, 
            ga4_purchases, ga4_revenue
        FROM ga4_data
        WHERE source_medium = 'Facebook / paidsocial'
        UNION ALL
        SELECT 'Google Ads' as channel, date::date, date_granularity, campaign_name::varchar,
            0 as spend, 0 as paid_purchases, 0 as paid_revenue,
            0 as sho_purchases, 0 as sho_ft_purchases, 
            ga4_purchases, ga4_revenue
        FROM ga4_data
        WHERE source_medium = 'google / cpc'
        UNION ALL
        SELECT 'TikTok' as channel, date::date, date_granularity, campaign_name::varchar,
            0 as spend, 0 as paid_purchases, 0 as paid_revenue,
            0 as sho_purchases, 0 as sho_ft_purchases, 
            ga4_purchases, ga4_revenue
        FROM ga4_data
        WHERE source_medium = 'tiktok / paid_social'
        UNION ALL
        SELECT 'Shopify' as channel, date::date, date_granularity, null as campaign_name,
            0 as spend, 0 as paid_purchases, 0 as paid_revenue,
            sho_purchases, 0 as sho_ft_purchases, 
            0 as ga4_purchases, 0 as ga4_revenue
        FROM sho_data
        UNION ALL
        SELECT 'Shopify' as channel, date::date, date_granularity, null as campaign_name,
            0 as spend, 0 as paid_purchases, 0 as paid_revenue,
            0 as sho_purchases, sho_ft_purchases, 
            0 as ga4_purchases, 0 as ga4_revenue
        FROM ft_data)
    GROUP BY 1,2,3,4,5),
    
dg_forecast_data as
({%- for date_granularity in date_granularity_list %}  
    SELECT '{{date_granularity}}' as date_granularity, {{date_granularity}} as date,
        COALESCE(SUM(facebook_spend),0) as facebook_spend, COALESCE(SUM(google_spend),0) as google_spend, COALESCE(SUM(new_customer_purchases),0) as new_customer_purchases
    FROM initial_forecast_data
    GROUP BY 1,2
    {% if not loop.last %}UNION ALL
    {% endif %}
{% endfor %}),

forecast_data as 
    (SELECT 'Forecast' as type, channel, date::date, date_granularity, null as campaign_name,
        COALESCE(SUM(spend),0) as spend, 0 as paid_purchases, 0 as paid_revenue,
        0 as sho_purchases, COALESCE(SUM(sho_ft_purchases),0) as sho_ft_purchases, 
        0 as ga4_purchases, 0 as ga4_revenue
    FROM
        (SELECT 'Meta' as channel, date::date, date_granularity, COALESCE(SUM(facebook_spend),0) as spend, 0 as sho_ft_purchases
        FROM dg_forecast_data
        GROUP BY 1,2,3
        UNION ALL 
        SELECT 'Google Ads' as channel, date::date, date_granularity, COALESCE(SUM(google_spend),0) as spend, 0 as sho_ft_purchases
        FROM dg_forecast_data
        GROUP BY 1,2,3
        UNION ALL 
        SELECT 'TikTok' as channel, date::date, date_granularity, COALESCE(SUM(google_spend),0) as spend, 0 as sho_ft_purchases
        FROM dg_forecast_data
        GROUP BY 1,2,3
        UNION ALL 
        SELECT 'Shopify' as channel, date::date, date_granularity, 0 as spend, COALESCE(SUM(new_customer_purchases),0) as sho_ft_purchases
        FROM dg_forecast_data
        GROUP BY 1,2,3)
    GROUP BY 1,2,3,4,5),
    
joined_data as
    (SELECT * FROM actual_data
    UNION ALL
    SELECT * FROM forecast_data)
    
SELECT type,
    channel,
    date,
    date_granularity,
    campaign_name,
    spend,
    paid_purchases,
    paid_revenue,
    sho_purchases,
    sho_ft_purchases,
    ga4_purchases, 
    ga4_revenue
FROM joined_data
