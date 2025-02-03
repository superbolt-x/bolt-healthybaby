{{ config (
    alias = target.database + '_blended_performance'
)}}

WITH blended_data as
    (SELECT channel, date::date, date_granularity, COALESCE(SUM(spend),0) as spend, COALESCE(SUM(clicks),0) as clicks, COALESCE(SUM(impressions),0) as impressions, 
        COALESCE(SUM(paid_purchases),0) as paid_purchases, COALESCE(SUM(paid_revenue),0) as paid_revenue, 
        COALESCE(SUM(sho_purchases),0) as sho_purchases, COALESCE(SUM(sho_ft_purchases),0) as sho_ft_purchases, COALESCE(SUM(sho_revenue),0) as sho_revenue, COALESCE(SUM(sho_ft_revenue),0) as sho_ft_revenue,
        COALESCE(SUM(sessions),0) as sessions, COALESCE(SUM(engaged_sessions),0) as engaged_sessions, COALESCE(SUM(ga4_purchases),0) as ga4_purchases, COALESCE(SUM(ga4_revenue),0) as ga4_revenue
    FROM
        (SELECT 'Meta' as channel, date, date_granularity, 
            spend, link_clicks as clicks, impressions, purchases as paid_purchases, revenue as paid_revenue,
            0 as sho_purchases, 0 as sho_ft_purchases, 0 as sho_revenue, 0 as sho_ft_revenue,
            0 as sessions, 0 as engaged_sessions, 0 as ga4_purchases, 0 as ga4_revenue
        FROM {{ source('reporting','facebook_ad_performance') }}
        UNION ALL
        SELECT 'Google Ads' as channel, date, date_granularity,
            spend, clicks, impressions, purchases as paid_purchases, revenue as paid_revenue,
            0 as sho_purchases, 0 as sho_ft_purchases, 0 as sho_revenue, 0 as sho_ft_revenue,
            0 as sessions, 0 as engaged_sessions, 0 as ga4_purchases, 0 as ga4_revenue
        FROM {{ source('reporting','googleads_campaign_performance') }}
        UNION ALL
        SELECT 'GA4' as channel, date, date_granularity,
            0 as spend, 0 as clicks, 0 as impressions, 0 as paid_purchases, 0 as paid_revenue,
            0 as sho_purchases, 0 as sho_ft_purchases, 0 as sho_revenue, 0 as sho_ft_revenue,
            sessions, engaged_sessions, purchase as ga4_purchases, purchase_value as ga4_revenue
        FROM {{ source('reporting','ga4_performance_by_campaign') }}
        WHERE source_medium IN ('google / cpc','Facebook / paidsocial')
        UNION ALL
        SELECT 'Shopify' as channel, date, date_granularity,
            0 as spend, 0 as clicks, 0 as impressions, 0 as paid_purchases, 0 as paid_revenue,
            orders as sho_purchases, first_orders as sho_ft_purchases, total_net_sales as sho_revenue, first_order_total_net_sales as sho_ft_revenue,
            0 as sessions, 0 as engaged_sessions, 0 as ga4_purchases, 0 as ga4_revenue
        FROM {{ source('reporting','shopify_sales') }}
        )
    GROUP BY channel, date, date_granularity)
    
SELECT channel,
    date,
    date_granularity,
    spend,
    clicks,
    impressions,
    paid_purchases,
    paid_revenue,
    sho_purchases,
    sho_ft_purchases,
    sho_revenue,
    sho_ft_revenue,
    sessions, 
    engaged_sessions, 
    ga4_purchases, 
    ga4_revenue
FROM blended_data
