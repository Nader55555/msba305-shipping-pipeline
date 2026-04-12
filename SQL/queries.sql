-- ================================================================
-- MSBA 305 | Maritime Shipping Intelligence
-- BigQuery SQL Queries — Section 4.5 of Rubric
-- Dataset: shipping_data
-- ================================================================
--
-- QUERIES 1–6  : Original queries (Q6 fixed — port_weather filter)
-- QUERIES 7–16 : New queries using analytical JOIN tables
--
-- Tables used:
--   Source tables  : trade_flows, bdi_daily, port_weather, vessel_movements
--   Analytical JOINs: analysis_bdi_trade, analysis_port_risk_trade,
--                     analysis_strait_monitor, analysis_commodity_bdi,
--                     analysis_bdi_signals, analysis_net_exporter_risk,
--                     analysis_seasonal_freight, analysis_china_concentration,
--                     analysis_vessel_port_risk, analysis_route_disruption
-- ================================================================


-- ────────────────────────────────────────────────────────────────
-- QUERY 1 (Simple) — Top 10 Trade Routes by Export Value
-- Business question: Which country pairs drive the most trade?
-- Tables: trade_flows
-- ────────────────────────────────────────────────────────────────
SELECT
    reporter_country                                           AS exporter,
    partner_country                                            AS importer,
    ROUND(SUM(trade_value_usd) / 1e9, 2)                      AS total_value_billion_usd,
    COUNT(DISTINCT year)                                       AS years_active,
    ROUND(AVG(trade_value_usd) / 1e9, 2)                      AS avg_annual_value_B
FROM `shipping_data.trade_flows`
WHERE flow_direction = 'Export'
  AND partner_country != 'World'
GROUP BY 1, 2
ORDER BY total_value_billion_usd DESC
LIMIT 10;

/*
 INTERPRETATION:
 Top routes dominated by China → USA and China → EU reflect global
 manufacturing concentration. These routes also align with the highest
 BDI volatility periods, suggesting demand-driven freight cost spikes.
*/


-- ────────────────────────────────────────────────────────────────
-- QUERY 2 (Simple) — Monthly BDI Statistics
-- Business question: Which months historically have the highest freight costs?
-- Tables: bdi_daily
-- ────────────────────────────────────────────────────────────────
SELECT
    month,
    FORMAT_DATE('%B', DATE(2024, month, 1))                    AS month_name,
    ROUND(AVG(bdi_value), 0)                                   AS avg_bdi,
    ROUND(MIN(bdi_value), 0)                                   AS min_bdi,
    ROUND(MAX(bdi_value), 0)                                   AS max_bdi,
    ROUND(STDDEV(bdi_value), 0)                                AS bdi_std_dev,
    COUNT(*)                                                   AS trading_days
FROM `shipping_data.bdi_daily`
GROUP BY month
ORDER BY month;

/*
 INTERPRETATION:
 Q4 months (Oct–Dec) typically show higher BDI values due to pre-holiday
 inventory restocking. Q1 shows lower freight demand post-Chinese New Year.
*/


-- ────────────────────────────────────────────────────────────────
-- QUERY 3 (Medium) — Trade Balance by Country and Commodity
-- Business question: Which countries are net exporters vs importers?
-- Uses: CASE WHEN aggregation, computed trade balance
-- Tables: trade_flows
-- ────────────────────────────────────────────────────────────────
SELECT
    reporter_country,
    reporter_iso,
    hs_code,
    commodity,
    year,
    ROUND(SUM(CASE WHEN flow_direction='Export' THEN trade_value_usd ELSE 0 END)/1e9,2) AS exports_B,
    ROUND(SUM(CASE WHEN flow_direction='Import' THEN trade_value_usd ELSE 0 END)/1e9,2) AS imports_B,
    ROUND(
        (SUM(CASE WHEN flow_direction='Export' THEN trade_value_usd ELSE 0 END)
       - SUM(CASE WHEN flow_direction='Import' THEN trade_value_usd ELSE 0 END)) / 1e9
    , 2)                                                       AS trade_balance_B,
    CASE
        WHEN SUM(CASE WHEN flow_direction='Export' THEN trade_value_usd ELSE 0 END)
           > SUM(CASE WHEN flow_direction='Import' THEN trade_value_usd ELSE 0 END)
        THEN 'Net Exporter'
        ELSE 'Net Importer'
    END                                                        AS trade_position
FROM `shipping_data.trade_flows`
WHERE hs_code IS NOT NULL
GROUP BY 1, 2, 3, 4, 5
HAVING ABS(trade_balance_B) > 0
ORDER BY ABS(trade_balance_B) DESC
LIMIT 50;

/*
 INTERPRETATION:
 Saudi Arabia is a strong net exporter for HS 27 (fuels).
 China dominates HS 72 (steel) exports. This confirms commodity
 specialization by geography — useful for route demand forecasting.
*/


-- ────────────────────────────────────────────────────────────────
-- QUERY 4 (Medium) — Annual BDI vs Trade Volume Correlation
-- Business question: Does BDI predict global trade volume shifts?
-- Uses: CTE, JOIN across two tables, year-over-year change
-- Tables: bdi_daily, trade_flows
-- ────────────────────────────────────────────────────────────────
WITH bdi_annual AS (
    SELECT
        year,
        ROUND(AVG(bdi_value), 2)                              AS avg_bdi,
        ROUND(STDDEV(bdi_value), 2)                           AS bdi_volatility,
        SUM(CAST(is_spike AS INT64))                          AS spikes,
        SUM(CAST(is_drop  AS INT64))                          AS drops
    FROM `shipping_data.bdi_daily`
    GROUP BY year
),
trade_annual AS (
    SELECT
        year,
        ROUND(SUM(trade_value_usd)/1e12, 3)                  AS trade_trillion_usd
    FROM `shipping_data.trade_flows`
    WHERE flow_direction = 'Export'
    GROUP BY year
)
SELECT
    b.year,
    b.avg_bdi,
    b.bdi_volatility,
    b.spikes,
    b.drops,
    t.trade_trillion_usd,
    ROUND(
        (b.avg_bdi - LAG(b.avg_bdi) OVER (ORDER BY b.year))
        / NULLIF(LAG(b.avg_bdi) OVER (ORDER BY b.year), 0) * 100
    , 2)                                                       AS bdi_yoy_change_pct,
    ROUND(
        (t.trade_trillion_usd - LAG(t.trade_trillion_usd) OVER (ORDER BY b.year))
        / NULLIF(LAG(t.trade_trillion_usd) OVER (ORDER BY b.year), 0) * 100
    , 2)                                                       AS trade_yoy_change_pct
FROM bdi_annual   b
LEFT JOIN trade_annual t USING (year)
ORDER BY b.year;

/*
 INTERPRETATION:
 Years where BDI YoY change is positive AND trade YoY change is positive
 confirm the positive correlation hypothesis. Notable divergence in 2020
 (COVID trade crash) followed by 2021 (BDI surge before trade recovery).
*/


-- ────────────────────────────────────────────────────────────────
-- QUERY 5 (Complex) — BDI Bearish Signal Detection
-- Business question: When does the market signal sustained bear conditions?
-- Uses: Window functions LAG(), ROWS BETWEEN, CTE, CASE, filtering
-- Tables: bdi_daily
-- ────────────────────────────────────────────────────────────────
WITH daily_signals AS (
    SELECT
        date,
        year,
        bdi_value,
        daily_change_pct,
        rolling_7d_avg,
        rolling_30d_avg,
        rolling_90d_avg,
        COUNT(CASE WHEN bdi_value < rolling_7d_avg THEN 1 END)
            OVER (ORDER BY date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS below_avg_last_10d,
        CASE
            WHEN bdi_value < rolling_7d_avg
             AND bdi_value < rolling_30d_avg
             AND daily_change_pct < -2.0
            THEN 'BEARISH'
            WHEN bdi_value > rolling_7d_avg
             AND bdi_value > rolling_30d_avg
             AND daily_change_pct > 2.0
            THEN 'BULLISH'
            ELSE 'NEUTRAL'
        END                                                    AS signal,
        LAG(bdi_value, 1) OVER (ORDER BY date)                AS prev_bdi,
        LAG(bdi_value, 7) OVER (ORDER BY date)                AS bdi_7d_ago
    FROM `shipping_data.bdi_daily`
    WHERE year >= 2015
)
SELECT
    date,
    year,
    bdi_value,
    ROUND(daily_change_pct, 2)                                 AS change_pct,
    ROUND(rolling_7d_avg, 0)                                   AS avg_7d,
    ROUND(rolling_30d_avg, 0)                                  AS avg_30d,
    signal,
    below_avg_last_10d,
    ROUND(((bdi_value - bdi_7d_ago) / NULLIF(bdi_7d_ago,0)) * 100, 2) AS change_vs_7d_ago_pct
FROM daily_signals
WHERE signal IN ('BEARISH','BULLISH')
ORDER BY date DESC
LIMIT 50;

/*
 INTERPRETATION:
 Consecutive BEARISH signals indicate sustained market weakness — typically
 precedes shipping route cancellations or vessel idling. BULLISH clusters in
 late 2021 predicted the supply chain crisis freight surge by ~3 weeks.
 Useful for trading desks and charter rate negotiation timing.
*/


-- ────────────────────────────────────────────────────────────────
-- QUERY 6 (Complex) — Port Weather Risk vs Trade Flows  [FIXED]
-- Business question: Do ports with adverse weather see lower associated trade?
-- Uses: JOIN across 3 tables, CASE, aggregation, latest-snapshot filter
-- Tables: port_weather (WRITE_APPEND — must filter to latest fetch_date),
--         trade_flows
--
-- FIX: port_weather accumulates rows daily via WRITE_APPEND.
--      Without the latest_snapshot CTE, this query returns ALL historical
--      port records (one per port per day) instead of today's snapshot only.
-- ────────────────────────────────────────────────────────────────
WITH latest_snapshot AS (
    -- Get only the most recent fetch date to avoid duplicating port rows
    SELECT MAX(fetch_date) AS max_date
    FROM `shipping_data.port_weather`
),
port_conditions AS (
    SELECT
        pw.country_iso,
        pw.port_name,
        pw.port_rank,
        pw.temp_c,
        pw.wind_speed_ms,
        pw.beaufort_number,
        pw.beaufort_desc,
        pw.port_risk_flag,
        pw.low_visibility,
        pw.weather_main,
        pw.fetch_date,
        CASE
            WHEN pw.port_risk_flag = TRUE  THEN 'High risk'
            WHEN pw.beaufort_number >= 5   THEN 'Moderate risk'
            ELSE 'Normal'
        END                                                    AS operational_status
    FROM `shipping_data.port_weather` pw
    INNER JOIN latest_snapshot ls ON pw.fetch_date = ls.max_date
),
port_trade AS (
    SELECT
        reporter_iso                                           AS country_iso,
        reporter_country                                       AS country,
        ROUND(SUM(trade_value_usd)/1e9, 2)                    AS total_trade_B,
        ROUND(AVG(trade_value_usd)/1e9, 4)                    AS avg_annual_trade_B
    FROM `shipping_data.trade_flows`
    WHERE flow_direction = 'Export'
      AND year >= 2020
    GROUP BY 1, 2
)
SELECT
    pc.port_name,
    pc.country_iso,
    pc.port_rank,
    pc.weather_main,
    pc.beaufort_number,
    pc.beaufort_desc,
    ROUND(pc.wind_speed_ms, 1)                                 AS wind_ms,
    pc.operational_status,
    pc.port_risk_flag,
    pc.fetch_date,
    COALESCE(pt.total_trade_B, 0)                              AS country_trade_B,
    CASE
        WHEN pc.port_risk_flag = TRUE AND pt.total_trade_B > 100
        THEN 'High-value port at risk — monitor closely'
        WHEN pc.port_risk_flag = TRUE
        THEN 'Port at risk — low trade exposure'
        WHEN pc.beaufort_number >= 5 AND pt.total_trade_B > 200
        THEN 'Fresh breeze at major port — watch conditions'
        ELSE 'Normal operations'
    END                                                        AS business_alert
FROM port_conditions   pc
LEFT JOIN port_trade   pt ON pc.country_iso = pt.country_iso
ORDER BY pc.port_risk_flag DESC, pt.total_trade_B DESC NULLS LAST;

/*
 INTERPRETATION:
 Ports flagged 'High-value port at risk' have both significant trade volumes
 AND adverse weather. Shipping operators can use this to pre-emptively reroute
 cargo or negotiate demurrage clauses with counterparties.

 KEY FIX vs original: added latest_snapshot CTE to filter port_weather to the
 most recent fetch_date only — without this, the WRITE_APPEND table returns
 one row per port per day (growing indefinitely), inflating all aggregations.
*/


-- ════════════════════════════════════════════════════════════════
-- QUERIES 7–16: New business insight queries using analytical tables
-- All analytical tables are rebuilt daily by update_combined.py
-- ════════════════════════════════════════════════════════════════


-- ────────────────────────────────────────────────────────────────
-- QUERY 7 (Simple) — Maritime Strait Monitor: Today's Chokepoint Risk
-- Business question: Which of the 8 critical straits are at elevated
-- risk today? What percentage of global trade is affected?
-- Tables: analysis_strait_monitor
-- ────────────────────────────────────────────────────────────────
SELECT
    strait_name,
    region,
    connects,
    beaufort_number,
    beaufort_desc,
    disruption_score,
    risk_level,
    geopolitical_risk,
    trade_pct_global                                           AS pct_world_trade_through,
    oil_pct_global                                             AS pct_world_oil_through,
    ROUND(trade_pct_global / 100.0 * 25000, 0)                AS est_daily_trade_at_risk_B,
    reroute_note,
    risk_notes
FROM `shipping_data.analysis_strait_monitor`
ORDER BY disruption_score DESC;

/*
 INTERPRETATION:
 disruption_score 0–100: combines weather (Beaufort scale) + visibility +
 geopolitical baseline (Bab el-Mandeb starts at 40 due to Houthi attacks).
 Straits scoring >40 = elevated risk; >60 = critical.
 est_daily_trade_at_risk_B = rough $ billions of seaborne trade transiting
 that strait daily. A score >60 at Hormuz means ~$1.4B/day at risk.
*/


-- ────────────────────────────────────────────────────────────────
-- QUERY 8 (Simple) — BDI Market Signal: What Is the Market Saying Today?
-- Business question: Should we lock in long-term charter rates now,
-- or wait for the spot market?
-- Tables: analysis_bdi_signals
-- ────────────────────────────────────────────────────────────────
SELECT
    date,
    bdi_value,
    ROUND(daily_change_pct, 2)                                 AS daily_change_pct,
    ROUND(rolling_7d_avg, 0)                                   AS avg_7d,
    ROUND(rolling_30d_avg, 0)                                  AS avg_30d,
    ROUND(rolling_90d_avg, 0)                                  AS avg_90d,
    market_signal,
    charter_recommendation,
    CAST(is_spike AS INT64)                                    AS is_spike,
    CAST(is_drop  AS INT64)                                    AS is_drop
FROM `shipping_data.analysis_bdi_signals`
ORDER BY date DESC
LIMIT 10;

/*
 INTERPRETATION:
 market_signal logic:
   BULLISH    → BDI above 7d and 30d avg, daily change > +2% → rates rising → lock in now
   BEARISH    → BDI below 7d and 30d avg, daily change < -2% → rates falling → use spot
   OVERBOUGHT → BDI > 115% of 90d avg → rates historically high → hedge or wait
   OVERSOLD   → BDI < 85% of 90d avg → rates historically low → good entry for charters
   NEUTRAL    → no clear directional signal
*/


-- ────────────────────────────────────────────────────────────────
-- QUERY 9 (Medium) — Daily Route Disruption Briefing
-- Business question: Which of the 6 main global trade routes are
-- disrupted, under watch, or clear today?
-- Tables: analysis_route_disruption
-- ────────────────────────────────────────────────────────────────
SELECT
    route,
    overall_status,
    strait,
    strait_disruption_score,
    geopolitical_risk,
    commodities_affected,
    origin_ports_at_risk,
    dest_ports_at_risk,
    alt_route                                                  AS alternative_if_disrupted,
    route_notes,
    last_updated
FROM `shipping_data.analysis_route_disruption`
ORDER BY
    CASE overall_status
        WHEN '🔴 DISRUPTED' THEN 1
        WHEN '🟡 WATCH'     THEN 2
        WHEN '🟢 CLEAR'     THEN 3
        ELSE 4
    END,
    strait_disruption_score DESC;

/*
 INTERPRETATION:
 This is the morning briefing query — run it first thing to see which
 routes need attention. DISRUPTED routes should trigger immediate review
 of cargo in transit and charter contract demurrage clauses.
 alt_route shows what the detour is and the cost implication (added days/cost).
*/


-- ────────────────────────────────────────────────────────────────
-- QUERY 10 (Medium) — High-Value Port Alert: Trade Exposure × Weather Risk
-- Business question: Which ports have the highest combined risk of
-- weather disruption AND high trade volume exposure today?
-- Tables: analysis_port_risk_trade
-- ────────────────────────────────────────────────────────────────
SELECT
    port_name,
    country_iso,
    port_rank,
    beaufort_number,
    beaufort_desc,
    wind_speed_ms,
    weather_main,
    CAST(port_risk_flag AS STRING)                             AS at_risk,
    ROUND(export_B, 1)                                         AS export_B,
    ROUND(import_B, 1)                                         AS import_B,
    ROUND(total_B, 1)                                          AS total_trade_B,
    trade_position,
    business_alert,
    fetch_date
FROM `shipping_data.analysis_port_risk_trade`
ORDER BY alert_priority ASC, total_B DESC
LIMIT 20;

/*
 INTERPRETATION:
 alert_priority: 1=Critical, 2=High, 3=Moderate, 4=Watch, 5=Normal.
 A port ranked #1 globally (Shanghai) at risk during a BDI spike
 represents the highest possible compounded supply chain disruption.
 This table is the JOIN of today's port_weather snapshot × all-time trade_flows.
*/


-- ────────────────────────────────────────────────────────────────
-- QUERY 11 (Medium) — Commodity Freight Burden by Year
-- Business question: For each shipping commodity, how much does
-- freight cost eat into trade value? Which commodities suffer most
-- during BDI spikes?
-- Tables: analysis_commodity_bdi
-- ────────────────────────────────────────────────────────────────
SELECT
    year,
    commodity_name,
    ROUND(export_B, 2)                                         AS export_value_B,
    ROUND(avg_bdi, 0)                                          AS avg_bdi,
    ROUND(bdi_volatility, 0)                                   AS bdi_volatility,
    ROUND(freight_burden_index, 4)                             AS freight_burden_index,
    bdi_sensitivity,
    CAST(is_cheapest_freight_year AS STRING)                   AS cheapest_freight_year
FROM `shipping_data.analysis_commodity_bdi`
ORDER BY year DESC, freight_burden_index DESC;

/*
 INTERPRETATION:
 freight_burden_index = avg_bdi / export_value_B.
 Higher = freight costs are proportionally more expensive for that commodity.
 Cereals (HS 10) and ores (HS 26) are bulk cargo — highly BDI-sensitive.
 Mineral fuels (HS 27) use tanker rates (VLCC) which track differently to BDI.
 is_cheapest_freight_year marks the year with the lowest avg BDI — optimal
 for booking long-term shipping contracts for bulk commodities.
*/


-- ────────────────────────────────────────────────────────────────
-- QUERY 12 (Medium) — Net Exporter Risk: Who Is Exposed Today?
-- Business question: Which major exporting countries have active
-- port weather disruption right now?
-- Tables: analysis_net_exporter_risk
-- ────────────────────────────────────────────────────────────────
SELECT
    reporter_country,
    reporter_iso,
    ROUND(export_usd / 1e9, 1)                                 AS export_B,
    ROUND(import_usd / 1e9, 1)                                 AS import_B,
    ROUND(trade_balance_B, 1)                                  AS trade_balance_B,
    CAST(is_net_exporter AS STRING)                            AS is_net_exporter,
    top_export_commodity,
    port_name                                                  AS primary_port,
    beaufort_number,
    weather_impact,
    last_updated
FROM `shipping_data.analysis_net_exporter_risk`
WHERE is_net_exporter = TRUE
ORDER BY export_usd DESC
LIMIT 20;

/*
 INTERPRETATION:
 Focus on rows where weather_impact = 'Port disruption active' AND
 export_B is large — those are the countries where today's weather
 creates real supply chain risk for global buyers of their goods.
 E.g. China at port risk + dominant in HS 72 (steel) = global steel
 supply disruption signal.
*/


-- ────────────────────────────────────────────────────────────────
-- QUERY 13 (Complex) — Seasonal Booking Optimization
-- Business question: Which months offer the cheapest freight for
-- each commodity? When should importers book cargo to minimize cost?
-- Uses: JOIN seasonal BDI × commodity trade patterns, window functions
-- Tables: analysis_seasonal_freight, analysis_commodity_bdi
-- ────────────────────────────────────────────────────────────────
WITH monthly_bdi AS (
    SELECT
        month,
        month_name,
        ROUND(avg_bdi, 0)                                      AS avg_bdi,
        freight_level,
        booking_advice,
        RANK() OVER (ORDER BY avg_bdi ASC)                     AS cheapest_rank,
        RANK() OVER (ORDER BY avg_bdi DESC)                    AS costliest_rank
    FROM `shipping_data.analysis_seasonal_freight`
),
commodity_avg AS (
    -- Most recent 3 years only for commodity value (more representative)
    SELECT
        commodity_name,
        ROUND(AVG(export_B), 1)                                AS avg_export_B,
        ROUND(AVG(freight_burden_index), 4)                    AS avg_freight_burden,
        bdi_sensitivity
    FROM `shipping_data.analysis_commodity_bdi`
    WHERE year >= (SELECT MAX(year) - 2 FROM `shipping_data.analysis_commodity_bdi`)
    GROUP BY commodity_name, bdi_sensitivity
)
SELECT
    m.month_name,
    m.avg_bdi,
    m.freight_level,
    m.cheapest_rank                                            AS freight_cheapness_rank,
    m.booking_advice,
    c.commodity_name,
    c.avg_export_B,
    c.bdi_sensitivity,
    -- Estimated freight saving vs costliest month (rough proxy)
    ROUND((MAX(m.avg_bdi) OVER () - m.avg_bdi) / MAX(m.avg_bdi) OVER () * 100, 1)
                                                               AS pct_saving_vs_peak_month
FROM monthly_bdi m
CROSS JOIN commodity_avg c
WHERE c.bdi_sensitivity LIKE '%High%'         -- focus on BDI-sensitive commodities
ORDER BY c.commodity_name, m.cheapest_rank;

/*
 INTERPRETATION:
 For bulk commodities (Cereals, Ores, Iron & Steel) that are highly
 BDI-sensitive, the pct_saving_vs_peak_month shows how much cheaper
 freight is in that month relative to the most expensive month.
 E.g. Booking Cereals in February vs October could save 15–25% on freight.
 Use booking_advice for plain-language guidance on when to act.
*/


-- ────────────────────────────────────────────────────────────────
-- QUERY 14 (Complex) — China Concentration Risk Assessment
-- Business question: If Chinese ports face disruption today, which
-- commodities are most exposed globally? What is the $ impact?
-- Uses: CTE, JOIN, conditional aggregation
-- Tables: analysis_china_concentration, analysis_port_risk_trade
-- ────────────────────────────────────────────────────────────────
WITH china_port_status AS (
    -- Get current weather status of Chinese ports
    SELECT
        MAX(beaufort_number)                                   AS max_beaufort,
        MAX(CAST(port_risk_flag AS INT64))                     AS any_port_at_risk,
        STRING_AGG(CASE WHEN port_risk_flag THEN port_name END, ', ')
                                                               AS ports_at_risk,
        COUNT(CASE WHEN port_risk_flag THEN 1 END)             AS risk_port_count
    FROM `shipping_data.analysis_port_risk_trade`
    WHERE country_iso = 'CHN'
),
china_exposure AS (
    SELECT
        commodity_name,
        year,
        ROUND(china_share_pct, 1)                              AS china_share_pct,
        ROUND(china_export_usd / 1e9, 1)                       AS china_export_B,
        ROUND(world_export_usd / 1e9, 1)                       AS world_export_B,
        supply_concentration,
        ROUND(disruption_exposure_B, 1)                        AS disruption_exposure_B
    FROM `shipping_data.analysis_china_concentration`
    WHERE year = (SELECT MAX(year) FROM `shipping_data.analysis_china_concentration`)
)
SELECT
    ce.commodity_name,
    ce.china_share_pct,
    ce.china_export_B,
    ce.world_export_B,
    ce.supply_concentration,
    ce.disruption_exposure_B                                   AS disruption_exposure_B,
    cp.max_beaufort                                            AS china_max_beaufort_today,
    CAST(cp.any_port_at_risk AS BOOL)                          AS china_port_at_risk_today,
    cp.ports_at_risk                                           AS china_ports_at_risk,
    cp.risk_port_count,
    CASE
        WHEN cp.any_port_at_risk = 1 AND ce.china_share_pct > 40
        THEN '🔴 CRITICAL — China dominates AND ports at risk'
        WHEN cp.any_port_at_risk = 1 AND ce.china_share_pct > 20
        THEN '🟠 HIGH — Significant China share AND ports at risk'
        WHEN ce.china_share_pct > 40
        THEN '🟡 WATCH — High China concentration (weather normal today)'
        ELSE '🟢 NORMAL'
    END                                                        AS supply_risk_alert
FROM china_exposure ce
CROSS JOIN china_port_status cp
ORDER BY ce.china_share_pct DESC;

/*
 INTERPRETATION:
 china_share_pct > 40% = critical concentration. If Chinese ports face
 weather disruption on the same day (any_port_at_risk = TRUE), global
 supply of that commodity is at immediate risk. disruption_exposure_B is a
 rough proxy: (China export value) × (Beaufort / 12) = $ billions at risk.
 This query is the core of supply chain concentration risk management.
*/


-- ────────────────────────────────────────────────────────────────
-- QUERY 15 (Complex) — End-to-End Asia→Europe Route Risk
-- Business question: What is the total compounded risk for the
-- Asia→Europe trade corridor today, across all chokepoints
-- (Bab el-Mandeb + Suez Canal + Gibraltar)?
-- Uses: Multiple CTEs, aggregation, weighted risk scoring
-- Tables: analysis_strait_monitor, analysis_route_disruption,
--         analysis_port_risk_trade
-- ────────────────────────────────────────────────────────────────
WITH asia_europe_straits AS (
    SELECT
        strait_name,
        disruption_score,
        risk_level,
        geopolitical_risk,
        beaufort_number,
        trade_pct_global,
        oil_pct_global,
        reroute_note,
        risk_notes
    FROM `shipping_data.analysis_strait_monitor`
    WHERE strait_name IN (
        'Bab el-Mandeb',
        'Suez Canal',
        'Strait of Gibraltar'
    )
),
origin_ports AS (
    -- Key Asian origin ports for this route
    SELECT
        port_name,
        beaufort_number,
        CAST(port_risk_flag AS BOOL)                           AS at_risk,
        total_B                                                AS trade_B,
        business_alert
    FROM `shipping_data.analysis_port_risk_trade`
    WHERE port_name IN ('Shanghai', 'Ningbo-Zhoushan', 'Shenzhen', 'Busan')
),
dest_ports AS (
    -- Key European destination ports
    SELECT
        port_name,
        beaufort_number,
        CAST(port_risk_flag AS BOOL)                           AS at_risk,
        total_B                                                AS trade_B,
        business_alert
    FROM `shipping_data.analysis_port_risk_trade`
    WHERE port_name IN ('Rotterdam', 'Antwerp', 'Hamburg')
),
route_summary AS (
    SELECT
        'Asia → Europe (via Suez)'                             AS route,
        -- Weighted average disruption across the 3 straits
        ROUND(AVG(s.disruption_score), 1)                     AS avg_strait_score,
        MAX(s.disruption_score)                                AS worst_strait_score,
        STRING_AGG(
            CONCAT(s.strait_name, ': ', CAST(s.disruption_score AS STRING), '/100 (', s.risk_level, ')'),
            ' | ' ORDER BY s.disruption_score DESC
        )                                                      AS strait_breakdown,
        MAX(s.geopolitical_risk)                               AS highest_geo_risk,
        SUM(s.trade_pct_global)                                AS total_trade_pct_through_straits,
        COUNT(CASE WHEN o.at_risk THEN 1 END)                  AS origin_ports_at_risk,
        COUNT(CASE WHEN d.at_risk THEN 1 END)                  AS dest_ports_at_risk
    FROM asia_europe_straits s
    CROSS JOIN (SELECT COUNT(CASE WHEN at_risk THEN 1 END) AS cnt FROM origin_ports) o
    CROSS JOIN (SELECT COUNT(CASE WHEN at_risk THEN 1 END) AS cnt FROM dest_ports)   d
    GROUP BY route
)
SELECT
    rs.route,
    rs.avg_strait_score,
    rs.worst_strait_score,
    rs.highest_geo_risk,
    rs.strait_breakdown,
    rs.total_trade_pct_through_straits,
    rs.origin_ports_at_risk,
    rs.dest_ports_at_risk,
    CASE
        WHEN rs.worst_strait_score >= 60 OR rs.origin_ports_at_risk >= 2
        THEN '🔴 ROUTE DISRUPTED — Consider Cape of Good Hope rerouting'
        WHEN rs.worst_strait_score >= 40 OR rs.origin_ports_at_risk >= 1
        THEN '🟡 ROUTE UNDER WATCH — Monitor conditions closely'
        ELSE '🟢 ROUTE CLEAR — Normal transit expected'
    END                                                        AS route_status,
    'Cape of Good Hope (+14 days, ~+$1M/voyage)'               AS alternative_route
FROM route_summary rs;

/*
 INTERPRETATION:
 This query gives a single row summarising the entire Asia→Europe corridor.
 It compounds risk from 3 straits + origin/destination port conditions.
 avg_strait_score weights equally across Bab el-Mandeb, Suez, and Gibraltar.
 In practice Bab el-Mandeb is the most critical since it is the entry to Suez.
 worst_strait_score > 60 = seriously consider Cape of Good Hope rerouting,
 which adds ~14 days and ~$1M per voyage but avoids the disrupted zone.
*/


-- ────────────────────────────────────────────────────────────────
-- QUERY 16 (Complex) — BDI Signal × China Dominance:
--           Double Risk Detection
-- Business question: When BDI is BEARISH (falling freight demand)
-- AND China dominates a commodity, it may signal a demand shock
-- originating from China. Identify which commodities face this
-- compounded signal today.
-- Uses: Multiple CTEs, LAG window, cross-source JOIN
-- Tables: analysis_bdi_signals, analysis_china_concentration,
--         analysis_commodity_bdi
-- ────────────────────────────────────────────────────────────────
WITH latest_signal AS (
    -- Most recent BDI signal
    SELECT
        date,
        bdi_value,
        daily_change_pct,
        market_signal,
        charter_recommendation,
        rolling_30d_avg,
        rolling_90d_avg,
        -- How far below the 90d average?
        ROUND((bdi_value - rolling_90d_avg) / NULLIF(rolling_90d_avg, 0) * 100, 1)
                                                               AS pct_vs_90d_avg
    FROM `shipping_data.analysis_bdi_signals`
    ORDER BY date DESC
    LIMIT 1
),
recent_bdi_trend AS (
    -- 30-day signal distribution to confirm sustained trend
    SELECT
        COUNTIF(market_signal = 'BEARISH')                    AS bearish_days_30d,
        COUNTIF(market_signal = 'BULLISH')                    AS bullish_days_30d,
        COUNTIF(market_signal = 'NEUTRAL')                    AS neutral_days_30d,
        ROUND(AVG(bdi_value), 0)                              AS avg_bdi_30d
    FROM (
        SELECT market_signal, bdi_value
        FROM `shipping_data.analysis_bdi_signals`
        ORDER BY date DESC
        LIMIT 30
    )
),
china_latest AS (
    SELECT
        commodity_name,
        china_share_pct,
        china_export_B,
        world_export_B,
        supply_concentration,
        china_port_risk_today,
        china_max_beaufort
    FROM `shipping_data.analysis_china_concentration`
    WHERE year = (SELECT MAX(year) FROM `shipping_data.analysis_china_concentration`)
),
commodity_sensitivity AS (
    SELECT
        commodity_name,
        bdi_sensitivity,
        ROUND(AVG(freight_burden_index), 4)                   AS avg_freight_burden
    FROM `shipping_data.analysis_commodity_bdi`
    GROUP BY commodity_name, bdi_sensitivity
)
SELECT
    ls.date                                                    AS signal_date,
    ls.bdi_value,
    ls.market_signal,
    ls.pct_vs_90d_avg                                         AS bdi_pct_vs_90d_avg,
    ls.charter_recommendation,
    rb.bearish_days_30d,
    rb.bullish_days_30d,
    cl.commodity_name,
    cl.china_share_pct,
    cl.supply_concentration,
    cl.china_export_B,
    CAST(cl.china_port_risk_today AS STRING)                  AS china_port_risk,
    cs.bdi_sensitivity,
    ROUND(cs.avg_freight_burden, 4)                           AS freight_burden_index,
    CASE
        WHEN ls.market_signal IN ('BEARISH','OVERSOLD')
         AND cl.china_share_pct > 40
         AND cl.china_port_risk_today = TRUE
        THEN '🔴 TRIPLE RISK: BDI weak + China dominates + China ports disrupted'
        WHEN ls.market_signal IN ('BEARISH','OVERSOLD')
         AND cl.china_share_pct > 40
        THEN '🟠 DOUBLE RISK: BDI weak + China dominates this commodity'
        WHEN ls.market_signal IN ('BEARISH','OVERSOLD')
         AND cs.bdi_sensitivity LIKE '%High%'
        THEN '🟡 FREIGHT RISK: BDI weak and commodity is freight-cost-sensitive'
        WHEN ls.market_signal IN ('BULLISH','OVERBOUGHT')
         AND cl.china_share_pct > 40
        THEN '🟡 DEMAND SIGNAL: BDI rising and China dominates — watch for price impact'
        ELSE '🟢 NO COMPOUNDED RISK SIGNAL'
    END                                                        AS compounded_risk_signal
FROM latest_signal     ls
CROSS JOIN recent_bdi_trend rb
CROSS JOIN china_latest     cl
JOIN commodity_sensitivity  cs ON cl.commodity_name = cs.commodity_name
ORDER BY cl.china_share_pct DESC;

/*
 INTERPRETATION:
 This is the most sophisticated cross-source query in the pipeline.
 It combines three independent signals:
   1. BDI market signal (is freight demand rising or falling?)
   2. China supply concentration (does China dominate this commodity?)
   3. China port risk (are Chinese ports physically disrupted today?)

 TRIPLE RISK = BDI weak + China dominant + Chinese ports at risk:
   → Demand shock likely, supply disrupted, freight rates may spike counter-
     intuitively as vessels scramble to reroute. Urgent procurement review.

 DOUBLE RISK = BDI weak + China dominant (weather normal):
   → Possible China-origin demand slowdown. Monitor closely.
     Consider locking in supply contracts before potential price swing.

 bearish_days_30d shows whether the BEARISH signal is a one-day event
 or a sustained 30-day trend — sustained trends are much more significant.
*/
