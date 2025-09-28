WITH period_table AS (
    SELECT '20240901' AS start_date, '20240927' AS end_date UNION ALL
    SELECT '20250801' AS start_date, '20250827' AS end_date
)
SELECT
    CONCAT(p.start_date, '~', p.end_date) AS 查询时段,
    d.store_code AS 门店编号,
    d.payment_time AS 日期,
    SUM(CASE WHEN d.payment_channel = '线上新增快手团购' THEN d.transaction_amount ELSE 0 END) AS 线上新增快手团购_流水,
    SUM(CASE WHEN d.payment_channel = '线上新增快手团购' THEN d.actual_amount ELSE 0 END) AS 线上新增快手团购_实收,
    SUM(CASE WHEN d.payment_channel = '线上新增快手团购' THEN d.discount_amount ELSE 0 END) AS 线上新增快手团购_优惠,
    SUM(CASE WHEN d.payment_channel = '线上新增快手团购' THEN d.order_count ELSE 0 END) AS 线上新增快手团购_订单数,
    SUM(CASE WHEN d.payment_channel = '线上新增美团团购' THEN d.transaction_amount ELSE 0 END) AS 线上新增美团团购_流水,
    SUM(CASE WHEN d.payment_channel = '线上新增美团团购' THEN d.actual_amount ELSE 0 END) AS 线上新增美团团购_实收,
    SUM(CASE WHEN d.payment_channel = '线上新增美团团购' THEN d.discount_amount ELSE 0 END) AS 线上新增美团团购_优惠,
    SUM(CASE WHEN d.payment_channel = '线上新增美团团购' THEN d.order_count ELSE 0 END) AS 线上新增美团团购_订单数,
    SUM(CASE WHEN d.payment_channel = '线上新增抖音团购' THEN d.transaction_amount ELSE 0 END) AS 线上新增抖音团购_流水,
    SUM(CASE WHEN d.payment_channel = '线上新增抖音团购' THEN d.actual_amount ELSE 0 END) AS 线上新增抖音团购_实收,
    SUM(CASE WHEN d.payment_channel = '线上新增抖音团购' THEN d.discount_amount ELSE 0 END) AS 线上新增抖音团购_优惠,
    SUM(CASE WHEN d.payment_channel = '线上新增抖音团购' THEN d.order_count ELSE 0 END) AS 线上新增抖音团购_订单数,
    SUM(d.transaction_amount) AS 新增汇总_流水,
    SUM(d.actual_amount) AS 新增汇总_实收,
    SUM(d.discount_amount) AS 新增汇总_优惠,
    SUM(d.order_count) AS 新增汇总_订单数
FROM imp_online_new_channel_supplement AS d
JOIN period_table AS p
  ON d.payment_time BETWEEN p.start_date AND p.end_date
GROUP BY
    p.start_date,
    p.end_date,
    d.payment_time,
    d.store_code
ORDER BY
    查询时段,
    门店编号,
    日期;