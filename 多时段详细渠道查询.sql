WITH period_table AS (
    SELECT '20240301' AS start_date, '20240331' AS end_date UNION ALL
    SELECT '20250301' AS start_date, '20250331' AS end_date UNION ALL
    SELECT '20250201' AS start_date, '20250228' AS end_date
)
SELECT
    CONCAT(pt.start_date, '~', pt.end_date) AS 查询时段, 
    shop_id AS 门店编号,
    DATE_FORMAT(details.pay_way_created_time, '%Y%m%d') AS 日期,
    -- 按渠道分组统计流水
    SUM(CASE WHEN payment_channels = 'pos' THEN amount ELSE 0 END) AS pos_流水,
    SUM(CASE WHEN payment_channels = '甜啦啦小程序' THEN amount ELSE 0 END) AS 甜啦啦小程序_流水,
    SUM(CASE WHEN payment_channels = '美团外卖' THEN amount ELSE 0 END) AS 美团外卖_流水,
    SUM(CASE WHEN payment_channels = '饿了么外卖' THEN amount ELSE 0 END) AS 饿了么外卖_流水,
    SUM(CASE WHEN payment_channels = '快手团购' THEN amount ELSE 0 END) AS 快手团购_流水,
    SUM(CASE WHEN payment_channels = '抖音团购' THEN amount ELSE 0 END) AS 抖音团购_流水,
    SUM(CASE WHEN payment_channels = '美团/大众点评团购' THEN amount ELSE 0 END) AS 美团大众点评团购_流水,
    SUM(CASE WHEN payment_channels = '美团/大众点评小程序' THEN amount ELSE 0 END) AS 美团大众点评小程序_流水,
    SUM(CASE WHEN payment_channels = '抖音小程序' THEN amount ELSE 0 END) AS 抖音小程序_流水,
    -- 按渠道分组统计实收
    SUM(CASE WHEN payment_channels = 'pos' THEN income ELSE 0 END) AS pos_实收,
    SUM(CASE WHEN payment_channels = '甜啦啦小程序' THEN income ELSE 0 END) AS 甜啦啦小程序_实收,
    SUM(CASE WHEN payment_channels = '美团外卖' THEN income ELSE 0 END) AS 美团外卖_实收,
    SUM(CASE WHEN payment_channels = '饿了么外卖' THEN income ELSE 0 END) AS 饿了么外卖_实收,
    SUM(CASE WHEN payment_channels = '快手团购' THEN income ELSE 0 END) AS 快手团购_实收,
    SUM(CASE WHEN payment_channels = '抖音团购' THEN income ELSE 0 END) AS 抖音团购_实收,
    SUM(CASE WHEN payment_channels = '美团/大众点评团购' THEN income ELSE 0 END) AS 美团大众点评团购_实收,
    SUM(CASE WHEN payment_channels = '美团/大众点评小程序' THEN income ELSE 0 END) AS 美团大众点评小程序_实收,
    SUM(CASE WHEN payment_channels = '抖音小程序' THEN income ELSE 0 END) AS 抖音小程序_实收,
    -- 按渠道分组统计优惠
    SUM(CASE WHEN payment_channels = 'pos' THEN discount ELSE 0 END) AS pos_优惠,
    SUM(CASE WHEN payment_channels = '甜啦啦小程序' THEN discount ELSE 0 END) AS 甜啦啦小程序_优惠,
    SUM(CASE WHEN payment_channels = '美团外卖' THEN discount ELSE 0 END) AS 美团外卖_优惠,
    SUM(CASE WHEN payment_channels = '饿了么外卖' THEN discount ELSE 0 END) AS 饿了么外卖_优惠,
    SUM(CASE WHEN payment_channels = '快手团购' THEN discount ELSE 0 END) AS 快手团购_优惠,
    SUM(CASE WHEN payment_channels = '抖音团购' THEN discount ELSE 0 END) AS 抖音团购_优惠,
    SUM(CASE WHEN payment_channels = '美团/大众点评团购' THEN discount ELSE 0 END) AS 美团大众点评团购_优惠,
    SUM(CASE WHEN payment_channels = '美团/大众点评小程序' THEN discount ELSE 0 END) AS 美团大众点评小程序_优惠,
    SUM(CASE WHEN payment_channels = '抖音小程序' THEN discount ELSE 0 END) AS 抖音小程序_优惠,
    -- 按渠道分组统计订单数（去重）
    COUNT(DISTINCT CASE WHEN payment_channels = 'pos' THEN details.order_id ELSE NULL END) AS pos_订单数,
    COUNT(DISTINCT CASE WHEN payment_channels = '甜啦啦小程序' THEN details.order_id ELSE NULL END) AS 甜啦啦小程序_订单数,
    COUNT(DISTINCT CASE WHEN payment_channels = '美团外卖' THEN details.order_id ELSE NULL END) AS 美团外卖_订单数,
    COUNT(DISTINCT CASE WHEN payment_channels = '饿了么外卖' THEN details.order_id ELSE NULL END) AS 饿了么外卖_订单数,
    COUNT(DISTINCT CASE WHEN payment_channels = '快手团购' THEN details.order_id ELSE NULL END) AS 快手团购_订单数,
    COUNT(DISTINCT CASE WHEN payment_channels = '抖音团购' THEN details.order_id ELSE NULL END) AS 抖音团购_订单数,
    COUNT(DISTINCT CASE WHEN payment_channels = '美团/大众点评团购' THEN details.order_id ELSE NULL END) AS 美团大众点评团购_订单数,
    COUNT(DISTINCT CASE WHEN payment_channels = '美团/大众点评小程序' THEN details.order_id ELSE NULL END) AS 美团大众点评小程序_订单数,
    COUNT(DISTINCT CASE WHEN payment_channels = '抖音小程序' THEN details.order_id ELSE NULL END) AS 抖音小程序_订单数,
    -- 汇总计算
    SUM(amount) AS 汇总_流水,
    SUM(income) AS 汇总_实收,
    SUM(discount) AS 汇总_优惠,
    COUNT(DISTINCT details.order_id) AS 汇总_订单数,
    -- 汇总营业天数：如果当日流水不等于 0，则计为 1 天
    SUM(CASE WHEN SUM(amount) > 0 THEN 1 ELSE 0 END) OVER (PARTITION BY shop_id, CONCAT(pt.start_date, '~', pt.end_date)) AS 汇总_营业天数
FROM
    dws_trd_mtpos_order_pay_channel_details_di AS details
CROSS JOIN
    period_table AS pt
WHERE 
    DATE_FORMAT(details.pay_way_created_time, '%Y%m%d') BETWEEN pt.start_date AND pt.end_date
GROUP BY
    CONCAT(pt.start_date, '~', pt.end_date),
    shop_id,
    DATE_FORMAT(details.pay_way_created_time, '%Y%m%d');