WITH period_table AS (
    SELECT '20240101' AS start_date, '20240430' AS end_date UNION ALL
    SELECT '20240901' AS start_date, '20240901' AS end_date UNION ALL
    SELECT '20250101' AS start_date, '20250430' AS end_date
)
SELECT
    CONCAT(pt.start_date, '~', pt.end_date) AS 查询时段,
    day_table.shop_id AS 门店编号,
    DATE_FORMAT(day_table.pay_way_created_time, '%Y%m%d') AS 日期,
    -- 按渠道分组统计流水（使用 SUM 聚合）
    SUM(CASE WHEN day_table.payment_channels = 'pos' THEN day_table.amount ELSE 0 END) AS pos_流水,
    SUM(CASE WHEN day_table.payment_channels = '甜啦啦小程序' THEN day_table.amount ELSE 0 END) AS 甜啦啦小程序_流水,
    SUM(CASE WHEN day_table.payment_channels = '美团外卖' THEN day_table.amount ELSE 0 END) AS 美团外卖_流水,
    SUM(CASE WHEN day_table.payment_channels = '饿了么外卖' THEN day_table.amount ELSE 0 END) AS 饿了么外卖_流水,
    SUM(CASE WHEN day_table.payment_channels = '快手团购' THEN day_table.amount ELSE 0 END) AS 快手团购_流水,
    SUM(CASE WHEN day_table.payment_channels = '抖音团购' THEN day_table.amount ELSE 0 END) AS 抖音团购_流水,
    SUM(CASE WHEN day_table.payment_channels = '美团/大众点评团购' THEN day_table.amount ELSE 0 END) AS 美团大众点评团购_流水,
    SUM(CASE WHEN day_table.payment_channels = '美团/大众点评小程序' THEN day_table.amount ELSE 0 END) AS 美团大众点评小程序_流水,
    SUM(CASE WHEN day_table.payment_channels = '抖音小程序' THEN day_table.amount ELSE 0 END) AS 抖音小程序_流水,
    SUM(CASE WHEN day_table.payment_channels = '京东外卖' THEN day_table.amount ELSE 0 END) AS 京东外卖_流水,
    -- 按渠道分组统计实收
    SUM(CASE WHEN day_table.payment_channels = 'pos' THEN day_table.income ELSE 0 END) AS pos_实收,
    SUM(CASE WHEN day_table.payment_channels = '甜啦啦小程序' THEN day_table.income ELSE 0 END) AS 甜啦啦小程序_实收,
    SUM(CASE WHEN day_table.payment_channels = '美团外卖' THEN day_table.income ELSE 0 END) AS 美团外卖_实收,
    SUM(CASE WHEN day_table.payment_channels = '饿了么外卖' THEN day_table.income ELSE 0 END) AS 饿了么外卖_实收,
    SUM(CASE WHEN day_table.payment_channels = '快手团购' THEN day_table.income ELSE 0 END) AS 快手团购_实收,
    SUM(CASE WHEN day_table.payment_channels = '抖音团购' THEN day_table.income ELSE 0 END) AS 抖音团购_实收,
    SUM(CASE WHEN day_table.payment_channels = '美团/大众点评团购' THEN day_table.income ELSE 0 END) AS 美团大众点评团购_实收,
    SUM(CASE WHEN day_table.payment_channels = '美团/大众点评小程序' THEN day_table.income ELSE 0 END) AS 美团大众点评小程序_实收,
    SUM(CASE WHEN day_table.payment_channels = '抖音小程序' THEN day_table.income ELSE 0 END) AS 抖音小程序_实收,
    SUM(CASE WHEN day_table.payment_channels = '京东外卖' THEN day_table.income ELSE 0 END) AS 京东外卖_实收,
    -- 按渠道分组统计优惠
    SUM(CASE WHEN day_table.payment_channels = 'pos' THEN day_table.discount ELSE 0 END) AS pos_优惠,
    SUM(CASE WHEN day_table.payment_channels = '甜啦啦小程序' THEN day_table.discount ELSE 0 END) AS 甜啦啦小程序_优惠,
    SUM(CASE WHEN day_table.payment_channels = '美团外卖' THEN day_table.discount ELSE 0 END) AS 美团外卖_优惠,
    SUM(CASE WHEN day_table.payment_channels = '饿了么外卖' THEN day_table.discount ELSE 0 END) AS 饿了么外卖_优惠,
    SUM(CASE WHEN day_table.payment_channels = '快手团购' THEN day_table.discount ELSE 0 END) AS 快手团购_优惠,
    SUM(CASE WHEN day_table.payment_channels = '抖音团购' THEN day_table.discount ELSE 0 END) AS 抖音团购_优惠,
    SUM(CASE WHEN day_table.payment_channels = '美团/大众点评团购' THEN day_table.discount ELSE 0 END) AS 美团大众点评团购_优惠,
    SUM(CASE WHEN day_table.payment_channels = '美团/大众点评小程序' THEN day_table.discount ELSE 0 END) AS 美团大众点评小程序_优惠,
    SUM(CASE WHEN day_table.payment_channels = '抖音小程序' THEN day_table.discount ELSE 0 END) AS 抖音小程序_优惠,
    SUM(CASE WHEN day_table.payment_channels = '京东外卖' THEN day_table.discount ELSE 0 END) AS 京东外卖_优惠,
		-- 按渠道分组统计订单数
    SUM(CASE WHEN day_table.payment_channels = 'pos' THEN day_table.order_cnt ELSE 0 END) AS pos_订单数,
    SUM(CASE WHEN day_table.payment_channels = '甜啦啦小程序' THEN day_table.order_cnt ELSE 0 END) AS 甜啦啦小程序_订单数,
    SUM(CASE WHEN day_table.payment_channels = '美团外卖' THEN day_table.order_cnt ELSE 0 END) AS 美团外卖_订单数,
    SUM(CASE WHEN day_table.payment_channels = '饿了么外卖' THEN day_table.order_cnt ELSE 0 END) AS 饿了么外卖_订单数,
    SUM(CASE WHEN day_table.payment_channels = '快手团购' THEN day_table.order_cnt ELSE 0 END) AS 快手团购_订单数,
    SUM(CASE WHEN day_table.payment_channels = '抖音团购' THEN day_table.order_cnt ELSE 0 END) AS 抖音团购_订单数,
    SUM(CASE WHEN day_table.payment_channels = '美团/大众点评团购' THEN day_table.order_cnt ELSE 0 END) AS 美团大众点评团购_订单数,
    SUM(CASE WHEN day_table.payment_channels = '美团/大众点评小程序' THEN day_table.order_cnt ELSE 0 END) AS 美团大众点评小程序_订单数,
    SUM(CASE WHEN day_table.payment_channels = '抖音小程序' THEN day_table.order_cnt ELSE 0 END) AS 抖音小程序_订单数,
    SUM(CASE WHEN day_table.payment_channels = '京东外卖' THEN day_table.order_cnt ELSE 0 END) AS 京东外卖_订单数,
		
		
    -- 汇总数据
    SUM(day_table.amount) AS 汇总_流水,
    SUM(day_table.income) AS 汇总_实收,
    SUM(day_table.discount) AS 汇总_优惠,
    MAX(day_table.total_order_cnt) AS 汇总_订单数, -- 直接使用总订单数字段
    -- 营业天数计算（统计有交易的日期数）
    COUNT(DISTINCT DATE_FORMAT(day_table.pay_way_created_time, '%Y%m%d')) AS 汇总_营业天数
FROM
    ads_dbs_trade_shop_pay_channel_di AS day_table
CROSS JOIN
    period_table AS pt
WHERE 
    DATE_FORMAT(day_table.pay_way_created_time, '%Y%m%d') BETWEEN pt.start_date AND pt.end_date
		AND (day_table.shop_id LIKE 'TLL%' OR day_table.shop_id LIKE 'ZYD%')
GROUP BY
    CONCAT(pt.start_date, '~', pt.end_date),  -- 按时间段分组
    day_table.shop_id,                        -- 按门店分组
    DATE_FORMAT(day_table.pay_way_created_time, '%Y%m%d');  -- 按日期分组