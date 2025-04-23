import sys
sys.path.append('../module')
from load_config import read_config
CONFIG = read_config('config.yaml')

import pandas as pd
import sqlite3
import numpy as np
db_path = CONFIG['db_path']
input_file_path = CONFIG['input_file_path']
data_df = pd.read_csv(input_file_path)
original_table = data_df.copy()
# 配置需要聚合的指标分类（分为常规分类和特殊分类）
categories_config = [
    {'new_prefix': 'pos小程序', 'parts': ['pos', '甜啦啦小程序']},
    {'new_prefix': '抖音', 'parts': ['抖音团购', '抖音小程序']},
    {'new_prefix': '美团团购', 'parts': ['美团大众点评团购', '美团大众点评小程序']},
    {'new_prefix': '线上外卖', 'parts': ['美团外卖', '饿了么外卖', '京东外卖']}
]
metrics = ['流水', '实收','优惠', '订单数']
# 统一处理所有分类
for category in categories_config:
    for metric in metrics:
        # 动态生成需要相加的列
        source_cols = [f"{part}_{metric}" for part in category['parts']]
        # 创建新列并求和
        data_df[f"{category['new_prefix']}_{metric}"] = data_df[source_cols].sum(axis=1)

# 新增“营业天数”列
def add_operating_days(data):
    for column in data.columns:
        if '流水' in column:  # 确保只处理包含“流水”的列
            channel_name = column.split('流水')[0]  # 提取渠道名称
            operating_days_column = f"{channel_name}营业天数"  # 替换列名为“渠道_营业天数”
            data[operating_days_column] = (data[column] > 0).astype(int)  # 流水 > 0 则营业天数为 1，否则为 0
add_operating_days(data_df)

# 自动识别“查询时段”列并定义“本期”、“环比期”、“同期”
def auto_identify_periods(data):
    query_periods = data['查询时段'].unique()
    period_dates = []

    for period in query_periods:
        start_date, end_date = period.split('~')
        period_dates.append((period, int(start_date), int(end_date)))

    # 按开始日期排序
    period_dates.sort(key=lambda x: x[1], reverse=True)

    # 分配“本期”、“环比期”、“同期”
    period_mapping = {
        period_dates[0][0]: '本期',  # 开始日期最大的为“本期”
        period_dates[1][0]: '环比期',  # 其次为“环比期”
        period_dates[2][0]: '同期'  # 再次为“同期”
    }

    return period_mapping

query_period_mapping = auto_identify_periods(data_df)

# 替换“查询时段”列为新的定义
data_df['查询时段'] = data_df['查询时段'].replace(query_period_mapping)

# 数据透视表：按门店编号和查询时段进行汇总
pivot_table = data_df.pivot_table(
    index=["门店编号"],  # 固定列：门店编号
    columns="查询时段",  # 按照“查询时段”展开
    aggfunc="sum"       # 聚合函数：直接求和
)
pivot_table.columns = pivot_table.columns.reorder_levels([1, 0]).map('_'.join)
processed_df = pivot_table.copy()

# 获取所有渠道和指标组合的排序顺序
def get_sorted_columns(df, priority_order, period_order, metric_order):
    sorted_columns = []

    # 遍历每个渠道（第一级排序）
    for priority in priority_order:
        # 遍历每个指标（第二级排序）
        for metric in metric_order:
            # 遍历每个时段（第三级排序）
            for period in period_order:
                column_pattern = f"{period}_{priority}_{metric}"
                matching_columns = [col for col in df.columns if column_pattern in col]
                sorted_columns.extend(matching_columns)

    # 去除包含日期的列
    sorted_columns = [s for s in sorted_columns if "日期" not in s]
    # 确保“门店编号”在最前面
    if '门店编号' in df.columns:
        sorted_columns.insert(0, '门店编号')
    # 如果有其他未匹配到的列（例如日期），可以在最后添加
    other_columns = [col for col in df.columns if col not in sorted_columns and "营业天数" not in col and "日期" not in col]
    sorted_columns.extend(other_columns)
    return sorted_columns

# 定义排序优先级
priority_order = ['汇总', 'pos', '美团外卖', '饿了么外卖', '快手团购', '抖音团购','美团大众点评团购', '美团大众点评小程序', '抖音小程序','京东外卖', '甜啦啦小程序','pos小程序','线上外卖','抖音','美团团购']
period_order = ['本期', '环比期', '同期']
metric_order = ['营业天数', '流水', '实收', '优惠', '订单数'] 

sorted_columns = get_sorted_columns(processed_df, priority_order, period_order, metric_order)
sorted_df = processed_df[sorted_columns].fillna(0)

# 本期数据筛选
current_period_df = data_df[data_df['查询时段'] == '本期']

# 对本期数据进行排序
def get_current_period_sorted_columns(df, priority_order, metric_order):
    sorted_columns = []

    # 遍历每个渠道（第一级排序）
    for priority in priority_order:
        # 遍历每个指标（第二级排序）
        for metric in metric_order:
            if metric == '营业天数':  # 本期数据不需要“营业天数”相关列
                continue
            column_pattern = f"{priority}_{metric}"
            matching_columns = [col for col in df.columns if column_pattern in col]
            sorted_columns.extend(matching_columns)

    # 确保“门店编号”和“日期”在最前面
    if '日期' in df.columns:
        sorted_columns.insert(0, '日期')
    if '门店编号' in df.columns:
        sorted_columns.insert(0, '门店编号')
    # 如果有其他未匹配到的列（例如日期），可以在最后添加
    other_columns = [col for col in df.columns if col not in sorted_columns and "营业天数" not in col and "日期" not in col]
    sorted_columns.extend(other_columns)
    return sorted_columns

current_period_sorted_columns = get_current_period_sorted_columns(current_period_df, priority_order, metric_order)
# 移除“查询时段”列
current_period_sorted_columns = [col for col in current_period_sorted_columns if col != '查询时段']
# 过滤并排序本期数据
current_period_sorted_df = current_period_df[current_period_sorted_columns].fillna(0)
current_period_sorted_df = current_period_sorted_df.sort_values(by=['日期', '门店编号'], ascending=[True, True])
# 对比数据移除日期相关列
sorted_df = sorted_df[[col for col in sorted_df.columns if "日期" not in col]]
query_period_df = pd.DataFrame(list(query_period_mapping.items()), columns=['时段', '期数'])
# 2025.4.21需求
# 筛选列名不包含“环比期”的列
tongbi_columns = [col for col in sorted_df.columns if "环比期" not in col]
tongbi_df = sorted_df[tongbi_columns].copy()

# 新增“动销门店列”
period_order = ['本期', '同期']
for period in period_order:
    for priority in priority_order:
        tongbi_df.loc[:,f'{period}_{priority}_动销门店'] =  tongbi_df[f'{period}_{priority}_流水'].apply(lambda x:1 if x > 0 else 0)

# 新增“收银同比存量”列
tongbi_df["汇总_存量"] = tongbi_df.apply(lambda row: "否" if row["本期_汇总_流水"] * row["同期_汇总_流水"] == 0 else "是", axis=1)
# 计算“同比（%）”列，避免除以 0 的情况
tongbi_df["同比（%）"] = np.where(
    tongbi_df["同期_汇总_流水"] != 0,
    (tongbi_df["本期_汇总_流水"] - tongbi_df["同期_汇总_流水"]) / tongbi_df["同期_汇总_流水"],
    np.nan  # 如果同期流水为 0，返回 NaN
)
tongbi_df.loc[:,"同比情况"] = tongbi_df["同比（%）"].apply( lambda x: "上升" if x > 0 else ("下降" if x < 0 else ""))


# 删除列名包含“优惠”的列
tongbi_df = tongbi_df.loc[:, ~tongbi_df.columns.str.contains('优惠')]
tongbi_df = tongbi_df.reset_index()
# 连接到SQLite数据库
conn = sqlite3.connect(db_path)
try:
    query_period_df.to_sql('期数', conn, if_exists='replace', index=False) 
    tongbi_df.to_sql('同比数据', conn, if_exists='replace', index=False)

finally:
    conn.close()
print("处理完成！结果已同步至数据库")