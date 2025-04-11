import pandas as pd
from openpyxl import Workbook
import os

# 读取用户输入的文件路径，并自动去除多余的引号
input_file_path = input("请输入输入文件的完整路径（例如 C:\\Users\\Administrator\\Desktop\\多时段详细渠道查询.csv）：").strip()
input_file_path = input_file_path.strip("'\"")  # 去除路径中的多余引号

# 确保路径是有效的
if not os.path.isfile(input_file_path):
    raise FileNotFoundError(f"文件未找到，请检查路径是否正确：{input_file_path}")

output_folder = os.path.dirname(input_file_path)  # 输出文件夹与输入文件夹一致
output_file_path = os.path.join(output_folder, "result.xlsx")

# 读取数据
data_df = pd.read_csv(input_file_path)

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

# 定义排序优先级
priority_order = [
    '汇总', 'pos', '甜啦啦小程序', '美团外卖', '饿了么外卖', '快手团购', '抖音团购',
    '美团/大众点评团购', '美团/大众点评小程序', '抖音小程序'
]

period_order = ['本期', '环比期', '同期']
metric_order = ['营业天数', '流水', '实收', '优惠', '订单数']  # 修改后的指标优先级

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

# 将结果写入 Excel 文件
query_period_df = pd.DataFrame(list(query_period_mapping.items()), columns=['时段', '期数'])

with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
    query_period_df.to_excel(writer, sheet_name='期数', index=False)
    current_period_sorted_df.to_excel(writer, sheet_name='本期数据', index=False)
    sorted_df.to_excel(writer, sheet_name='对比数据', index=True)

print(f"处理完成！结果已保存至 {output_file_path}")