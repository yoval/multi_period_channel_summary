import pandas as pd
import sqlite3
import numpy as np
import sys
from cleaning_module import cleaning_sales_data
sys.path.append('../module')

# ---------------------- 常量定义 ----------------------
METRICS = ['流水', '实收', '优惠', '订单数']  # 核心统计指标
PERIOD_TYPES = ['本期', '环比期', '同期']    # 时段类型顺序
CHANNEL_CATEGORIES = [  # 渠道分类配置
    {'new_prefix': 'pos小程序', 'parts': ['pos', '甜啦啦小程序']},
    {'new_prefix': '美团团购', 'parts': ['美团大众点评团购', '美团大众点评小程序']},
    {'new_prefix': '线上外卖', 'parts': ['美团外卖', '饿了么外卖', '京东外卖']},
    {'new_prefix': '抖音', 'parts': ['抖音团购', '抖音小程序','快手团购']},
]
PRIORITY_ORDER = [  # 指标优先级顺序
    '汇总','pos小程序', 'pos','甜啦啦小程序',
    '美团团购','美团大众点评团购', '美团大众点评小程序',
    '线上外卖', '美团外卖', '饿了么外卖', '京东外卖',
    '抖音','快手团购', '抖音团购', '抖音小程序'
]

# ---------------------- 核心功能函数 ----------------------
def process_sales_data(raw_data: pd.DataFrame, category_config: list, 
                      metrics: list, priority_order: list) -> tuple[pd.DataFrame, pd.DataFrame]:
    #global processed_data
    """
    处理原始销售数据，生成同比分析结果和时段映射表
    
    参数:
        raw_data: 原始销售数据DataFrame
        category_config: 渠道分类配置（包含新前缀和组成部分）
        metrics: 需要统计的核心指标列表
        priority_order: 指标优先级排序
    
    返回:
        (同比分析结果DataFrame, 时段映射关系DataFrame)
    """
    print("开始处理原始数据...")
    processed_data = raw_data.copy()

    # 步骤1：创建组合指标（如pos+小程序合并为pos小程序）
    for category in category_config:
        for metric in metrics:
            source_columns = [f"{part}_{metric}" for part in category['parts']]
            processed_data[f"{category['new_prefix']}_{metric}"] = processed_data[source_columns].sum(axis=1)

    # 步骤2：计算各渠道营业天数（流水>0的天数）
    def calculate_operating_days(data: pd.DataFrame) -> None:
        """计算每个渠道的营业天数"""
        print("计算营业天数...")
        for col in data.columns:
            if '流水' in col:
                channel = col.split('流水')[0]
                operating_days_col = f"{channel}营业天数"
                data[operating_days_col] = (data[col] > 0).astype(int)
    calculate_operating_days(processed_data)

    # 步骤3：自动识别时段类型（本期/环比期/同期）
    def identify_period_mapping(data: pd.DataFrame) -> dict:
       
        """识别查询时段与标准时段类型的映射关系"""
        print("自动识别查询时段...")
        unique_periods = data['查询时段'].unique()
        period_list = []
        for period in unique_periods:
            start, end = period.split('~')
            period_list.append((period, int(start), int(end)))
        
        # 按开始日期倒序排序（最新为本期）
        period_list.sort(key=lambda x: x[1], reverse=True)
        
        if len(period_list) == 3:
            return {
                period_list[0][0]: '本期',
                period_list[1][0]: '环比期',
                period_list[2][0]: '同期'
            }
        return {  # 兼容2个时段的情况
            period_list[0][0]: '本期',
            period_list[1][0]: '同期'
        }

    period_mapping = identify_period_mapping(processed_data)
    print('查询时段识别成功！')
    processed_data['查询时段'] = processed_data['查询时段'].replace(period_mapping)

    # 步骤4：创建透视表并整理列名（此时会丢失日期列，因为日期不是聚合字段）
    pivot_table = processed_data.pivot_table(
        index=["门店编号"],
        columns="查询时段",
        aggfunc="sum"
    )
    pivot_table.columns = pivot_table.columns.reorder_levels([1, 0]).map('_'.join)
    structured_data = pivot_table.reset_index()

    # 步骤5：按优先级排序列
    def sort_columns(data: pd.DataFrame, priority: list, periods: list, metrics: list) -> list:
        """根据优先级规则生成排序后的列名列表"""
        sorted_cols = []
        for p in priority:
            for m in metrics:
                for period in periods:
                    col_pattern = f"{period}_{p}_{m}"
                    sorted_cols.extend([col for col in data.columns if col_pattern in col])
        
        # 过滤无效列并调整顺序
        sorted_cols = [col for col in sorted_cols if "日期" not in col]
        if '门店编号' in data.columns:
            sorted_cols.insert(0, '门店编号')
        
        # 添加未匹配的其他列
        other_cols = [col for col in data.columns 
                     if col not in sorted_cols 
                     and "营业天数" not in col 
                     and "日期" not in col]
        sorted_cols.extend(other_cols)
        return sorted_cols

    sorted_columns = sort_columns(structured_data, priority_order, PERIOD_TYPES, ['营业天数', '流水', '实收', '优惠', '订单数'])
    ordered_data = structured_data[sorted_columns].fillna(0)

    # 步骤6：处理本期数据子集（关键修复：从透视表前的原始处理数据获取，保留日期列）
    current_period_data = processed_data[processed_data['查询时段'] == '本期']  # 这里包含原始日期列
    
    def get_current_period_sorted_columns(data: pd.DataFrame, priority_order: list, metrics: list) -> list:
        """获取当前时段（本期）排序后的列名（处理明细数据，包含日期列）"""
        sorted_cols = []
        # 基础列（确保日期和门店编号在前列）
        basic_cols = []
        if '日期' in data.columns:
            basic_cols.append('日期')
        if '门店编号' in data.columns:
            basic_cols.insert(0, '门店编号')
        
        # 按优先级添加业务指标（排除营业天数，因为当前是明细数据非聚合结果）
        for priority in priority_order:
            for metric in metrics:
                if metric == '营业天数':
                    continue  # 营业天数在透视表中计算，明细数据不需要
                column_pattern = f"{priority}_{metric}"
                matching_columns = [col for col in data.columns if column_pattern in col]
                sorted_cols.extend(matching_columns)
        
        # 组合所有列（基础列+业务指标+其他列）
        other_columns = [col for col in data.columns 
                       if col not in basic_cols + sorted_cols 
                       and col != '查询时段']  # 排除已处理的查询时段列
        return basic_cols + sorted_cols + other_columns

    # 当前时段有效指标（排除营业天数，因为明细数据中营业天数是每行计算的，非聚合结果）
    current_metrics = [m for m in metrics if m != '营业天数']
    current_sorted_cols = get_current_period_sorted_columns(current_period_data, priority_order, current_metrics)
    current_period_ordered = current_period_data[current_sorted_cols].fillna(0)
    current_period_ordered = current_period_ordered.sort_values(
        by=['日期', '门店编号'], 
        ascending=[True, True]
    )

    # 步骤7：生成同比分析结果（基于聚合后的透视表数据）
    yoy_columns = [col for col in ordered_data.columns if "环比期" not in col]
    yoy_analysis = ordered_data[yoy_columns].copy()

    # 计算动销门店和存量状态
    for period in ['本期', '同期']:
        for channel in priority_order:
            yoy_analysis[f'{period}_{channel}_动销门店'] = yoy_analysis[f'{period}_{channel}_流水'].apply(lambda x: 1 if x > 0 else 0)

    yoy_analysis["汇总_存量"] = yoy_analysis.apply(
        lambda row: "是" if row["本期_汇总_流水"] * row["同期_汇总_流水"] != 0 else "否", axis=1
    )

    # 计算同比增长率
    yoy_analysis["同比（%）"] = np.where(
        yoy_analysis["同期_汇总_流水"] != 0,
        (yoy_analysis["本期_汇总_流水"] - yoy_analysis["同期_汇总_流水"]) / yoy_analysis["同期_汇总_流水"],
        np.nan
    )
    yoy_analysis["同比情况"] = yoy_analysis["同比（%）"].apply(
        lambda x: "上升" if x > 0 else ("下降" if x < 0 else "无变化")
    )

    # 过滤优惠相关列（根据业务需求）
    yoy_analysis = yoy_analysis.loc[:, ~yoy_analysis.columns.str.contains('优惠')]

    # 生成时段映射表
    period_mapping_df = pd.DataFrame(
        list(period_mapping.items()), 
        columns=['原始时段', '标准时段']
    )

    print("数据处理完成！")
    return yoy_analysis.reset_index(drop=True), period_mapping_df

def save_to_sqlite_db(data: pd.DataFrame, table_name: str, db_path: str) -> None:
    """
    将数据保存到SQLite数据库
    
    参数:
        data: 待保存的DataFrame
        table_name: 数据库表名
        db_path: 数据库文件路径
    """
    print(f"开始保存数据到数据库表 {table_name}...")
    with sqlite3.connect(db_path) as conn: 
        data.to_sql(table_name, conn, if_exists='replace', index=False)
    print(f"数据已成功保存到数据库表 {table_name}.")

def filter_monthly_valid_stores(daily_data: pd.DataFrame) -> pd.DataFrame:
    """
    筛选出在2024和2025年各月均有有效营业额的门店
    
    参数:
        daily_data: 原始日度销售数据
    
    返回:
        筛选后的日度数据（仅包含符合条件的门店记录）
    """
    print("开始处理月度数据...")
    processed_daily = daily_data.copy()
    
    # 日期格式转换与年月提取
    processed_daily['日期'] = pd.to_datetime(processed_daily['日期'], format='%Y%m%d')
    processed_daily['年份'] = processed_daily['日期'].dt.year
    processed_daily['月份'] = processed_daily['日期'].dt.month

    # 计算门店-年月的月总流水
    monthly_revenue = processed_daily.groupby(['门店编号', '年份', '月份'])['汇总_流水'].sum().reset_index()

    # 筛选2024和2025年均有有效流水的门店-月份组合
    def is_valid_monthly_store(group: pd.DataFrame) -> bool:
        """判断门店-月份组合是否在两年均有有效流水"""
        has_2024 = (group['年份'] == 2024).any()
        has_2025 = (group['年份'] == 2025).any()
        if not (has_2024 and has_2025):
            return False
        revenue_2024 = group.loc[group['年份'] == 2024, '汇总_流水'].iloc[0] > 0
        revenue_2025 = group.loc[group['年份'] == 2025, '汇总_流水'].iloc[0] > 0
        return revenue_2024 and revenue_2025

    valid_monthly_groups = monthly_revenue.groupby(['门店编号', '月份']).filter(is_valid_monthly_store)
    valid_monthly_groups['是否存量'] = '是'

    # 创建辅助映射列并合并结果
    valid_monthly_groups['门店_月份'] = valid_monthly_groups['门店编号'] + '_' + valid_monthly_groups['月份'].astype(str)
    processed_daily['门店_月份'] = processed_daily['门店编号'] + '_' + processed_daily['月份'].astype(str)

    # 左连接保留有效记录
    mapping = valid_monthly_groups[['门店_月份', '是否存量']].drop_duplicates()
    processed_daily = processed_daily.merge(mapping, on='门店_月份', how='left')
    processed_daily['是否存量'] = processed_daily['是否存量'].fillna('否')

    # 筛选并清理数据
    filtered_daily = processed_daily[processed_daily['是否存量'] == '是']
    filtered_daily = filtered_daily.drop(columns=['门店_月份', '是否存量'])
    filtered_daily['日期'] = filtered_daily['日期'].dt.strftime('%Y%m%d')  # 恢复日期格式
    print("月度数据处理完成！")
    return filtered_daily

# ---------------------- 主程序执行 ----------------------
if __name__ == "__main__":
    # 加载配置和原始数据
    from load_config import read_config
    config = read_config('config.yaml')
    db_path = config['db_path']
    sales_data = config['sales_data_path']
    supplemental_data = config['supplemental_data_path']
    raw_sales_data = cleaning_sales_data(sales_data,supplemental_data)
    raw_sales_data_copy = raw_sales_data.copy()  # 保留原始数据副本用于月度处理

    # 执行核心处理逻辑
    print("开始执行数据处理...")
    yoy_analysis_df, period_mapping_df = process_sales_data(
        raw_sales_data, CHANNEL_CATEGORIES, METRICS, PRIORITY_ORDER
    )
    print("完成同比数据分析...")

    monthly_filtered_data = filter_monthly_valid_stores(raw_sales_data_copy)
    print("完成月度数据分析...")

    yoy_cunliang_df, _ = process_sales_data(
        monthly_filtered_data, CHANNEL_CATEGORIES, METRICS, PRIORITY_ORDER
    )
    columns_to_drop = ['本期_年份','本期_月份','同期_年份','同期_月份']
    #yoy_cunliang_df.drop(columns=list(set(columns_to_drop)), inplace=True)
    
    print("完成同比数据(存量)分析...")

    # 保存结果到数据库
    save_to_sqlite_db(yoy_analysis_df, '同比数据', db_path)
    save_to_sqlite_db(yoy_cunliang_df, '同比数据(存量)', db_path)
    save_to_sqlite_db(period_mapping_df, '期数', db_path)   
    print("所有数据已同步至数据库")