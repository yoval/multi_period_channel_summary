# -*- coding: utf-8 -*-
import sqlite3
import pandas as pd
from load_config import load_config as read_config

def calculate_goals(sales_df, goal_df, period_df):

    """
    计算门店的渠道目标完成情况
    
    参数:
        sales_df (pd.DataFrame): 销售数据（包含本期各渠道指标）
        goal_df (pd.DataFrame): 目标数据（包含月度目标值）
        period_df (pd.DataFrame): 期数数据（包含本期时间范围）
        
    返回:
        pd.DataFrame: 包含目标完成情况的计算结果
    """
    # 获取本期起止日期
    benqi = period_df[period_df["标准时段"] == "本期"]["原始时段"].iloc[0]
    start_date, end_date = benqi.split('~')
    
    # 生成月份列表
    months = pd.date_range(
        pd.to_datetime(start_date), 
        pd.to_datetime(end_date), 
        freq='MS'
    ).strftime('%Y%m').tolist()

    # 处理目标数据
    goal_processed = process_goals(goal_df, months)
    
    # 处理销售数据
    sales_processed = process_sales(sales_df)
    
    # 合并销售和目标数据
    merged_df = pd.merge(sales_processed, goal_processed, on='门店编号', how='left')
    merged_df = merged_df.fillna(0)
    
    # 计算各渠道目标完成情况
    result = calculate_target_completion(merged_df)
    
    return result

def process_goals(goal_df, months):
    """
    处理目标数据，计算总目标值
    
    参数:
        goal_df (pd.DataFrame): 原始目标数据
        months (list): 目标月份列表
        
    返回:
        pd.DataFrame: 处理后的目标数据
    """
    # 创建目标列列表
    target_columns = []
    for month in months:
        target_columns.extend([
            f'全渠道{month}',
            f'外卖渠道{month}'
        ])
    
    # 转换数值类型
    for col in target_columns:
        if col in goal_df.columns:
            goal_df[col] = pd.to_numeric(goal_df[col], errors='coerce').fillna(0)
    
    # 初始化目标列
    goal_df['全渠道目标'] = 0.0
    goal_df['外卖目标'] = 0.0
    
    # 累加月度目标值
    for month in months:
        if f'全渠道{month}' in goal_df.columns:
            goal_df['全渠道目标'] += goal_df[f'全渠道{month}']
        if f'外卖渠道{month}' in goal_df.columns:
            goal_df['外卖目标'] += goal_df[f'外卖渠道{month}']
    
    # 保留所需列
    return goal_df[['门店编号', '全渠道池', '全渠道目标', '外卖池', '外卖目标']]

def process_sales(sales_df):
    """
    处理销售数据，筛选本期相关字段
    
    参数:
        sales_df (pd.DataFrame): 原始销售数据
        
    返回:
        pd.DataFrame: 处理后的销售数据
    """
    # 筛选门店编号和本期相关字段
    return sales_df.filter(
        regex='^(门店编号|本期)', 
        axis=1
    )

def calculate_target_completion(df):
    """
    计算各渠道目标完成情况
    
    参数:
        df (pd.DataFrame): 合并后的数据
        
    返回:
        pd.DataFrame: 计算结果
    """
    # 转换池值为数值类型
    df['外卖池'] = pd.to_numeric(df['外卖池'], errors='coerce')
    df['全渠道池'] = pd.to_numeric(df['全渠道池'], errors='coerce')
    
    # 计算各渠道目标完成情况
    for channel in ['线上外卖', 'pos小程序', '美团团购', '抖音']:
        pool_type = '外卖池' if channel == '线上外卖' else '全渠道池'
        
        for metric in ['流水', '实收', '订单数']:
            src_col = f'本期_{channel}_{metric}'
            target_col = f'{pool_type}_{channel}_{metric}'
            df[target_col] = df[src_col] * df[pool_type]
    
    # 选择最终结果列
    result_columns = [
        '门店编号', '全渠道目标',
        '全渠道池_pos小程序_流水', '全渠道池_pos小程序_实收', '全渠道池_pos小程序_订单数',
        '全渠道池_美团团购_流水', '全渠道池_美团团购_实收', '全渠道池_美团团购_订单数',
        '全渠道池_抖音_流水', '全渠道池_抖音_实收', '全渠道池_抖音_订单数',
        '外卖目标',
        '外卖池_线上外卖_流水', '外卖池_线上外卖_实收', '外卖池_线上外卖_订单数'
    ]
    
    return df[result_columns]

def main():
    # 读取配置文件
    config = read_config('config.yaml')
    db_path = config['db_path']
    
    # 连接数据库
    conn = sqlite3.connect(db_path)
    
    # 读取数据表
    period_df = pd.read_sql_query("SELECT * FROM 期数", conn)
    goal_df = pd.read_sql_query("SELECT * FROM goal", conn)
    sales_df = pd.read_sql_query("SELECT * FROM 同比数据", conn)
    
    # 计算目标完成情况
    result_df = calculate_goals(sales_df, goal_df, period_df)
    
    # 打印结果（实际应用中可将结果保存或输出）
    result_df.to_excel('test.xlsx')
    
    # 关闭数据库连接
    conn.close()

if __name__ == "__main__":
    main()