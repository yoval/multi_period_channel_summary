import pandas as pd

def cleaning_sales_data(input_file_path: str, supplemental_data_path: str) -> pd.DataFrame:
    """
    处理销售数据：加载、合并、聚合字段，并清理冗余列。
    
    参数:
        input_file_path (str): 主销售数据 CSV 文件路径
        supplemental_data_path (str): 补充数据 CSV 文件路径
    
    返回:
        pd.DataFrame: 处理后的销售数据
    """

    # 1. 加载数据
    sales_df = pd.read_csv(input_file_path)
    supplement_df = pd.read_csv(supplemental_data_path)

    # 2. 合并主数据和补充数据
    merge_keys = ['查询时段', '门店编号', '日期']
    merged_df = pd.merge(
        left=sales_df,
        right=supplement_df,
        how='outer',
        on=merge_keys,
        suffixes=("", "_tg")
    )

    # 3. 定义聚合关系（每个主渠道 ← 子渠道）
    aggregation_mapping = {
        '甜啦啦小程序': ['甜啦啦小程序-储值业务'],
        '美团大众点评团购': ['线上新增美团团购'],
        '抖音团购': ['线上新增抖音团购'],
        '快手团购': ['线上新增快手团购'],
        '汇总': ['新增汇总']
    }

    # 4. 循环聚合字段（确保 float 类型 & 填充 NaN）
    metrics = ['流水', '实收', '优惠', '订单数']

    for main_channel, sub_channels in aggregation_mapping.items():
        for sub_channel in sub_channels:
            for metric in metrics:
                main_col = f"{main_channel}_{metric}"
                sub_col = f"{sub_channel}_{metric}"

                # 确保子渠道列存在
                if sub_col not in merged_df.columns:
                    continue

                # 填充 NaN 并转换为 float
                merged_df[sub_col] = merged_df[sub_col].fillna(0).astype(float)

                # 如果主渠道列不存在，则创建
                if main_col not in merged_df.columns:
                    merged_df[main_col] = 0.0

                merged_df[main_col] = merged_df[main_col].fillna(0).astype(float)

                # 聚合加法
                merged_df[main_col] += merged_df[sub_col]

    # 5. 删除所有子渠道字段
    columns_to_drop = []
    for main_channel, sub_channels in aggregation_mapping.items():
        for sub_channel in sub_channels:
            for metric in metrics:
                columns_to_drop.append(f"{sub_channel}_{metric}")

    # 添加所有以 _tg 结尾的列
    columns_to_drop += [col for col in merged_df.columns if col.endswith('_tg')]

    # 去重后删除
    merged_df.drop(columns=list(set(columns_to_drop)), inplace=True, errors='ignore')

    return merged_df