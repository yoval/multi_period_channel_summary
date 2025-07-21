import yaml


def load_config(file_path):
    """加载YAML配置文件

    Args:
        file_path (str): 配置文件路径

    Returns:
        dict: 配置内容字典
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


# 默认配置加载
CONFIG = load_config('config.yaml')