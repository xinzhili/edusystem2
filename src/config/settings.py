# PostgreSQL 配置
PG_CONFIG = {
    "host": "localhost",
    "port": 5433,  # 根据您的docker映射端口
    "user": "postgres",
    "password": "123456",  # 替换为实际密码
    "database": "learning_db"
    # "database": "postgres"
}

# 千问API配置
QWEN_CONFIG = {
    "api_key": "you api key ",  # 替换为实际API密钥
    "model": "qwen-max-latest"
}
