# EDUSYSTEM/setup.py
from setuptools import setup, find_packages

setup(
    name="edusystem",
    version="0.1",
    package_dir={"": "src"},  # 关键：声明源码在src目录下
    packages=find_packages(where="src"),  # 从src目录查找包
    install_requires=[
        "psycopg2-binary>=2.9",
        "dashscope>=1.23.9",  # 使用实际存在的版本
        "pytest>=7.0"
    ],
)