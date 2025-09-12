from core.learning_analysis import LearningAnalyzer
from utils.qwen_integration import call_qwen
from typing import Dict, List
import psycopg2
from typing import List, Dict
from config.settings import PG_CONFIG, QWEN_CONFIG

def get_student_data(student_id: int) -> Dict:
    """从学情分析表获取学生数据，合并所有details到一个字典"""
    conn = psycopg2.connect(**PG_CONFIG)
    print(f'connected to database: {PG_CONFIG["database"]}')
    with conn.cursor() as cur:
        cur.execute("""
            SELECT details
            FROM study_detail
            WHERE student_id = %s
            LIMIT 3
        """, (student_id,))
        print("Executing query to fetch student data", student_id)
        
        # 获取所有行并合并details到一个字典
        result = {}
        for i, row in enumerate(cur.fetchall(), 1):
            result[f"details_{i}"] = row[0]  # 使用details_1, details_2等作为键
        
        print(f'result', result)
        
        return result

def analyze_learning_progress(student_id: int) -> Dict:
    # 1. 查询数据库
    print(f"Analyzing learning progress for student ID: {student_id}")
    student_data = get_student_data(student_id)
    
    # 2. 构建千问提示词
    prompt = f"""
    根据以下学情数据为每个错题知识点生成3到相似练习题：
    {student_data}
    要求：
    1. 列出学科和知识点
    2. 针对错题推荐3道相似练习题
    清晰输出，该换行时换行，增加可读性
    """
    
    # 3. 调用千问模型
    result = call_qwen(prompt, QWEN_CONFIG["api_key"])
    
    return {
        "student_id": student_id,
        "raw_data": student_data,
        "ai_analysis": result
    }

if __name__ == "__main__":
    print("学情分析结果：")
    print('新生成错题',analyze_learning_progress(1))  # 测试分析学号2025001
