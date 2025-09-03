from core.learning_analysis import LearningAnalyzer
from utils.qwen_integration import call_qwen
from config.settings import QWEN_CONFIG
from typing import Dict

def analyze_learning_progress(student_id: str) -> Dict:
    # 1. 查询数据库
    analyzer = LearningAnalyzer()
    student_data = analyzer.get_student_data(student_id)
    
    # 2. 构建千问提示词
    prompt = f"""
    根据以下学情数据生成分析报告（需包含改进建议）：
    {student_data}
    要求：
    1. 按学科分类指出知识薄弱点
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
    print("学情分析结果2025005：")
    print(analyze_learning_progress("2025005"))  # 测试分析学号2025001
    # print("学情分析结果2025005：")
    # print(analyze_learning_progress("2025002"))  # 测试分析学号2025002