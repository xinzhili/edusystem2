import os
import json
import psycopg2
from datetime import datetime
from typing import List, Dict, Optional
from qwen_agent.agents import Assistant
# from qwen_agent import Client
from config.settings import QWEN_CONFIG
from dashscope import Generation
from datetime import datetime
import random
import json
from utils.qwen_integration import call_qwen


# 然后正常初始化


# 1. 数据库配置（根据图片1和4的表结构）
DB_CONFIG = {
            "dbname":"learning_db",  
            "host":"localhost",
            "port":5433,
            "user":"postgres",
            "password":"123456"
            }


# 2. 千问工具定义（官方推荐格式）
tools = [
        {
        "type": "function",
        "function": {
            "name": "get_student_errors",
            "description": "获取学生错题记录，必须包含：学生姓名、学科、开始日期和结束日期",
            "parameters": {
                "type": "object",
                "properties": {
                    "student_name": {"type": "string"},
                    "subject": {"type": "string", "enum": ["数学", "语文", "英语"]},
                    "start_date": {"type": "string", "format": "date"},  # 改为独立参数
                    "end_date": {"type": "string", "format": "date"}     # 改为独立参数
                },
                "required": ["student_name", "subject", "start_date", "end_date"]
            }
        }
        }]

# 3. 数据库访问函数
def get_student_errors(
    student_name: str,
    subject: str,
    start_date: str,  # YYYY-MM-DD
    end_date: str     # YYYY-MM-DD
):
    """获取学生错题记录（关联students和study_detail表）"""
    conn = psycopg2.connect(**DB_CONFIG)
    query = """
        SELECT 
            s.student_id, s.grade,
            sd.details->>'subject' AS subject,
            sd.details->>'question' AS question,
            sd.details->>'error_type' AS error_type,
            sd.details->>'knowledge_points' AS knowledge_points,
            sd.details->>'difficulty' AS difficulty,
            sd.created_at
        FROM study_detail sd
        JOIN students s ON sd.student_id = s.student_id
        WHERE s.name = %s
        AND sd.details->>'subject' = %s
        AND sd.created_at BETWEEN %s AND %s
    """
    with conn.cursor() as cursor:
        cursor.execute(query, (
            student_name,
            subject,  # 新增参数
            start_date,
            end_date + " 23:59:59"
        ))
        return [dict(zip(
            [desc[0] for desc in cursor.description], 
            row
        )) for row in cursor.fetchall()]

def save_summary(student_id: int,grade: int, analysis_result: Dict, start_date: str,end_date: str, subject: str):
    """将分析结果存入summary表（图片4结构）"""
    conn = psycopg2.connect(**DB_CONFIG)
    print(f"准备存储分析结果: {analysis_result}")
    print(f"学生ID: {student_id}, 年级: {grade}, 学科: {subject}, 日期范围: {start_date} 至 {end_date}")
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO summary (
                    student_id, grade, from_date, to_date, 
                    subject, details
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                student_id,
                grade,
                start_date,
                end_date,
                subject or "全科",
                json.dumps({
                    "strength": analysis_result.get("strength", []),
                    "weakness": analysis_result.get("weakness", []),
                    "progress": analysis_result.get("progress", ""),
                    "remarks": analysis_result.get("remarks", "")
                })
            ))
            print("✅ 存储学情分析结果成功")
            print(f'插入了 {cursor.rowcount} 行数据')
        conn.commit()
    except Exception as e:
        print(f"存储失败: {str(e)}")
        conn.rollback()

# 4. 千问交互核心函数
def analyze_with_qwen(messages):
    """端到端分析流程"""
    """按照官方标准重写的分析函数"""
    print(f"查询内容: {messages[0]['content']}")
    print("DASHSCOPE_API_KEY:", os.getenv("DASHSCOPE_API_KEY"))
    print("messages:", messages)
    response = Generation.call(
        # 若没有配置环境变量，请用百炼API Key将下行替换为：api_key="sk-xxx",
        api_key=os.getenv("DASHSCOPE_API_KEY"),
        model="qwen-max-latest",  # 模型列表：https://help.aliyun.com/zh/model-studio/getting-started/models
        messages=messages,
        tools=tools,
        seed=random.randint(
            1, 10000
        ),  # 设置随机数种子seed，如果没有设置，则随机数种子默认为1234
        result_format="message",  # 将输出设置为message形式
    )
    print(f"模型响应: {response}")
    # return response
    

    print(f'返回类型 type(response)): {type(response)}')
    print(f"模型响应: {response}")  
    
    if hasattr(response.output.choices[0].message, 'tool_calls'):
        print("触发了工具调用")
        print(f"工具调用内容: {response.output.choices[0].message.tool_calls}")
        tool_call = response.output.choices[0].message.tool_calls[0]
        # tool_args = json.loads(tool_call.function.arguments)
        tool_args = json.loads(tool_call['function']['arguments'])
        print(f"工具调用参数: {tool_args}")
          
        records = get_student_errors(
            student_name=tool_args["student_name"],
            subject=tool_args["subject"],
            start_date=tool_args["start_date"],
            end_date=tool_args["end_date"]
        )
        print(f"获取到 {len(records)} 条错题记录")
        print(f"records: {records}")
        # return records
    else:
        return {"error": "未触发工具调用"}
        
        # 构建分析提示词
    prompt = f"""请分析以下学习数据：
            学生：{tool_args['student_name']}（{records[0]['grade']}年级）
            时间：{tool_args['start_date']} 至 {tool_args['end_date']}
            学科：{tool_args.get('subject', '全科')}
            
            错题特征：
            - 错误类型：{records[0]['error_type']}
            - 知识点：{records[0]['knowledge_points']}
            - 题目难度：{records[0]['difficulty']}
            
            请返回JSON格式的分析结果，包含以下字段:  
            1. strength（强项知识点）
            2. weakness（薄弱环节）
            3. progress（进步情况）
            4. remarks（学习建议）
            返回前要求：
    -请只返回JSON格式的结果,不要包含任何其他文本
    -返回前请严格检查json格式,一个字段和值都不能少,避免重复和遗漏.
    -不要返回```json```,直接返回JSON value
        """
    print(f"提示词: {prompt}")
    print(f'api_key: {QWEN_CONFIG["api_key"]}')
        # 获取最终分析结果
    analysis = call_qwen(prompt, QWEN_CONFIG["api_key"])
        
    print(f"分析结果: {analysis}")
        # 存储到summary表（图片4结构）
        # 修正代码（直接访问字典值）
    analysis_data = analysis['analysis']  # 提取JSON字符串
    print(f"提取的分析数据: {analysis_data}")
    analysis_result = json.loads(analysis_data)  # ✅ 此时才是合法JSON字符串
    save_summary(
            student_id=records[0]["student_id"],
            grade=records[0]['grade'],
            analysis_result=analysis_result,
            start_date=tool_args['start_date'],
            end_date=tool_args['end_date'],
            subject=tool_args.get("subject")
        )
        
    print(f"存储到数据库的分析结果: {analysis_result}")

    

def call_with_messages():
        print("\n")
        messages = [
            {
                "content": input(
                    "请输入："
                ),  # 提问示例："请分析张三在2025-08-01到2025-08-15的数学学习情况"
                "role": "user",
            }
        ]
        first_response = analyze_with_qwen(messages)


# 5. 使用示例
if __name__ == "__main__":
    # 示例1：自然语言查询
    result = call_with_messages()
    print(result)
    
    # 示例2：直接获取分析报告
    # records = get_student_errors(
    #     student_name="张三",
    #     date_range={"from_date": "2025-08-01", "to_date": "2025-08-15"}
    # )
    # print(f"获取到{len(records)}条错题记录")