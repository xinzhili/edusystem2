from fastmcp import FastMCP, Client
import psycopg2
import asyncio
import json
from typing import List, Dict, Optional
import logging


logger = logging.getLogger(__name__)

DB_CONFIG = {
            "dbname":"learning_db",  
            "host":"localhost",
            "port":5433,
            "user":"postgres",
            "password":"123456"
            }
mcp = FastMCP("My MCP Server")

@mcp.tool
def get_student_errors(
    student_name: str,
    subject: str,
    start_date: str,  # YYYY-MM-DD
    end_date: str     # YYYY-MM-DD
):
    """获取学生错题记录（关联students和study_detail表）"""
    print(f"Connecting to database with config: {DB_CONFIG}")
    conn = psycopg2.connect(**DB_CONFIG)
    print(f'student_name: {student_name}, subject: {subject}, start_date: {start_date}, end_date: {end_date}    ')
    print(f"Connecting to database with config: {DB_CONFIG}")
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
        print(f"Executed query: {cursor.query}")
        return [dict(zip(
            [desc[0] for desc in cursor.description], 
            row
        )) for row in cursor.fetchall()]
        print("查询结果:", result)

@mcp.tool
def save_summary(
    student_id: int,
    grade: int,
    analysis_result: dict,
    start_date: str,
    end_date: str,
    subject: str
):
    """将分析结果存入summary表"""
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info(f"✅ 数据库连接成功")

        # 检查 analysis_result 格式
        if not isinstance(analysis_result, dict):
            raise ValueError("analysis_result 必须是字典")

        # 提取字段，确保是字符串
        details = {
            "strength": analysis_result.get("strength", ""),
            "weakness": analysis_result.get("weakness", ""),
            "progress": analysis_result.get("progress", ""),
            "remarks": analysis_result.get("remarks", "")
        }

        logger.info(f"准备插入数据: student_id={student_id}, subject={subject}")

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
                json.dumps(details, ensure_ascii=False)  # 中文不转义
            ))
            logger.info(f"✅ SQL 执行成功，影响 {cursor.rowcount} 行")

        conn.commit()
        logger.info("✅ 事务提交成功")
        return {"status": "success", "message": "分析结果保存成功"}

    except Exception as e:
        if conn:
            conn.rollback()
            logger.error(f"❌ 事务回滚: {e}")
        else:
            logger.error(f"❌ 连接未建立: {e}")
        return {"status": "error", "message": str(e)}

    finally:
        if conn:
            conn.close()
            logger.info("🔗 数据库连接已关闭")
# @mcp.tool
# def list_tools():
#     """返回当前MCP Server中所有可用工具的描述"""
#     return [
#         {
#             "name": tool.name,
#             "description": tool.description,
#             "parameters": tool.parameters
#         }
#         for tool in mcp.registered_tools.values()
#     ]

if __name__ == "__main__":
    # 默认走 stdio 模式，方便被 fastmcp 的 Client 调用
    print("Starting MCP server...")
    mcp.run()
    print("MCP server is running.")

# 创建 FastMCP 客户端
    

# fastmcp run D:\vsc\edusystem\src\core\mcp_server.py:mcp --transport http --port 5000
# 这个只有fastmacp 3.0版本才支持，但是现在还没有发布，我的fastmcp版本是2.0，所以不能用这个方式

# client = Client(mcp)

# async def call_tool(student_name: str,
#                         subject: str,
#                         start_date: str,  # YYYY-MM-DD
#                         end_date: str):
#     async with client:
#             params = {
#                     "student_name": student_name,
#                     "subject": subject,
#                     "start_date": start_date,
#                     "end_date": end_date
# }
#             result = await client.call_tool("get_student_errors", params) # 调用工具
#             print('调用:',result)

# asyncio.run(call_tool(student_name="张三",subject="数学", start_date="2025-08-01", end_date="2025-08-20")) # 调用示例