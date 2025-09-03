from fastapi import FastAPI
from fastmcp import FastMCP

mcp = FastMCP("EduSystem MCP Server")

@mcp.tool
def get_student_errors(student_name: str, subject: str, start_date: str, end_date: str):
    return {"message": f"查询 {student_name} 在 {subject} 从 {start_date} 到 {end_date} 的错题"}

app = FastAPI()

# 提供一个 HTTP 接口，手动调用 MCP 工具
@app.post("/mcp/get_student_errors")
async def call_get_student_errors(student_name: str, subject: str, start_date: str, end_date: str):
    return get_student_errors(student_name, subject, start_date, end_date)