from openai import OpenAI
import os
import asyncio
import json
from fastmcp import Client
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 设置 Qwen 的 OpenAI 兼容接口
client = OpenAI(
    api_key=os.getenv("DASHSCOPE_API_KEY"),  # 你的千问 API Key
    base_url="https://dashscope.aliyuncs.com/compatible-mode/v1"  # 注意：这是 Qwen 的 OpenAI 兼容 endpoint
)

async def analyze_with_qwen_openai(messages):
    """
    使用 OpenAI 兼容接口调用 Qwen
    """
    print(f"查询内容: {messages[0]['content']}")

    # 连接 MCP Server 获取工具定义
    async with Client(transport="d:/vsc/edusystem/src/core/mcp_server.py") as mcp_client:
        tools = await mcp_client.list_tools()
        print("Available tools:", tools)

        # 转换为 OpenAI 风格的 tools
        openai_tools = []
        for tool in tools:
            openai_tools.append({
                "type": "function",
                "function": {
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": tool.inputSchema  # 必须是有效的 JSON Schema
                }
            })

        # 调用 Qwen 模型
        response = client.chat.completions.create(
            model="qwen-max-latest",  # 也可以是 qwen-plus, qwen-turbo 等
            messages=messages,
            tools=openai_tools,
            tool_choice="auto",  # 让模型自动决定是否调用工具
            max_tokens=1024,
            temperature=0.5
        )

        print(f"第一次模型响应: {response}")

        # 检查是否调用工具
        if response.choices[0].message.tool_calls:
            print("第一次✅ 触发了工具调用")
            tool_call = response.choices[0].message.tool_calls[0]
            tool_name = tool_call.function.name
            tool_args = json.loads(tool_call.function.arguments)
            print(f"第一次调用工具: {tool_name}, 参数: {tool_args}")

            # 调用 MCP Server 的实际工具
            tool_result = await mcp_client.call_tool(tool_name, tool_args)
            print(f"第一次tool_result: {tool_result}")

            # 将工具结果构造成清晰的上下文（可用于分析）
        tool_text = tool_result.content[0].text  # 获取 TextContent 对象中的 text 字段
        print(f"第一次工具返回内容: {tool_text}")
        student_errors = json.loads(tool_text)  # 将 JSON 字符串解析为 Python 列表
        print(f'第一次student_errors: {student_errors}')


        # === 第三轮：让模型生成结构化 JSON 分析结果 ===
        # 更新 messages 历史
        print(f"第一次call 之后message: {messages}")
        messages.append(response.choices[0].message)  # 添加模型的 tool_call 请求
        print(f"第一次添加了tool call 的message: {messages}")
        messages.append({
            "role": "tool",
            "content": json.dumps(student_errors, ensure_ascii=False, indent=2),
            "tool_call_id": tool_call.id
        })
        print(f"第二次之前，添加了tool取到的数据: {messages}")

        # 强调：要求模型返回 JSON 格式
        analysis_prompt = {
            "role": "user",
            "content": (
                "请根据以下学生错题数据，进行学习情况分析，并返回一个 JSON 对象，包含以下字段：\n"
                "student_id: 学生ID从提示词里面取，默认为整数'1'\n"
                "grade: 年级从提示词里面取，默认为9\n"
                "analysis_result: 分析结果(JSON 对象),包含一下字段\n"
                    "strength: 强项知识点（如：计算能力强、逻辑清晰等）\n"
                    "weakness: 薄弱环节（如：理解题意差、粗心等）\n"
                    "progress: 进步情况（对比历史表现，是否有提升）\n"
                    "remarks: 学习建议（个性化建议，不少于50字）\n"
                "start_date: 开始日期（字符串，格式为 YYYY-MM-DD）\n"
                "end_date: 结束日期（字符串，格式为 YYYY-MM-DD）\n"
                "subject: 学科（字符串）\n"
                "注意：必须返回纯 JSON，不要额外解释，使用中文。"
            )
        }
        messages.append(analysis_prompt)
        print(f"第二次调用前的最终messages: {messages}")

        final_response = client.chat.completions.create(
            model="qwen-max-latest",
            messages=messages,
            response_format={"type": "json_object"},  # ✅ 强制返回 JSON
            max_tokens=1024,
            temperature=0.5
        )

        raw_content = final_response.choices[0].message.content.strip()
        print(f"第二轮大模型回复(原始): {raw_content}")

        save_prompt = {
            "role": "user",
            "content": "请将以下分析结果存入数据库的 summary 表中:\n"+json.dumps(raw_content, ensure_ascii=False, indent=2)
        }

        messages.append(save_prompt)
        print(f"第三次调用前的message: {messages}")

        response3 = client.chat.completions.create(
            model="qwen-max-latest",  # 也可以是 qwen-plus, qwen-turbo 等
            messages=messages,
            tools=openai_tools,
            tool_choice="auto",  # 让模型自动决定是否调用工具
            max_tokens=1024,
            temperature=0.5
        )

        print(f"第三次模型响应: {response3}")

        # 检查是否调用工具
        if response3.choices[0].message.tool_calls:
            print("第三次调用✅ 触发了工具调用")
            print(response3.choices[0].message.tool_calls)
            tool_call3 = response3.choices[0].message.tool_calls[0]
            tool_name3 = tool_call3.function.name
            tool_args3 = json.loads(tool_call3.function.arguments)
            print(f"第三次调用工具: {tool_name3}, 参数: {tool_args3}")

            # 调用 MCP Server 的实际工具
            tool_result3 = await mcp_client.call_tool(tool_name3, tool_args3)
            logger.info(f"第三次准备存储分析结果: {tool_result3}")
            print(f"第三次tool_result: {tool_result3}")

        else:
            return response.choices[0].message.content
            print("❌ 未触发工具调用")

async def main():
    messages = [
        {"role": "user", "content": "分析张三从2025-08-01到2025-08-20的数学错题"}
    ]
    result = await analyze_with_qwen_openai(messages)
    print("最终回复：", result)

if __name__ == "__main__":
    asyncio.run(main())
