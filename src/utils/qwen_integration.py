import dashscope
from dashscope import Generation
from typing import Optional, Dict

def call_qwen(prompt: str, api_key: str) -> Optional[Dict]:
    """调用千问大模型API"""
    dashscope.api_key = api_key
    
    try:
        response = Generation.call(
            model="qwen-max-latest",
            prompt=prompt,
            temperature=0.3
        )
        return {
            "analysis": response.output.text,
            "usage": response.usage
        }
    except Exception as e:
        print(f"API调用失败: {e}")
        return None