from openai import OpenAI
import os
import base64
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any, Union, Generator
from PIL import Image
import mimetypes
from dotenv import load_dotenv
import json
from pdf2image import convert_from_path
import fitz

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('vl_text_summary.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class VLTextSummarizer:
    """A class for image text recognition and summarization using Qwen-VL-Max."""
    
    SUPPORTED_FORMATS = {
        'PNG': 'image/png',
        'PNG': 'image/png',
        'JPEG': 'image/jpeg',
        'JPG': 'image/jpeg',
        'WEBP': 'image/webp',
        'PDF': 'application/pdf'  # 新增PDF支持
    }
    
    def __init__(self, api_key: Optional[str] = None):
        """Initialize the VLTextSummarizer with API credentials."""
        load_dotenv()  # Load environment variables from .env file
        self.api_key = api_key or os.getenv('DASHSCOPE_API_KEY')
        if not self.api_key:
            raise ValueError("API key must be provided either directly or through DASHSCOPE_API_KEY environment variable")
        
        self.client = OpenAI(
            api_key=self.api_key,
            base_url="https://dashscope.aliyuncs.com/compatible-mode/v1"
        )
        logger.info("VLTextSummarizer initialized successfully")

    def encode_image(self, image_path: str) -> str:
        """
        Encode an image file to base64 string.
        
        Args:
            image_path: Path to the image file(支持图片或PDF)
            
        Returns:
            Base64 encoded string of the image
        """
        try:
            with open(image_path, "rb") as image_file:
                return base64.b64encode(image_file.read()).decode("utf-8")
        except Exception as e:
            logger.error(f"Error encoding image {image_path}: {str(e)}")
            raise

    def get_image_format(self, image_path: str) -> Union[str, Generator[Image.Image, None, None]]:
            """
            获取文件格式（图片返回MIME类型，PDF返回生成器逐页生成PIL Image对象）
            
            Args:
                image_path: 文件路径（支持图片或PDF）
                
            Returns:
                - 图片: 返回MIME类型字符串（如 'image/jpeg'）
                - PDF: 返回生成器，逐页生成PIL Image对象
            """
            try:
                file_ext = os.path.splitext(image_path)[1].lower()
                
                # 处理PDF文件
                if image_path.lower().endswith('.pdf'):
                    images = convert_from_path(image_path, dpi=300)  # 转换为300DPI图片
                    image_path = "temp_converted.jpg"  # 临时文件
                    images[0].save(image_path)  # 仅处理第一页（根据需求调整）

                with Image.open(image_path) as img:
                        format = img.format
                        if format and format.upper() not in self.SUPPORTED_FORMATS:
                            raise ValueError(
                                f"Unsupported image format: {format}. "
                                f"Supported formats: {list(self.SUPPORTED_FORMATS.keys())}"
                            )
                        return self.SUPPORTED_FORMATS.get(format.upper(), 'application/octet-stream')
                        
            except Exception as e:
                logger.error(f"Error processing {image_path}: {str(e)}")
                raise

    def _convert_pdf_to_image(self, pdf_path: str) -> str:
        """将PDF转换为临时图片文件，返回图片路径"""
        try:
            from pdf2image import convert_from_path
            images = convert_from_path(pdf_path, dpi=200)
            temp_path = os.path.join(os.path.dirname(pdf_path), f"temp_{os.path.basename(pdf_path)}.jpg")
            images[0].save(temp_path, "JPEG", quality=90)
            return temp_path
        except Exception as e:
            logger.error(f"PDF转换失败: {pdf_path} - {str(e)}")
            raise

    def analyze_image(self, image_path: str, prompt: str) -> str:
        """
        Analyze an image using Qwen-VL-Max model.
        
        Args:
            image_path: Path to the image file
            prompt: Question or prompt for the model
            
        Returns:
            Model's response/analysis of the image
        """
        try:
            # Validate image path
            if not Path(image_path).exists():
                raise FileNotFoundError(f"Image file not found: {image_path}")
            

            temp_image_path = None
            if image_path.lower().endswith('.pdf'):
                temp_image_path = self._convert_pdf_to_image(image_path)
                image_path = temp_image_path  # 后续使用转换后的图片
                print(f"Converted PDF to image: {image_path}")

            # Encode image and get format
            print(f"Encoding image: {image_path}")
            base64_image = self.encode_image(image_path)
            print(f"Encoded image size: {len(base64_image)} characters")
            image_format = self.get_image_format(image_path)
            print(f"Image format: {image_format}")
            # Create completion request
            completion = self.client.chat.completions.create(
                model="qwen-vl-max-latest",
                # model="qwen2.5-vl-72b-instruct",
                messages=[

                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:{image_format};base64,{base64_image}"
                                }
                            },
                            {
                                "type": "text",
                                "text": prompt
                            }
                        ]
                    }
                ]
            )
            request_id = completion.id
            print(f"Request ID: {request_id}")
            response = completion.choices[0].message.content

            logger.info(f"Successfully analyzed image: {image_path}")
            return response

        except Exception as e:
            logger.error(f"Error analyzing image {image_path}: {str(e)}")
            raise

        # finally:
        #     # 清理临时文件
        #     if temp_image_path and os.path.exists(temp_image_path):
        #         os.remove(temp_image_path)

def analyze_document(image_path: str):
    """Example function demonstrating document analysis capabilities."""
    try:
        # Initialize analyzer
        analyzer = VLTextSummarizer()

         # 在调用analyze_image前提取图片ID（如从路径中解析）
        image_id = os.path.splitext(os.path.basename(image_path))[0]  # 示例：从路径"D:/.../exam123.png"提取"exam123"
        print(f"Extracted image ID: {image_id}")  # 输出图片ID以便调试

        analysis_prompt = f"""请对这个图片的内容进行深入分析,
1. 仅当同时满足以下条件时判定为错题：
   - 存在红色批改符号（如叉号、圈划）
   - 学生答案 ≠ 正确答案
2. 忽略以下情况：
   - 批改符号旁答案实际正确
   - 模糊无法辨别的标记
3. 输出格式要求：
    将图中每一道真正错题信息转为JSON格式:
    -all_data (包含以下字段，格式为列表)
        - original_input_id:(当前错题ID,数据类型整型）
        - wrong_q_sum(包含以下字段，格式为列表:  
                - question (题目内容)   
                - student_answer (学生答案)
                - correct_answer (正确答案)
                - error_type (错误类型,如计算错误、理解错误等)  
                - analysis (总的错误分析)
                - subject (学科,数据类型为字符串，请根据实际情况填写)
                - knowledge_grade (知识点相关年级,数据类型为整数,请根据实际情况填写)
                - knowledge_points (知识点列表,数据类型为列表,每个元素为字符串)
                - difficulty (难度等级,数据类型为整数,范围1-5)  
                - true_false_flag (是否为真正错题,数据类型为布尔值,默认值为True)
                )
            
返回前要求：
    -请只返回JSON格式的结果,不要包含任何其他文本
    -返回前请严格检查json格式,一个字段和值都不能少,避免重复和遗漏.

"""
        
        print("\n=== 文档内容分析 ===")
        raw_response = analyzer.analyze_image(image_path, analysis_prompt)
        print("原始响应",raw_response)



        # 2. 提取JSON部分（处理大模型可能返回的非纯JSON内容）
        json_start = raw_response.find('{')
        json_end = raw_response.rfind('}') + 1
        json_str = raw_response[json_start:json_end]

        # 3. 解析为Python字典
        error_data = json.loads(json_str)
        # print(f"解析后的错误数据: {error_data}")
        
        # 4. 验证必要字段
        required_fields = ["all_data"]
        if not all(field in error_data for field in required_fields):
            raise ValueError("返回数据缺少必要字段")

        return error_data

    except json.JSONDecodeError as e:
        print(f"JSON解析失败: {e}\n原始内容: {raw_response}")
        return {"wrong_q_sum": []}  # 返回安全数据

            
    except Exception as e:
        print(f"分析过程中出现错误: {str(e)}")
        raise




if __name__ == "__main__":
    # Example usage
    image_path = r"D:\vsc\edusystem\src\core\wrongquestion.png"  # 替换为你的图片路径
    analyze_document(image_path) 