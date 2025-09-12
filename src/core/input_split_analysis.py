import json
import os
import dashscope
import numpy as np
import psycopg2
from datetime import datetime
from pgvector.psycopg2 import register_vector
from typing import List, Dict
from config.settings import PG_CONFIG
from core.image_analyzer_new import VLTextSummarizer
from core.image_analyzer_new import analyze_document

class ErrorRecordManager:

    def __init__(self):
        self.conn = psycopg2.connect(
                            dbname="learning_db",  # 显式覆盖
                            host="localhost",
                            port=5433,
                            user="postgres",
                            password="123456")
        register_vector(self.conn)  # 注册pgvector类型
        print("当前API Key:", os.getenv("DASHSCOPE_API_KEY"))
        with self.conn.cursor() as cur:  # 正确用法
            cur.execute("SELECT current_database()")
            db_name = cur.fetchone()[0]
            print(f"当前实际连接的数据库: {db_name}")

    def _generate_embeddings(self, texts: List[str]) -> List[np.ndarray]:
        """调用千问API生成向量"""
        try:
            resp = dashscope.TextEmbedding.call(
                model="text-embedding-v4",
                input=texts,
                text_type="document"  # 可选：指定文本类型（document/query）
            )

            return [np.array(item['embedding']) for item in resp.output['embeddings']] #大模型返回的数据存在embeddings字段，而不是‘data'
        except Exception as e:
            print(f"API调用失败: {e}")
            raise

    def save_individual_errors(self, student_id: str, error_data: Dict):
        """将每个错题作为独立记录存储"""
        try:
            print("saving individual errors...")
            print(f"error_data 类型: {type(error_data)}")  # 应输出 <class 'dict'>
            # print(f'input error data',error_data)  
            
            with self.conn.cursor() as cursor:
                # 1. 提取所有错题数据
                all_errors = error_data.get("all_data", [])
                print(f"找到 {len(all_errors)} 组错题数据")
                
                # 2. 处理每组错题
                questions_to_vectorize = []  # 用于存储需要向量化的文本
                for error_group in all_errors:
                    original_input_id = error_group.get("original_input_id", "unknown")
                    wrong_q_list = error_group.get("wrong_q_sum", [])  
                    print(f"\n处理错题组 {original_input_id}，包含 {len(wrong_q_list)} 道题")
                    # 3. 过滤有效错题（学生答案≠正确答案）
                    valid_errors = [
                        q for q in wrong_q_list 
                        if q.get("correct_answer") != q.get("student_answer")
                               ] 
                    for q in valid_errors:
                        # 为每道题生成唯一ID（组ID+题目ID）
                        question_id = q.get("question_id", str(hash(q["question"])))
                        unique_id = f"{original_input_id}_{question_id}"
                        questions_to_vectorize.append((unique_id, json.dumps(q)))
                    print(f"收集到 {len(valid_errors)} 个有效错题")
                    print(f"收集到 {len(questions_to_vectorize)} 个待向量化的文本")
                    
                    # 5. 生成向量（批处理）
                batch_size = 50
                question_vectors = {}
                for i in range(0, len(questions_to_vectorize), batch_size):
                                batch = questions_to_vectorize[i:i + batch_size]
                                texts = [q[1] for q in batch]
                                vectors = self._generate_embeddings(texts)
                                print(f"向量化文本： {texts}")
                                question_vectors.update({
                                    q[0]: vec for q, vec in zip(batch, vectors)
                                })

                                for (q_id, _), vec in zip(batch, vectors): # 关联问题ID和向量，只有文本做了向量化
                                    question_vectors[q_id] = vec
                                print(f"为 {len(question_vectors)} 道题生成了向量")
                            # 6. 存入数据库
                # for q in valid_errors:
                for unique_id, q_text in questions_to_vectorize:
                                try:
                                    cursor.execute("""
                                                INSERT INTO study_detail(
                                                    student_id,original_input_id,details,details_embedding,created_at
                                                ) VALUES (%s, %s, %s, %s, %s)
                                            """, (
                                                student_id,
                                                unique_id.split("_")[0],
                                                q_text,
                                                json.dumps(question_vectors[unique_id].tolist()),
                                                datetime.now()
                                            ))
                                    
                                    print(f"已存储 {len(questions_to_vectorize)} 道错题到数据库")
                                except psycopg2.Error as e:
                                        print(f"数据库错误: {e}")
                                        self.conn.rollback()  # 显式回滚
                                        continue  # 继续处理下一个错题组
                
                self.conn.commit()
                return True
                
        except Exception as e:
            print(f"数据库操作失败: {e}")
            self.conn.rollback()
            return False
            

    def __del__(self):
        self.conn.close()


if __name__ == "__main__":

    image_path = r"D:\vsc\edusystem\src\core\wrongquestion.png"  # 替换为你的图片路径
    print("开始分析图片中的错题信息...")
    error_data = analyze_document(image_path) 
    print("分析结果:", error_data)
    manager = ErrorRecordManager()
    if manager.save_individual_errors(1,error_data):
        print("错题数据存储成功")
    else:
        print("存储失败")