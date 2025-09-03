import json
import os
import dashscope
import numpy as np
import psycopg2
from datetime import datetime
from pgvector.psycopg2 import register_vector
from typing import List, Dict
from config.settings import PG_CONFIG
from core.image_analyzer import VLTextSummarizer
from core.image_analyzer import analyze_document

class ErrorRecordManager:

    def __init__(self):
        self.conn = psycopg2.connect(**PG_CONFIG)
        register_vector(self.conn)  # 注册pgvector类型
        print("当前API Key:", os.getenv("DASHSCOPE_API_KEY"))

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
    
    def save_individual_errors(self, error_data: Dict):
        """将每个错题作为独立记录存储"""
        try:
            with self.conn.cursor() as cursor:
                # 提取基础信息
                base_info = {
                    "student_id": error_data.get("student_id", "unknown"),
                    "subject": error_data["subject"],
                    "grade": error_data.get("grade", ""),
                    "textbook_version": error_data.get("textbook_version", "unknown"),
                    "week_num": error_data.get("week_num", 0)
                                    }

                # 1. 提取所有需要向量化的错题文本

                # 预处理：收集需要向量化的错题
                all_questions = []
                for weak_point in error_data["wrong_q_sum"]:
                    valid_errors = [
                        q for q in weak_point["error_questions"]
                        if q["correct_answer"] != q["student_answer"]
                    ]
                    all_questions.extend([
                        (
                            q.get("question_id", f"temp_{hash(q['question'])}"),
                            # 关键修改：组合更多字段
                            f"知识点：{weak_point['weak_knowledge_point']}\n"
                            f"错题id:{q['question_id']}\n"
                            f"问题：{q['question']}\n"
                            f"正确答案：{q['correct_answer']}\n"
                            f"学生错误答案：{q['student_answer']}"
                        )
                        for q in valid_errors
                    ])

                print(f"Total questions to vectorize: {len(all_questions)}")
                print(f"文本内容示例: {all_questions[:5]}")  # 打印前5个问题的内容

                # 2. 批量生成向量（每批次50条）
                batch_size = 50
                question_vectors = {}
                for i in range(0, len(all_questions), batch_size):
                    batch = all_questions[i:i + batch_size]
                    texts = [q[1] for q in batch]
                    print(f"texts: {texts}")  # 打印当前批次的文本内容
                    vectors = self._generate_embeddings(texts)
                    for (q_id, _), vec in zip(batch, vectors): # 关联问题ID和向量，只有文本做了向量化
                        question_vectors[q_id] = vec

                print(f"Generated vectors for {len(question_vectors)} questions")
                print(f"question_vectors: {question_vectors}")

                # 遍历每个知识点下的错题
                for weak_point in error_data["wrong_q_sum"]:
                    weak_knowledge_point = weak_point["weak_knowledge_point"]
                    print(f"Processing weak knowledge point: {weak_knowledge_point}")
                    
                    for error_questions in weak_point["error_questions"]:
                        print(f"Inserting error question: {error_questions}")
                        print(f"With base info: {base_info}")
                                        # 校验并过滤错题
                        # 先打印调试信息
                        for q in weak_point["error_questions"]:
                            print("题目答案比对:", q["correct_answer"], "vs", q["student_answer"])
                        valid_errors = [
                            q for q in weak_point["error_questions"]
                            if q["correct_answer"] != q["student_answer"]  # 关键校验条件
                            ]   
                            
                        if not valid_errors:
                           continue  # 无真实错题则跳过

                        # 构建插入语句
                        cursor.execute("""
                            INSERT INTO student_learning_analysis (
                                student_id, subject, grade, textbook_version,
                                week_num, weak_knowledge_point, error_questions
                                ,record_timestamp,question_vector
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT ON CONSTRAINT unique_student_subject_knowledge_time
                            DO UPDATE SET
                                analysis_date = NOW()
                        """, (
                            base_info["student_id"],
                            base_info["subject"],
                            base_info["grade"],
                            base_info["textbook_version"],
                            base_info["week_num"],
                            weak_knowledge_point,
                            json.dumps(error_questions, ensure_ascii=False),
                            datetime.now(),
                            question_vectors[q["question_id"]]
                        ))
                
                self.conn.commit()
                return True
                
        except Exception as e:
            print(f"数据库操作失败: {e}")
            self.conn.rollback()
            return False
        
    def search_similar_questions(self, query_text: str, top_k: int = 3) -> List[tuple]:
        """语义搜索相似错题"""
        try:
            # 1. 生成查询向量
            query_vec = self._generate_embeddings([query_text])[0]

            # 2. 执行相似度搜索
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, question, 1 - (question_vector <=> %s) AS similarity
                    FROM student_learning_analysis
                    ORDER BY question_vector <=> %s
                    LIMIT %s
                """, (query_vec, query_vec, top_k))
                return cursor.fetchall()

        except Exception as e:
            print(f"搜索失败: {e}")
            return []


    def __del__(self):
        self.conn.close()


if __name__ == "__main__":
#     error_data = {
#         "student_id": "2025003",
#         "subject": "数学",
#         "grade": "一年级",
#         "textbook_version": "3",
#         "week_num": 0,
#         "wrong_q_sum": [
#             {
#                 "weak_knowledge_point": "数位的认识",
#                 "error_questions": [
#                     {
#                         "question": "数位从（ ）边起，第1位是个位，第2位是（ ）位。",
#                         "correct_answer": "右 十",
#                         "student_answer": "左 十"
#                     }
#                 ]
#             },
#             # 其他错题...
#         ]
# }

        # error_data = {
        #             "student_id": "未知",
        #             "subject": "数学",
        #             "grade": "一年级",
        #             "textbook_version": "5",
        #             "week_num": "2",
        #             "wrong_q_sum": [
        #                 {
        #                     "weak_knowledge_point": "数位的认识",
        #                     "error_questions": [
        #                         {   
        #                             "question_id": "q1",
        #                             "question": "数位从（ ）边起，第1位是个位，第2位是（ ）位。",
        #                             "correct_answer": "右 十",
        #                             "student_answer": "左 十"
        #                         }
        #                     ]
        #                 },
        #                 {
        #                     "weak_knowledge_point": "数字的组成",
        #                     "error_questions": [
        #                         {   
        #                             "question_id": "q2",
        #                             "question": "15里面有（ ）个十和（ ）个一。",
        #                             "correct_answer": "1 5",
        #                             "student_answer": "1 1"
        #                         }
        #                     ]
        #                 },
        #                 {
        #                     "weak_knowledge_point": "相邻数的认识",
        #                     "error_questions": [
        #                         {
        #                             "question_id": "q3",
        #                             "question": "与19相邻的两个数分别是（ ）和（ ）。",
        #                             "correct_answer": "18 20",
        #                             "student_answer": "18 20"
        #                         }
        #                                         ]                           
        #                 },
        #                 {
        #                     "weak_knowledge_point": "数字的顺序",
        #                     "error_questions": [
        #                         {
        #                             "question_id": "q4",
        #                             "question": "14前面的第3个数是（ ），后面的第3个数是（ ）。",
        #                             "correct_answer": "11 17",
        #                             "student_answer": "17 11"
        #                         }
        #                     ]
        #                 }
        #             ]
        #         }
    image_path = r"D:\vsc\edusystem\src\core\wrongquestion.png"  # 替换为你的图片路径
    # vLTextSummarizer = VLTextSummarizer()
    print("开始分析图片中的错题信息...")
    error_data = analyze_document(image_path) 
    # 存储到数据库
    print("分析结果:", error_data)
    manager = ErrorRecordManager()
    if manager.save_individual_errors(error_data):
        print("错题数据存储成功")
    else:
        print("存储失败")