from typing import List, Dict
import dashscope
import numpy as np
import psycopg2
from datetime import datetime
from pgvector.psycopg2 import register_vector
from config.settings import PG_CONFIG
    
def search_similar_questions(query_text: str, top_k: int = 3) -> List[tuple]:
        """语义搜索相似错题"""
        try:
            # 1. 生成查询向量
            print(f"Searching for similar questions to: {query_text}")

            resp = dashscope.TextEmbedding.call(
                    model="text-embedding-v4",
                    input=query_text,
                    text_type="document"  # 可选：指定文本类型（document/query）
                )
            query_vec = np.array(resp.output['embeddings'][0]['embedding'])
            print(f"Generated query vector: {query_vec}")
                

            # 2. 执行相似度搜索
            conn = psycopg2.connect(**PG_CONFIG)
            register_vector(conn)  # 注册pgvector类型
            with conn.cursor() as cursor:
                            cursor.execute("""
                                    SELECT id, weak_knowledge_point, 1 - (question_vector <=> %s) AS similarity
                                    FROM student_learning_analysis
                                    ORDER BY question_vector <=> %s
                                    LIMIT %s
                                """, (query_vec, query_vec, top_k))
                            return cursor.fetchall()

        except Exception as e:
             print(f"搜索失败: {e}")
             return []
        

if __name__ == "__main__":
    # 测试代码
    # qembedding = ErrorRecordManager()
    query_text = "数位的认识做错了吗？"
    results = search_similar_questions(query_text,top_k=2)
    for res in results:
        print(f"ID: {res[0]}, Question: {res[1]}, Similarity: {res[2]}")