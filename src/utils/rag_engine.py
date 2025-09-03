
import psycopg2
from utils.rag_engine import RAGEngine
from config.settings import PG_CONFIG

conn = psycopg2.connect(**PG_CONFIG)
rag = RAGEngine(conn)
print(rag.retrieve_knowledge("一次函数"))

# from pgvector.psycopg2 import register_vector
# import numpy as np

# class RAGEngine:
#     def __init__(self, conn):
#         self.conn = conn
#         register_vector(self.conn)
        
#     def retrieve_knowledge(self, query: str, top_k: int = 3) -> List[str]:
#         """检索教材知识点（需先安装pgvector扩展）"""
#         with self.conn.cursor() as cur:
#             # 假设已创建包含知识点的表（需预先向量化）
#             cur.execute("""
#                 SELECT content FROM knowledge_points 
#                 ORDER BY embedding <=> %s::vector 
#                 LIMIT %s
#             """, (self._get_embedding(query), top_k))
#             return [row[0] for row in cur.fetchall()]
    
#     def _get_embedding(self, text: str) -> np.ndarray:
#         """模拟生成文本向量（实际应调用嵌入模型）"""
#         return np.random.rand(384)  # 替换为实际嵌入模型

