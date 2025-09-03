import psycopg2
from typing import List, Dict
from config.settings import PG_CONFIG

class LearningAnalyzer:
    def __init__(self):
        self.conn = psycopg2.connect(**PG_CONFIG)
        
    def get_student_data(self, student_id: str) -> List[Dict]:
        """从学情分析表获取学生数据"""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT subject, grade, textbook_version,week_num,weak_knowledge_point,error_questions
                FROM student_learning_analysis 
                WHERE student_id = %s
                ORDER BY analysis_date DESC
                LIMIT 10
            """, (student_id,))
            print("Executing query to fetch student data", student_id)
            return [
                {
                    "subject": row[0],
                    "grade": row[1],
                    "textbook_version": row[2],
                    "week_num": row[3],
                    "weak_knowledge_point": row[4],
                    "error_questions": row[5]
                }
                for row in cur.fetchall()
            ]
