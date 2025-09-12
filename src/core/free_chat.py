"""
Enterprise NL2SQL Demo for PostgreSQL
=====================================

A refactored demonstration of a Text-to-SQL pipeline for PostgreSQL, 
showcasing complex educational data analysis scenarios.

Key Features:
1. PostgreSQL backend with pgvector extension
2. Educational data schema (students, study details, learning summaries)
3. Multi-table join analysis capabilities
4. Vector retrieval for schema matching

Author: Leo (Adapted by Gemini)
License: MIT
"""

import os
import json
import logging
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor

# Conditional imports for LLM providers
try:
    from openai import OpenAI
except ImportError:
    OpenAI = None

# --- Configuration Block ----------------------------------------------------
CONFIG = {
    "database": {
        "dbname": "learning_db",
        "user": "postgres",
        "password": "123456",
        "host": "localhost",
        "port": "5433"
    },
    "embedding_model": "text-embedding-v4",
    "llm": {
        "provider": "dashscope",  # or "openai"
        "api_key_env": "DASHSCOPE_API_KEY",
        "models": {
            "sql_generation": "qwen-plus",
            "answer_generation": "qwen-plus",
        }
    },
    "prompts": {
        "sql_generation": """
你是一位PostgreSQL数据库专家。根据给定的数据库模式和自然语言问题，生成准确且可执行的SQL查询。

**重要约束**：
1. 只能使用提供的数据库模式中明确存在的表和字段
2. 如果问题要求的数据在给定的DDL中不存在，必须拒绝生成SQL
3. 拒绝时返回：SCHEMA_INSUFFICIENT: [具体说明缺少什么数据]
4. PostgreSQL特有语法：使用ILIKE代替LIKE进行不区分大小写的匹配

### 数据库模式:
{schema_context}

### 问题:
{question}

### 要求:
- 如果所需字段都存在：返回纯SQL语句，不要有任何解释或markdown格式
- 如果缺少必要字段：返回 SCHEMA_INSUFFICIENT: [说明原因]

SQL:
""",
        "answer_generation": """
你是一位专业的教学数据分析师。基于用户的问题、SQL查询和数据结构摘要，提供有价值的分析回答。

注意：出于数据安全考虑，你收到的是数据结构摘要而非实际数据值。请基于查询逻辑和数据结构提供专业分析。

### 用户问题:
{question}

### 执行的SQL查询:
{sql_query}

### 数据结构摘要:
{data_summary}

### 分析要求:
1. 基于SQL查询逻辑分析教学问题
2. 解释查询涉及的教学指标和关系
3. 根据数据结构提供合理的教学洞察
4. 提供数据驱动的教学建议（如适用）

### 专业分析:
"""
    }
}

# --- Logging Setup ----------------------------------------------------------
log_format = '%(asctime)s - %(levelname)s - [%(name)s] %(message)s'

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if logger.handlers:
    logger.handlers.clear()

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter(log_format)
console_handler.setFormatter(console_formatter)

file_handler = logging.FileHandler('pg_nl2sql_demo.log', mode='w', encoding='utf-8')
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter(log_format)
file_handler.setFormatter(file_formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)

# --- Data Classes -----------------------------------------------------------
@dataclass
class TableSchema:
    """Represents a database table schema with metadata."""
    name: str
    ddl: str
    description: str
    
@dataclass
class QueryResult:
    """Represents the result of a SQL query execution."""
    success: bool
    data: List[Dict[str, Any]]
    sql: str
    error: Optional[str] = None

# --- PostgreSQL Components --------------------------------------------------

class PGManager:
    """Manages all PostgreSQL database interactions."""
    def __init__(self, db_config: Dict[str, Any]):  
        self.db_config = db_config
        logger.info(f"PGManager initialized for database: {db_config['dbname']}")
        self._init_database()
    
    def _init_database(self):
        """Initializes the database with educational schema if not present."""
        logger.info("Initializing PostgreSQL database schema...")
        
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    # Check if tables already exist
                    cursor.execute("""
                        SELECT table_name 
                        FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = 'students'
                    """)
                    if cursor.fetchone():
                        logger.info("Database schema already exists. Skipping creation.")
                        return

                    logger.info("Creating educational schema...")
                    self._create_educational_schema(cursor)
                    self._insert_sample_data(cursor)
                    
                    conn.commit()
                    logger.info("Database initialized successfully.")
        except Exception as e:
            logger.error(f"Database initialization failed: {e}", exc_info=True)
            raise

    def _get_connection(self):
        """Returns a new database connection."""
        return psycopg2.connect(**self.db_config)

    def _create_educational_schema(self, cursor):
        """Creates the educational database schema."""
        # 1. 学生表
        cursor.execute("""
        CREATE TABLE students (
            student_id SERIAL PRIMARY KEY,   --学生ID
            name VARCHAR(20) NOT NULL,       --学生姓名
            grade SMALLINT NOT NULL,         --年级
            date_of_birth DATE NOT NULL,    --出生日期
            gender TEXT NOT NULL,           --性别
            region VARCHAR(20) NOT NULL,    --地区（如：华东、华北、华南等）
            textbook_version VARCHAR(20) NOT NULL,  --教材版本（如：人教版、北师大版、苏教版等）
            school VARCHAR(30) NOT NULL,    --学校
            photo BYTEA,   --学生照片，存储为二进制数据
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP  --    创建时间
        );
        """)
        
        # 2. 原始输入表
        cursor.execute("""
        CREATE TABLE original_input (
            original_input_id SERIAL PRIMARY KEY,    --原始输入ID
            student_id INT REFERENCES students(student_id) ON DELETE SET NULL,  --关联学生ID
            content BYTEA,           --原始输入内容，存储为二进制数据（如图片、文档等）
            content_hash VARCHAR(32) UNIQUE NOT NULL,    --内容的MD5哈希值
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP   --创建时间
        );
        """)
        
        # 3. 学生学习错题表
        cursor.execute("""
        CREATE TABLE study_detail (
            study_detail_id SERIAL PRIMARY KEY,   --学习明细ID
            student_id INT REFERENCES students(student_id) ON DELETE CASCADE,   --关联学生ID
            original_input_id INT REFERENCES original_input(original_input_id) ON DELETE SET NULL,  --关联原始输入ID
            details JSONB NOT NULL,   --错题详情，存储为JSON格式，包含错题、知识点等信息
            details_embedding vector(1024),  --错题详情的向量化表示，使用pgvector存储
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        # 4. 学情汇总表
        cursor.execute("""
        CREATE TABLE summary (
            summary_id SERIAL PRIMARY KEY,   --学情汇总ID
            student_id INT REFERENCES students(student_id) ON DELETE CASCADE, --    关联学生ID
            grade SMALLINT NOT NULL,         --年级
            from_date DATE NOT NULL,         --汇总开始日期
            to_date DATE NOT NULL,           --汇总结束日期
            subject TEXT NOT NULL,           --学科（如：数学、语文、英语等）
            details JSONB NOT NULL,          --汇总详情，存储为JSON格式，包含学科优势、薄弱环节等信息
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        logger.info("4 educational tables created successfully.")

    def _insert_sample_data(self, cursor):
        """Inserts comprehensive sample data for educational analysis."""
        # 1. 插入学生数据
        cursor.executemany("""
        INSERT INTO students (name, grade, date_of_birth, gender, region, textbook_version, school)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, [
            ('张三', 7, '2010-05-15', '男', '华东', '人教版', '第一中学'),
            ('李四', 8, '2009-08-22', '男', '华北', '北师大版', '实验中学'),
            ('王五', 7, '2010-03-10', '男', '华南', '苏教版', '外国语学校'),
            ('赵六', 8, '2009-11-18', '女', '华东', '人教版', '第一中学'),
            ('钱七', 9, '2008-07-05', '女', '华北', '北师大版', '实验中学'),
            ('孙八', 7, '2010-01-30', '男', '华南', '苏教版', '外国语学校')
        ])
        
        # 2. 插入原始输入数据
        cursor.executemany("""
        INSERT INTO original_input (student_id, content_hash)
        VALUES (%s, %s)
        """, [
            (1, 'hash1'),
            (2, 'hash2'),
            (3, 'hash3'),
            (4, 'hash4'),
            (5, 'hash5'),
            (6, 'hash6')
        ])
        
        # 3. 插入学生学习错题表
        study_details = [
            {
                "student_id": 1,
                "original_input_id": 1,
                "details": {
                    "student_question": "解方程: 2x + 5 = 15",
                    "student_answer": "x = 10",
                    "correct_answer": "x = 5",
                    "error_type": "计算错误",
                    "analysis": "忘记在等式两边同时减5",
                    "subject": "数学",
                    "knowledge_grade": "七年级",
                    "knowledge_points": ["一元一次方程", "等式性质"],
                    "difficulty": 2,
                    "true_false_flag": False
                }
            },
            {
                "student_id": 1,
                "original_input_id": 1,
                "details": {
                    "student_question": "计算圆的面积，半径r=3cm",
                    "student_answer": "18.84",
                    "correct_answer": "28.26",
                    "error_type": "公式错误",
                    "analysis": "使用了周长公式2πr而非面积公式πr²",
                    "subject": "数学",
                    "knowledge_grade": "七年级",
                    "knowledge_points": ["圆的面积"],
                    "difficulty": 3,
                    "true_false_flag": False
                }
            },
            {
                "student_id": 2,
                "original_input_id": 2,
                "details": {
                    "student_question": "光合作用的产物是什么？",
                    "student_answer": "氧气",
                    "correct_answer": "氧气和有机物",
                    "error_type": "知识不完整",
                    "analysis": "只回答了部分产物",
                    "subject": "生物",
                    "knowledge_grade": "八年级",
                    "knowledge_points": ["光合作用"],
                    "difficulty": 3,
                    "true_false_flag": False
                }
            },
            {
                "student_id": 3,
                "original_input_id": 3,
                "details": {
                    "student_question": "《红楼梦》的作者是谁？",
                    "student_answer": "曹雪芹",
                    "correct_answer": "曹雪芹",
                    "error_type": None,
                    "analysis": "回答正确",
                    "subject": "语文",
                    "knowledge_grade": "七年级",
                    "knowledge_points": ["文学常识"],
                    "difficulty": 1,
                    "true_false_flag": True
                }
            }
        ]
        
        for detail in study_details:
            cursor.execute("""
            INSERT INTO study_detail (student_id, original_input_id, details)
            VALUES (%s, %s, %s)
            """, (detail["student_id"], detail["original_input_id"], json.dumps(detail["details"])))
        
        # 4. 插入学情汇总数据
        summaries = [
            {
                "student_id": 1,
                "grade": 7,
                "from_date": "2024-01-01",
                "to_date": "2024-03-31",
                "subject": "数学",
                "details": {
                    "strength": "代数基础扎实",
                    "weakness": "几何图形计算",
                    "progress": "从60分提高到75分",
                    "remarks": "需要加强几何练习"
                }
            },
            {
                "student_id": 2,
                "grade": 8,
                "from_date": "2024-01-01",
                "to_date": "2024-03-31",
                "subject": "生物",
                "details": {
                    "strength": "实验操作熟练",
                    "weakness": "理论知识记忆",
                    "progress": "保持85分以上",
                    "remarks": "需要加强知识点记忆"
                }
            }
        ]
        
        for summary in summaries:
            cursor.execute("""
            INSERT INTO summary (student_id, grade, from_date, to_date, subject, details)
            VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                summary["student_id"],
                summary["grade"],
                summary["from_date"],
                summary["to_date"],
                summary["subject"],
                json.dumps(summary["details"])
            ))
        
        logger.info("Sample educational data inserted successfully.")

    def get_all_schemas(self) -> List[TableSchema]:
        """Retrieves DDL and descriptions for all tables using standard PostgreSQL queries."""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    # Get all tables
                    cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_type = 'BASE TABLE'
                    """)
                    tables = [row[0] for row in cursor.fetchall()]
                    
                    schemas = []
                    for table in tables:
                        # 使用标准的信息模式查询获取表结构
                        cursor.execute(f"""
                            SELECT 
                                c.column_name, 
                                c.data_type, 
                                c.is_nullable,
                                c.column_default,
                                pg_catalog.col_description((c.table_schema || '.' || c.table_name)::regclass, c.ordinal_position) AS column_comment
                            FROM information_schema.columns c
                            WHERE c.table_name = '{table}' 
                            AND c.table_schema = 'public'
                            ORDER BY c.ordinal_position
                            """)
                            
                        columns = cursor.fetchall()
                        
                        # 构建自定义的DDL字符串
                        ddl_parts = [f"CREATE TABLE {table} ("]
                        for col in columns:
                            col_def = f"  {col[0]} {col[1]}"
                            if col[2] == 'NO':
                                col_def += " NOT NULL"
                            if col[3]:
                                col_def += f" DEFAULT {col[3]}"
                            if col[4]:  # 如果有列注释
                                col_def += f"  -- {col[4]}"
                                print(f"Column {col[0]} comment: {col[4]}")
                            ddl_parts.append(col_def)
                        
                        ddl_parts.append(");")
                        ddl = "\n".join(ddl_parts)
                        # print(f"DDL for table {table}:\n{ddl}\n")   
                        
                        # Add descriptions
                        descriptions = {
                            'students': '学生基本信息表，包含姓名、年级、出生日期、性别、地区、教材版本和学校等信息。',
                            'original_input': '原始输入数据表，存储学生的原始学习材料，如图片、文档等。',
                            'study_detail': '学习明细表(存储学生错题记录,包含字段:details->>error_type标识错误类型)',
                            'summary': '学情汇总表，按时间段记录学生的学科优势和弱点分析。'
                        }
                        
                        schemas.append(TableSchema(
                            name=table,
                            ddl=ddl,
                            description=descriptions.get(table, '')
                        ))
                        # print(f'schemas: {schemas}')
                    
                    return schemas
        except Exception as e:
            logger.error(f"Failed to get schemas: {e}", exc_info=True)
            return []

    def execute_sql(self, sql: str) -> QueryResult:
        """Executes a given SQL query and returns the result."""
        logger.info(f"Executing PostgreSQL SQL: {sql.strip()}")
        try:
            with self._get_connection() as conn:
                with conn.cursor(cursor_factory=DictCursor) as cursor:
                    cursor.execute(sql)
                    
                    if cursor.description:  # For SELECT queries
                        rows = cursor.fetchall()
                        data = [dict(row) for row in rows]
                        logger.info(f"SQL executed successfully, returned {len(data)} rows.")
                        return QueryResult(success=True, data=data, sql=sql)
                    else:  # For INSERT/UPDATE/DELETE
                        conn.commit()
                        logger.info("SQL executed successfully (no results returned).")
                        return QueryResult(success=True, data=[], sql=sql)
        except Exception as e:
            logger.error(f"PostgreSQL execution failed: {e}", exc_info=True)
            return QueryResult(success=False, data=[], error=str(e), sql=sql)

class VectorStore:
    """Handles embedding creation and retrieval of relevant schemas."""
    def __init__(self, model_name: str = "text-embedding-v4"):
        try:
            if OpenAI is None:
                raise ImportError("OpenAI package not installed. Please run 'pip install openai'.")
            self.model_name = model_name
            self.client = OpenAI(
                api_key=os.getenv("DASHSCOPE_API_KEY"),
                base_url="https://dashscope.aliyuncs.com/compatible-mode/v1"
            )
            logger.info(f"VectorStore initialized with DashScope model: {model_name}")
        except Exception as e:
            logger.error(f"Failed to initialize DashScope embedding client: {e}", exc_info=True)
            raise
        self.schemas: List[TableSchema] = []
        self.schema_embeddings: Optional[np.ndarray] = None

    def get_embeddings(self, texts: List[str]) -> np.ndarray:
        """Get embeddings using DashScope API"""
        try:
            all_embeddings = []
            for text in texts:
                response = self.client.embeddings.create(
                    model=self.model_name,
                    input=text,
                    dimensions=1024,
                    encoding_format="float"
                )
                print(f"Embedding text: {text}")
                # print(f"Embedding response: {response}")

                all_embeddings.append(response.data[0].embedding)
            return np.array(all_embeddings)
        except Exception as e:
            logger.error(f"Failed to get embeddings: {e}")
            raise

    def build_embeddings(self, schemas: List[TableSchema]):
        """Creates and stores vector embeddings for the given schemas."""
        self.schemas = schemas
        if not self.schemas:
            logger.warning("No schemas provided to build embeddings.")
            return
            
        descriptions = []
        for schema in self.schemas:
            text = f"Table: {schema.name}\nDescription: {schema.description}\nDDL: {schema.ddl}"
            descriptions.append(text)
            logger.info(f"Leo Schema for embedding:\n{text}\n")
        logger.info(f"Creating embeddings for {len(descriptions)} schemas...")
        self.schema_embeddings = self.get_embeddings(descriptions)
        logger.info(f"Built embeddings for {len(self.schemas)} schemas.")

    def retrieve_relevant_schemas(self, question: str, top_k: int = 3) -> List[TableSchema]:
        """Finds the most relevant schemas for a question using cosine similarity."""
        if self.schema_embeddings is None or not self.schemas:
            logger.warning("Embeddings not built. Cannot retrieve schemas.")
            return []
        
        question_embedding = self.get_embeddings([question])
        similarities = cosine_similarity(question_embedding, self.schema_embeddings)[0]
        
        k = min(top_k, len(self.schemas))
        top_indices = np.argsort(similarities)[-k:][::-1]
        
        relevant_schemas = [self.schemas[i] for i in top_indices]
        logger.info(f"Retrieved {len(relevant_schemas)} relevant schemas for the question.")
        for i in top_indices:
            logger.info(f"  - {self.schemas[i].name} (Similarity: {similarities[i]:.4f})")
            
        return relevant_schemas
    
    def multi_path_retrieve_schemas(self, question: str, llm_provider, top_k_per_path: int = 2) -> List[TableSchema]:
        """使用LLM分析查询维度，然后多路召回DDL表结构"""
        analysis_prompt = f"""
分析这个教学查询需要哪些数据维度，输出2-3个具体的查询方向。
每个方向要使用最直接、简洁的关键词，便于匹配数据表名称。

查询: {question}

输出格式，每行一个维度：
students
study_detail
summary
original_input
"""
        
        logger.info("LLM analyzing query dimensions...")
        print(f'analysis_prompt: {analysis_prompt}')
        dimensions_text = llm_provider._call_llm(analysis_prompt, "qwen-plus")
        print(f'dimensions_text: {dimensions_text}')
        
        dimensions = [dim.strip() for dim in dimensions_text.split('\n') if dim.strip()]
        logger.info(f"Identified {len(dimensions)} query dimensions: {dimensions}")
        
        all_retrieved_schemas = []
        seen_table_names = set()
        
        for dimension in dimensions:
            logger.info(f"Retrieving dimension: {dimension}")
            
            dimension_embedding = self.get_embeddings([dimension])
            similarities = cosine_similarity(dimension_embedding, self.schema_embeddings)[0]
            
            k = min(top_k_per_path, len(self.schemas)) # Ensure k does not exceed available schemas
            top_indices = np.argsort(similarities)[-k:][::-1] # Top-k indices
            
            for i in top_indices:
                schema = self.schemas[i]
                if schema.name not in seen_table_names:   # Avoid duplicates
                    all_retrieved_schemas.append(schema)
                    seen_table_names.add(schema.name)
                    logger.info(f"  Retrieved {schema.name} (Similarity: {similarities[i]:.4f})")
        
        logger.info(f"Multi-path retrieval completed, retrieved {len(all_retrieved_schemas)} relevant tables")
        return all_retrieved_schemas

class LLMProvider:
    """A wrapper for LLM API calls using OpenAI-compatible interface."""
    def __init__(self, llm_config: Dict[str, Any]):
        self.provider = llm_config.get("provider")
        self.models = llm_config.get("models", {})
        api_key = os.environ.get(llm_config.get("api_key_env", ""))
        
        if not api_key:
            raise ValueError(f"API key not found. Please set the {llm_config.get('api_key_env')} environment variable.")

        if OpenAI is None:
            raise ImportError("OpenAI SDK not installed. Please run 'pip install openai'.")
        
        if self.provider == "dashscope":
            self.client = OpenAI(
                api_key=api_key,
                base_url="https://dashscope.aliyuncs.com/compatible-mode/v1"
            )
        elif self.provider == "openai":
            self.client = OpenAI(api_key=api_key)
        else:
            raise ValueError(f"Unsupported LLM provider: {self.provider}")
        
        logger.info(f"LLMProvider initialized for '{self.provider}'.")

    def _call_llm(self, prompt: str, model: str) -> str:
        """Internal method to make the actual API call."""
        logger.info(f"Calling LLM ({self.provider}, model: {model})...")
        try:
            response = self.client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0
            )
            print(f"LLM response: {response}")
            return response.choices[0].message.content or ""
        except Exception as e:
            logger.error(f"LLM API call failed: {e}", exc_info=True)
            return f"Error: LLM call failed. {e}"

    def generate_sql(self, prompt: str) -> str:
        """Generates SQL from a prompt."""
        model = self.models.get("sql_generation", "qwen-plus")
        print(f'Generating SQL with model: {model}')
        # print(f'Generating SQL Prompt: {prompt}')
        sql = self._call_llm(prompt, model)
        print(f'Raw Generated SQL: {sql}')
        return sql.replace("```sql", "").replace("```", "").strip()
    
    def generate_answer(self, prompt: str) -> str:
        """Generates a natural language answer from a prompt."""
        # print(f'Generating Answer Prompt: {prompt}')
        model = self.models.get("answer_generation", "qwen-plus")
        print(f'Generating Answer with model: {model}')
        return self._call_llm(prompt, model).strip()

class NL2SQLPipeline:
    """Orchestrates the Text-to-SQL process for educational data analysis."""
    def __init__(self, config: Dict[str, Any]):
        logger.info("Initializing NL2SQL Pipeline for PostgreSQL...")
        self.db_manager = PGManager(config['database'])
        self.vector_store = VectorStore(config['embedding_model'])
        self.llm_provider = LLMProvider(llm_config=config['llm'])
        
        all_schemas = self.db_manager.get_all_schemas()
        logger.info(f"Creating embeddings for {len(all_schemas)} schemas...")
        self.vector_store.build_embeddings(all_schemas)
        
        self.sql_prompt_template = config['prompts']['sql_generation']
        self.answer_prompt_template = config['prompts']['answer_generation']
        logger.info("NL2SQL Pipeline initialized successfully.")

    def ask(self, question: str) -> Dict[str, Any]:
        """Executes the full Text-to-SQL pipeline for a given question."""
        import time
        start_time = time.time()
        
        logger.info("=" * 80)
        logger.info(f"Processing question: {question}")
        logger.info("=" * 80)

        # 1. Retrieve relevant schemas
        logger.info("Step 1: Starting multi-path vector retrieval...")
        retrieval_start = time.time()
        relevant_schemas = self.vector_store.multi_path_retrieve_schemas(question, self.llm_provider)
        retrieval_time = time.time() - retrieval_start
        
        schema_context = "\n\n".join([f"--- Table: {s.name} ---\n{s.ddl}" for s in relevant_schemas])

        # 2. Generate SQL
        logger.info("Step 2: Starting SQL generation...")
        sql_start = time.time()
        sql_prompt = self.sql_prompt_template.format(schema_context=schema_context, question=question)
        print(f'sql_prompt: {sql_prompt}')
        sql_query = self.llm_provider.generate_sql(sql_prompt)
        sql_time = time.time() - sql_start
        
        if sql_query.strip().startswith("SCHEMA_INSUFFICIENT:"):
            logger.warning("LLM rejected SQL generation due to insufficient DDL")
            
            total_time = time.time() - start_time
            return {
                'question': question,
                'relevant_schemas': [s.name for s in relevant_schemas],
                'sql_query': None,
                'data': [],
                'answer': f"当前数据库结构无法满足查询需求。{sql_query.replace('SCHEMA_INSUFFICIENT:', '').strip()}",
                'query_success': False,
                'schema_insufficient': True,
                'performance': {
                    'retrieval_time': retrieval_time,
                    'sql_generation_time': sql_time,
                    'execution_time': 0,
                    'answer_generation_time': 0,
                    'total_time': total_time
                }
            }
        
        logger.info(f"Generated SQL: {sql_query}")

        # 3. Execute SQL
        logger.info("Step 3: Starting database query execution...")
        exec_start = time.time()
        query_result = self.db_manager.execute_sql(sql_query)
        exec_time = time.time() - exec_start
        
        # 4. Generate Answer
        logger.info("Step 4: Starting natural language answer generation...")
        answer_start = time.time()
        answer = ""
        
        if query_result.success:
            if not query_result.data:
                answer = "查询成功但没有匹配的数据。"
            else:
                data_summary = self._create_data_summary(query_result.data)
                answer_prompt = self.answer_prompt_template.format(
                    question=question,
                    sql_query=sql_query,
                    data_summary=data_summary
                )
                print(f'answer_prompt: {answer_prompt}')
                answer = self.llm_provider.generate_answer(answer_prompt)
        else:
            answer = f"查询执行出错: {query_result.error}"
        
        answer_time = time.time() - answer_start
        total_time = time.time() - start_time
        
        return {
            "question": question,
            "relevant_schemas": [s.name for s in relevant_schemas],
            "sql_query": query_result.sql,
            "query_success": query_result.success,
            "query_error": query_result.error,
            "data": query_result.data,
            "answer": answer,
            "performance": {
                "retrieval_time": retrieval_time,
                "sql_generation_time": sql_time,
                "execution_time": exec_time,
                "answer_generation_time": answer_time,
                "total_time": total_time
            }
        }

    def _create_data_summary(self, data: List[Dict[str, Any]]) -> str:
        """创建数据摘要，避免泄露敏感信息"""
        if not data:
            return "没有可用数据。"

        all_columns = set()
        column_types = {}
        
        for record in data:
            all_columns.update(record.keys())
            for key, value in record.items():
                if key not in column_types:
                    column_types[key] = type(value).__name__

        summary_data = {
            "total_records": len(data),
            "columns_info": {
                col: column_types.get(col, "unknown") 
                for col in sorted(list(all_columns))
            },
            "data_structure": "教学数据分析结果",
            "privacy_note": "实际数据值已省略"
        }
        
        return json.dumps(summary_data, indent=2, ensure_ascii=False)

# --- Demo Execution ---------------------------------------------------------
def run_demo():
    """Sets up the pipeline and runs a demo with educational questions."""
    print("=" * 60)
    print("📚 教育数据分析 NL2SQL 演示 (PostgreSQL)")
    print("=" * 60)

    try:
        api_key_env = CONFIG['llm']['api_key_env']
        if not os.environ.get(api_key_env):
            print(f"\n❌ 错误：环境变量 '{api_key_env}' 未设置。")
            print("请设置您的API密钥以继续。")
            return
    
        pipeline = NL2SQLPipeline(CONFIG)
        
        demo_questions = [
            # {
            #     "question": "请查找张三的8月的学情汇总，只返回details字段？",
            #     "description": "查询学生学情汇总"
            # }
            {
                "question": "请查找张三的错题详情？",
                "description": "查询学生错题明细"
            }
            # {
            #     "question": "请查找张三的学习明细？",
            #     "description": "查询学生学习明细"
            # }
            # {
            #     "question": "张三在学情汇总表有多少条记录？",
            #     "description": "查询学生学情汇总数量"
            # },
            # {
            #     "question": "学习明细表里有多少条记录？",
            #     "description": "查询学生错题数量"
            # }
            # {
            #     "question": "比较不同地区(华东、华北、华南)学生使用不同教材版本的学习效果差异",
            #     "description": "🌍 地区教材效果对比"
            # },
            # {
            #     "question": "找出在几何知识点上表现最差的学生，并分析他们的错题模式",
            #     "description": "🔍 特定知识点学生分析"
            # }
        ]
        
        print("\n教育数据分析演示开始 - 展示复杂教学场景的多表查询分析")
        print("数据库规模: 4个核心教学数据表")
        print("=" * 90)
        
        import time
        total_start_time = time.time()
        demo_stats = {
            "total_questions": len(demo_questions),
            "successful_queries": 0,
            "failed_queries": 0,
            "total_tables_used": set(),
            "total_execution_time": 0,
            "performance_breakdown": [],
            "schema_insufficient_queries": 0
        }
        
        for i, demo in enumerate(demo_questions, 1):
            question = demo["question"]
            description = demo["description"]
            
            print(f"\n[分析任务 {i}/{len(demo_questions)}] {description}")
            print(f"问题: {question}")
            print("处理中...")
            
            demo_start_time = time.time()
            result = pipeline.ask(question)
            demo_time = time.time() - demo_start_time
            
            demo_stats["total_execution_time"] += demo_time
            demo_stats["total_tables_used"].update(result['relevant_schemas'])
            
            if result['query_success']:
                demo_stats["successful_queries"] += 1
            elif result.get('schema_insufficient', False):
                demo_stats["failed_queries"] += 1
                demo_stats["schema_insufficient_queries"] += 1
            else:
                demo_stats["failed_queries"] += 1
            
            demo_stat = {
                "demo_number": i,
                "description": description,
                "success": result['query_success'],
                "schema_insufficient": result.get('schema_insufficient', False),
                "tables_used": len(result['relevant_schemas']),
                "table_names": result['relevant_schemas'],
                "data_records": len(result['data']) if result['query_success'] else 0,
                "execution_time": demo_time,
                "performance": result.get('performance', {})
            }
            demo_stats["performance_breakdown"].append(demo_stat)
            
            print("-" * 80)
            print("分析结果:")
            print("-" * 80)
            print(f"AI识别的相关表: {', '.join(result['relevant_schemas'])}")
            print(f"\n生成的SQL查询:")
            print(f"```sql")
            print(result['sql_query'])
            print(f"```")
            
            if result['query_success']:
                print(f"\n查询执行成功，返回 {len(result['data'])} 条结果")
                print(f"\nAI分析:")
                print("=" * 50)
                print(result['answer'])
                print("=" * 50)
                
                if result['data']:
                    print(f"\n关键数据摘要 ({len(result['data'])} 条记录):")
                    for idx, record in enumerate(result['data'][:3], 1):
                        print(f"  {idx}. {record}")
                    if len(result['data']) > 3:
                        print(f"  ... 和 {len(result['data']) - 3} 条更多记录")
            else:
                print(f"\n查询执行遇到问题: {result.get('query_error', result.get('answer', '未知错误'))}")
            
            print("=" * 90)
            if i < len(demo_questions):
                print("准备下一个分析任务... (2秒)")
                time.sleep(2)
        
        total_demo_time = time.time() - total_start_time
        
        print("\n" + "=" * 60)
        print("教育数据分析演示完成!")
        print("=" * 60)
        
        print("\nAI展示了强大的教学数据分析能力:")
        print("   ✓ 智能关联4个核心教学数据表")
        print("   ✓ 自动生成复杂教学分析的SQL查询")
        print("   ✓ 多维度数据聚合和深度洞察")
        
        print(f"\n技术统计:")
        print(f"   分析任务数量: {demo_stats['total_questions']} 个教学场景")
        print(f"   查询成功率: {demo_stats['successful_queries']}/{demo_stats['total_questions']} ({demo_stats['successful_queries']/demo_stats['total_questions']*100:.1f}%)")
        print(f"   涉及数据表: {len(demo_stats['total_tables_used'])} 个教学核心表")
        print(f"   平均表关联数: {sum(len(stat['table_names']) for stat in demo_stats['performance_breakdown'])/demo_stats['total_questions']:.1f} 表/查询")
        print(f"   总时间: {total_demo_time:.1f}秒 (平均 {total_demo_time/demo_stats['total_questions']:.1f}秒/问题)")
        
        print("\n这是新一代教育数据分析NL2SQL系统的真实能力!")

    except (ValueError, ImportError) as e:
        print(f"\n❌ 设置过程中发生错误: {e}")
    except Exception as e:
        logger.error("An unexpected error occurred during the demo.", exc_info=True)
        print(f"\n❌ 发生意外错误: {e}")

if __name__ == "__main__":
    run_demo()