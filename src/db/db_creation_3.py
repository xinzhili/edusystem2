import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sys


def initialize_database():
    # 管理员连接配置（创建数据库）
    admin_config = {
        "user": "postgres",
        "password": "123456",  # 替换为您的PostgreSQL管理员密码
        "host": "localhost",
        "port": "5433",
        "dbname": "postgres"  # 明确指定连接到默认数据库
    }

    # 数据库配置
    db_config = {
        "dbname": "learning_db", # 替换为您的数据库名字
        "user": "postgres",
        "password": "123456",  # 替换为您的数据库密码
        "host": "localhost",
        "port": "5433"
    }

    try:
        # 步骤1: 创建数据库（使用自动提交）
        print("步骤1: 创建数据库")
        # 连接到默认的postgres数据库
        with psycopg2.connect(**admin_config) as conn:
            # 设置连接为自动提交模式
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            with conn.cursor() as cursor:
                try:
                    # 检查数据库是否已存在
                    cursor.execute("SELECT 1 FROM pg_database WHERE datname='learning_db'")
                    exists = cursor.fetchone()

                    if not exists:
                        # 关键修复：使用原始字符串执行避免隐式事务
                        cursor.execute("COMMIT;")  # 确保结束任何可能的事务
                        cursor.execute("CREATE DATABASE learning_db")
                        print("✅ 数据库创建成功: learning_db")
                    else:
                        print("ℹ️ 数据库已存在，跳过创建")
                except psycopg2.errors.DuplicateDatabase:
                    print("ℹ️ 数据库已存在，跳过创建")
                except Exception as e:
                    print(f"❌ 创建数据库失败: {str(e)}")
                    raise

        # 步骤2: 在新数据库中创建扩展和表
        print("\n步骤2: 创建数据库表结构")
        with psycopg2.connect(**db_config) as conn:
            conn.autocommit = True  # 确保自动提交
            with conn.cursor() as cursor:
                try:
                    # 启用pgvector扩展
                    cursor.execute("CREATE EXTENSION IF NOT EXISTS vector")
                    print("✅ 启用pgvector扩展")
                except Exception as e:
                    print(f"❌ 创建pgvector扩展失败: {str(e)}")
                    raise

                # 创建学生表
                try:
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS students (
                            student_id SERIAL PRIMARY KEY,
                            name VARCHAR(20) NOT NULL,
                            grade SMALLINT NOT NULL,
                            date_of_birth DATE NOT NULL,
                            gender TEXT NOT NULL,
                            region VARCHAR(20) NOT NULL,
                            textbook_version VARCHAR(20) NOT NULL,
                            school VARCHAR(30) NOT NULL,
                            photo BYTEA NOT NULL,
                            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    print("✅ 创建学生表")
                except Exception as e:
                    print(f"❌ 创建学生表失败: {str(e)}")
                    raise

                # 创建原始输入表
                try:
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS original_input (
                            original_input_id SERIAL PRIMARY KEY,
                            student_id INT REFERENCES students(student_id) ON DELETE SET NULL,
                            content BYTEA NOT NULL,
                            content_hash VARCHAR(32) UNIQUE NOT NULL,
                            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    print("✅ 创建原始输入表")
                except Exception as e:
                    print(f"❌ 创建原始输入表失败: {str(e)}")
                    raise

                # 创建错题表
                try:
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS study_detail (
                            study_detail_id SERIAL PRIMARY KEY,
                            student_id INT REFERENCES students(student_id) ON DELETE CASCADE,
                            original_input_id INT REFERENCES original_input(original_input_id) ON DELETE SET NULL,
                            details JSONB NOT NULL,
                            details_embedding vector,
                            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                            #  details = {
                                # 'student_question': '题目内容',
                                # 'student_answer': '学生答案',
                                # 'correct_answer': '正确答案',
                                # 'error_type': '计算错误',
                                # 'analysis': '错误分析',
                                # ‘subject:’ ‘数学’,
                                # ‘knowledge_grade:’ ‘知识点相关年级’,
                                # 'knowledge_points': ['知识点1','知识点2'],
                                # 'difficulty': 3
                                # ‘true_false_flag’: false
                            # }
                    print("✅ 创建学习明细表")
                except Exception as e:
                    print(f"❌ 创建学习明细表失败: {str(e)}")
                    raise

                # 创建学情表
                try:
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS summary (
                            summary_id SERIAL PRIMARY KEY,
                            student_id INT REFERENCES students(student_id) ON DELETE CASCADE,
                            grade SMALLINT NOT NULL,
                            from_date DATE NOT NULL,
                            to_date DATE NOT NULL,
                            subject TEXT NOT NULL,
                            details JSONB NOT NULL,
                            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                            # details = {
                            #     'strength': 'xxx',
                            #     'weakness': 'xxx',
                            #     'progress': 'xxx', -- 进步情况
                            #     'remarks': 'xxx',}

                    print("✅ 创建学情表")
                except Exception as e:
                    print(f"❌ 创建学情表失败: {str(e)}")
                    raise

                # 创建向量索引
                # try:
                #     cursor.execute("""
                #         CREATE INDEX IF NOT EXISTS idx_questions_embedding
                #         ON questions USING ivfflat (embedding vector_cosine_ops)
                #         WITH (lists = 100)
                #     """)
                #     print("✅ 创建向量索引")
                # except Exception as e:
                #     print(f"⚠️ 创建向量索引失败: {str(e)}")
                #     print("这可能是因为表中还没有数据，可以在有数据后重新创建索引")
                print("ℹ️ 向量索引将在有数据后手动创建")

        # 步骤3: 创建应用用户（可选）
        print("\n步骤3: 创建应用用户")
        with psycopg2.connect(**admin_config) as conn:
            # 设置连接为自动提交模式
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            conn.autocommit = True

            with conn.cursor() as cursor:
                try:
                    # 检查用户是否已存在
                    cursor.execute("SELECT 1 FROM pg_roles WHERE rolname='learning_user'") # 修改为你要创建的用户名
                    exists = cursor.fetchone()

                    if not exists:
                        cursor.execute("CREATE USER learning_user WITH PASSWORD '123456'")  # 修改为你要的用户名和密码
                        print("✅ 应用用户创建成功: learning_user")
                    else:
                        print("ℹ️ 应用用户已存在，跳过创建")
                except psycopg2.errors.DuplicateObject:
                    print("ℹ️ 应用用户已存在，跳过创建")
                except Exception as e:
                    print(f"❌ 创建应用用户失败: {str(e)}")

        # 步骤4: 授予权限
        print("\n步骤4: 授予权限")
        with psycopg2.connect(**db_config) as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                try:
                    # 授予数据库权限
                    cursor.execute("GRANT ALL PRIVILEGES ON DATABASE learning_db TO learning_user")

                    # 授予表权限
                    for table in ["students", "original_input", "study_detail", "summary"]:
                        cursor.execute(f"GRANT ALL PRIVILEGES ON TABLE {table} TO learning_user")

                    # 授予序列权限
                    cursor.execute("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO learning_user")

                    print("✅ 权限授予成功")
                except Exception as e:
                    print(f"❌ 权限授予失败: {str(e)}")

        print("\n🎉 数据库初始化完成")

    except Exception as e:
        print(f"\n❌ 数据库初始化失败: {str(e)}")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(f"错误发生在: 行号 {exc_tb.tb_lineno}")
        raise e


if __name__ == "__main__":
    print("=" * 50)
    print("开始数据库初始化")
    print("=" * 50)

    initialize_database()

    print("\n" + "=" * 50)
    print("数据库初始化完成")
    print("=" * 50)