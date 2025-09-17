from pywebio import start_server
from pywebio.input import *
from pywebio.output import *
from pywebio.session import set_env, run_js, eval_js   
from pywebio.pin import *
import time
import psycopg2
from psycopg2 import sql

# 数据库配置
DB_CONFIG = {
    'host': 'localhost',
    'database': 'learning_db',
    'user': 'learning_user',
    'password': '123456',
    'port': '5432'
}

def connect_db():
    """连接PostgreSQL数据库"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        put_error(f"数据库连接失败: {e}")
        return None

def check_user(username, password):
    """检查用户是否存在"""
    conn = connect_db()
    if not conn:
        return False
    
    try:
        with conn.cursor() as cursor:
            query = sql.SQL("SELECT password, name FROM students WHERE student_id = %s")
            cursor.execute(query, (username,))
            result = cursor.fetchone()
            
            if result and result[0] == password:
                put_text(f"欢迎登录系统: {result[1]}")
                run_js(f"sessionStorage.setItem('userNAME', '{result[1]}');")
                return True
            return False
    except Exception as e:
        put_error(f"查询出错: {e}") 
        return False
    finally:
        conn.close()

def login_page():
    """登录页面"""
    set_env(title="用户登录")
    
    put_text("欢迎使用系统，请登录").style('font-size: 20px; color: #333;')
    
    login_info = input_group(
        "登录信息",
        [
            input("用户名", name="student_id", required=True),
            input("密码", name="password", type=PASSWORD, required=True)
        ]
    )
    
    username = login_info['student_id']
    password = login_info['password']
    
    if check_user(username, password):
        put_success("登录成功！")
        toast(f"欢迎, {username}!")
        # 这里可以跳转到主页面或其他操作
        put_text("3秒后将自动跳转到主页...")
        time.sleep(3)
        run_js(f"sessionStorage.setItem('userID', '{login_info['student_id']}');")
     
        run_js('window.location.href = "/?app=%2Fhome";')
    else:
        put_error("用户名或密码错误！")

# 定义各个页面内容
def home_page():
    put_markdown("""
    # 欢迎来到首页
    
    这是一个使用PyWebIO创建的多页面应用示例。
    
    - 点击左侧菜单栏可以切换不同页面
    - 右侧内容区域会显示对应的页面内容
    """)
    put_image('https://via.placeholder.com/600x300?text=Home+Page', width='100%')
    user_id = eval_js("sessionStorage.getItem('userID');")
    user_name = eval_js("sessionStorage.getItem('userNAME');")
    put_text(f"用户ID: {user_id}")
    put_text(f"学生姓名: {user_name}")

def product_page():
    put_markdown("""
    # 产品介绍
    
    这是我们公司的产品系列：
    
    1. 产品A - 高性能解决方案
    2. 产品B - 经济型选择
    3. 产品C - 定制化服务
    """)
    
    put_table([
        ['产品名称', '价格', '库存'],
        ['产品A', '$199', '100'],
        ['产品B', '$99', '250'],
        ['产品C', '$299', '50']
    ])

def user_page():
    put_markdown("""
    # 用户管理
    
    在这里可以管理用户账户：
    """)
    
    put_input('search', label='搜索用户', placeholder='输入用户名或邮箱')
    put_buttons(['查询', '重置'], onclick=lambda btn: toast(f'点击了{btn}按钮'))
    
    put_table([
        ['ID', '用户名', '邮箱', '操作'],
        [1, 'user1', 'user1@example.com', put_buttons(['编辑', '删除'], small=True)],
        [2, 'user2', 'user2@example.com', put_buttons(['编辑', '删除'], small=True)],
        [3, 'user3', 'user3@example.com', put_buttons(['编辑', '删除'], small=True)]
    ])

def settings_page():
    put_markdown("""
    # 系统设置
    
    配置您的应用程序设置：
    """)
    
    put_checkbox('options', options=['启用通知', '自动更新', '暗黑模式'], label='偏好设置')
    put_select('theme', options=['默认', '蓝色', '绿色', '红色'], label='主题颜色')
    put_buttons(['保存设置', '恢复默认'], onclick=lambda btn: toast(f'{btn}成功'))

def about_page():
    put_markdown("""
    # 关于我们
    
    ## 公司简介
    
    我们是一家致力于提供优质软件解决方案的技术公司。
    
    ## 联系方式
    
    - 电话: 123-456-7890
    - 邮箱: contact@example.com
    - 地址: 某市某区某街道123号
    """)
    
    put_link('访问我们的网站', url='https://example.com', new_window=True)

def main():
    clear()
    # 设置页面标题和样式
    set_env(title='PyWebIO多页面应用', output_max_width='3000px')
    
    # 自定义CSS样式
    style = """
    .container {
        display: flex;
        width: 100%;
    }
    .sidebar {
        width: 200px;
        background-color: #f0f0f0;
        padding: 10px;
        border-right: 1px solid #ddd;
    }
    .content {
        flex: 1;
        padding: 20px;
    }
    .menu-item {
        padding: 10px;
        margin: 5px 0;
        cursor: pointer;
        border-radius: 4px;
    }
    .menu-item:hover {
        background-color: #e0e0e0;
    }
    .menu-item.active {
        background-color: #4CAF50;
        color: white;
    }
    """
    put_html(f'<style>{style}</style>')
    
    # 定义页面内容
    pages = {
        '首页': home_page,
        '产品介绍': product_page,
        '用户管理': user_page,
        '设置': settings_page,
        '关于': about_page
    }
    
    # 初始显示首页
    current_page = ['首页']  # 使用列表以便在嵌套函数中修改
    
    def switch_page(page_name):
        current_page[0] = page_name
        
        # 更新菜单按钮样式
        clear('sidebar')
        for name in pages.keys():
            button_style = 'success' if name == page_name else 'secondary'
            put_button(name, onclick=lambda p=name: switch_page(p), 
                      scope='sidebar', color=button_style)
        
        # 更新内容区域
        clear('content')
        with use_scope('content'):
            pages[page_name]()
    
    # 创建布局
    with use_scope('main', clear=True):
        put_row([
            put_scope('sidebar').style('width:200px'),
            put_scope('content')
        ])
        
        # 左侧菜单栏
        with use_scope('sidebar'):
            for page_name in pages.keys():
                button_style = 'success' if page_name == current_page[0] else 'secondary'
                put_button(page_name, onclick=lambda p=page_name: switch_page(p), 
                          color=button_style)
        
        # 右侧内容区域
        with use_scope('content'):
            pages[current_page[0]]()


if __name__ == '__main__':
    start_server({
        '/': login_page,
        '/home': lambda: main()
    }, port=8080, debug=False,auto_open_webbrowser=False)