### 1. 申请DASHSCOPE_API_KEY
    https://bailian.console.aliyun.com/?tab=model#/efm/model_experience_center/text
### 2. Docker 安装PGSQL
    - docker run -d --name pg_custom -e POSTGRES_PASSWORD=123456 -p 5433:5432 -v
    - 启动postgres :docker exec -it pg_custom psql -U postgres 
    - 如果你有多个DB,可以切换到你的DB:\c learning_db
### 3. 安装pip依赖，requirements.txt
### 4. python文件功能描述
        - image_analyzer:调用千问视觉模型从PDF或者图片提取错题
        - input_split_analysis: 调用image_analyzer将图片错题保存再study_detail
        - summary_generation: function calling 从study_detail获取错题详情，调用大模型生成summary并存入summary 表中
        - rag_search: 从details_embedding字段相似度搜索和问题匹配的错题
        - free_chat: texttosql 查询
        - MCP client/server：功能和summary_generation一样，改为MCP studio模式
        
