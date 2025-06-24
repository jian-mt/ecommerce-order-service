FROM python:3.11-slim

WORKDIR /app

# 安装依赖
COPY requirements.txt .
RUN pip install -i https://pypi.tuna.tsinghua.edu.cn/simple --no-cache-dir -r requirements.txt

# 复制应用代码
COPY main.py .

# 暴露端口
EXPOSE 8000

# 启动应用
CMD ["python", "main.py"]
