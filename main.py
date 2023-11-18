from prometheus_client import start_http_server, Gauge
import psutil
import time

# 创建一个名为cpu_usage的Gauge指标
cpu_usage = Gauge('cpu_usage', 'CPU usage in percentage')

# 启动一个HTTP服务器，监听8000端口
start_http_server(8000)

# 循环获取CPU使用率并更新指标
while True:
    # 获取CPU使用率
    usage = psutil.cpu_percent(interval=1)
    # 设置Gauge指标的值
    cpu_usage.set(usage)
    # 等待1秒
    time.sleep(1)
