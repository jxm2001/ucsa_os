# ucsa_os
安装依赖
```
pip install -r requirements.txt
```
启动服务，3000 端口为 grafana，默认账号密码均为 admin
9090 端口为 Prometheus
9100 端口为  node-exporter 
```
docker compose up -d
python main.py
```
