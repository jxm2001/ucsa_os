from prometheus_client import start_http_server, Gauge, Counter
import pandas as pd
import os
import time
import re

class Node_Info:
    def __init__(self, cpu_info, stat_info, mem_info, disk_size_info, disk_io_info, net_info, proc_info):
        self.cpu_info = cpu_info 
        self.stat_info = stat_info 
        self.mem_info = mem_info
        self.disk_size_info = disk_size_info
        self.disk_io_info = disk_io_info
        self.net_info = net_info
        self.proc_info = proc_info

def convert_to_bytes(s):
    units = {'K': 1024, 'M': 1024**2, 'G': 1024**3, 'T': 1024**4}
    size, unit = s[:-1], s[-1]
    return int(float(size) * units[unit])

def get_node_info():
    # cpu info
    cpu_info = {}
    cpu_info['cpu_num'] = int(os.popen("grep 'processor' /proc/cpuinfo | sort -u | wc -l").read())
    cpu_info['core_num'] = int(os.popen("grep 'core id' /proc/cpuinfo | sort -u | wc -l").read())
    # stat info
    stat_info = {}
    with open('/proc/stat', 'r') as file:
        line = file.readline().split()
        stat_info['cpu_time'] = [int(line[i]) for i in range(1, len(line))]
        for line in file:
            if line.startswith('procs_running'):
                stat_info['procs_running'] = int(line.split()[1])
            elif line.startswith('procs_blocked'):
                stat_info['procs_blocked'] = int(line.split()[1])
    # mem info
    mem_info = {}
    with open('/proc/meminfo', 'r') as file:
        for line in file:
            parts = line.split(':')
            key = parts[0].strip()
            value = parts[1].strip().split()[0]
            mem_info[key] = int(value)
    # disk size info
    df = pd.read_csv(os.popen("df -h"), delim_whitespace=True)
    selected_rows = df[df['Filesystem'].str.startswith('/dev/sd') |
                       df['Filesystem'].str.startswith('/dev/nvme') |
                       df['Filesystem'].str.startswith('/dev/vd')]
    disk_size_info = selected_rows[['Size', 'Avail']]
    disk_size_info.loc[:, 'Size'] = disk_size_info['Size'].apply(convert_to_bytes)
    disk_size_info.loc[:, 'Avail'] = disk_size_info['Avail'].apply(convert_to_bytes)
    # disk io info
    disk_io_info = {'read_count' : 0, 'write_count' : 0}
    with open('/proc/diskstats', 'r') as file:
        for line in file:
            fields = line.strip().split()
            device_name = fields[2]
            if re.match(r'(sd[a-z]|nvme\d+n\d+|vd[a-z])$', device_name):
                read_count = int(fields[3])
                write_count = int(fields[7])
                disk_io_info['read_count'] += read_count
                disk_io_info['write_count'] += write_count 
    # net info
    net_info = {'receive_bytes' : 0, 'transmit_bytes' : 0}
    with open('/proc/net/dev', 'r') as f:
        data = f.readlines()
        for line in data[2:]:
            parts = line.split()
            interface = parts[0].strip(':')
            receive_bytes = int(parts[1])
            transmit_bytes = int(parts[9])
            net_info['receive_bytes'] += receive_bytes
            net_info['transmit_bytes'] += transmit_bytes
    # proc info
    output_lines = os.popen("ps axo pid,rss,vsz,comm").read().split('\n')
    data = [line.split() for line in output_lines[1:] if line]
    proc_info = pd.DataFrame(data, columns=["PID", "RSS", "VSZ", "COMMAND"])
    return Node_Info(cpu_info, stat_info, mem_info, disk_size_info, disk_io_info, net_info, proc_info)

cpu_num = Gauge('cpu_num', 'CPU Number')
core_num = Gauge('core_num', 'Core Number')
cpu_time = Counter('node_cpu_seconds_total', 'CPU idle time', ['mode'])
procs_blocked_count = Gauge('node_procs_blocked', 'Procs blocked number')
procs_running_count = Gauge('node_procs_running', 'Procs running number')
mem_buffer_size = Gauge('node_memory_Buffers_bytes', 'Memory buffers size')
mem_cached_size = Gauge('node_memory_Cached_bytes', 'Memory cached size')
mem_free_size = Gauge('node_memory_MemFree_bytes', 'Memory free size')
mem_total_size = Gauge('node_memory_MemTotal_bytes', 'Memory total size')
filesystem_avail_size = Gauge('node_filesystem_avail_bytes', 'Filesystem avail size')
filesystem_size = Gauge('node_filesystem_size_bytes', 'Filesystem size')
disk_reads_count = Counter('node_disk_reads_completed_total', 'Disk reads completed count')
disk_writes_count = Counter('node_disk_writes_completed_total', 'Disk writes completed count')
network_receive_size = Counter('node_network_receive_bytes_total', 'Network receive bytes')
network_transmit_size = Counter('node_network_transmit_bytes_total', 'Network transmit bytes')

proc_vm_size = Gauge('node_process_virtual_memory_bytes', 'Process virtual memory size', ['pid'])
cur_info = get_node_info()

start_http_server(8000)
cpu_time.labels(mode='all').inc(sum(cur_info.stat_info['cpu_time']))
cpu_time.labels(mode='idle').inc(cur_info.stat_info['cpu_time'][3])
disk_reads_count.inc(cur_info.disk_io_info['read_count'])
disk_writes_count.inc(cur_info.disk_io_info['write_count'])
network_receive_size.inc(cur_info.net_info['receive_bytes'])
network_transmit_size.inc(cur_info.net_info['transmit_bytes'])

while True:
    last_info = cur_info
    cur_info = get_node_info()
    cpu_num.set(cur_info.cpu_info['cpu_num'])
    core_num.set(cur_info.cpu_info['core_num'])
    cpu_time.labels(mode='all').inc(sum(cur_info.stat_info['cpu_time']) - sum(last_info.stat_info['cpu_time']))
    cpu_time.labels(mode='idle').inc(cur_info.stat_info['cpu_time'][3] - last_info.stat_info['cpu_time'][3])
    procs_blocked_count.set(cur_info.stat_info['procs_blocked'])
    procs_running_count.set(cur_info.stat_info['procs_running'])
    mem_buffer_size.set(cur_info.mem_info['Buffers'])
    mem_cached_size.set(cur_info.mem_info['Cached'])
    mem_free_size.set(cur_info.mem_info['MemFree'])
    mem_total_size.set(cur_info.mem_info['MemTotal'])
    filesystem_avail_size.set(cur_info.disk_size_info['Avail'].sum())
    filesystem_size.set(cur_info.disk_size_info['Size'].sum())
    disk_reads_count.inc(cur_info.disk_io_info['read_count'] - last_info.disk_io_info['read_count'])
    disk_writes_count.inc(cur_info.disk_io_info['write_count'] - last_info.disk_io_info['write_count'])
    network_receive_size.inc(cur_info.net_info['receive_bytes'] - last_info.net_info['receive_bytes'])
    network_transmit_size.inc(cur_info.net_info['transmit_bytes'] - last_info.net_info['transmit_bytes'])
    for index, row in cur_info.proc_info.iterrows():
        proc_vm_size.labels(pid=row['PID']).set(row['VSZ'])
    time.sleep(1)
