from prometheus_client import start_http_server, Gauge, Counter
import pandas as pd
import os
import time
import re
import subprocess
import signal
import sys

arguments = sys.argv

enablePerf=True
for arg in arguments[1:]:
    if arg.startswith("--perf="):
        if arg.split("=")[1] == "off":
            enablePerf=False

class Node_Info:
    def __init__(self, cpu_info, stat_info, mem_info, disk_size_info, disk_io_info, net_info, proc_info, perf_info):
        self.cpu_info = cpu_info 
        self.stat_info = stat_info 
        self.mem_info = mem_info
        self.disk_size_info = disk_size_info
        self.disk_io_info = disk_io_info
        self.net_info = net_info
        self.proc_info = proc_info
        self.perf_info = perf_info

def convert_unit_to_bytes(unit):
    if unit == 'B':
        return 1
    elif unit == 'KiB' or unit == 'K':
        return 1024
    elif unit == 'MiB' or unit == 'M':
        return 1024**2
    elif unit == 'GiB' or unit == 'G':
        return 1024**3
    elif unit == 'TiB' or unit == 'T':
        return 1024**4
    else:
        raise ValueError("Invalid unit")

def convert_to_bytes(s):
    s = s.strip()
    size, unit = s[:-1], s[-1]
    return int(float(size) * convert_unit_to_bytes(unit))

def get_filesystem_type(filesystem):
    output = os.popen(f"blkid -s TYPE -o value {filesystem}").read().strip()
    return output

def get_node_info():
    # cpu info
    cpu_info = {}
    with os.popen("lscpu") as file:
        for line in file:
            line = line.strip()
            if line.startswith('Thread(s)'):
                cpu_info['threadPerCore'] = int(line.split(':')[1])
            elif line.startswith('Core(s)'):
                cpu_info['corePerSocket'] = int(line.split(':')[1])
            elif line.startswith('Socket(s)'):
                cpu_info['socket_num'] = int(line.split(':')[1])
            elif line.startswith('L1d'):
                parts = line.split()
                cpu_info['l1d_size'] = int(parts[2]) * convert_unit_to_bytes(parts[3])
            elif line.startswith('L1i'):
                parts = line.split()
                cpu_info['l1i_size'] = int(parts[2]) * convert_unit_to_bytes(parts[3])
            elif line.startswith('L2'):
                parts = line.split()
                cpu_info['l2_size'] = int(parts[2]) * convert_unit_to_bytes(parts[3])
            elif line.startswith('L3'):
                parts = line.split()
                cpu_info['l3_size'] = int(parts[2]) * convert_unit_to_bytes(parts[3])
    # stat info
    stat_info = {}
    with open('/proc/stat', 'r') as file:
        line = file.readline().split()
        stat_info['cpu_time'] = [int(line[i]) / 100 for i in range(1, len(line))]
        for line in file:
            if line.startswith('procs_running'):
                stat_info['procs_running'] = int(line.split()[1])
            elif line.startswith('procs_blocked'):
                stat_info['procs_blocked'] = int(line.split()[1])
        stat_info['procs_total'] = int(os.popen("ps -e | wc -l").read())
    output = os.popen("uptime").read().strip().split()
    stat_info['uptime_1'] = float(output[-3].replace(',',''))
    stat_info['uptime_5'] = float(output[-2].replace(',',''))
    stat_info['uptime_15'] = float(output[-1].replace(',',''))
    # mem info
    mem_info = {}
    with open('/proc/meminfo', 'r') as file:
        for line in file:
            parts = line.split(':')
            key = parts[0].strip()
            value = parts[1].strip().split()[0]
            mem_info[key] = int(value) * 1024
    # disk size info
    df = pd.read_csv(os.popen("df -h"), delim_whitespace=True)
    selected_rows = df[df['Filesystem'].str.startswith('/dev/sd') |
                       df['Filesystem'].str.startswith('/dev/nvme') |
                       df['Filesystem'].str.startswith('/dev/vd')]
    disk_size_info = selected_rows[['Filesystem', 'Size', 'Avail', 'Mounted']]
    disk_size_info.loc[:, 'Size'] = disk_size_info['Size'].apply(convert_to_bytes)
    disk_size_info.loc[:, 'Avail'] = disk_size_info['Avail'].apply(convert_to_bytes)
    disk_size_info = disk_size_info.copy()
    disk_size_info['Type'] = disk_size_info['Filesystem'].apply(get_filesystem_type)
    # disk io info
    disk_io_info = {}
    with open('/proc/diskstats', 'r') as file:
        for line in file:
            fields = line.strip().split()
            device_name = fields[2]
            if re.match(r'(sd[a-z]|nvme\d+n\d+|vd[a-z])$', device_name):
                read_count = int(fields[3])
                read_size = int(fields[5]) * 512
                write_count = int(fields[7])
                write_size = int(fields[9]) * 512
                disk_io_info[device_name] = {'read_count' : read_count, 'read_size' : read_size, 'write_count' : write_count, 'write_size' : write_size}
    # net info
    net_info = {}
    with open('/proc/net/dev', 'r') as f:
        data = f.readlines()
        for line in data[2:]:
            parts = line.split()
            interface = parts[0].strip(':')
            receive_bytes = int(parts[1])
            transmit_bytes = int(parts[9])
            net_info[interface] = {'receive_bytes' : receive_bytes, 'transmit_bytes' : transmit_bytes}
    # proc info
    output_lines = os.popen("ps axo pid,rss,vsz,comm").read().split('\n')
    data = [line.split(maxsplit=3) for line in output_lines[1:] if line]
    proc_info = pd.DataFrame(data, columns=["PID", "RSS", "VSZ", "COMMAND"])
    # perf info
    if enablePerf:
        try:
            proc = subprocess.Popen(['perf', 'stat', '-M', 'Cache_Misses,CPI'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            time.sleep(1)
            proc.send_signal(signal.SIGINT)
            stdout, stderr = proc.communicate()
            perf_output = stderr.decode()
            for line in perf_output.split('\n'):
                if "L1MPKI" in line:
                    l1mpki = float(line.split()[3])
                elif "L2MPKI" in line and "PKI_All" not in line:
                    l2mpki = float(line.split()[3])
                elif "L3MPKI" in line:
                    l3mpki = float(line.split()[3])
                elif "CPI" in line:
                    cpi = float(line.split()[3])
            perf_info = {
                "cache_L1_miss_rate": l1mpki/1000,
                "cache_L2_miss_rate": l2mpki/1000,
                "cache_L3_miss_rate": l3mpki/1000,
                "system_ipc": 1/cpi
            }
        except subprocess.CalledProcessError as e:
            print("Error running perf stat:", e)
    else:
        perf_info = {
            "cache_L1_miss_rate": 0.1,
            "cache_L2_miss_rate": 0.05,
            "cache_L3_miss_rate": 0.01,
            "system_ipc": 0.25
        }
    return Node_Info(cpu_info, stat_info, mem_info, disk_size_info, disk_io_info, net_info, proc_info, perf_info)

cpu_num = Gauge('cpu_num', 'CPU Number')
core_num = Gauge('core_num', 'Core Number')
l1_size = Gauge('cache_L1_max_size_bype', 'L1 cache size')
l2_size = Gauge('cache_L2_max_size_bype', 'L2 cache size')
l3_size = Gauge('cache_L3_max_size_bype', 'L3 cache size')
cpu_time = Counter('node_cpu_seconds_total', 'CPU idle time', ['mode'])
procs_blocked_count = Gauge('node_procs_blocked', 'Procs blocked number')
procs_running_count = Gauge('node_procs_running', 'Procs running number')
procs_total_count = Gauge('node_procs_total', 'Procs total number')
load_averange = Gauge('load_averange', 'Load Averange', ['time'])
mem_buffer_size = Gauge('node_memory_Buffers_bytes', 'Memory buffers size')
mem_cached_size = Gauge('node_memory_Cached_bytes', 'Memory cached size')
mem_free_size = Gauge('node_memory_MemFree_bytes', 'Memory free size')
mem_available_size = Gauge('node_memory_MemAvailable_bytes', 'Memory available size')
mem_total_size = Gauge('node_memory_MemTotal_bytes', 'Memory total size')
filesystem_avail_size = Gauge('node_filesystem_avail_bytes', 'Filesystem avail size', ['device', 'fstype', 'mountpoint'])
filesystem_size = Gauge('node_filesystem_size_bytes', 'Filesystem size', ['device', 'fstype', 'mountpoint'])
disk_reads_count = Counter('node_disk_reads_completed_total', 'Disk reads completed count', ['device'])
disk_reads_size = Counter('node_disk_read_bytes_total', 'Disk reads size', ['device'])
disk_writes_count = Counter('node_disk_writes_completed_total', 'Disk writes completed count', ['device'])
disk_writes_size = Counter('node_disk_write_bytes_total', 'Disk writes size', ['device'])
network_receive_size = Counter('node_network_receive_bytes_total', 'Network receive bytes', ['device'])
network_transmit_size = Counter('node_network_transmit_bytes_total', 'Network transmit bytes', ['device'])
cache_L1_miss_rate = Gauge('cache_L1_miss_rate', 'Cache L1 miss rate')
cache_L2_miss_rate = Gauge('cache_L2_miss_rate', 'Cache L2 miss rate')
cache_L3_miss_rate = Gauge('cache_L3_miss_rate', 'Cache L3 miss rate')
system_ipc = Gauge('system_ipc', 'System ipc')

proc_vm_size = Gauge('node_process_virtual_memory_bytes', 'Process virtual memory size', ['pid'])
cur_info = get_node_info()

start_http_server(8000)
cpu_time.labels(mode='all').inc(sum(cur_info.stat_info['cpu_time']))
cpu_time.labels(mode='user').inc(cur_info.stat_info['cpu_time'][0])
cpu_time.labels(mode='nice').inc(cur_info.stat_info['cpu_time'][1])
cpu_time.labels(mode='system').inc(cur_info.stat_info['cpu_time'][2])
cpu_time.labels(mode='idle').inc(cur_info.stat_info['cpu_time'][3])
cpu_time.labels(mode='iowait').inc(cur_info.stat_info['cpu_time'][4])
for key in cur_info.disk_io_info:
    disk_reads_count.labels(device=key).inc(cur_info.disk_io_info[key]['read_count'])
    disk_reads_size.labels(device=key).inc(cur_info.disk_io_info[key]['read_size'])
    disk_writes_count.labels(device=key).inc(cur_info.disk_io_info[key]['write_count'])
    disk_writes_size.labels(device=key).inc(cur_info.disk_io_info[key]['write_size'])
for key in cur_info.net_info:
    network_receive_size.labels(device=key).inc(cur_info.net_info[key]['receive_bytes'])
    network_transmit_size.labels(device=key).inc(cur_info.net_info[key]['transmit_bytes'])
while True:
    last_info = cur_info
    cur_info = get_node_info()
    cpu_num.set(cur_info.cpu_info['socket_num'] * cur_info.cpu_info['corePerSocket'])
    core_num.set(cur_info.cpu_info['socket_num'] * cur_info.cpu_info['corePerSocket'] * cur_info.cpu_info['threadPerCore'])
    l1_size.set(cur_info.cpu_info['l1i_size'] + cur_info.cpu_info['l1d_size'])
    l2_size.set(cur_info.cpu_info['l2_size'])
    l3_size.set(cur_info.cpu_info['l3_size'])
    cpu_time.labels(mode='all').inc(sum(cur_info.stat_info['cpu_time']) - sum(last_info.stat_info['cpu_time']))
    cpu_time.labels(mode='user').inc(cur_info.stat_info['cpu_time'][0] - last_info.stat_info['cpu_time'][0])
    cpu_time.labels(mode='nice').inc(cur_info.stat_info['cpu_time'][1] - last_info.stat_info['cpu_time'][1])
    cpu_time.labels(mode='system').inc(cur_info.stat_info['cpu_time'][2] - last_info.stat_info['cpu_time'][2])
    cpu_time.labels(mode='idle').inc(cur_info.stat_info['cpu_time'][3] - last_info.stat_info['cpu_time'][3])
    cpu_time.labels(mode='iowait').inc(cur_info.stat_info['cpu_time'][4] - last_info.stat_info['cpu_time'][4])
    procs_blocked_count.set(cur_info.stat_info['procs_blocked'])
    procs_running_count.set(cur_info.stat_info['procs_running'])
    procs_total_count.set(cur_info.stat_info['procs_total'])
    load_averange.labels(time='1').set(cur_info.stat_info['uptime_1'])
    load_averange.labels(time='5').set(cur_info.stat_info['uptime_5'])
    load_averange.labels(time='15').set(cur_info.stat_info['uptime_15'])
    mem_buffer_size.set(cur_info.mem_info['Buffers'])
    mem_cached_size.set(cur_info.mem_info['Cached'])
    mem_free_size.set(cur_info.mem_info['MemFree'])
    mem_available_size.set(cur_info.mem_info['MemAvailable'])
    mem_total_size.set(cur_info.mem_info['MemTotal'])
    for index, row in cur_info.disk_size_info.iterrows():
        filesystem_avail_size.labels(device=row['Filesystem'],fstype=row['Type'],mountpoint=row['Mounted']).set(row['Avail'])
        filesystem_size.labels(device=row['Filesystem'],fstype=row['Type'],mountpoint=row['Mounted']).set(row['Size'])
    for key in cur_info.disk_io_info:
        disk_reads_count.labels(device=key).inc(cur_info.disk_io_info[key]['read_count'] - last_info.disk_io_info[key]['read_count'])
        disk_reads_size.labels(device=key).inc(cur_info.disk_io_info[key]['read_size'] - last_info.disk_io_info[key]['read_size'])
        disk_writes_count.labels(device=key).inc(cur_info.disk_io_info[key]['write_count'] - last_info.disk_io_info[key]['write_count'])
        disk_writes_size.labels(device=key).inc(cur_info.disk_io_info[key]['write_size'] - last_info.disk_io_info[key]['write_size'])
    for key in cur_info.net_info:
        network_receive_size.labels(device=key).inc(cur_info.net_info[key]['receive_bytes'] - last_info.net_info[key]['receive_bytes'])
        network_transmit_size.labels(device=key).inc(cur_info.net_info[key]['transmit_bytes'] - last_info.net_info[key]['transmit_bytes'])
    for index, row in cur_info.proc_info.iterrows():
        proc_vm_size.labels(pid=row['PID']).set(row['VSZ'])
    cache_L1_miss_rate.set(cur_info.perf_info['cache_L1_miss_rate'])
    cache_L2_miss_rate.set(cur_info.perf_info['cache_L2_miss_rate'])
    cache_L3_miss_rate.set(cur_info.perf_info['cache_L3_miss_rate'])
    system_ipc.set(cur_info.perf_info['system_ipc'])
