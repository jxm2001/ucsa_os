from prometheus_client import start_http_server, Gauge, Counter
import pandas as pd
import os
import time
import re
import subprocess
import signal

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

def get_node_info():
    # cpu info
    cpu_info = {}
    cpu_info['cpu_num'] = int(os.popen("grep 'processor' /proc/cpuinfo | sort -u | wc -l").read())
    cpu_info['core_num'] = int(os.popen("grep 'core id' /proc/cpuinfo | sort -u | wc -l").read())
    with os.popen("lscpu") as file:
        for line in file:
            line = line.strip()
            if line.startswith('L1d'):
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
            mem_info[key] = int(value) * 1024
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
    data = [line.split(maxsplit=3) for line in output_lines[1:] if line]
    proc_info = pd.DataFrame(data, columns=["PID", "RSS", "VSZ", "COMMAND"])
    # perf info
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
    return Node_Info(cpu_info, stat_info, mem_info, disk_size_info, disk_io_info, net_info, proc_info, perf_info)

cpu_num = Gauge('cpu_num', 'CPU Number')
core_num = Gauge('core_num', 'Core Number')
l1_size = Gauge('cache_L1_max_size_bype', 'L1 cache size')
l2_size = Gauge('cache_L2_max_size_bype', 'L2 cache size')
l3_size = Gauge('cache_L3_max_size_bype', 'L3 cache size')
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
cache_L1_miss_rate = Gauge('cache_L1_miss_rate', 'Cache L1 miss rate')
cache_L2_miss_rate = Gauge('cache_L2_miss_rate', 'Cache L2 miss rate')
cache_L3_miss_rate = Gauge('cache_L3_miss_rate', 'Cache L3 miss rate')
system_ipc = Gauge('system_ipc', 'System ipc')

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
    l1_size.set(cur_info.cpu_info['l1i_size'] + cur_info.cpu_info['l1d_size'])
    l2_size.set(cur_info.cpu_info['l2_size'])
    l3_size.set(cur_info.cpu_info['l3_size'])
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
    cache_L1_miss_rate.set(cur_info.perf_info['cache_L1_miss_rate'])
    cache_L2_miss_rate.set(cur_info.perf_info['cache_L2_miss_rate'])
    cache_L3_miss_rate.set(cur_info.perf_info['cache_L3_miss_rate'])
    system_ipc.set(cur_info.perf_info['system_ipc'])
