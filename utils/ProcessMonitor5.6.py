# coding=utf-8
import os

import csv
import datetime
# os.system("tasklist")
import time

import psutil

os.system("export PYTHONPATH=$PYTHONPATH:/usr/lib64/python2.7:/usr/lib/python2.7")
os.system("export LD_LIBRARY_PATH=/usr/lib:/usr/lib64")
os.system("export PYTHONPATH=/usr/lib64/python2.7:/usr/lib/python2.7")

desk = r"/opt/zhengwei/ProcessMonitorResult/"
if os.path.exists(desk):
    pass
else:
    os.mkdir(desk)
# kafka ="Process monitoring"+datetime.datetime.now().strftime('%Y-%m-%d %H_%M_%S')+ ".csv"
temp = desk + "temp" + ".txt"
kafka = desk + "Process monitoring kafka" + ".csv"
Elasticsearch = desk + "Process monitoring elasticsearch" + ".csv"
Postgresql = desk + "Process monitoring postgresql" + ".csv"
Proxy = desk + "Process monitoring proxy" + ".csv"
Rest = desk + "Process monitoring rest" + ".csv"
Streamsets = desk + "Process monitoring streamsets" + ".csv"
Flink = desk + "Process monitoring flink" + ".csv"
StreamEvent = desk + "Process monitoring streamEvent" + ".csv"
sleepTime = 300


def danwei(newstr):
    if 'g' in newstr:
        newstr = str(int(float(newstr[:-1]) * 1024)) + 'm'
        return newstr
    elif 't' in newstr:
        newstr = str(int(float(newstr[:-1]) * 1024 * 1024)) + 'm'
        return newstr
    elif 'm' in newstr:
        newstr = str(int(newstr[:-1])) + 'm'
        return newstr
    else:
        newstr = str(int(float(newstr[:-1]) / 1024)) + 'm'
        return newstr


def run():
    a = os.popen('ps aux|grep deps/kafka | grep server.properties |grep -v grep|awk \'{print $2}\'').read()
    b = os.popen('ps aux|grep Elasticsearch|grep -v grep|awk \'{print $2}\'').read()
    c = os.popen('ps aux|grep postgresql |grep -v grep|awk \'{print $2}\'').read()
    d = os.popen('ps aux|grep /opt/nsfocus/espc/deps/tomcat/proxy |grep -v grep|awk \'{print $2}\'').read()
    e = os.popen('ps aux|grep /opt/nsfocus/espc/deps/tomcat/conf/ |grep -v grep|awk \'{print $2}\'').read()
    i = os.popen('ps aux|grep streamsets-datacollector |grep -v grep | grep java|awk \'{print $2}\'').read()
    g = os.popen('ps aux|grep org.apache.flink.runtime |grep -v grep |grep java | xargs |awk \'{print $2}\'').read()
    h = os.popen('ps aux|grep stream-event-service |grep -v grep |grep java|awk \'{print $2}\'').read()
    P_kafka = psutil.Process(int(a))
    P_elasticsearch = psutil.Process(int(b))
    P_postgresql = psutil.Process(int(c))
    P_proxy = psutil.Process(int(d))
    P_rest = psutil.Process(int(e))
    P_streamsets = psutil.Process(int(i))
    P_flink = psutil.Process(int(g))
    P_event = psutil.Process(int(h))
    cmd = "top  -p {} -b -n 1".format(int(a))
    cmd2 = "pidstat -p  {} -d 1 10".format(int(a))

    cmd_elasticsearch = "top  -p {} -b -n 1".format(int(b))
    cmd_elasticsearch2 = "pidstat -p  {} -d 1 10".format(int(b))

    cmd_postgresql = "top  -p {} -b -n 1".format(int(c))
    cmd_postgresql2 = "pidstat -p  {} -d 1 10".format(int(c))

    cmd_proxy = "top  -p {} -b -n 1".format(int(d))
    cmd_proxy2 = "pidstat -p  {} -d 1 10".format(int(d))

    cmd_rest = "top  -p {} -b -n 1".format(int(e))
    cmd_rest2 = "pidstat -p  {} -d 1 10".format(int(e))

    cmd_streamsets = "top  -p {} -b -n 1".format(int(i))
    cmd_streamsets2 = "pidstat -p  {} -d 1 10".format(int(i))

    cmd_flink = "top  -p {} -b -n 1".format(int(g))
    cmd_flink2 = "pidstat -p  {} -d 1 10".format(int(g))

    cmd_event = "top  -p {} -b -n 1".format(int(h))
    cmd_event2 = "pidstat -p  {} -d 1 10".format(int(h))

    result = os.popen(cmd)
    result2 = os.popen(cmd2)

    result_elasticsearch = os.popen(cmd_elasticsearch)
    result_elasticsearch2 = os.popen(cmd_elasticsearch2)

    result_postgresql = os.popen(cmd_postgresql)
    result_postgresql2 = os.popen(cmd_postgresql2)

    result_proxy = os.popen(cmd_proxy)
    result_proxy2 = os.popen(cmd_proxy2)

    result_rest = os.popen(cmd_rest)
    result_rest2 = os.popen(cmd_rest2)

    result_streamsets = os.popen(cmd_streamsets)
    result_streamsets2 = os.popen(cmd_streamsets2)

    result_flink = os.popen(cmd_flink)
    result_flink2 = os.popen(cmd_flink2)

    result_event = os.popen(cmd_event)
    result_event2 = os.popen(cmd_event2)

    lines = result.readlines()
    result_lines = lines[-1].split()
    lines2 = result2.readlines()
    result_lines2 = lines2[-1].split()
    io_res_kafka = float(result_lines2[3]) + float(result_lines2[4])

    lines_elasticsearch = result_elasticsearch.readlines()
    result_elasticsearch = lines_elasticsearch[-1].split()
    lines_elasticsearch2 = result_elasticsearch2.readlines()
    result_elasticsearch2 = lines_elasticsearch2[-1].split()
    iores_elasticsearch = float(result_elasticsearch2[3]) + float(result_elasticsearch2[4])

    linespost = result_postgresql.readlines()
    resultpost = linespost[-1].split()
    linespost2 = result_postgresql2.readlines()
    resultpost2 = linespost2[-1].split()
    iorespost = float(resultpost2[3]) + float(resultpost2[4])

    linesproxy = result_proxy.readlines()
    result_proxy = linesproxy[-1].split()
    lines_proxy2 = result_proxy2.readlines()
    result_proxy2 = lines_proxy2[-1].split()
    iores_proxy = float(result_proxy2[3]) + float(result_proxy2[4])

    lines_rest = result_rest.readlines()
    result_rest = lines_rest[-1].split()
    lines_rest2 = result_rest2.readlines()
    result_rest2 = lines_rest2[-1].split()
    iores_rest = float(result_rest2[3]) + float(result_rest2[4])

    lines_streamsets = result_streamsets.readlines()
    result_streamsets = lines_streamsets[-1].split()
    lines_streamsets2 = result_streamsets2.readlines()
    result_streamsets2 = lines_streamsets2[-1].split()
    ioresstreamsets = float(result_streamsets2[3]) + float(result_streamsets2[4])

    lines_flink = result_flink.readlines()
    result_flink = lines_flink[-1].split()
    lines_flink2 = result_flink2.readlines()
    result_flink2 = lines_flink2[-1].split()
    iores_flink = float(result_flink2[3]) + float(result_flink2[4])

    linesevent = result_event.readlines()
    result_event = linesevent[-1].split()
    linesevent2 = result_event2.readlines()
    result_event2 = linesevent2[-1].split()
    ioresevent = float(result_event2[3]) + float(result_event2[4])

    if not os.path.exists(kafka):
        with open(kafka, "w") as K:
            if '-' in str(result_lines2):
                result_lines2[9] = 0
            K.write(
                "time" + ',' + "name" + ',' + "PID" + ',' + "CPU(%)" + ',' + "MEM" + ',' + "MEM(%)" + ',' + "IO(k/s)")
            K.write('\n' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ',' + "kafka" + ',' + str(
                result_lines[0]) + ',' + str(P_kafka.cpu_percent(interval=1.5)) + ',' + str(
                danwei(str(result_lines[5]))) + ',' + str(
                result_lines[9]) + ',' + str(io_res_kafka))

        with open(Elasticsearch, "w") as elas:
            if '-' in str(result_elasticsearch2):
                result_lines2[9] = 0
            elas.write(
                "time" + ',' + "name" + ',' + "PID" + ',' + "CPU(%)" + ',' + "MEM" + ',' + "MEM(%)" + ',' + "IO(k/s)")
            elas.write('\n' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ',' + "Elasticsearch" + ',' + str(
                result_elasticsearch[0]) + ',' + str(P_elasticsearch.cpu_percent(interval=1.5)) + ',' + str(
                danwei(str(result_elasticsearch[5]))) + ',' + str(
                result_elasticsearch[9]) + ',' + str(iores_elasticsearch))

        with open(Postgresql, "w") as post:
            if '-' in str(resultpost2):
                resultpost2[9] = 0
            post.write(
                "time" + ',' + "name" + ',' + "PID" + ',' + "CPU(%)" + ',' + "MEM" + ',' + "MEM(%)" + ',' + "IO(k/s)")
            post.write('\n' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ',' + "Postgresql" + ',' + str(
                resultpost[0]) + ',' + str(P_postgresql.cpu_percent(interval=1.5)) + ',' + str(
                danwei(str(resultpost[5]))) + ',' + str(
                resultpost[9]) + ',' + str(iorespost))

        with open(Proxy, "w") as proxy:
            if '-' in str(result_proxy2):
                result_proxy2[9] = 0
            proxy.write(
                "time" + ',' + "name" + ',' + "PID" + ',' + "CPU(%)" + ',' + "MEM" + ',' + "MEM(%)" + ',' + "IO(k/s)")
            proxy.write('\n' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ',' + "Proxy" + ',' + str(
                result_proxy[0]) + ',' + str(P_proxy.cpu_percent(interval=1.5)) + ',' + str(
                danwei(str(result_proxy[5]))) + ',' + str(
                result_proxy[9]) + ',' + str(iores_proxy))

        with open(Rest, "w") as rest:
            if '-' in str(result_rest2):
                result_rest2[9] = 0
            rest.write(
                "time" + ',' + "name" + ',' + "PID" + ',' + "CPU(%)" + ',' + "MEM" + ',' + "MEM(%)" + ',' + "IO(k/s)")
            rest.write('\n' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ',' + "Rest" + ',' + str(
                result_rest[0]) + ',' + str(P_rest.cpu_percent(interval=1.5)) + ',' + str(
                danwei(str(result_rest[5]))) + ',' + str(
                result_rest[9]) + ',' + str(iores_rest))

        with open(Streamsets, "w") as streamsets:
            if '-' in str(result_streamsets2):
                result_streamsets2[9] = 0
            streamsets.write(
                "time" + ',' + "name" + ',' + "PID" + ',' + "CPU(%)" + ',' + "MEM" + ',' + "MEM(%)" + ',' + "IO(k/s)")
            streamsets.write(
                '\n' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ',' + "Streamsets" + ',' + str(
                    result_streamsets[0]) + ',' + str(P_streamsets.cpu_percent(interval=1.5)) + ',' + str(
                    danwei(str(result_streamsets[5]))) + ',' + str(
                    result_streamsets[9]) + ',' + str(ioresstreamsets))

        with open(Flink, "w") as flink:
            if '-' in str(result_flink2):
                result_flink2[9] = 0
            flink.write(
                "time" + ',' + "name" + ',' + "PID" + ',' + "CPU(%)" + ',' + "MEM" + ',' + "MEM(%)" + ',' + "IO(k/s)")
            flink.write('\n' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ',' + "Flink" + ',' + str(
                result_flink[0]) + ',' + str(P_flink.cpu_percent(interval=1.5)) + ',' + str(
                danwei(str(result_flink[5]))) + ',' + str(
                result_flink[9]) + ',' + str(iores_flink))

        with open(StreamEvent, "w") as event:
            if '-' in str(result_event2):
                result_event2[9] = 0
            event.write(
                "time" + ',' + "name" + ',' + "PID" + ',' + "CPU(%)" + ',' + "MEM" + ',' + "MEM(%)" + ',' + "IO(k/s)")
            event.write('\n' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ',' + "Streamevent" + ',' + str(
                result_event[0]) + ',' + str(P_event.cpu_percent(interval=1.5)) + ',' + str(
                danwei(str(result_event[5]))) + ',' + str(
                result_event[9]) + ',' + str(ioresevent))
    else:
        with open(kafka, "a") as K:
            if '-' in str(result_lines2):
                result_lines2[9] = 0
            K.write('\n' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ',' + "kafka" + ',' + str(
                result_lines[0]) + ',' + str(P_kafka.cpu_percent(interval=1.5)) + ',' + str(
                danwei(str(result_lines[5]))) + ',' + str(
                result_lines[9]) + ',' + str(io_res_kafka))
        with open(Elasticsearch, "a") as elas:
            if '-' in str(result_elasticsearch2):
                result_lines2[9] = 0
            elas.write('\n' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ',' + "Elasticsearch" + ',' + str(
                result_elasticsearch[0]) + ',' + str(P_elasticsearch.cpu_percent(interval=1.5)) + ',' + str(
                danwei(str(result_elasticsearch[5]))) + ',' + str(
                result_elasticsearch[9]) + ',' + str(iores_elasticsearch))
        with open(Postgresql, "a") as post:
            if '-' in str(resultpost2):
                resultpost2[9] = 0
            post.write('\n' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ',' + "Postgresql" + ',' + str(
                resultpost[0]) + ',' + str(P_postgresql.cpu_percent(interval=1.5)) + ',' + str(
                danwei(str(resultpost[5]))) + ',' + str(
                resultpost[9]) + ',' + str(iorespost))
        with open(Proxy, "a") as proxy:
            if '-' in str(result_proxy2):
                result_proxy2[9] = 0
            proxy.write('\n' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ',' + "Proxy" + ',' + str(
                result_proxy[0]) + ',' + str(P_proxy.cpu_percent(interval=1.5)) + ',' + str(
                danwei(str(result_proxy[5]))) + ',' + str(
                result_proxy[9]) + ',' + str(iores_proxy))
        with open(Rest, "a") as rest:
            if '-' in str(result_rest2):
                result_rest2[9] = 0
            rest.write('\n' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ',' + "Rest" + ',' + str(
                result_rest[0]) + ',' + str(P_rest.cpu_percent(interval=1.5)) + ',' + str(
                danwei(str(result_rest[5]))) + ',' + str(
                result_rest[9]) + ',' + str(iores_rest))
        with open(Streamsets, "a") as streamsets:
            if '-' in str(result_streamsets2):
                result_streamsets2[9] = 0
            streamsets.write(
                '\n' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ',' + "Streamsets" + ',' + str(
                    result_streamsets[0]) + ',' + str(P_streamsets.cpu_percent(interval=1.5)) + ',' + str(
                    danwei(str(result_streamsets[5]))) + ',' + str(
                    result_streamsets[9]) + ',' + str(ioresstreamsets))
        with open(Flink, "a") as flink:
            if '-' in str(result_flink2):
                result_flink2[9] = 0
            flink.write('\n' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ',' + "Flink" + ',' + str(
                result_flink[0]) + ',' + str(P_flink.cpu_percent(interval=1.5)) + ',' + str(
                danwei(str(result_flink[5]))) + ',' + str(
                result_flink[9]) + ',' + str(iores_flink))
        with open(StreamEvent, "a") as event:
            if '-' in str(result_event2):
                result_event2[9] = 0
            event.write('\n' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ',' + "Streamevent" + ',' + str(
                result_event[0]) + ',' + str(P_event.cpu_percent(interval=1.5)) + ',' + str(
                danwei(str(result_event[5]))) + ',' + str(
                result_event[9]) + ',' + str(ioresevent))


while True:
    run()
    time.sleep(sleepTime)
