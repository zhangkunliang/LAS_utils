# coding=utf-8
import os
import datetime
import psutil
import psycopg2
from Postgresql_connect import connect


conn = connect("espc", "espcpostgres", "85e11609511899a4928ec8900667baaf", "10.65.189.176", "5432")
cursor = conn.cursor()
def write_points_pg(time, name, PID, CPU_P, CPU, MEM_P, IO):
    sql = """INSERT INTO monitor_info (time, name, PID, CPU_P, CPU, MEM_P, IO) VALUES (%s, %s, %s, %s, %s, %s, %s)"""
    params = (time, name, PID, CPU_P, CPU, MEM_P, IO)
    cursor.execute(sql, params)
    conn.commit()


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


# 收集日志
def monitor():
    os.system("export PYTHONPATH=$PYTHONPATH:/usr/lib64/python2.7:/usr/lib/python2.7")
    os.system("export LD_LIBRARY_PATH=/usr/lib:/usr/lib64")
    os.system("export PYTHONPATH=/usr/lib64/python2.7:/usr/lib/python2.7")
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

    result_elasticsearch = os.popen("top  -p {} -b -n 1".format(int(b)))
    result_elasticsearch2 = os.popen("pidstat -p  {} -d 1 10".format(int(b)))

    result_postgresql = os.popen("top  -p {} -b -n 1".format(int(c)))
    result_postgresql2 = os.popen("pidstat -p  {} -d 1 10".format(int(c)))

    result_proxy = os.popen("top  -p {} -b -n 1".format(int(d)))
    result_proxy2 = os.popen("pidstat -p  {} -d 1 10".format(int(d)))

    result_rest = os.popen("top  -p {} -b -n 1".format(int(e)))
    result_rest2 = os.popen("pidstat -p  {} -d 1 10".format(int(e)))

    result_streamsets = os.popen("top  -p {} -b -n 1".format(int(i)))
    result_streamsets2 = os.popen("pidstat -p  {} -d 1 10".format(int(i)))

    result_flink = os.popen("top  -p {} -b -n 1".format(int(g)))
    result_flink2 = os.popen("pidstat -p  {} -d 1 10".format(int(g)))

    result_event = os.popen("top  -p {} -b -n 1".format(int(h)))
    result_event2 = os.popen("pidstat -p  {} -d 1 10".format(int(h)))

    result_lines = os.popen("top  -p {} -b -n 1".format(int(a))).readlines()[-1].split()
    result_lines2 = os.popen("pidstat -p  {} -d 1 10".format(int(a))).readlines()[-1].split()
    io_res_kafka = float(result_lines2[3]) + float(result_lines2[4])

    result_elasticsearch = result_elasticsearch.readlines()[-1].split()
    result_elasticsearch2 = result_elasticsearch2.readlines()[-1].split()
    io_res_elasticsearch = float(result_elasticsearch2[3]) + float(result_elasticsearch2[4])

    result_post = result_postgresql.readlines()[-1].split()
    result_post2 = result_postgresql2.readlines()[-1].split()
    io_res_post = float(result_post2[3]) + float(result_post2[4])

    result_proxy = result_proxy.readlines()[-1].split()
    result_proxy2 = result_proxy2.readlines()[-1].split()
    io_res_proxy = float(result_proxy2[3]) + float(result_proxy2[4])

    result_rest = result_rest.readlines()[-1].split()
    result_rest2 = result_rest2.readlines()[-1].split()
    io_res_rest = float(result_rest2[3]) + float(result_rest2[4])

    result_streamsets = result_streamsets.readlines()[-1].split()
    result_streamsets2 = result_streamsets2.readlines()[-1].split()
    io_res_streamsets = float(result_streamsets2[3]) + float(result_streamsets2[4])

    result_flink = result_flink.readlines()[-1].split()
    result_flink2 = result_flink2.readlines()[-1].split()
    io_res_flink = float(result_flink2[3]) + float(result_flink2[4])

    result_event = result_event.readlines()[-1].split()
    result_event2 = result_event2.readlines()[-1].split()
    io_res_event = float(result_event2[3]) + float(result_event2[4])
    # if '-' in str(result_lines2):
    #     result_lines2[9] = 0
    write_points_pg(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "kafka", str(result_lines[0]),
                    str(P_kafka.cpu_percent(interval=1.5)),
                    str(danwei(str(result_lines[5]))), str(result_lines[9]), str(io_res_kafka))

    # if '-' in str(result_elasticsearch2):
    #     result_elasticsearch2[9] = 0
    write_points_pg(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "Elasticsearch", str(result_elasticsearch[0]), str(P_elasticsearch.cpu_percent(interval=1.5)),
                    str(danwei(str(result_elasticsearch[5]))), str(result_elasticsearch[9]),
                    str(io_res_elasticsearch))

    # if '-' in str(result_postgresql2):
    #     result_post2[9] = 0
    write_points_pg(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "Postgresql", str(result_post[0]), str(P_postgresql.cpu_percent(interval=1.5)),
                    str(danwei(str(result_post[5]))), str(result_post[9]), str(io_res_post))

    # if '-' in str(result_proxy2):
    #     result_proxy2[9] = 0
    write_points_pg(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "Proxy", str(result_proxy[0]), str(P_proxy.cpu_percent(interval=1.5)),
                    str(danwei(str(result_proxy[5]))), str(result_proxy[9]), str(io_res_proxy))

    # if '-' in str(result_rest2):
    #     result_rest2[9] = 0
    write_points_pg(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "Rest", str(result_rest[0]), str(P_rest.cpu_percent(interval=1.5)),
                    str(danwei(str(result_rest[5]))), str(result_rest[9]), str(io_res_rest))

    # if '-' in str(result_streamsets2):
    #     result_streamsets2[9] = 0
    write_points_pg(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "Streamsets", str(result_streamsets[0]), str(P_streamsets.cpu_percent(interval=1.5)),
                    str(danwei(str(result_streamsets[5]))), str(result_streamsets[9]), str(io_res_streamsets))

    # if '-' in str(result_flink2):
    #     result_flink2[9] = 0
    write_points_pg(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "Flink", str(result_flink[0]), str(P_flink.cpu_percent(interval=1.5)),
                    str(danwei(str(result_flink[5]))), str(result_flink[9]), str(io_res_flink))

    # if '-' in str(result_event2):
    #     result_event2[9] = 0
    write_points_pg(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "Streamevent", str(result_event[0]), str(P_event.cpu_percent(interval=1.5)),
                    str(danwei(str(result_event[5]))), str(result_event[9]), str(io_res_event))




