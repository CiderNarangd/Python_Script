#-- coding: utf-8 --
import os
import sys
from os import path
from datetime import datetime, timedelta
import logging
import subprocess
#######
## ũ�������� master�������� ������� ���������Ŀ� ����ð� ����. 
## �ð��� ���������� �����Ͱ� ���̰� ����� �뷮�� Ŀ�����μ� �����ð��� ���� �ʾ����װ�.. �����ؾ���//
##
#####

### Define dir ####
basedir = f"/db/mysqlbackup/"

backup_date = datetime.now()
backup_date_str = backup_date.strftime("%Y%m%d")

delete_date = backup_date - timedelta(days = 3)
delete_date_str = delete_date.strftime("%Y%m%d")

#### Logger init
start_time = datetime.now().strftime("%Y%m%dT%H%M%S")
log_file_name = "./mysql_manager_logs/mysql_manager_log"+' '+ start_time + ".txt"

logging.basicConfig(filename=log_file_name, level=logging.INFO, 
                    format="[ %(asctime)s] %(message)s", 
                    datefmt="%Y-%m-%d %H:%M:%S")

logger = logging.getLogger()
logger.setLevel(logging.INFO)

stream_hander = logging.StreamHandler()
logger.addHandler(stream_hander)

##Read DB host list..
host_List = open("backup_host_list.txt","r")

for node in host_List:

    host_info = node.split("\t")
    hostport = host_info[1]
    hostname = host_info[0]

    ##�ش� ȣ��Ʈ�� ������ ����� ����
    if(path.exists(f"{basedir}{hostname}/{backup_date_str}")== True):
        cmd = f"sudo rm -r {basedir}{hostname}/{delete_date_str}"
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout, stderr = process.communicate()

    if(path.exists(f"{basedir}{backup_date_str}")== True):
        ## ��� ���丮 ���Ѻ���
        change_chown_cmd = f"chown -R kinam:kinam {basedir}{hostname}/{backup_date_str}"
        process = subprocess.Popen(change_chown_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout, stderr = process.communicate()
        ## ��ũ ����
        link_cmd = f"ln -Tfs {basedir}{hostname}/{backup_date_str} ~/sandboxes/{hostname}/data"
        process = subprocess.Popen(link_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout, stderr = process.communicate()

    else:
        logger.info("Symbolic Linking Fail... Not Exists Dir")
        continue;
        ## ������ ���� �α� ���ܳ���.

sys.exit()