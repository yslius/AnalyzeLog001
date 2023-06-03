import subprocess
import os
from datetime import datetime
import config


def execute_cmd(name_server, name_file, path_save):
    str_cmd = 'ssh {0} "sudo cat /var/log/{1}" > {2}{1}'.format(name_server, name_file, path_save)
    print("{}".format(str_cmd))
    subprocess.run(str_cmd, shell=True)


def get_haproxy_log(name_server, path_save):
    name_file = "haproxy.log"
    execute_cmd(name_server, name_file, path_save)

    name_file = "haproxy.log.1"
    execute_cmd(name_server, name_file, path_save)

    name_file = "haproxy.log.2.gz"
    execute_cmd(name_server, name_file, path_save)

    name_file = "haproxy.log.3.gz"
    execute_cmd(name_server, name_file, path_save)

    name_file = "haproxy.log.4.gz"
    execute_cmd(name_server, name_file, path_save)

    name_file = "haproxy.log.5.gz"
    execute_cmd(name_server, name_file, path_save)

    name_file = "haproxy.log.6.gz"
    execute_cmd(name_server, name_file, path_save)

    name_file = "haproxy.log.7.gz"
    execute_cmd(name_server, name_file, path_save)


if __name__ == '__main__':
    # 日時ディレクトリ作る
    name_datetime = datetime.now().strftime("%Y%m%d%H%M")
    print(name_datetime)

    for i in range(0, 2):
        name_server = "ico-coin-lb{0:02}".format(i + 1)
        path_save = "{}{}/{}/".format(config.PATH_LOG_SAVED, name_server, name_datetime)
        print(path_save)
        os.mkdir(path_save)
        get_haproxy_log(name_server, path_save)
