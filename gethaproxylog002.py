import subprocess
from datetime import datetime
import config


def printlog(str_log):
    str_time_log = "{} {}".format(datetime.now().strftime("%H:%M:%S"), str_log)
    print(str_time_log)
    str_datetime = datetime.now().strftime("%Y%m%d")
    name_logfile = "gethaproxylog002"
    path_logfile = "{}{}_{}.log".format(config.PATH_BATCH_LOG, name_logfile, str_datetime)
    file_object = open(path_logfile, 'a+')
    file_object.writelines(str_time_log + "\n")
    file_object.close()


def execute_cmd(name_server, name_file, path_save):
    try:
        str_cmd = 'ssh {0} "sudo cat /var/log/{1}" > {2}'.format(name_server, name_file, path_save)
        printlog(str_cmd)
        subprocess.run(str_cmd, shell=True)
    except Exception as e:
        printlog("Error {}".format(e))


def get_haproxy_log(name_server, path_save):
    name_file = "haproxy.log.2.gz"
    execute_cmd(name_server, name_file, path_save)


if __name__ == '__main__':

    # 日時取得
    name_datetime = datetime.now().strftime("%Y%m%d%H%M")
    printlog(name_datetime)

    name_savefile = "haproxy.log.2_{}.gz".format(name_datetime)

    for i in range(0, 2):
        name_server = "ico-coin-lb{0:02}".format(i + 1)
        path_save = "{}{}/{}".format(config.PATH_ANALIZELOG_SAVED, name_server, name_savefile)
        printlog(path_save)
        get_haproxy_log(name_server, path_save)
