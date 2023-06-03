import pandas as pd
from datetime import datetime
from glob import glob
import os
import config


def printlog(str_log):
    str_time_log = "{} {}".format(datetime.now().strftime("%H:%M:%S"), str_log)
    print(str_time_log)
    str_datetime = datetime.now().strftime("%Y%m%d")
    name_logfile = "analyzelog002"
    path_logfile = "{}{}_{}.log".format(config.PATH_BATCH_LOG, name_logfile, str_datetime)
    file_object = open(path_logfile, 'a+')
    file_object.writelines(str_time_log + "\n")
    file_object.close()


def convert_datetime(x):
    return datetime.strptime(x, "[%d/%b/%Y:%H:%M:%S.%f]")


def trim_str_before(x):
    return x[1:]


def trim_str_after(x):
    return x[:-1]


def get_log_list(path_log, str_date):
    path_search = "{}haproxy.log.2_{}*.gz".format(path_log, str_date)
    list_log = glob(path_search)
    printlog("len(list_log):{}".format(len(list_log)))
    return list_log


def list_to_dataframe(list_log):
    df_log_new001 = pd.DataFrame(
        columns=['Process name and PID', 'Client IP:port', 'Accept date', 'Frontend name', 'Backend name',
                 'Time codes', 'Status code', 'Bytes read', 'Connection codes', 'Queued requests',
                 'method', 'Request', 'HTTP method'])

    # 読み取ったファイル分回す
    for path_log in list_log:
        print(path_log)
        df_log_new002 = pd.read_csv(
            path_log,
            # sep=r'¥s(?=(:[^"]*"[^"]*")*[^*]*$)(?![^¥[]*¥])',
            sep='\s+',
            engine='python',
            header=None,
            usecols=[4, 5, 6, 7, 8,
                     9, 10, 11, 15, 16,
                     17, 18, 19],
            names=['Process name and PID', 'Client IP:port', 'Accept date', 'Frontend name', 'Backend name',
                   'Time codes', 'Status code', 'Bytes read', 'Connection codes', 'Queued requests',
                   'method', 'Request', 'HTTP method'],
            converters={
                'Accept date': convert_datetime,
                'method': trim_str_before,
                'HTTP method': trim_str_after
            },
        )
        # 読み取ったログに結合する
        print("len(df_log_new002):{}".format(len(df_log_new002)))
        df_log_new001 = pd.concat([df_log_new001, df_log_new002])

        # 重複を排除する
        print("len(df_log_new001):{}".format(len(df_log_new001)))
        df_log_new001.drop_duplicates(inplace=True)
        print("len(df_log_new001):{}".format(len(df_log_new001)))

    return df_log_new001


def append_dataframe(df_log01, df_log02):
    path_log_today = "{}dataframe/df_log_{}.pickle".format(config.PATH_PROJECT, str_date)

    # 取得したログを結合する
    df_log_new = pd.concat([df_log01, df_log02])
    df_log_new.sort_values(by="Accept date", ascending=True, inplace=True)
    df_log_new.reset_index(drop=True, inplace=True)

    # 当日ログの取得（なければ作成）
    if os.path.isfile(path_log_today):
        df_log_today = pd.read_pickle(path_log_today)
    else:
        df_log_today = pd.DataFrame(
            columns=['Process name and PID', 'Client IP:port', 'Accept date', 'Frontend name', 'Backend name',
                     'Time codes', 'Status code', 'Bytes read', 'Connection codes', 'Queued requests',
                     'method', 'Request', 'HTTP method'])

    print("len(df_log_today):{}".format(len(df_log_today)))

    # 取得したログを結合する
    df_log_today = pd.concat([df_log_today, df_log_new])
    df_log_today.sort_values(by="Accept date", ascending=True, inplace=True)
    df_log_today.reset_index(drop=True, inplace=True)

    # 重複を排除する
    print("len(df_log_today):{}".format(len(df_log_today)))
    df_log_today.drop_duplicates(inplace=True)
    print("len(df_log_today):{}".format(len(df_log_today)))

    return df_log_today


if __name__ == '__main__':
    str_date = datetime.now().strftime("%Y%m%d")
    str_date = "20230603"

    # ログのリストを取得する
    list_log01 = get_log_list("{}/file/ico-coin-lb01/".format(config.PATH_PROJECT), str_date)
    list_log02 = get_log_list("{}/file/ico-coin-lb02/".format(config.PATH_PROJECT), str_date)

    # リストからデータフレームにする
    df_log01 = list_to_dataframe(list_log01)
    df_log02 = list_to_dataframe(list_log02)
    df_log01["server"] = "ico-coin-lb01"
    df_log02["server"] = "ico-coin-lb02"

    # 当日のデータフレームに結合する
    df_log_today = append_dataframe(df_log01, df_log02)

    # 保存する
    path_log_today = "{}dataframe/df_log_{}.pickle".format(config.PATH_PROJECT, str_date)
    df_log_today.to_pickle("{}dataframe/df_log_{}.pickle".format(config.PATH_PROJECT, str_date))
    df_log_today.to_csv("{}csv/log_haproxy_{}.csv".format(config.PATH_PROJECT, str_date), index=False)
