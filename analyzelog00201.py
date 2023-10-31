import pandas as pd
from datetime import datetime
from glob import glob
import os
import config
import shutil


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
    try:
        return datetime.strptime(x, "[%d/%b/%Y:%H:%M:%S.%f]")
    except Exception:
        return None


def trim_str_before(x):
    if x is None:
        return x
    return x[1:]


def trim_str_after(x):
    if x is None:
        return x
    return x[:-1]


def get_log_list(path_log, str_date):
    path_search = "{}haproxy.log.2_{}*.gz".format(path_log, str_date)
    list_log = sorted(glob(path_search))
    printlog("len(list_log):{}".format(len(list_log)))
    return list_log


def move_file(path_log):
    path_log_delete = path_log.replace("file/ico-coin-lb", "file/delete/ico-coin-lb")
    shutil.move(path_log, path_log_delete)


def list_to_dataframe(list_log):
    df_log_new001 = pd.DataFrame(
        columns=['Process name and PID', 'Client IP:port', 'Accept date', 'Frontend name', 'Backend name',
                 'Time codes', 'Status code', 'Bytes read', 'Connection codes', 'Queued requests',
                 'method', 'Request', 'HTTP method'])

    # 読み取ったファイル分回す
    # cnt = 0
    for path_log in list_log:
        # if cnt > 2:
        #     break
        printlog("Path:{}".format(path_log))
        size_file = os.path.getsize(path_log)
        printlog("size_file:{}".format(size_file))
        # if size_file < 4000000:
        #     move_file(path_log)
        #     # ファイルを削除する
        #     # os.remove(path_log)
        #     continue
        try:
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
            printlog("len(df_log_new002):{}".format(len(df_log_new002)))
            df_log_new001 = pd.concat([df_log_new001, df_log_new002])

            # 日付でソートする
            df_log_new001.sort_values(by="Accept date", ascending=True, inplace=True)
            df_log_new001.reset_index(drop=True, inplace=True)

            # 重複を排除する
            printlog("len(df_log_new001):{}".format(len(df_log_new001)))
            df_log_new001.drop_duplicates(inplace=True)
            printlog("len(df_log_new001):{}".format(len(df_log_new001)))
            # cnt += 1
            # ファイルを移動する
            # move_file(path_log)
            # ファイルを削除する
            os.remove(path_log)

        except Exception as e:
            printlog("Error! {}".format(e))
            move_file(path_log)

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

    printlog("len(df_log_today):{}".format(len(df_log_today)))

    # 取得したログを結合する
    df_log_today = pd.concat([df_log_today, df_log_new])
    # 日付でソートする
    df_log_today.sort_values(by="Accept date", ascending=True, inplace=True)
    df_log_today.reset_index(drop=True, inplace=True)

    # 重複を排除する
    printlog("len(df_log_today):{}".format(len(df_log_today)))
    df_log_today.drop_duplicates(inplace=True)
    printlog("len(df_log_today):{}".format(len(df_log_today)))

    return df_log_today


if __name__ == '__main__':
    # 過去のログをループさせる
    list_str_date = ["20230919", "20230920", "20230921", "20230922", "20230923", "20230924", "20230925"
        , "20230926", "20230927", "20230928", "20230929", "20230930", "20231001", "20231002"
        , "20231006", "20231007", "20231008", "20231009", "20231010", "20231011", "20231012", "20231013"]

    for str_date in list_str_date:
        printlog("{} start".format(str_date))
        # ログのリストを取得する
        list_log01 = get_log_list("{}file/ico-coin-lb01/".format(config.PATH_PROJECT), str_date)
        list_log02 = get_log_list("{}file/ico-coin-lb02/".format(config.PATH_PROJECT), str_date)

        # リストからデータフレームにする
        df_log01 = list_to_dataframe(list_log01)
        df_log02 = list_to_dataframe(list_log02)
        df_log01["server"] = "ico-coin-lb01"
        df_log02["server"] = "ico-coin-lb02"

        # 当日のデータフレームに結合する
        df_log_today = append_dataframe(df_log01, df_log02)

        # 保存する
        path_log_today = "{}dataframe/df_log_{}.pickle".format(config.PATH_PROJECT, str_date)
        df_log_today.to_pickle(path_log_today)
        printlog("to_pickle OK {}".format(path_log_today))

        printlog("{} end".format(str_date))
