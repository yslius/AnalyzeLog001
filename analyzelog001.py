import pandas as pd
from datetime import datetime
import config


def convert_datetime(x):
    return datetime.strptime(x, "[%d/%b/%Y:%H:%M:%S.%f]")


def trim_str_before(x):
    return x[1:]


def trim_str_after(x):
    return x[:-1]


def read_log(path_file):
    df_log = pd.read_csv(
        path_file,
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
    print("len(df_log):{}".format(len(df_log)))
    return df_log


if __name__ == '__main__':
    targ_date = '202306031555'

    df_log01 = read_log("{}ico-coin-lb01/{}/haproxy.log".format(config.PATH_LOG_SAVED, targ_date))
    df_log02 = read_log("{}ico-coin-lb02/{}/haproxy.log".format(config.PATH_LOG_SAVED, targ_date))

    df_log01["server"] = "ico-coin-lb01"
    df_log02["server"] = "ico-coin-lb02"

    # 1つのdfに加工
    df_log = pd.concat([df_log01, df_log02])
    df_log.sort_values(by="Accept date", ascending=True, inplace=True)
    df_log.reset_index(drop=True, inplace=True)

    # 保存する
    df_log.to_pickle("pickle/df_log_{}.pickle".format(targ_date))
    df_log.to_csv("csv/log_haproxy_{}.csv".format(targ_date), index=False)
