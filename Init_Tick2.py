import datetime
import tushare as ts
import numpy as np
import pymysql
import re
import time

if __name__ == '__main__':
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='future', charset='utf8')
    cursor = db.cursor()
    cons = ts.get_apis()
    future_code = 'AG1812'

    # 先清空最近一天的数据库记录
    sql_init = "SET SQL_SAFE_UPDATES = 0"
    cursor.execute(sql_init)
    db.commit()
    sql_max_dt = "select max(b.state_dt) from future_tick2 b where b.future_code = '%s'"%(future_code)
    cursor.execute(sql_max_dt)
    done_set_max = cursor.fetchall()
    db.commit()
    if len(done_set_max) > 0:
        sql_delete = "delete a.* from future_tick2 a where a.state_dt = '%s' and a.future_code = '%s'" %(str(done_set_max[0][0]),future_code)
        cursor.execute(sql_delete)
        db.commit()

    try:
        temp_day = ts.bar(future_code, conn=cons, asset='X')
        #temp_day = ts.tick(future_code,conn=cons,asset='X',date='2017-12-20')
        date_seq = list(temp_day.index)
        date_seq = [str(x)[:10] for x in date_seq][::-1]
        print(date_seq)
    except Exception as aa:
        print('No DATA Code:')
    for i in range(len(date_seq)):
        if int(time.mktime(time.strptime(str(date_seq[i]), "%Y-%m-%d"))) < int(time.mktime(time.strptime(str('2018-05-18'), "%Y-%m-%d"))):
            continue

        sql_check = "select * from future_tick2 a where a.state_dt = '%s' and a.future_code = '%s'"%(date_seq[i],future_code)
        cursor.execute(sql_check)
        done_set_check = cursor.fetchall()
        db.commit()
        if len(done_set_check) > 0:
            print(str(date_seq[i]) + '   Already Exists')
            continue
        cnt_try = 0
        c_len = -1
        while True:
            try:
                temp_price = ts.tick(future_code,conn=cons,asset='X',date=date_seq[i])
                #print(temp_price)
                c_len = temp_price.shape[0]
                if c_len >= 0:
                    break
            except Exception as bb:
                cnt_try += 1
                cons = ts.get_apis()
                print(cnt_try)
                time.sleep(17)
                continue

        print(str(date_seq[i]) + '  Tick Len : ' + str(c_len))
        if c_len > 0:
            for j in range(c_len):
                resu = list(temp_price.ix[j])
                state_dt = str(resu[0])[:10]
                state_ts = int(time.mktime(time.strptime(str(resu[0]), "%Y-%m-%d %H:%M:%S")))
                state_time = datetime.datetime.fromtimestamp(int(time.mktime(time.strptime(str(resu[0]), "%Y-%m-%d %H:%M:%S")))).strftime("%Y-%m-%d-%H-%M-%S")
                future_price = float(resu[1])
                future_vol = int(resu[2])
                deal_type = resu[4]
                sql_insert = "insert into future_tick2(state_dt,state_time,future_code,future_price,future_vol,deal_type,state_ts)values('%s','%s','%s','%.2f','%i','%s','%i')"%(state_dt,state_time,future_code,future_price,future_vol,deal_type,state_ts)
                try:
                    cursor.execute(sql_insert)
                    db.commit()
                except Exception as cc:
                    continue


    print('All Finished!')
    db.close()



