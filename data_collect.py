import pymysql
import copy
import pandas as pd

if __name__ == '__main__' :

    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='future', charset='utf8')
    cursor = db.cursor()

    sql_date_seq = "select distinct state_time from future_tick3 where state_dt is not null and state_time is not null and state_ts is not null and state_time >'20180806 22:21:02.0' order by state_ts asc limit 7000"
    cursor.execute(sql_date_seq)
    done_set = cursor.fetchall()
    db.commit()
    date_seq = [x[0] for x in done_set]
    print(len(date_seq))
    total_seq = len(date_seq)

    # sql_future_pool = "select distinct future_code from future_tick3 where state_dt is not null and state_time is not null and state_ts is not null"
    # cursor.execute(sql_future_pool)
    # done_set2 = cursor.fetchall()
    # db.commit()
    # future_pool = [x[0] for x in done_set2]
    # print(future_pool)
    future_pool = ['sn1808','sn1809','sn1810','sn1811','sn1812','sn1901','sn1902','sn1903','sn1904','sn1905','sn1906','sn1907']
    data_dic = {}
    for i in range(len(future_pool)):
        bid = -1.0
        ask = -1.0
        bid_vol = -1.0
        ask_vol = -1.0
        list_bid = []
        list_bid_vol = []
        list_ask = []
        list_ask_vol = []
        for j in range(len(date_seq)):
            sql = "select state_time,future_code,bid1,bid1_vol,ask1,ask1_vol from future_tick3 a where a.future_code = '%s' and a.state_time = '%s' limit 1"%(future_pool[i],date_seq[j])
            cursor.execute(sql)
            done_temp = cursor.fetchall()
            db.commit()
            if len(done_temp) > 0:
                bid = round(float(done_temp[0][2]),2)
                bid_vol = round(float(done_temp[0][3]),2)
                ask = round(float(done_temp[0][4]),2)
                ask_vol = round(float(done_temp[0][5]),2)
            list_bid.append(bid)
            list_bid_vol.append(bid_vol)
            list_ask.append(ask)
            list_ask_vol.append(ask_vol)
            print(str(i+1) + '  Future_Code : ' + str(future_pool[i]) + '   Seq : ' + str(j+1) + '  of total : ' + str(total_seq))
        data_dic[future_pool[i]+str('_bid')] = copy.copy(list_bid)
        data_dic[future_pool[i]+str('_bid_vol')] = copy.copy(list_bid_vol)
        data_dic[future_pool[i]+str('_ask')] = copy.copy(list_ask)
        data_dic[future_pool[i]+str('_ask_vol')] = copy.copy(list_ask_vol)

    resu = pd.DataFrame(data_dic,index=date_seq)
    resu.to_csv('C:\\Users\\wmx2yjn\Desktop\\resu.csv',mode='a',header=0)
    print(date_seq[-1])
    print('ALL FINISHED !!')





