import tushare as ts
import pymysql
import matplotlib.pyplot as plt
import numpy as np


if __name__ == '__main__' :

    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='future', charset='utf8')
    cursor = db.cursor()
    cons = ts.get_apis()
    # temp_price = ts.tick('AG1812', conn=cons, asset='X', date='2018-04-13')
    # print(temp_price)

    future_code_src = 'AG1812'
    future_code_comp = 'AG1905'
    para_interval = 2.0


    sql_select_comp = "select * from future_tick2 a where a.future_code = '%s' order by a.state_ts asc"%(future_code_comp)
    cursor.execute(sql_select_comp)
    done_set_select_comp = cursor.fetchall()
    db.commit()
    price_list_src = []
    price_list_comp = []
    delt_list = []
    price_x_src = []
    price_x_comp = []
    std_list = []
    v_line = []
    ts_list = []
    avg_list = []
    up_list = []
    low_list = []
    temp_lock_ts = 0
    for i in range(0,len(done_set_select_comp)):
        print(str(i+1) + '  of  ' + str(len(done_set_select_comp)))
        sql_select_src = "select * from future_tick2 a where a.future_code = '%s' and a.state_ts >= %i order by a.state_ts asc limit 1"%(future_code_src,done_set_select_comp[i][-1])
        cursor.execute(sql_select_src)
        temp_done_set = cursor.fetchall()
        db.commit()
        if len(temp_done_set) > 0:
            price_list_comp.append(done_set_select_comp[i][3])
            price_x_comp.append(i)
            if temp_done_set[0][-1] > temp_lock_ts :
                temp_lock_ts = temp_done_set[0][-1]
                price_list_src.append(float(temp_done_set[0][3]))
                price_x_src.append(i)
                delt = float(done_set_select_comp[i][3]) - float(temp_done_set[0][3])
                std = np.array(delt_list).std()
                mean = np.array(delt_list).mean()
                avg_list.append(mean)
                up_list.append(mean+std*para_interval)
                low_list.append(mean-std*para_interval)
                if abs(delt - mean) > abs(std)*para_interval and len(delt_list) >= 100:
                    v_line.append(i)
                    ts_list.append(done_set_select_comp[i][-1])
                delt_list.append(delt)


    print(ts_list)

    fig = plt.figure(figsize=(20, 12))
    ax = fig.add_subplot(211)
    plt.plot(price_x_src,price_list_src,color='blue')
    plt.plot(price_x_comp,price_list_comp,color='red')
    # for a in range(len(v_line)):
    #     plt.axvline(v_line[a], color='green')
    ax2 = fig.add_subplot(212)
    plt.plot(price_x_src,delt_list,color='red')
    plt.plot(price_x_src,avg_list,color='blue')
    #plt.plot(price_x_src,np.array(avg_list)*1.3,color='black')
    plt.plot(price_x_src,up_list,color='black')
    plt.plot(price_x_src,low_list,color='black')
    #
    # for b in range(len(v_line)):
    #     plt.axvline(v_line[b], color='green')

    plt.show()






