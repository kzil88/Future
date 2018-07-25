import pymysql
import numpy as np
import MY_CAP
import Operator as op
import matplotlib.pyplot as plt


def signal(delt_list,cur_delt,para_interval,para_interval_sold):
    std = np.array(delt_list).std()
    mean = np.array(delt_list).mean()
    delt = cur_delt-mean
    if delt > float(std)*para_interval > 0:
        return 1
    elif 0 > -float(std)*para_interval>delt :
        return -1
    elif -float(std)*para_interval_sold < delt < float(std)*para_interval_sold:
        return 2
    else:
        return 0

if __name__ == '__main__' :

    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='future', charset='utf8')
    cursor = db.cursor()

    future_code_comp = 'AG1905'
    future_code_src = 'AG1812'
    para_interval = 1.0
    para_interval_sold = 0.5
    sf_capital = 100000.00
    unit_per_vol = 15.00
    laverage = 0.06

    sql_truncate1 = 'truncate table my_cap'
    cursor.execute(sql_truncate1)
    db.commit()
    sql_truncate2 = 'truncate table my_pool'
    cursor.execute(sql_truncate2)
    db.commit()

    MY_CAP.write_init(sf_capital,unit_per_vol,laverage)

    # 回测时间序列，以非主力合约的行情序列为准
    sql = "select * from future_tick2 a where a.future_code = '%s' order by a.state_ts asc"%(future_code_comp)
    cursor.execute(sql)
    done_set = cursor.fetchall()
    db.commit()
    # 非主力行情中前100个先验样本用于信号获取，真正的回测从第101个开始
    price_list_comp = []
    price_list_src = []
    show_x_comp = []
    show_x_src = []
    delt_list = []
    up_list = []
    low_list= []
    up_list_sold = []
    low_list_sold = []
    mean_list = []
    show_x_after99 = []
    signal_list = []

    temp_ts_lock = 0
    for i in range(len(done_set)):
        print(done_set[i][0] + str('   ') + str(i+1) + str('  of  ') + str(len(done_set)))
        price_list_comp.append(done_set[i][3])
        show_x_comp.append(i)
        sql_select_src = "select * from future_tick2 a where a.future_code = '%s' and a.state_ts >= %i order by a.state_ts asc limit 1" % (future_code_src, done_set[i][-1])
        cursor.execute(sql_select_src)
        done_set_src = cursor.fetchall()
        db.commit()
        if len(done_set_src) > 0:
            if done_set_src[0][-1] > temp_ts_lock:
                temp_ts_lock = done_set_src[0][-1]
                delt = float(done_set[i][3]) - float(done_set_src[0][3])
                delt_list.append(delt)
                price_list_src.append(done_set_src[0][3])
                show_x_src.append(i)
                if i > 49:
                    ans = signal(delt_list[0:len(delt_list)],delt_list[-1],para_interval,para_interval_sold)
                    std = np.array(delt_list[0:len(delt_list)]).std()
                    mean = np.array(delt_list[0:len(delt_list)]).mean()
                    mean_list.append(mean)
                    up_list.append(mean+std*para_interval)
                    low_list.append(mean-std*para_interval)
                    up_list_sold.append(mean+std*para_interval_sold)
                    low_list_sold.append(mean-std*para_interval_sold)
                    show_x_after99.append(i)
                    if ans == 1:
                        sql_check1 = "select * from future_tick2 a where a.future_code = '%s' and a.state_ts >= %i and a.state_ts < %i limit 1"%(future_code_comp,int(done_set_src[0][-1])+1,int(done_set_src[0][-1])+11)
                        cursor.execute(sql_check1)
                        temp_done_set1 = cursor.fetchall()
                        if len(temp_done_set1) > 0:
                            sql_check2 = "select * from future_tick2 a where a.future_code = '%s' and a.state_ts >= %i and a.state_ts < %i limit 1" % (future_code_src, int(done_set_src[0][-1]) + 2, int(done_set_src[0][-1]) + 12)
                            cursor.execute(sql_check2)
                            temp_done_set2 = cursor.fetchall()
                            if len(temp_done_set2) > 0:
                                buy1 = op.buy(future_code_comp,int(done_set_src[0][-1])+1,int(-1),int(1))
                                buy2 = op.buy(future_code_src,int(done_set_src[0][-1])+2,int(1),int(1))
                                signal_list.append(i)

                    elif ans == -1:
                        sql_check1 = "select * from future_tick2 a where a.future_code = '%s' and a.state_ts >= %i and a.state_ts < %i limit 1" % (future_code_comp, int(done_set_src[0][-1]) + 1, int(done_set_src[0][-1]) + 11)
                        cursor.execute(sql_check1)
                        temp_done_set1 = cursor.fetchall()
                        if len(temp_done_set1) > 0:
                            sql_check2 = "select * from future_tick2 a where a.future_code = '%s' and a.state_ts >= %i and a.state_ts < %i limit 1" % (future_code_src, int(done_set_src[0][-1]) + 2, int(done_set_src[0][-1]) + 12)
                            cursor.execute(sql_check2)
                            temp_done_set2 = cursor.fetchall()
                            if len(temp_done_set2) > 0:
                                buy1 = op.buy(future_code_comp, int(done_set_src[0][-1]) + 1, int(1), int(1))
                                buy2 = op.buy(future_code_src, int(done_set_src[0][-1]) + 2, int(-1), int(1))
                                signal_list.append(i)

                    elif ans == 2:
                        sql_check1 = "select * from future_tick2 a where a.future_code = '%s' and a.state_ts >= %i and a.state_ts < %i limit 1" % (future_code_comp, int(done_set_src[0][-1]) + 1, int(done_set_src[0][-1]) + 11)
                        cursor.execute(sql_check1)
                        temp_done_set1 = cursor.fetchall()
                        if len(temp_done_set1) > 0:
                            sql_check2 = "select * from future_tick2 a where a.future_code = '%s' and a.state_ts >= %i and a.state_ts < %i limit 1" % (future_code_src, int(done_set_src[0][-1]) + 2, int(done_set_src[0][-1]) + 12)
                            cursor.execute(sql_check2)
                            temp_done_set2 = cursor.fetchall()
                            if len(temp_done_set2) > 0:
                                sql_pool = "select * from my_pool a where a.future_vol > 0"
                                cursor.execute(sql_pool)
                                done_set_sell = cursor.fetchall()
                                db.commit()
                                for j in range(len(done_set_sell)):
                                    sell = op.sell(done_set_sell[j][0],int(done_set_src[0][-1]) + 1,done_set_sell[j][-1],done_set_sell[j][1],-1)
                                    signal_list.append(i)

    print('ALL Finished !!')


    deal_buy_more = []
    deal_buy_less = []
    deal_sold = []
    deal_buy_more_y = []
    deal_buy_less_y = []
    deal_sold_y = []
    sql_show = "select * from my_cap a where a.seq > 1 order by seq"
    cursor.execute(sql_show)
    done_set_show = cursor.fetchall()
    db.commit()
    #deal_buy_more = [int(x[-1]) for x in done_set_show if x[7] == '买入' and x[-2] == '开多']
    #deal_buy_less = [int(x[-1]) for x in done_set_show if x[7] == '买入' and x[-2] == '开空']
    #deal_sold = [int(x[-1]) for x in done_set_show if x[7] == '卖出']
    # for l in range(len(done_set_show)):
    #     min_buy_more_y = 0.00
    #     min_buy_less_y = 0.00
    #     min_sold_y = 0.00
    #     min_index = done_set[-1][-1]
    #     for m in range(len(done_set)):
    #         if abs(int(done_set[m][-1]) - int(done_set_show[l][-1]))


    for l in range(len(done_set)):
        for m in range(len(done_set_show)):
            if int(done_set[l][-1]) == done_set_show[m][-1]:
                if done_set_show[m][7] == '买入' and done_set_show[m][-2] == '开多':
                    deal_buy_more.append(l)
                    deal_buy_more_y.append(float(done_set_show[m][8]))
                elif done_set_show[m][7] == '买入' and done_set_show[m][-2] == '开空':
                    deal_buy_less.append(l)
                    deal_buy_less_y.append(float(done_set_show[m][8]))
                else:
                    deal_sold.append(l)
                    deal_sold_y.append(float(done_set_show[m][8]))
    print(deal_buy_more_y)
    print(deal_buy_less_y)
    print(deal_sold_y)


    fig = plt.figure(figsize=(20, 12))
    ax = fig.add_subplot(211)
    plt.plot(show_x_src,price_list_src,color='blue')
    plt.plot(show_x_comp,price_list_comp,color='red')
    for a in range(len(signal_list)):
        plt.axvline(signal_list[a], color='grey')
    for a2 in range(len(deal_buy_more)):
        plt.plot(deal_buy_more[a2],deal_buy_more_y[a2],color='red',marker='o')
        #plt.axvline(deal_buy_more[a2], color='red',marker='o')
    for a3 in range(len(deal_buy_less)):
        plt.plot(deal_buy_less[a3], deal_buy_less_y[a3], color='green', marker='o')
        #plt.axvline(deal_buy_less[a3], color='green',marker='o')
    for a4 in range(len(deal_sold)):
        plt.plot(deal_sold[a4],deal_sold_y[a4], color='blue', marker='x')
        #plt.axvline(deal_sold[a4], color='blue',marker='x')

    ax2 = fig.add_subplot(212)
    plt.plot(show_x_src,delt_list,color='red')
    plt.plot(show_x_after99,mean_list,color='blue')
    plt.plot(show_x_after99,up_list,color='black')
    plt.plot(show_x_after99,low_list,color='black')
    plt.plot(show_x_after99, up_list_sold, color='grey')
    plt.plot(show_x_after99, low_list_sold, color='grey')

    for b in range(len(signal_list)):
        plt.axvline(signal_list[b], color='grey')
    # for b2 in range(len(deal_buy_more)):
    #     plt.plot(deal_buy_more[b2],deal_buy_more_y[b2],color='red',marker='o')
    #     #plt.axvline(deal_buy_more[b2], color='red',marker='o')
    # for b3 in range(len(deal_buy_less)):
    #     plt.plot(deal_buy_less[b3], deal_buy_less_y[b3], color='green', marker='o')
    #     #plt.axvline(deal_buy_less[b3], color='green',marker='o')
    # for b4 in range(len(deal_sold)):
    #     plt.plot(deal_sold[b4],deal_sold_y[b4], color='blue', marker='x')
    #     #plt.axvline(deal_sold[b4], color='blue',marker='x')

    # dis_list = []
    # dis_index = []
    # show_mean = np.array(delt_list).mean()
    # show_std = np.array(delt_list).std()
    # print(show_mean)
    # print(show_std)
    # bound_up = show_mean + show_std*(-3.0)
    # for c in range(-29,31,2):
    #     temp_list = []
    #     bound_low = bound_up
    #     bound_up = show_mean + show_std*c/10
    #     for d in range(len(delt_list)):
    #         if bound_low<delt_list[d]<=bound_up and d not in dis_index:
    #             temp_list.append(delt_list[d])
    #             dis_index.append(d)
    #     dis_list.append(temp_list)
    # ax = fig.add_subplot(111)
    # for e in range(len(dis_list)):
    #     plt.bar(e,len(dis_list[e]),color='blue')
    # plt.axvline(len(dis_list)/2, color='red')
    plt.show()



