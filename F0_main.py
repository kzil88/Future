import tushare as ts
import numpy as np
import DC
from sklearn import svm
import pymysql
import datetime


def future_evaluate(future):
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='future', charset='utf8')
    cursor = db.cursor()

    sql_dt_select = "select distinct state_dt from future_all a where a.future_code = '%s' order by a.state_dt asc"%(future)
    cursor.execute(sql_dt_select)
    done_set_dt = cursor.fetchall()
    date_seq = [x[0] for x in done_set_dt]
    start_dt = date_seq[0]
    test_seq = date_seq[-31:-1]

    sql_truncate_model_test = 'truncate table future_model_test'
    cursor.execute(sql_truncate_model_test)
    db.commit()

    return_flag = 0
    for d in range(len(test_seq)):
        model_test_new_start = start_dt
        model_test_new_end = test_seq[d]
        try:
            dc = DC.data_collect2(future, model_test_new_start, model_test_new_end)
        except Exception as exp:
            print(exp)
            return_flag = 1
            break
        train = dc.data_train
        target = dc.data_target
        test_case = [dc.test_case]
        if dc.cnt_pos == 0:
            return_flag = 1
            break
        w = 5 * (len(target) / dc.cnt_pos)
        if len(target) / dc.cnt_pos == 1:
            return_flag = 1
            break
        model = svm.SVC(class_weight={1: w})
        model.fit(train, target)
        ans2 = model.predict(test_case)

        sql_insert = "insert into future_model_test(state_dt,future_code,resu_predict)values('%s','%s','%.2f')" % (model_test_new_end, future, float(ans2[0]))
        cursor.execute(sql_insert)
        db.commit()
    if return_flag == 1:
        acc = recall = acc_neg = f1 = 0
    else:
        return_flag2 = 0
        for i in range(len(test_seq) - 4):
            sql_select = "select * from future_all a where a.future_code = '%s' and a.state_dt >= '%s' and a.state_dt <= '%s'" % (future, test_seq[i], test_seq[i + 4])
            cursor.execute(sql_select)
            done_set2 = cursor.fetchall()
            close_list = [x[3] for x in done_set2]
            if len(close_list) <= 1:
                return_flag2 = 1
                break
            after_max = max(close_list[1:len(close_list)])
            after_min = min(close_list[1:len(close_list)])
            resu = 0
            if after_max / close_list[0] >= 1.03:
                resu = 1
            elif after_min / close_list[0] < 0.97:
                resu = -1
            sql_update = "update future_model_test w set w.resu_real = '%.2f' where w.state_dt = '%s' and w.future_code = '%s'" % (resu, test_seq[i], future)
            cursor.execute(sql_update)
            db.commit()
        if return_flag2 == 1:
            acc = recall = acc_neg = f1 = 0
        else:
            sql_resu_acc_son = "select count(*) from future_model_test a where a.resu_real is not null and a.resu_predict = 1 and a.resu_real = 1"
            cursor.execute(sql_resu_acc_son)
            acc_son = cursor.fetchall()[0][0]
            sql_resu_acc_mon = "select count(*) from future_model_test a where a.resu_real is not null and a.resu_real = 1"
            cursor.execute(sql_resu_acc_mon)
            acc_mon = cursor.fetchall()[0][0]
            if acc_mon == 0:
                acc = recall = acc_neg = f1 = 0
            else:
                acc = acc_son / acc_mon

            sql_resu_recall_son = "select count(*) from future_model_test a where a.resu_real is not null and a.resu_predict = a.resu_real"
            cursor.execute(sql_resu_recall_son)
            recall_son = cursor.fetchall()[0][0]
            sql_resu_recall_mon = "select count(*) from future_model_test a where a.resu_real is not null"
            cursor.execute(sql_resu_recall_mon)
            recall_mon = cursor.fetchall()[0][0]
            recall = recall_son / recall_mon

            sql_resu_acc_neg_son = "select count(*) from future_model_test a where a.resu_real is not null and a.resu_predict = -1 and a.resu_real = -1"
            cursor.execute(sql_resu_acc_neg_son)
            acc_neg_son = cursor.fetchall()[0][0]
            sql_resu_acc_neg_mon = "select count(*) from future_model_test a where a.resu_real is not null and a.resu_real = -1"
            cursor.execute(sql_resu_acc_neg_mon)
            acc_neg_mon = cursor.fetchall()[0][0]
            if acc_neg_mon == 0:
                acc_neg_mon = -1
            else:
                acc_neg = acc_neg_son / acc_neg_mon
            if acc + recall == 0:
                acc = recall = acc_neg = f1 = 0
            else:
                f1 = (2 * acc * recall) / (acc + recall)
    sql_predict = "select resu_predict from future_model_test a where a.state_dt = '%s'" % (test_seq[-1])
    cursor.execute(sql_predict)
    done_predict = cursor.fetchall()
    predict = 0
    if len(done_predict) != 0:
        predict = int(done_predict[0][0])

    sql_final_insert = "insert into future_good_pool(state_dt,future_code,acc,recall,f1,acc_neg,bz,predict)values('%s','%s','%.4f','%.4f','%.4f','%.4f','%s','%s')" % (test_seq[-1], future, acc, recall, f1, acc_neg, 'svm', str(predict))
    cursor.execute(sql_final_insert)
    db.commit()
    db.close()
    print('Future : ' + str(future) + '   ACC : ' + str(acc) + '   RECALL : ' + str(recall) + '   ACC_NEG : ' + str(acc_neg) + '   F1 : ' + str(f1))
    return 1


if __name__ == '__main__':
    # # start = '2017-01-01'
    # # end = '2018-01-01'
    # # dc = DC.data_collect2('RU1805',start,end)
    # # train = dc.data_train
    # # target = dc.data_target
    # # test_case = [dc.test_case]
    # # if dc.cnt_pos == 0:
    # #     return_flag = 1
    # #     print('Error 1 !!')
    # # w = 5 * (len(target) / dc.cnt_pos)
    # # if len(target) / dc.cnt_pos == 1:
    # #     return_flag = 1
    # #     print('Error 2 !!')
    # # model = svm.SVC(class_weight={1: w})
    # # model.fit(train, target)
    # # ans2 = model.predict(test_case)
    # print(ans2)
    ans = future_evaluate('RU1805')



