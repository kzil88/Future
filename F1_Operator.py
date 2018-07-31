import pymysql
import MY_CAP

def buy(future_code,state_ts,future_type,vol):

    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='future', charset='utf8')
    cursor = db.cursor()

    cap = MY_CAP.MY_CAP()
    sql = "select * from future_tick2 a where a.future_code = '%s' and a.state_ts >= %i order by a.state_ts limit 1"%(future_code,state_ts)
    cursor.execute(sql)
    done_set = cursor.fetchall()
    db.commit()
    if len(done_set) > 0 and int(done_set[0][-1]) - int(state_ts) < 10:
        cur_price = float(done_set[0][3])
        money_need = vol*float(cap.unit_per_vol)*cur_price*1.0002
        if cap.cash_capital > money_need:
            new_state_dt = done_set[0][0]
            new_state_time = done_set[0][1]
            new_state_ts = done_set[0][-1]
            new_future_capital = float(cap.future_capital) + money_need/1.0002
            new_cash_capital = float(cap.cash_capital) - money_need
            new_total_capital = new_cash_capital+new_future_capital
            new_sf_capital = float(cap.sf_capital) - (money_need/1.0002)*float(cap.laverage) - (money_need/1.0002)*0.0002
            if future_type == 1:
                bz = '开多'
            else:
                bz = '开空'
            sql_cap = "insert into my_cap(state_dt,state_time,total_capital,future_capital,cash_capital,sf_capital,operation,deal_price,deal_vol,unit_per_vol,laverage,future_code,bz,state_ts)values('%s','%s','%.2f','%.2f','%.2f','%.2f','%s','%.2f','%f','%f','%.2f','%s','%s','%i')"%(new_state_dt,new_state_time,new_total_capital,new_future_capital,new_cash_capital,new_sf_capital,'买入',cur_price,float(vol),cap.unit_per_vol,cap.laverage,future_code,bz,int(new_state_ts))
            cursor.execute(sql_cap)
            db.commit()

            sql_check = "select * from my_pool a where a.future_vol > 0 and a.future_code = '%s'"%(future_code)
            cursor.execute(sql_check)
            done_set_check = cursor.fetchall()
            db.commit()
            if future_code in [str(x[0]) for x in done_set_check] and int(future_type) in [int(x[-1]) for x in done_set_check]:
                sql_pool = "update my_pool w set w.future_vol = w.future_vol + %i where w.future_code = '%s' and w.future_type = %i"%(int(vol),future_code,int(future_type))
                cursor.execute(sql_pool)
                db.commit()
            else:
                sql_pool = "insert into my_pool(future_code,future_vol,buy_price,buy_ts,future_type)values('%s','%i','%.2f','%i','%i')"%(future_code,int(vol),cur_price,int(new_state_ts),int(future_type))
                cursor.execute(sql_pool)
                db.commit()
    cursor.close()
    db.close()
    return 1

def sell(future_code,state_ts,future_type,vol,signal):
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='future', charset='utf8')
    cursor = db.cursor()

    cap = MY_CAP.MY_CAP()
    if signal == -1 :
        sql = "select * from future_tick2 a where a.future_code = '%s' and a.state_ts >= %i order by a.state_ts limit 1" % (future_code, state_ts)
        cursor.execute(sql)
        done_set = cursor.fetchall()
        db.commit()
        if len(done_set) > 0 and int(done_set[0][-1]) - int(state_ts) < 10:
            cur_price = float(done_set[0][3])
            money_sold = vol * cap.unit_per_vol * cur_price
            fee = money_sold*0.0002
            new_state_dt = done_set[0][0]
            new_state_time = done_set[0][1]
            new_state_ts = done_set[0][-1]
            new_future_capital = cap.future_capital - money_sold - fee
            new_cash_capital = cap.cash_capital + money_sold
            new_total_capital = new_future_capital + new_cash_capital
            if future_type == 1:
                profit = money_sold-fee-cap.future_price_dict[future_code]*vol*cap.unit_per_vol*1.0002
                bz = '平多'
            else:
                profit = cap.future_price_dict[future_code]*vol*cap.unit_per_vol*1.0002 - money_sold-fee
                bz = '平空'
            base_fee = cap.future_price_dict[future_code] * vol * cap.unit_per_vol * 1.0002 * 0.06
            profit_rate = (base_fee + profit) / base_fee
            new_sf_capital = cap.sf_capital + profit
            sql_cap = "insert into my_cap(state_dt,state_time,total_capital,future_capital,cash_capital,sf_capital,operation,deal_price,deal_vol,unit_per_vol,laverage,future_code,profit,profit_rate,bz,state_ts)values('%s','%s','%.2f','%.2f','%.2f','%.2f','%s','%.2f','%f','%f','%.2f','%s','%.2f','%.2f','%s','%i')" % (new_state_dt, new_state_time, new_total_capital, new_future_capital, new_cash_capital, new_sf_capital, '卖出',cur_price, float(vol), cap.unit_per_vol, cap.laverage, future_code,profit ,profit_rate,bz, int(new_state_ts))
            cursor.execute(sql_cap)
            db.commit()

            sql_init = "SET SQL_SAFE_UPDATES = 0"
            cursor.execute(sql_init)
            db.commit()
            if vol < cap.future_vol_dict[future_code]:
                sql_pool = "update my_pool w set w.future_vol = w.future_vol - %i where w.future_code = '%s' and w.future_type = %i"%(int(vol),future_code,future_type)
                cursor.execute(sql_pool)
                db.commit()
            elif vol == cap.future_vol_dict[future_code]:
                sql_pool = "delete a.* from my_pool a where a.future_code = '%s' and a.future_type = %i"%(future_code,int(future_type))
                cursor.execute(sql_pool)
                db.commit()
    cursor.close()
    db.commit()
    return 1









