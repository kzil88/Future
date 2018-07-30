import pymysql


def write_init(sf_capital, unit_per_vol, laverage):
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='future', charset='utf8')
    cursor = db.cursor()
    sql_truncate = "truncate table my_cap"
    cursor.execute(sql_truncate)
    db.commit()
    sql = "insert into my_cap(total_capital,future_capital,cash_capital,sf_capital,unit_per_vol,laverage)values('%.2f','%.2f','%.2f','%.2f','%.2f','%.2f')" % (
    float(sf_capital / laverage), float(0.00), float(sf_capital / laverage), float(sf_capital), float(unit_per_vol),
    float(laverage))
    cursor.execute(sql)
    db.commit()
    cursor.close()
    db.close()

class MY_CAP(object):
    total_capital = 0.00
    future_capital = 0.00
    cash_capital = 0.00
    sf_capital = 0.00
    unit_per_vol = 0.00
    laverage = 0.00
    future_pool = []
    future_type = []
    future_vol_dict = {}
    future_price_dict = {}
    future_ts_dict = {}




    def __init__(self):
        db = pymysql.connect(host='127.0.0.1', user='root', passwd='admin', db='future', charset='utf8')
        cursor = db.cursor()
        sql_cap = "select * from my_cap order by seq desc limit 1"
        cursor.execute(sql_cap)
        done_set = cursor.fetchall()
        db.commit()
        self.total_capital = float(done_set[0][3])
        self.future_capital = float(done_set[0][4])
        self.cash_capital = float(done_set[0][5])
        self.sf_capital = float(done_set[0][6])
        self.unit_per_vol = float(done_set[0][10])
        self.laverage = float(done_set[0][11])
        sql_pool = "select * from my_pool where future_vol > 0"
        cursor.execute(sql_pool)
        done_set_pool = cursor.fetchall()
        db.commit()
        if len(done_set_pool) > 0:
            for i in range(len(done_set_pool)):
                if int(done_set_pool[i][4]) == 1:
                    self.future_pool.append(str(done_set_pool[i][0])+'1')
                    self.future_vol_dict[str(done_set_pool[i][0])+'1'] = float(done_set_pool[i][1])
                    self.future_price_dict[str(done_set_pool[i][0])+'1'] = float(done_set_pool[i][2])
                    self.future_ts_dict[str(done_set_pool[i][0])+'1'] = float(done_set_pool[i][3])
                else:
                    self.future_pool.append(str(done_set_pool[i][0]) + '0')
                    self.future_vol_dict[str(done_set_pool[i][0]) + '0'] = float(done_set_pool[i][1])
                    self.future_price_dict[str(done_set_pool[i][0]) + '0'] = float(done_set_pool[i][2])
                    self.future_ts_dict[str(done_set_pool[i][0]) + '0'] = float(done_set_pool[i][3])
        cursor.close()
        db.close()
