import sqlite3
import matplotlib.pyplot as plt
import datetime
import argparse


def getActorsStatistic(by: str, figname: str):
    conn = sqlite3.connect("./Actors.db")
    print("Connect database successfully!")
    cursor = conn.cursor()
    countSQL = 'select count(id) from actors'
    cursor.execute(countSQL)
    count = cursor.fetchone()[0]
    now = datetime.datetime.now()
    lastday = now-datetime.timedelta(days=1)
    lastday_year = lastday.year
    lastday_month = lastday.month
    lastday_day = lastday.day
    if lastday_month < 10:
        lastday_month = '0{}'.format(lastday_month)
    if lastday_day < 10:
        lastday_day = '0{}'.format(lastday_day)
    lastday_hour = lastday.hour
    if lastday_hour < 10:
        lastday_hour = '0{}'.format(lastday_hour)
    lastday_minute = lastday.minute
    if lastday_minute < 10:
        lastday_minute = '0{}'.format(lastday_minute)
    lastday_second = lastday.second
    if lastday_second < 10:
        lastday_second = '0{}'.format(lastday_second)
    updateSQL = "select count(id) from actors where last_update > datetime('{}-{}-{} {}:{}:{}')"\
        .format(lastday_year, lastday_month, lastday_day, lastday_hour, lastday_minute, lastday_second)
    cursor.execute(updateSQL)
    updatecount = cursor.fetchone()[0]
    fields = by.split(',')
    res = dict()
    res['total'] = count
    res['total-updated'] = updatecount
    for field in fields:
        selectSQL = "select {},count(id) from actors group by {}".format(
            field, field)
        fielddict = dict()
        cursor.execute(selectSQL)
        rows = cursor.fetchall()
        for row in rows:
            fielddict[row[0]] = format(row[1]/count*100, "0.1f")
        res['by-{}'.format(field)] = fielddict
    conn.close()
    plt.figure()
    fieldnum = len(fields)
    for i in range(1, fieldnum+1):
        plt.subplot(1, fieldnum, i)
        fieldDict = res['by-{}'.format(fields[i-1])]
        keys = fieldDict.keys()
        values = fieldDict.values()
        plt.pie(x=values, labels=keys, autopct="%0.2f%%")
    plt.title('total:{}\nupdate-total:{}'.format(count, updatecount))
    plt.savefig("./imgs/{}".format(figname))
    plt.cla()
    plt.clf()
    plt.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--by', type=str, required=True)
    parser.add_argument('--figname', type=str, required=True)
    args = parser.parse_args()
    by = args.by
    figname = args.figname
    getActorsStatistic(by, figname)
