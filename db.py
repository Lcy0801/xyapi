import sqlite3
import json
import datetime
import matplotlib.pyplot as plt


def createTb():
    conn = sqlite3.connect("./Actors.db")
    print("Create database successfully!")
    cursor = conn.cursor()

    createTbSQL = '''create table if not exists `actors`
                (id integer primary key autoincrement,
                name text not  null,
                actor_id integer not null,
                country text not null,
                birthday date default (date('1800-01-01')) not null,
                deathday date default (date('2100-01-01')) not null,
                gender text,
                show text,
                last_update datetime not null);'''
    cursor.execute(createTbSQL)
    print("Create actor table successfully!")
    conn.commit()
    conn.close()


def insertTb(actor: dict):
    if actor == None:
        return
    conn = sqlite3.connect("./Actors.db")
    print("Connect database successfully!")
    cursor = conn.cursor()
    # format birthday
    birthday = actor['birthday']
    if birthday == None:
        birth_year = '1800'
        birth_month = '01'
        birth_day = '01'
    else:
        birthStrs = birthday.split('-')
        birth_year = birthStrs[0]
        birth_month = birthStrs[1]
        birth_day = birthStrs[2]

    # format deathday
    deathday = actor['deathday']
    if deathday == None:
        death_year = '2100'
        death_month = '01'
        death_day = '01'
    else:
        deathStrs = deathday.split('-')
        death_year = deathStrs[0]
        death_month = deathStrs[1]
        death_day = deathStrs[2]
    shows = json.dumps(actor['shows'])
    shows = shows.replace("'", "''")

    # format last_update
    last_update = actor['last_update']
    last_update_year = last_update.year
    last_update_month = last_update.month
    last_update_day = last_update.day
    if last_update_month < 10:
        last_update_month = '0{}'.format(last_update_month)
    if last_update_day < 10:
        last_update_day = '0{}'.format(last_update_day)
    last_update_hour = last_update.hour
    if last_update_hour < 10:
        last_update_hour = '0{}'.format(last_update_hour)
    last_update_minute = last_update.minute
    if last_update_minute < 10:
        last_update_minute = '0{}'.format(last_update_minute)
    last_update_second = last_update.second
    if last_update_second < 10:
        last_update_second = '0{}'.format(last_update_second)

    insertSQL = '''insert into actors(name,actor_id,country,birthday,deathday,gender,show,last_update)
     values('{}',{},'{}',date('{}-{}-{}'),date('{}-{}-{}'),'{}','{}',datetime('{}-{}-{} {}:{}:{}'))'''\
        .format(actor['name'], actor['actor_id'], actor['country'], birth_year, birth_month, birth_day,
                death_year, death_month, death_day, actor['gender'].upper(
    ), shows, last_update_year,
        last_update_month, last_update_day, last_update_hour, last_update_minute, last_update_second)
    cursor.execute(insertSQL)
    conn.commit()
    conn.close()
    print("Insert data successfully!")


def selectDb(fieldv, type=0):
    '''
    type=0 select by name
    type=1 select by id
    '''
    if type:
        selectSQL = "select * from actors where id = {}".format(fieldv)
    else:
        selectSQL = "select * from actors where name like '%{}%'".format(
            fieldv)
    conn = sqlite3.connect("./Actors.db")
    print("Connect database successfully!")
    cursor = conn.cursor()
    cursor.execute(selectSQL)
    row = cursor.fetchone()
    if row == None:
        return None
    actor = dict()
    actor['id'] = row[0]
    actor['name'] = row[1]
    actor['actor_id'] = row[2]
    actor['country'] = row[3]
    actor['birthday'] = row[4]
    actor['deathday'] = row[5]
    actor['gender'] = row[6]
    actor['shows'] = row[7]
    actor['last_update'] = row[8]
    conn.close()
    return actor


def selectBrothers(id):
    conn = sqlite3.connect("./Actors.db")
    print("Connect database successfully!")
    cursor = conn.cursor()
    selectSQL = "select max(id) from actors where id < {}".format(id)
    cursor.execute(selectSQL)
    row = cursor.fetchone()
    if row:
        previousId = row[0]
    else:
        previousId = -1
    selectSQL = "select min(id) from actors where id > {}".format(id)
    cursor.execute(selectSQL)
    row = cursor.fetchone()
    if row:
        nextId = row[0]
    else:
        nextId = -1
    conn.close()
    return previousId, nextId


def deleteActorById(id: int):
    conn = sqlite3.connect("./Actors.db")
    print("Connect database successfully!")
    cursor = conn.cursor()
    deleteSQL = 'delete from actors where id = {}'.format(id)
    cursor.execute(deleteSQL)
    conn.commit()
    conn.close()


def updateActorById(id: int, data: dict):
    ''''
    return -1 actor doesn't exist!
    return 0 update failed!
    return 1 update successfully
    '''
    conn = sqlite3.connect("./Actors.db")
    print("Connect database successfully!")
    cursor = conn.cursor()
    selectSQL = "select * from actors where id = {}".format(id)
    cursor.execute(selectSQL)
    row = cursor.fetchone()
    if row == None:
        return -1
    try:
        for key, value in data.items():
            if key in ["name", "country", "gender"]:
                updateSQL = "update actors set {} = '{}' where id = {}".format(
                    key, value, id)
                cursor.execute(updateSQL)
                conn.commit()
                continue
            if key == "actor_id":
                updateSQL = "update actors set actor_id = {} where id = {}".format(
                    value, id)
                cursor.execute(updateSQL)
                conn.commit()
                continue
            if key in ["birthday", "deathday"]:
                dateStrs = value.split('-')
                year = dateStrs[2]
                month = dateStrs[1]
                day = dateStrs[0]
                updateSQL = "update actors set {} = date('{}-{}-{}') where id = {}".format(
                    key, year, month, day, id)
                cursor.execute(updateSQL)
                conn.commit()
                continue
            if key == 'shows':
                selectSQL = 'select show from actors where id = {}'.format(id)
                cursor.execute(selectSQL)
                row = cursor.fetchone()
                oldshow = json.loads(row[0])
                newshow = oldshow.extend(value)
                newshow = json.dumps(newshow)
                newshow = newshow.replace("'", "''")
                updateSQL = "update actors set show = {} where id = {}".format(
                    newshow, id)
                cursor.execute(updateSQL)
                conn.commit()
    except:
        print(123)
        return 0
    last_update = datetime.datetime.now()
    last_update_year = last_update.year
    last_update_month = last_update.month
    last_update_day = last_update.day
    if last_update_month < 10:
        last_update_month = '0{}'.format(last_update_month)
    if last_update_day < 10:
        last_update_day = '0{}'.format(last_update_day)
    last_update_hour = last_update.hour
    if last_update_hour < 10:
        last_update_hour = '0{}'.format(last_update_hour)
    last_update_minute = last_update.minute
    if last_update_minute < 10:
        last_update_minute = '0{}'.format(last_update_minute)
    last_update_second = last_update.second
    if last_update_second < 10:
        last_update_second = '0{}'.format(last_update_second)
    updateSQL = "update actors set last_update = datetime('{}-{}-{} {}:{}:{}') where id = {}"\
        .format(last_update_year, last_update_month, last_update_day, last_update_hour, last_update_minute, last_update_second, id)
    cursor.execute(updateSQL)
    conn.commit()
    conn.close()
    return 1


def retrieveAllActors(order: str, page: int, size: int, filter: str):
    conn = sqlite3.connect("./Actors.db")
    print("Connect database successfully!")
    cursor = conn.cursor()
    orderfields = order.split(',')
    m = (page-1)*size+1
    n = page*size
    selectSQL = "select {} from actors order by ".format(filter)
    for orderField in orderfields:
        flag = orderField[0]
        fieldName = orderField[1:]
        if flag == '+':
            selectSQL += '{} asc,'.format(fieldName)
        else:
            selectSQL += '{} desc,'.format(fieldName)
    selectSQL = selectSQL[0:-1]
    selectSQL += " limit {} offset {}".format(n-m+1, m)
    cursor.execute(selectSQL)
    rows = cursor.fetchall()
    res = dict({"page": page, "page-siez": size})
    if rows == None:
        return res
    fields = filter.split(',')
    fieldsnum = len(fields)
    actors = []
    for row in rows:
        actor = dict()
        for i in range(fieldsnum):
            actor[fields[i]] = row[i]
        actors.append(actor)
    res['actors'] = actors
    _links = dict()
    _links["self"] = dict(
        {"href": "http://127.0.0.1:5000/actors?order={}&page={}&size={}&filter={}".format(order, page, size, filter)})
    countSQL = 'select count(id) from actors'
    cursor.execute(countSQL)
    count = cursor.fetchone()[0]
    pagemax = count//size+1
    if page < pagemax:
        _links["next"] = dict(
            {"href": "http://127.0.0.1:5000/actors?order={}&page={}&size={}&filter={}".format(order, page+1, size, filter)})
    res['_links'] = _links
    conn.close()
    return res


def getActorsStatistic(format_: str, by: str):
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
    if format_ == 'json':
        return res
    else:
        plt.figure()
        fieldnum = len(fields)
        for i in range(1, fieldnum+1):
            plt.subplot(1, fieldnum, i)
            fieldDict = res['by-{}'.format(fields[i-1])]
            keys = fieldDict.keys()
            values = fieldDict.values()
            plt.pie(x=values, labels=keys, autopct="%0.2f%%")
        plt.show()
    return res


if __name__ == '__main__':
    # createTb()
    # print(selectDb('Mi'))
    # deleteActorById(1)
    # retrieveAllActors("+id", 1, 10, "id,name")
    getActorsStatistic(1, 1)
