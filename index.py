from flask import Flask, request, make_response, send_from_directory
import json
from actor import *
from db import *
import os
import multiprocessing
import shutil

app = Flask('__name__')
app.config['IMAGEFOLDER'] = "./imgs"


def formatParameter(parameter: str):
    parameter = parameter.replace(',', ' ')
    parameter = parameter.replace('，', ' ')
    parameter = parameter.replace('-', ' ')
    parameter = parameter.replace('_', ' ')
    parameter = parameter.replace('?', ' ')
    parameter = parameter.replace('？', ' ')
    parameter = parameter.replace('+', ' ')
    return parameter.strip()


@app.route('/actors', methods=['post'])
def addActor():
    name = request.args.get('name')
    name = formatParameter(name)
    actor = selectDb(name)
    if actor == None:
        actor = crawlActor(name)
        if actor:
            insertTb(actor)
            actor = selectDb(name)
        else:
            return make_response("Sorry! There is no such person in our database!")
    if actor:
        res = dict()
        res['id'] = actor['id']
        res['last_update'] = actor['last_update']
        res['_links'] = dict({'self': dict(
            {'href': 'http://127.0.0.1:5000/actors/{}'.format(actor['actor_id'])})})
        return make_response(json.dumps(res))
    else:
        return make_response("Sorry! There is no such person in our database!")


@app.route('/actors/<int:id>', methods=['GET'])
def getActor(id):
    if id < 0:
        return make_response("Sorry! The id should be nonnegative integer!")
    actor = selectDb(id, 1)
    if actor == None:
        return make_response("Sorry! There is no such a person in our database!")
    res = dict()
    res['id'] = actor['id']
    res['last_update'] = actor['last_update']
    res['name'] = actor['name']
    res['country'] = actor['country']
    if actor['birthday'] == '1800-01-01':
        res['birthday'] = 'unknow'
    else:
        res['birthday'] = actor['birthday']
    if actor['deathday'] == '2100-01-01':
        res['deathday'] = 'unknow'
    else:
        res['deathday'] = actor['deathday']
    shows = json.loads(actor['shows'])
    res['shows'] = shows
    previousId, nextId = selectBrothers(res['id'])
    selfLink = dict(
        {"href": 'http://127.0.0.1:5000/actors/{}'.format(res['id'])})
    _links = dict({"self": selfLink})
    if previousId != -1:
        previousLink = dict(
            {"href": 'http://127.0.0.1:5000/actors/{}'.format(previousId)})
        _links['previous'] = previousLink
    if nextId != -1:
        nextLink = dict(
            {"href": 'http://127.0.0.1:5000/actors/{}'.format(previousId)})
        _links['next'] = nextLink
    res['_link'] = _links
    return make_response(json.dumps(res))


@app.route('/actors/<int:id>', methods=['DELETE'])
def delActor(id):
    if id < 0:
        return make_response("Sorry! The id should be nonnegative integer!")
    deleteActorById(id)
    res = dict()
    res['id'] = id
    res['message'] = "The actor with id {} was removed from the database!".format(
        id)
    return make_response(json.dumps(res))


@app.route('/actors/<int:id>', methods=['PATCH'])
def updateActor(id):
    data = request.form
    flag = updateActorById(id, data)
    if flag == -1:
        return make_response("Sorry! There is no such a person in our database!")
    elif flag == 0:
        return make_response("update failed!")
    else:
        res = dict()
        res['id'] = id
        res['last_update'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        res['_links'] = dict(
            {"self": dict({"href": "http://127.0.0.1:5000/actors/{}".format(id)})})
        return make_response(json.dumps(res))


@app.route("/actors", methods=['GET'])
def retrieveAll():
    # fetch query parameters
    parameterDict = request.args
    keys = parameterDict.keys()
    order = "+id"
    page = 1
    size = 10
    filter = "id,name"
    if 'order' in keys:
        order = parameterDict['order']
    if 'page' in keys:
        page = int(parameterDict['page'])
    if 'size' in keys:
        size = int(parameterDict['size'])
    if 'filter' in keys:
        filter = parameterDict['filter']
    if page <= 0:
        page = 1
    if size <= 0:
        size = 10

    res = retrieveAllActors(order, page, size, filter)
    return make_response(json.dumps(res))


def draw(by, figname):
    os.system("python drawpie.py --by={} --figname={}".format(by, figname))


@app.route('/actors/statistic', methods=['GET'])
def getStatistic():
    format = request.args.get("format")
    by = request.args.get("by")
    if format == 'json':
        res = getActorsStatistic(format, by)
        return make_response(json.dumps(res))
    else:
        figname = datetime.datetime.now().timestamp()*pow(10, 6)
        figname = str(int(figname))+".png"
        p = multiprocessing.Process(target=draw, args=(by, figname))
        p.start()
        p.join()
        return send_from_directory(
            app.config["IMAGEFOLDER"], figname, as_attachment=True)


if __name__ == "__main__":
    app.run()
