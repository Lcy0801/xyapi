from operator import le
import requests
import json
import datetime


def crawlActor(name):
    req = requests.get(
        url='https://api.tvmaze.com/search/people', params={'q': name})
    actors = json.loads(req.text)
    n = len(actors)
    if n == 0:
        return None
    firstOne = actors[0]['person']
    actor = dict()
    actor['actor_id'] = firstOne['id']
    actor['name'] = firstOne['name']
    if firstOne['country']:
        actor['country'] = firstOne['country']['name']
    else:
        actor['country'] ='unknow'
    actor['birthday'] = firstOne['birthday']
    actor['deathday'] = firstOne['deathday']
    actor['gender'] = firstOne['gender']
    actor['last_update'] = datetime.datetime.now()
    # search shows by this actors'id
    actorId = actor['actor_id']
    print(actorId)
    req = requests.get(
        url='https://api.tvmaze.com/people/{}/castcredits'.format(actorId))
    shows = json.loads(req.text)
    showList = []
    for show in shows:
        showHref = show['_links']['show']['href']
        req = requests.get(url=showHref)
        showDetail = json.loads(req.text)
        showName = showDetail['name']
        showList.append(showName)
    actor['shows'] = showList
    return actor


if __name__ == "__main__":
    print(crawlActor("Rache "))
