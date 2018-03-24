from dbco import *
import time


def getCountOfTweetsXHours(hours):
    return db.tweet.find({'timestamp': {'$gte': time.time() - int(hours) * 3600}}).count()


def getTrendingHashtagsXHours(hours):
    match = {'$match': {'timestamp': {'$gte': time.time() - int(hours) * 3600}}}
    unwind = {'$unwind': '$hashtags'}
    group = {'$group': {'_id': '$hashtags', 'total': {'$sum': 1}}}
    sort = {'$sort': {'total': -1}}
    limit = {'$limit': 10}
    pipeline = [match, unwind, group, sort, limit]
    result = list()
    for elem in list(db.tweet.aggregate(pipeline)):
        result.append(elem['_id'])
    return result


def getTrendingMentionsXHours(hours):
    match = {'$match': {'timestamp': {'$gte': time.time() - int(hours) * 3600}}}
    unwind = {'$unwind': '$mentions_name'}
    group = {'$group': {'_id': '$mentions_name', 'total': {'$sum': 1}}}
    sort = {'$sort': {'total': -1}}
    limit = {'$limit': 10}
    pipeline = [match, unwind, group, sort, limit]
    result = list()
    for elem in list(db.tweet.aggregate(pipeline)):
        result.append(elem['_id'])
    return result


def getTrendingKeywordsXHours(hours):
    match = {'$match': {'timestamp': {'$gte': time.time() - int(hours) * 3600}}}
    unwind = {'$unwind': '$keywords'}
    group = {'$group': {'_id': '$keywords', 'total': {'$sum': 1}}}
    sort = {'$sort': {'total': -1}}
    limit = {'$limit': 10}
    pipeline = [match, unwind, group, sort, limit]
    result = list()
    for elem in list(db.tweet.aggregate(pipeline)):
        result.append(elem['_id'])
    return result


def getActiveAuthorsXHours(hours):
    match = {'$match': {'timestamp': {'$gte': time.time() - int(hours) * 3600}}}
    group = {'$group': {'_id': '$author_name', 'total': {'$sum': 1}}}
    sort = {'$sort': {'total': -1}}
    limit = {'$limit': 10}
    pipeline = [match, group, sort, limit]
    result = list()
    for elem in list(db.tweet.aggregate(pipeline)):
        result.append(elem['_id'])
    return result


def getAuthorFollowersCount(name):
    match = {'$match': {'author_name': name}}
    sort = {'$sort': {'timestamp': -1}}
    limit = {'$limit': 1}
    pipeline = [match, sort, limit]
    return list(db.tweet.aggregate(pipeline))[0]['author_followers_count']


def getAuthorTweets(name):
    match = {'$match': {'author_name': name}}
    sort = {'$sort': {'timestamp': -1}}
    pipeline = [match, sort]
    result = list()
    for elem in list(db.tweet.aggregate(pipeline)):
        result.append(elem['text'])
    return result


def timestampify(date):
    t_obj = time.strptime(date, "%Y-%m-%d")
    return time.mktime(t_obj)


def getCountOfTweetsDate(date):
    t = timestampify(date)
    return db.tweet.find({'timestamp': {'$gte': t, '$lt': t + 24 * 3600}}).count()


def getTrendingHashtagsDate(date):
    t = timestampify(date)
    match = {'$match': {'timestamp': {'$gte': t, '$lt': t + 24 * 3600}}}
    unwind = {'$unwind': '$hashtags'}
    group = {'$group': {'_id': '$hashtags', 'total': {'$sum': 1}}}
    sort = {'$sort': {'total': -1}}
    limit = {'$limit': 10}
    pipeline = [match, unwind, group, sort, limit]
    result = list()
    for elem in list(db.tweet.aggregate(pipeline)):
        result.append(elem['_id'])
    return result


def getTrendingMentionsDate(date):
    t = timestampify(date)
    match = {'$match': {'timestamp': {'$gte': t, '$lt': t + 24 * 3600}}}
    unwind = {'$unwind': '$mentions_name'}
    group = {'$group': {'_id': '$mentions_name', 'total': {'$sum': 1}}}
    sort = {'$sort': {'total': -1}}
    limit = {'$limit': 10}
    pipeline = [match, unwind, group, sort, limit]
    result = list()
    for elem in list(db.tweet.aggregate(pipeline)):
        result.append(elem['_id'])
    return result


def getTrendingKeywordsDate(date):
    t = timestampify(date)
    match = {'$match': {'timestamp': {'$gte': t, '$lt': t + 24 * 3600}}}
    unwind = {'$unwind': '$keywords'}
    group = {'$group': {'_id': '$keywords', 'total': {'$sum': 1}}}
    sort = {'$sort': {'total': -1}}
    limit = {'$limit': 10}
    pipeline = [match, unwind, group, sort, limit]
    result = list()
    for elem in list(db.tweet.aggregate(pipeline)):
        result.append(elem['_id'])
    return result


def getSampleTweetsDate(date):
    t = timestampify(date)
    match = {'$match': {'timestamp': {'$gte': t, '$lt': t + 24 * 3600}}}
    sample = {'$sample': {'size': 10}}
    pipeline = [match, sample]
    result = list()
    for elem in list(db.tweet.aggregate(pipeline)):
        result.append(elem['text'])
    return result


def getSampleTweetsFromHashtags(hashtagList):
    match = {'$match': {'hashtags': {'$in': hashtagList}}}
    sample = {'$sample': {'size': 10}}
    sort = {'$sort': {'timestamp': -1}}
    pipeline = [match, sample, sort]
    result = list()
    for elem in list(db.tweet.aggregate(pipeline)):
        result.append(elem['text'])
    return result


def getSampleTweetsFromMentions(mentionList):
    match = {'$match': {'mentions_name': {'$in': mentionList}}}
    sample = {'$sample': {'size': 10}}
    sort = {'$sort': {'timestamp': -1}}
    pipeline = [match, sample, sort]
    result = list()
    for elem in list(db.tweet.aggregate(pipeline)):
        result.append(elem['text'])
    return result


def getSampleTweetsFromKeywords(keywordList):
    match = {'$match': {'keywords': {'$in': keywordList}}}
    sample = {'$sample': {'size': 10}}
    sort = {'$sort': {'timestamp': -1}}
    pipeline = [match, sample, sort]
    result = list()
    for elem in list(db.tweet.aggregate(pipeline)):
        result.append(elem['text'])
    return result


def getHashtagsTimeline(hashtag, daysLoad, intevalHour=24):
    bucketSize = int(intevalHour) * 3600
    startTime = time.time() - daysLoad * 24 * 3600
    startTime -= startTime % bucketSize
    endTime = time.time()
    match = {'$match': {'hashtags': hashtag, 'timestamp': {'$gte': startTime, '$lt': endTime}}}
    project = {'$project': {'tsMod': {'$subtract': ['$timestamp', {'$mod': ['$timestamp', bucketSize]}]}}}
    group = {'$group': {'_id': '$tsMod', 'count': {'$sum': 1}}}
    sort = {'$sort': {'_id': 1}}
    pipeline = [match, project, group, sort]
    result = list()
    ts = startTime
    for elem in list(db.tweet.aggregate(pipeline)):
        if ts < elem['_id']:
            while ts < elem['_id']:
                if (intevalHour >= 24):
                    t_str = time.strftime("%Y-%m-%d", time.localtime(ts))
                else:
                    t_str = time.strftime("%Y-%m-%d-%H", time.localtime(ts))
                result.append({'time': t_str, 'count': 0})
                ts += bucketSize
        if (intevalHour >= 24):
            t_str = time.strftime("%Y-%m-%d", time.localtime(ts))
        else:
            t_str = time.strftime("%Y-%m-%d-%H", time.localtime(ts))
        result.append({'time': t_str, 'count': elem['count']})
        ts += bucketSize
    while ts < endTime:
        if (intevalHour >= 24):
            t_str = time.strftime("%Y-%m-%d", time.localtime(ts))
        else:
            t_str = time.strftime("%Y-%m-%d-%H", time.localtime(ts))
        result.append({'time': t_str, 'count': 0})
        ts += bucketSize
    return result


def getMentionsTimeline(name, daysLoad, intevalHour=24):
    bucketSize = int(intevalHour) * 3600
    startTime = time.time() - daysLoad * 24 * 3600
    startTime -= startTime % bucketSize
    endTime = time.time()
    match = {'$match': {'mentions_name': name, 'timestamp': {'$gte': startTime, '$lt': endTime}}}
    project = {'$project': {'tsMod': {'$subtract': ['$timestamp', {'$mod': ['$timestamp', bucketSize]}]}}}
    group = {'$group': {'_id': '$tsMod', 'count': {'$sum': 1}}}
    sort = {'$sort': {'_id': 1}}
    pipeline = [match, project, group, sort]
    result = list()
    ts = startTime
    for elem in list(db.tweet.aggregate(pipeline)):
        if ts < elem['_id']:
            while ts < elem['_id']:
                if (intevalHour >= 24):
                    t_str = time.strftime("%Y-%m-%d", time.localtime(ts))
                else:
                    t_str = time.strftime("%Y-%m-%d-%H", time.localtime(ts))
                result.append({'time': t_str, 'count': 0})
                ts += bucketSize
        if (intevalHour >= 24):
            t_str = time.strftime("%Y-%m-%d", time.localtime(ts))
        else:
            t_str = time.strftime("%Y-%m-%d-%H", time.localtime(ts))
        result.append({'time': t_str, 'count': elem['count']})
        ts += bucketSize
    while ts < endTime:
        if (intevalHour >= 24):
            t_str = time.strftime("%Y-%m-%d", time.localtime(ts))
        else:
            t_str = time.strftime("%Y-%m-%d-%H", time.localtime(ts))
        result.append({'time': t_str, 'count': 0})
        ts += bucketSize
    return result


def getKeywordsTimeline(kw, daysLoad, intevalHour=24):
    bucketSize = int(intevalHour) * 3600
    startTime = time.time() - daysLoad * 24 * 3600
    startTime -= startTime % bucketSize
    endTime = time.time()
    match = {'$match': {'keywords': kw, 'timestamp': {'$gte': startTime, '$lt': endTime}}}
    project = {'$project': {'tsMod': {'$subtract': ['$timestamp', {'$mod': ['$timestamp', bucketSize]}]}}}
    group = {'$group': {'_id': '$tsMod', 'count': {'$sum': 1}}}
    sort = {'$sort': {'_id': 1}}
    pipeline = [match, project, group, sort]
    result = list()
    ts = startTime
    for elem in list(db.tweet.aggregate(pipeline)):
        if ts < elem['_id']:
            while ts < elem['_id']:
                if (intevalHour >= 24):
                    t_str = time.strftime("%Y-%m-%d", time.localtime(ts))
                else:
                    t_str = time.strftime("%Y-%m-%d-%H", time.localtime(ts))
                result.append({'time': t_str, 'count': 0})
                ts += bucketSize
        if (intevalHour >= 24):
            t_str = time.strftime("%Y-%m-%d", time.localtime(ts))
        else:
            t_str = time.strftime("%Y-%m-%d-%H", time.localtime(ts))
        result.append({'time': t_str, 'count': elem['count']})
        ts += bucketSize
    while ts < endTime:
        if (intevalHour >= 24):
            t_str = time.strftime("%Y-%m-%d", time.localtime(ts))
        else:
            t_str = time.strftime("%Y-%m-%d-%H", time.localtime(ts))
        result.append({'time': t_str, 'count': 0})
        ts += bucketSize
    return result


def getRelatedHashtags(ht, daysLoad=7):
    match = {'$match': {'hashtags': ht, 'timestamp': {'$gte': time.time() - daysLoad * 24 * 3600}}}
    unwind = {'$unwind': '$hashtags'}
    group = {'$group': {'_id': '$hashtags', 'count': {'$sum': 1}}}
    sort = {'$sort': {'count': -1}}
    limit = {'$limit': 11}
    pipeline = [match, unwind, group, sort, limit]
    dataList = list(db.tweet.aggregate(pipeline))
    result = list()
    for i in range(1, len(dataList)):
        result.append(dataList[i]['_id'])
    return result


def getRelatedMentions(name, daysLoad=7):
    match = {'$match': {'mentions_name': name, 'timestamp': {'$gte': time.time() - daysLoad * 24 * 3600}}}
    unwind = {'$unwind': '$mentions_name'}
    group = {'$group': {'_id': '$mentions_name', 'count': {'$sum': 1}}}
    sort = {'$sort': {'count': -1}}
    limit = {'$limit': 11}
    pipeline = [match, unwind, group, sort, limit]
    dataList = list(db.tweet.aggregate(pipeline))
    result = list()
    for i in range(1, len(dataList)):
        result.append(dataList[i]['_id'])
    return result


def getRelatedKeywords(kw, daysLoad=7):
    match = {'$match': {'keywords': kw, 'timestamp': {'$gte': time.time() - daysLoad * 24 * 3600}}}
    unwind = {'$unwind': '$keywords'}
    group = {'$group': {'_id': '$keywords', 'count': {'$sum': 1}}}
    sort = {'$sort': {'count': -1}}
    limit = {'$limit': 11}
    pipeline = [match, unwind, group, sort, limit]
    dataList = list(db.tweet.aggregate(pipeline))
    result = list()
    for i in range(1, len(dataList)):
        result.append(dataList[i]['_id'])
    return result
