from flask import Flask, render_template
from apiFunctions import *

app = Flask(__name__)


@app.route('/')
def index():
	return render_template('index.html')


@app.route('/general')
def general():
	return render_template('general.html', countOneHour=getCountOfTweetsXHours(1), count24Hours=getCountOfTweetsXHours(24), hList=getTrendingHashtagsXHours(24), mList=getTrendingMentionsXHours(24), kList=getTrendingKeywordsXHours(24), aList=getActiveAuthorsXHours(24))


@app.route('/author/<name>')
def author(name):
	return render_template('author.html', name=name, count=getAuthorFollowersCount(name), tList=getAuthorTweets(name))


@app.route('/date/<dateTime>')
def date(dateTime):
	return render_template('date.html', date=dateTime, count=getCountOfTweetsDate(dateTime), hList=getTrendingHashtagsDate(dateTime), mList=getTrendingMentionsDate(dateTime), kList=getTrendingKeywordsDate(dateTime), tList=getSampleTweetsDate(dateTime))


@app.route('/hashtag/<ht>')
def hashtag(ht):
	return render_template('hashtag.html', hashtag=ht, tList=getSampleTweetsFromHashtags([ht]), timelineListOneDay=getHashtagsTimeline(ht, 1, 3), timelineListOneWeek=getHashtagsTimeline(ht, 7), timelineList8Weeks=getHashtagsTimeline(ht, 7 * 8, 24 * 7), hList=getRelatedHashtags(ht))


@app.route('/mention/<mt>')
def mention(mt):
	return render_template('mention.html', mention=mt, tList=getSampleTweetsFromMentions([mt]), timelineListOneDay=getMentionsTimeline(mt, 1, 3), timelineListOneWeek=getMentionsTimeline(mt, 7), timelineList8Weeks=getMentionsTimeline(mt, 7 * 8, 24 * 7), mList=getRelatedMentions(mt))


@app.route('/keyword/<kw>')
def keyword(kw):
	return render_template('keyword.html', keyword=kw, tList=getSampleTweetsFromKeywords([kw]), timelineListOneDay=getKeywordsTimeline(kw, 1, 3), timelineListOneWeek=getKeywordsTimeline(kw, 7), timelineList8Weeks=getKeywordsTimeline(kw, 7 * 8, 24 * 7), kList=getRelatedKeywords(kw))


if __name__ == "__main__":
	app.run(host='0.0.0.0', port=5000)
