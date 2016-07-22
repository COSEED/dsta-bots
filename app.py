import os
import redis
import tweepy
import json
import time
import random
import requests
from datetime import timedelta

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from celery import Celery, Task
from statsd.defaults.env import statsd

app = Celery('app', broker=os.getenv('REDIS_URL'))

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i+n]

def get_cassandra():
	auth = PlainTextAuthProvider(username=os.getenv('CASSANDRA_USERNAME'), password=os.getenv('CASSANDRA_PASSWORD'))
	cluster = Cluster(os.getenv('CONTACT_POINTS').split(","), auth_provider=auth)
	cs = cluster.connect("dsta")
	return (cluster, cs)
	
def get_twitter(cs):
	tokens_rows = cs.execute("SELECT * FROM twitter_applications_tokens")
	for row in tokens_rows:
		auth = tweepy.OAuthHandler(row.consumer_key, row.consumer_secret)
		auth.set_access_token(row.access_token, row.access_token_secret)
		tweepi = tweepy.API(auth)
		return tweepi

def get_redis():
	rds = redis.Redis()
	return rds

class InstrumentedTask(Task):
	def on_success(self, rv, task_id, args, kwargs):
		statsd.incr("celery.task." + self.name + ".success")
	def on_failure(self, exc, task_id, args, kwargs, einfo):
		statsd.incr("celery.task." + self.name + ".error")

@app.task(base=InstrumentedTask)
def batch_user_profile(user_ids):
	(cluster, cs) = get_cassandra()
	try:
		tw = get_twitter(cs)
		with statsd.timer('twitter.req.users-lookup'):
			user_profiles = tw.lookup_users(user_ids)
		stmt_insert = cs.prepare("INSERT INTO users_observations (user_id, blob) values (?, ?)")
		for profile in user_profiles:
			cs.execute(stmt_insert, (profile.id, json.dumps(profile._json)))
	finally:
		cluster.shutdown()

@app.task(base=InstrumentedTask)
def batch_user_profile_enqueue():
	(cluster, cs) = get_cassandra()
	requests.get('https://cronitor.link/X7wGyK/run', timeout=5)
	try:
		targets = cs.execute("SELECT * FROM targets")
		chunked_user_ids = chunks([target.user_id for target in targets], 100)

		for chunk in chunked_user_ids:
			batch_user_profile.apply_async((chunk,))
		requests.get('https://cronitor.link/X7wGyK/complete', timeout=5)
	finally:
		cluster.shutdown()

@app.task(base=InstrumentedTask)
def ringo_conversation_tweet():
	(cluster, cs) = get_cassandra()
	rds = get_redis()
	try:
		tw = get_twitter(cs)
		setted = rds.setnx('ringo-conversation-tweet', '1')
		if not setted:
			return
		rds.expire('ringo-conversation-tweet', 86400)
		targets = cs.execute("SELECT * FROM targets")
		target_user_ids = [row.user_id for row in targets]
		choice = random.choice(target_user_ids)
		observation = cs.execute("SELECT * FROM users_observations WHERE user_id = %s", (choice,))
		if observation is None:
			return
		observation = list(observation)
		if len(observation) == 0:
			return
		row = list(observation)[0]
		profile = json.loads(row.blob)
		screen_name = profile['screen_name']
		status_text = "Hey @%s %s" % (screen_name, str(time.time()))
		with statsd.timer('twitter.req.statuses-update'):
			tw.update_status(status_text)
	except:
		rds.delete('ringo-conversation-tweet')
		raise
	finally:
		cluster.shutdown()


app.conf.update({
	'CELERYBEAT_SCHEDULE': {
		'crawl-everyones-profile-every-30-seconds': {
			'task': 'app.batch_user_profile_enqueue',
			'schedule': timedelta(seconds=30),
			'args': (),
		},
		'spam-jwgur': {
			'task': 'app.ringo_conversation_tweet',
			'schedule': timedelta(seconds=30),
			'args': (),
		},

	}
})
