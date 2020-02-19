#!/usr/bin/env python

import re
import os
import sys
import json
import time
import numpy as np
from state import db_state
from datetime import datetime
from pymongo import MongoClient
import sentiment.sentiment_analyzer as sa


print('Connecting to MongoDB and get collections ...')
state = db_state()
client = MongoClient(state['db_server_address'], state['db_server_port'])
db = client[state['db_name']]
tweet_coll = db[state['twitter_collection']]
movie_coll = db[state['movie_collection']]
print('Connected ...')

# done
def get_sentiment(sent):
    prediction, confidence = sa.sentiment(sent)
    return 1.0 if prediction == 'pos' else 0.0

def get_imdb_score(ts):
    return 6.0 + round((ts * 4), 2)

# ------------------------------------- Tweet collection modification functions
    # -------------- Insert
def tweet_insert_json_folder(tweet_folder):
    tweets = []
    for json_file in tweet_folder:
        tweet = json.loads(open(json_file, 'r').read())
        tweets.append(tweet)
    tweet_insert_all(tweets)

# done
def tweet_insert_all(tweets):
    result = tweet_coll.insert_many(list(tweets))

# done
def tweet_insert_one(tweet):
    result = tweet_coll.insert_one(tweet)

    # -------------- Update
# done
def tweet_update_all(tweets):
    for tweet in tweets:
        tweet_update_one(tweet)        

# done
def tweet_update_one(tweet):
    result = tweet_coll.update({'_id':tweet['_id']}, {"$set": tweet}, upsert=False)

    # -------------- Delete
def tweet_delete_json_folder(tweet_folder):
    tweets = []
    for json_file in tweet_folder:
        tweet = json.loads(open(json_file, 'r').read())
        tweets.append(tweet)
    tweet_delete_all(tweets)

# done
def tweet_collection_delete():
    tweet_coll.drop()

# done
def tweet_delete_everything():
    tweet_coll.delete_many({})

# done
def tweet_delete_all(query):
    result = tweet_coll.delete_many(query)

# done
def tweet_delete_one(query):
    result = tweet_coll.delete_one(query)

    # -------------- Find
# done
def tweet_find_all(query):
    tweets = tweet_coll.find(query).limit(state['twitter_limit'])
    return tweets

# done
def tweet_find_one(query):
    tweet = tweet_coll.find_one(query)
    return tweet

# ------------------------------------- Movie collection modification functions
    # -------------- Insert
def movie_insert_json_folder(movie_folder):
    movies = []
    for json_file in movie_folder:
        movie = json.loads(open(json_file, 'r').read())
        movies.append(movie)
    movie_insert_all(movies)

# done
def movie_insert_all(movies):
    result = movie_coll.insert_many(list(movies))

# done
def movie_insert_one(movie):
    result = movie_coll.insert_one(movie)

    # -------------- Update
# done
def movie_update_all(movies):
    for movie in movies:
        movie_update_one(movie)        

# done
def movie_update_one(movie):
    result = movie_coll.update({'_id':movie['_id']}, {"$set": movie}, upsert=False)

    # -------------- Delete
def movie_delete_json_folder(movie_folder):
    movies = []
    for json_file in movie_folder:
        movie = json.loads(open(json_file, 'r').read())
        movies.append(movie)
    movie_delete_all(movies)

# done
def movie_collection_delete():
    movie_coll.drop()

# done
def movie_delete_everything():
    movie_coll.delete_many({})

# done
def movie_delete_all(query):
    result = movie_coll.delete_many(query)

# done
def movie_delete_one(query):
    result = movie_coll.delete_one(query)

    # -------------- Find
# done
def movie_find_all(query):
    movies = movie_coll.find(query)
    return movies

# done
def movie_find_one(query):
    movie = movie_coll.find_one(query)
    return movie

# done
def get_movie_with_id(mid):
    movie = movie_coll.find_one({'_id': mid})
    return movie

# ------------------------------------- Desired Queries

# done
def search_movie_by_title(title):
    movies = movie_find_all({'Title': {'$regex' : '(?<!\w)' + title.lower() + '(?!\w)', '$options' : 'i'} })
    return movies

# done
def get_matched_person_names(name):
    re = '(?<!\w)' + name.lower() + '(?!\w)'
    actor_movies = movie_find_all({'Actors': {'$regex' : re, '$options' : 'i'} })
    writer_movies = movie_find_all({'Writer': {'$regex' : re, '$options' : 'i'} })
    director_movies = movie_find_all({'Director': {'$regex' : re, '$options' : 'i'} })
    people = {}
    for m in actor_movies:
        actors = m['Actors'].split(',')
        for actor in actors:
            if re.match(name.lower(), actor):
                if actor not in people:
                    actor = re.sub('\(.*\)', '', actor)
                    actor = re.sub('^ *', '', actor)
                    actor = re.sub(' *$', '', actor)
                    people.append(actor)
    for m in writer_movies:
        writers = m['Writer'].split(',')
        for writer in writers:
            if re.match(name.lower(), writer):
                if writer not in people:
                    writer = re.sub('\(.*\)', '', writer)
                    writer = re.sub('^ *', '', writer)
                    writer = re.sub(' *$', '', writer)
                    people.append(writer)
    for m in director_movies:
        directors = m['Director'].split(',')
        for director in directors:
            if re.match(name.lower(), director):
                if director not in people:
                    director = re.sub('\(.*\)', '', director)
                    director = re.sub('^ *', '', director)
                    director = re.sub(' *$', '', director)
                    people.append(director)

    return people

# done
def get_movies_involving_person(name):
    re = '(?<!\w)' + name.lower() + '(?!\w)'
    actor_movies = movie_find_all({'Actors': {'$regex' : re, '$options' : 'i'} })
    writer_movies = movie_find_all({'Writer': {'$regex' : re, '$options' : 'i'} })
    director_movies = movie_find_all({'Director': {'$regex' : re, '$options' : 'i'} })
    actor_movie_list = {}
    for m in actor_movies:
        actor_movie_list[m['Title']] = m['_id']
    writer_movie_list = {}
    for m in writer_movies:
        writer_movie_list[m['Title']] = m['_id']
    director_movie_list = {}
    for m in director_movies:
        director_movie_list[m['Title']] = m['_id']

    return (actor_movie_list, writer_movie_list, director_movie_list)
    
# done
def get_tweets_contains(word_list):
    if len(word_list) > 1:
        query = []
        for word in word_list:
            query.append({"text": {'$regex': '(?<!\w)' + word.lower() + '(?!\w)', '$options' : 'i'}})

        tweets = tweet_find_all({'$and' : query})
    else:
        tweets = tweet_find_all({'text' : {'$regex': '(?<!\w)' + word_list[0].lower() + '(?!\w)', '$options' : 'i'}})
    return tweets

def get_movie_tweets_contains(movie, word_list=None):
    help_words = ['movie', 'cinema']
    query = []
    query = {'$or' : [{'text' : {'$regex': '#' + movie['Title'].lower() + '(?!\w)', '$options' : 'i'}}, 
                {'$and' : [{'text' : {'$regex': '(?<!\w)' + movie['Title'].lower() + '(?!\w)', '$options' : 'i'}},
                    {'$or' : [{'text' : {'$regex': w.lower(), '$options' : 'i'}} for w in help_words]}
                ]}
            ]}

    if word_list: 
        if len(word_list) > 1:
            add_query = []
            for word in word_list:
                add_query.append({"text": {'$regex': '(?<!\w)' + word.lower() + '(?!\w)', '$options' : 'i'}})
            query = {'$and' : [query, {'$or' : add_query}]}
        else:
            query = {'$and' : [query, {"text": {'$regex': '(?<!\w)' + word_list[0].lower() + '(?!\w)', '$options' : 'i'}}]}
        
        
    # print query
    tweets = tweet_find_all(query)
    return tweets

# done
def get_tweets_sentiment(tweets):
    upd_tweet_list = []
    sentiment_sum = 0
    tweet_count = 0
    for tweet in tweets:
        tweet_count += 1
        if 'sentiment' not in tweet.keys():
            sentiment = get_sentiment(tweet['text'])
            tweet['sentiment'] = sentiment
            upd_tweet_list.append(tweet)
            sentiment_sum += sentiment
        else:
            sentiment_sum += tweet['sentiment']

    if len(upd_tweet_list) > 1:
        tweet_update_all(upd_tweet_list)
    elif len(upd_tweet_list) > 0:
        tweet_update_one(upd_tweet_list[0])

    return (sentiment_sum / float(tweet_count)) if tweet_count > 0 else '---'

# done
def get_movie_tweet_sentiment(movie):
    if 'sentiment' not in movie.keys():
        # tweets = get_tweets_contains([movie['Title']])
        tweets = get_movie_tweets_contains(movie)
        sentiment = get_tweets_sentiment(tweets)
        movie['sentiment'] = sentiment
        movie_update_one(movie)
        return sentiment
    else:
        return movie['sentiment']

# done
def get_movie_tweets(movie):
    # return get_tweets_contains([movie['Title']])
    return get_movie_tweets_contains(movie)

# done
def get_person_tweet_sentiment(name):
    tweets = get_tweets_contains([name])
    sentiment = get_tweets_sentiment(tweets)
    return sentiment

# done
def get_movie_person_tweet_sentiment(movie, person):
    # tweets = get_tweets_contains([movie['Title'], person])
    tweets = get_movie_tweets_contains(movie, person.split())
    sentiment = get_tweets_sentiment(tweets)
    return sentiment

# done
def get_time_value(tweet):
    mt = re.sub('\+\d\d\d\d ', '', tweet['created_at'])
    dt = datetime.strptime(mt, '%a %b %d %H:%M:%S %Y')
    return dt

# done
def get_str_time_value(ts):
    mt = re.sub('\+\d\d\d\d ', '', ts)
    dt = datetime.strptime(mt, '%a %b %d %H:%M:%S %Y')
    return dt

# done
def get_temporal_tweet_sentiment(tweets):
    if len(tweets) > 0:
        sorted_tweets = sorted(tweets, key=get_time_value)
        sentiment_tweets = [get_sentiment(t['text']) for t in sorted_tweets]
        date_tweets = [get_str_time_value(t['created_at']) for t in sorted_tweets]
        avg_sentiment_tweets = [0]
        counter = 0
        for t in sentiment_tweets:
            counter += 1
            avg_sentiment_tweets.append(((avg_sentiment_tweets[-1] * (counter-1))+t)/float(counter))
        avg_sentiment_tweets = avg_sentiment_tweets[1:]

        return (date_tweets, avg_sentiment_tweets)
    return None

# done
def get_temporal_movie_tweet_sentiment(movie):
    tweets = get_movie_tweets(movie)
    tl = [t for t in tweets]
    temporal_sentiment = get_temporal_tweet_sentiment(tl)
    return temporal_sentiment

# done
def get_temporal_person_tweet_sentiment(name):
    tweets = get_tweets_contains([name])
    tl = [t for t in tweets]
    temporal_sentiment = get_temporal_tweet_sentiment(tl)
    return temporal_sentiment

# done
def get_movie_person_raiting(movie, people_list):
    people_dict = []
    pl = people_list.split(',')
    for person in pl:
        people_dict.append([person, get_movie_person_tweet_sentiment(movie, person.lower())])
    return people_dict

# done
# direct call
def get_matched_movie_names(title):
    movies = movie_find_all({'Title': {'$regex' : '(?<!\w)' + title.lower() + '(?!\w)', '$options' : 'i'} })
    return [[m['_id'],m['Title']] for m in movies]

def update_people_score(people_list):
    for i in range(len(people_list)):
        people_list[i][1] = get_imdb_score(float(people_list[i][1])) if people_list[i][1]!='---' else '---'
    return people_list

# done
# direct call
def get_movie_page_data(movie_id):
    start_time = time.time()
    movie_data = {}
    s_time = time.time()
    movie = movie_find_one({'_id': movie_id })
    print('get movies in '+ str(time.time()-s_time) + ' s')
    movie_data['id'] = movie['_id']
    movie_data['title'] = movie['Title']
    movie_data['poster'] = movie['Poster'] if 'Poster' in movie else '---'
    movie_data['plot'] = movie['Plot'] if 'Plot' in movie else '---'
    movie_data['rated'] = movie['Rated'] if 'Rated' in movie else '---'
    movie_data['language'] = movie['Language'] if 'Language' in movie else '---'
    movie_data['country'] = movie['Country'] if 'Country' in movie else '---'
    movie_data['released'] = movie['Released'] if 'Released' in movie else '---'
    movie_data['year'] = movie['Year'] if 'Year' in movie else '---'
    movie_data['genre'] = movie['Genre'] if 'Genre' in movie else '---'
    movie_data['imdbVotes'] = movie['imdbVotes'] if 'imdbVotes' in movie else '---'
    movie_data['imdbRating'] = movie['imdbRating'] if 'imdbRating' in movie else '---'

    # ------------------------------------------------------------
    upd_movie = False
    s_time = time.time()
    movie_data['twitterRating'] = get_imdb_score(get_movie_tweet_sentiment(movie))
    print('get movie sentiment in '+ str(time.time()-s_time) + ' s')
    s_time = time.time()
    if 'temporal_sentiment' in movie:
        temporal_sentiment = movie['temporal_sentiment']
        temporal_sentiment[1] = [get_imdb_score(float(value)) for value in temporal_sentiment[1]]
        movie_data['temporalTwitterRating'] = temporal_sentiment
    else:
        movie_data['temporalTwitterRating'] = get_temporal_movie_tweet_sentiment(movie)
        movie['temporal_sentiment'] = movie_data['temporalTwitterRating']
        upd_movie = True
    print('get temporal movie sentiment in '+ str(time.time()-s_time) + ' s')

    # ------------------------------------------------------------

    s_time = time.time()
    if 'director_sentiment' in movie:
        movie_data['director'] = movie['director_sentiment']
    else:
        movie_data['director'] = get_movie_person_raiting(movie, movie['Director']) if 'Director' in movie else []
        movie['director_sentiment'] = movie_data['director']
        upd_movie = True

    movie_data['director'] = update_people_score(movie_data['director'])

    print('get director sentiment in '+ str(time.time()-s_time) + ' s')
    s_time = time.time()
    if 'writer_sentiment' in movie:
        movie_data['writer'] = movie['writer_sentiment']
    else:
        movie_data['writer'] = get_movie_person_raiting(movie, movie['Writer']) if 'Writer' in movie else []
        movie['writer_sentiment'] = movie_data['writer']
        upd_movie = True

    movie_data['writer'] = update_people_score(movie_data['writer'])

    print('get writer sentiment in '+ str(time.time()-s_time) + ' s')
    s_time = time.time()
    if 'actors_sentiment' in movie:
        movie_data['actors'] = movie['actors_sentiment']
    else:
        movie_data['actors'] = get_movie_person_raiting(movie, movie['Actors']) if 'Actors' in movie else []
        movie['actors_sentiment'] = movie_data['actors']
        upd_movie = True

    movie_data['actors'] = update_people_score(movie_data['actors'])

    print('get actors sentiment in '+ str(time.time()-s_time) + ' s')

    if upd_movie:
        movie_update_one(movie)

    print('The request has been executed in '+ str(time.time()-start_time) + ' s')

    return movie_data
