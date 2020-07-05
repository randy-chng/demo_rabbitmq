import requests
import requests_oauthlib
import json
import yaml

from mq_app import mq_send_twitter_feed


def get_tweets():

    with open('config.yaml', 'r') as stream:
        details = yaml.safe_load(stream)

    my_auth = requests_oauthlib.OAuth1(
        details['consumer_key'],
        details['consumer_secret'],
        details['access_token'],
        details['access_secret']
    )

    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('language', 'en'), ('locations', '-74,40,-73,41'), ('track', '#')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])

    response = requests.get(
        query_url,
        auth=my_auth,
        stream=True
    )

    print(query_url, response)

    return response


def send_tweets_to_mq(http_resp):

    for line in http_resp.iter_lines():

        try:
            if len(line) != 0:
                full_tweet = json.loads(line)
                tweet_text = full_tweet['text']

                mq_send_twitter_feed(tweet_text)

        except Exception as e:
            print('Error:', e)


if __name__ == "__main__":

    print('Connected... Starting getting tweets.')
    resp = get_tweets()
    send_tweets_to_mq(resp)
