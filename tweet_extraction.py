from tweepy import StreamListener
from tweepy import OAuthHandler, API, Stream
import pandas as pd
import time
import key

time_tweet = 60                                                     # time period to collect tweets. (sec)
csv_file_path = 'file-handling/first_tweet_extraction.csv'          # file where tweets are stored.
hashtag_list = ['#WhatsApp']

# Twitter Access
auth = OAuthHandler(key.CONSUMER_KEY, key.CONSUMER_SECRET)
auth.set_access_token(key.ACCESS_TOKEN, key.ACCESS_TOKEN_SECRET)
api = API(auth, wait_on_rate_limit=True)

dataset = []
start_time = time.time()
limit = float(time_tweet)


class MyStreamListener(StreamListener):
    def on_status(self, status):
        # if not status.retweeted and 'RT @' not in status.text:
        # if don't want retweets
        if (time.time() - start_time) < float(limit):
            text = status.text
            tweet = (status.user.screen_name, status.user.name, status.user.location, status.id, status.user.id,
                     status.created_at, text)
            dataset.append(tweet)
            print(str(len(dataset)) + " :" + str(status.created_at) + "  :" + text)
        else:
            return False
        return True

    def on_error(self, status_code):
        if (time.time() - start_time) >= limit:
            if status_code == 420:
                return False


def call_stream():
    myStreamListener = MyStreamListener()
    myStream = Stream(auth=auth, listener=myStreamListener, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    myStream.filter(track=hashtag_list, languages=['en'])
    return myStream


def create_csv_file():
    # Adding to dataframe
    df = pd.DataFrame(dataset, columns=['user_screen_name', 'Username', 'user_location', 'tweet_id', 'user_id',
                                        'created_at', 'text'])
    # df = remove_duplicate(df)
    print(df)
    df.to_csv(csv_file_path)
    return df


# remove duplicate and retweets
def remove_duplicate(df):
    duplicated_id = []
    tweet_check_list = []
    i = 1
    all_tweets = df['text'].to_list()

    for current_tweet in all_tweets:
        # If tweet doesn't exist in the list
        if current_tweet not in tweet_check_list:
            tweet_check_list.append(current_tweet)
        else:
            duplicated_id.append(i)
        i += 1

    df = df.drop(index=duplicated_id)
    print('%d duplicate tweets', len(duplicated_id))
    return df


call_stream()
create_csv_file()
