from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import numpy as np
import re
import datetime
from email.utils import mktime_tz, parsedate_tz
import urllib.request
import urllib.parse
from fkscore import fkscore
from profanity_check import predict, predict_prob
from textblob import TextBlob
from nltk.corpus import stopwords
from nltk import word_tokenize
from nltk.stem import PorterStemmer
from afinn import Afinn
import preprocessor
from twython import Twython
import twitter_dev_credentials as twitter_dev
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart




def remove_pattern(input_txt, pattern):
    r = re.findall(pattern, input_txt)
    for i in r:
        input_txt = re.sub(i, '', input_txt)
    return input_txt


def clean_tweets(raw_tweet):

    """
    There are some parts of the tweets that in fact does not help us to analyze its sentiment,
    like URLs, some other user_ids, numbers, etc.
    :param raw_tweet
    """

    # remove twitter Return handles (RT @xxx:)
    raw_tweet = np.vectorize(remove_pattern)(raw_tweet, "RT @[\w]*:")
    # remove twitter handles (@xxx)
    raw_tweet = np.vectorize(remove_pattern)(raw_tweet, "@[\w]*")
    # remove URL links (httpxxx)
    raw_tweet = np.vectorize(remove_pattern)(raw_tweet, "https?://[A-Za-z0-9./]*")
    # remove special characters, numbers, punctuations (except for #)
    raw_tweet = np.core.defchararray.replace(raw_tweet, "[^a-zA-Z#]", " ")
    return str(raw_tweet)

def parse_datetime(tweet_timing):

    """
    Function used so as to correctly parse either tweet date of creation or user platform creation.
    :param tweet_timing
    """

    time_tuple = parsedate_tz(tweet_timing)
    timestamp = mktime_tz(time_tuple)

    return datetime.datetime.fromtimestamp(timestamp)


def translate(to_translate, to_language="en", from_language="auto"):

    """
    Since our program will receive fast and unlimited tweets, using the module Google Trans will not allow us
    to translate the endless number of words contained in tweets. Due to its limitations of translating 100.000
    characters every 100 seconds per project and per user, the following function will replace it.
    :param to_translate: Tweet text
    :param to_language: English
    :param from_language: Language auto-detection
    :return: English-translated sentence.
    """

    agent = {'User-Agent':
                 "Mozilla/4.0 (\
                     compatible;\
                     MSIE 6.0;\
                     Windows NT 5.1;\
                     SV1;\
                     .NET CLR 1.1.4322;\
                     .NET CLR 2.0.50727;\
                     .NET CLR 3.0.04506.30\
                     )"}

    base_link = "http://translate.google.com/m?hl=%s&sl=%s&q=%s"
    to_translate = urllib.parse.quote(to_translate)
    link = base_link % (to_language, from_language, to_translate)
    request = urllib.request.Request(link, headers=agent)
    raw_data = urllib.request.urlopen(request).read()
    data = raw_data.decode("utf-8")
    expr = r'class="t0">(.*?)<'
    re_result = re.findall(expr, data)
    if len(re_result) == 0:
        result = ""
    else:
        result = re_result[0]
    return result


def vader_class(tweetext):

    """
    VADER (Valence Aware Dictionary and Sentiment Reasoner) is a lexicon and rule-based sentiment analysis tool that is
    specifically attuned to sentiments expressed in social media. VADER uses a combination of A sentiment lexicon is a
    list of lexical features (e.g., words) which are generally labelled according to their semantic orientation as
    either positive or negative.
    :param tweetext:
    """

    analyser = SentimentIntensityAnalyzer()
    score = analyser.polarity_scores(tweetext)
    compound = score['compound']
    if compound >= 0.75:
        return 'Upper Positive'
    elif (compound < 0.75) and (compound >= 0.50):
        return 'Positive'
    elif (compound < 0.50) and (compound >= 0.25):
        return 'Lower Positive'
    elif (compound < 0.25) and (compound > -0.25):
        return 'Neutral'
    elif (compound <= -0.25) and (compound > -0.50):
        return 'Lower Negative'
    elif (compound <= -0.50) and (compound > -0.75):
        return 'Negative'
    elif compound <= -0.75:
        return 'Upper Negative'


def vader_score(tweetext):
    analyser = SentimentIntensityAnalyzer()
    score = analyser.polarity_scores(tweetext)
    return score['compound']


def afinn_score(string):

    """
    The AFINN lexicon is perhaps one of the simplest and most popular lexicons that can be used extensively for
    sentiment analysis.
    The current version of the lexicon is AFINN-en-165. txt and it contains over 3,300+ words with a polarity score
    associated with each word.
    :param string:
    """

    af = Afinn()
    stop_words = set(stopwords.words('english'))
    ps = PorterStemmer()

    tweet = word_tokenize(preprocessor.clean(string))
    new_sentence = []
    steemed_sentence = []
    for w in tweet:
        if w not in stop_words:
            new_sentence.append(w)

    for w in new_sentence:
        steemed_sentence.append(ps.stem(w))
    normalized_sentence = ' '.join(steemed_sentence)

    return af.score(normalized_sentence)


def entreprise_match(str):

    """
    Identify to which company the tweet is directed, returning the value of 'Cabify' or 'Uber' in those cases in which
    the name of the company appears throughout the text (through a mention) or by returning 'Both' in those cases in
    which both companies are mentioned.
    :param str:
    """

    both = ['@cabify_espana', '@Uber_ES']

    if all(x in str.lower() for x in both):
        return 'both'
    elif 'uber' in str.lower():
        return 'Uber'
    elif 'cabify' in str.lower():
        return 'Cabify'


def fkScore(string):

    """
    A Python module implementation of the Flesch Kincaid readability score algorithm which assesses the readability
    of a given text
    :param string:
    """

    f = fkscore(string)
    score = f.score
    return score.get('readability')


def fkStats(string):
    f = fkscore(string)
    stats = f.stats
    return stats.get('num_words')


def profanity_check_prob(string):

    """
    A fast, robust Python library to check for profanity or offensive language in strings. It uses a linear SVM model
    trained on 200k human-labeled samples of clean and profane text strings.
    :param string:
    """

    return float(np.asarray(predict_prob([string])))


def profanity_check_bool(string):
    if predict([string]) == 1:
        return True
    else:
        return False


def tb_find_polarity(string):

    """
    Polarity is float which lies in the range of [-1,1] where 1 means positive statement and -1 means a
    negative statement.
    :param string:
    """
    return TextBlob(string).sentiment.polarity


def tb_find_subjectivity(string):
    return TextBlob(string).sentiment.subjectivity


def twitterbot(tweet_id):

    """
    Having a bot automatically ReTweeting those @cabify tweets from all across the board. The objetive will be to
    promote those tweets considered as "Promoters" so as to skyrocket the enterprise social media reputation.
    :param tweet_id:
    """

    twitter = Twython(twitter_dev.__API_KEY,
                      twitter_dev.__API_SECRET_KEY,
                      twitter_dev.__ACCESS_TOKEN,
                      twitter_dev.__ACCESS_SECRET_TOKEN)

    twitter.retweet(id=tweet_id)


def send_low_performace_email(user_name, id_tweet, vaderclass):

    """
    Due to the existence of tweets considered as detractors of the company, a quick response from them would be a
    fundamental way to combat the bad reputation in social networks. All those tweets that meet the negative parameters
    will be sent to a central email address that will give an immediate response by the social media team to them.
    :param user_name:
    :param id_tweet:
    :param vaderclass:
    """

    __SUBJECT = id_tweet + ": " + vaderclass + " Tweet submitted."

    email = MIMEMultipart()
    email["From"] = twitter_dev.__GMAIL_SENDER_EMAIL
    email["To"] = twitter_dev.__GMAIL_RECEIVER_EMAIL
    email["Subject"] = __SUBJECT
    email["Bcc"] = twitter_dev.__GMAIL_RECEIVER_EMAIL

    html = """\
    <html>
      <body>
        <p>Hi,<br>
           The user @""" + user_name + """ has submitted a new tweet with a """ + vaderclass + """ feeling.<br>
           Check out the tweet and give an appropriate response.
           <a href="https://twitter.com/davgmg/status/""" + id_tweet + """ ">Tweet 

        </p>
      </body>
    </html>
    """

    body = MIMEText(html, "html")

    email.attach(body)

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(twitter_dev.__GMAIL_SENDER_EMAIL, twitter_dev.__GMAIL_PASSWORD)
        server.sendmail(
            twitter_dev.__GMAIL_SENDER_EMAIL, twitter_dev.__GMAIL_RECEIVER_EMAIL, email.as_string()
        )


def tweet_parser(raw_tweet):

    """
    1- Tweet General Features
        1.1- tm_tweet_created_at: UTC time when this Tweet was created.
        1.2- ds_type: Type of object; could be Tweet or Reply (Retweets are not inserted in MongoDB).
        1.3- id_tweet: Unique identifier of the Tweet.
        1.4- ds_text: UTF-8. Processed text without links, mentions and other non-relevant information.
        1.5- ds_raw_text: UTF-8; Original text of the tweet.

    2- User Features
        2.1- id_user: Unique identifier of the User
        2.2- tm_user_created_at: The UTC datetime that the user account was created on Twitter.
        2.3- ds_user_name: The name of the user.
        2.3.1- profile_pic: Link to user profile picture.
        2.4- ds_user_location: User-defined location (Nullable)
        2.5- nu_followers: Followers.
        2.6- nu_friends: Followings.
        2.6- nu_listed: The number of public lists that this user is a member of
        2.7- nu_favourites: Tweets this user has liked in the account’s lifetime
        2.8- nu_statuses: Published Tweets
        2.9- nu_friends

    3- Tweet Specific Features
        3.1- ds_hashtags: Tweet hastags associated
        3.2- ds_mentions: Tweet mentions associated
        3.3- ds_language: Tweet original tweet language

    4- Tweet Vader Feeling Analysis
        4.1- ds_vaderclass
        4.2- nu_vaderscore
        4.3- nu_afinnrscore
        4.4- nu_tweet_words: Total number of words contained in Tweet
        4.5- nu_fkscore: Readability
        4.6- bool_profanity: Profanity
        4.7- nu_profanity_prob
        4.8- nu_polarity: Textblob
        4.9- nu_subjectivity

    5- Database Insertion Timing
        5.1- tm_audited_at

    6- TwitterBot Cabify Possitive Tweets Insertion on @davgmg

    7- Send Email Cabify Negative Tweets
    """

    tweet_original_dict = {}

    cabify_accounts = [
        '@cabify_arg',
        '@cabify_ecuador‏',
        '@Cabify_Mexico',
        '@cabify_colombia',
        '@cabify_espana',
        '@Cabify_Chile',
        '@cabify_panama',
        '@Cabify_Do',
        '@cabify_uruguay',
        '@cabify_portugal',
        '@cabifybrasil',
        '@Cabify_Peru',
        'cabify',
        'Cabify'
    ]

    if not raw_tweet['retweeted'] and 'RT @' not in raw_tweet['text']:

        # 1- Tweet General Features

        tweet_original_dict['tm_tweet_created_at'] = parse_datetime(raw_tweet['created_at'])
        if raw_tweet['in_reply_to_status_id_str'] is None:
            tweet_original_dict['ds_type'] = 'Tweet'
        else:
            tweet_original_dict['ds_type'] = 'Reply'
        tweet_original_dict['id_tweet'] = raw_tweet['id']
        if 'extended_tweet' in raw_tweet:
            tweet_original_dict['ds_text'] = clean_tweets(raw_tweet['extended_tweet']['full_text'])
            tweet_original_dict['ds_raw_text'] = raw_tweet['extended_tweet']['full_text']
        else:
            tweet_original_dict['ds_text'] = clean_tweets(raw_tweet['text'])
            tweet_original_dict['ds_raw_text'] = raw_tweet['text']
        if 'extended_tweet' in raw_tweet:
            tweet_original_dict['ds_raw_text'] = raw_tweet['extended_tweet']['full_text']
            sentiment_text = translate(clean_tweets(raw_tweet['extended_tweet']['full_text']))
        else:
            tweet_original_dict['ds_raw_text'] = raw_tweet['text']
            sentiment_text = translate(clean_tweets(raw_tweet['text']))

        # 2- User Features

        tweet_original_dict['id_user'] = raw_tweet['user']['id']
        tweet_original_dict['tm_user_created_at'] = parse_datetime(raw_tweet['user']['created_at'])
        tweet_original_dict['ds_user_name'] = raw_tweet['user']['screen_name']
        tweet_original_dict['profile_pic'] = 'https://twitter.com/' + \
                                             raw_tweet['user']['screen_name'] + \
                                             '/profile_image?size=original'
        tweet_original_dict['ds_user_location'] = raw_tweet['user']['location']
        tweet_original_dict['nu_followers'] = raw_tweet['user']['followers_count']
        tweet_original_dict['nu_friends'] = raw_tweet['user']['friends_count']
        tweet_original_dict['nu_listed'] = raw_tweet['user']['listed_count']
        tweet_original_dict['nu_favourites'] = raw_tweet['user']['favourites_count']
        tweet_original_dict['nu_statuses'] = raw_tweet['user']['statuses_count']
        tweet_original_dict['nu_friends'] = raw_tweet['user']['friends_count']

        # 3- Tweet Specific Features

        tweet_original_dict['ds_hashtags'] = raw_tweet['entities']['hashtags']
        tweet_original_dict['ds_mentions'] = ", ".join([mention['name']
                                                        for mention in raw_tweet['entities']['user_mentions']])
        tweet_original_dict['ds_hashtags'] = ", ".join([hashtag_item['text']
                                                        for hashtag_item in raw_tweet['entities']['hashtags']])
        tweet_original_dict['ds_language'] = raw_tweet['lang']

        # 4- Tweet Vader Feeling Analysis

        tweet_original_dict['ds_vaderclass'] = vader_class(sentiment_text)
        tweet_original_dict['nu_vaderscore'] = vader_score(sentiment_text)
        tweet_original_dict['nu_afinnrscore'] = afinn_score(sentiment_text)
        tweet_original_dict['nu_tweet_words'] = fkStats(sentiment_text)
        tweet_original_dict['nu_fkscore'] = fkScore(sentiment_text)
        tweet_original_dict['bool_profanity'] = profanity_check_bool(sentiment_text)
        tweet_original_dict['nu_profanity_prob'] = profanity_check_prob(sentiment_text)
        tweet_original_dict['nu_polarity'] = tb_find_polarity(sentiment_text)
        tweet_original_dict['nu_subjectivity'] = tb_find_subjectivity(sentiment_text)

        # 5- Database Insertion Timing

        tweet_original_dict['tm_audited_at'] = datetime.datetime.now()

        # 6- TwitterBot Cabify Possitive Tweets Insertion on @davgmg

        if (any(x in tweet_original_dict['ds_raw_text'] for x in cabify_accounts)) and \
                (vader_score(sentiment_text) > 0.50) and \
                (raw_tweet['in_reply_to_status_id_str'] is None):
            twitterbot(raw_tweet['id'])

        # 7- Send Email Cabify Negative Tweets

        if (any(x in tweet_original_dict['ds_raw_text'] for x in cabify_accounts)) and \
                (vader_score(sentiment_text) <= -0.25) and \
                (raw_tweet['in_reply_to_status_id_str'] is None):
            send_low_performace_email(raw_tweet['user']['screen_name'], str(raw_tweet['id']), vader_class(sentiment_text))

        return tweet_original_dict



