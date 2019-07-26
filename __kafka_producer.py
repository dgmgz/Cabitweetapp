from kafka import KafkaProducer
import tweepy
import twitter_dev_credentials as twitter_dev


CABIFY_TOKENS = [
                 '@cabify_arg',      '@Uber_ARG',         # AR
                 '@cabify_ecuador‏',  '@Uber_Ecuador'      # EC
                 '@Cabify_Mexico',   '@Uber_MEX',         # MX
                 '@cabify_colombia', '@Uber_Col',         # CO
                 '@cabify_espana',   '@Uber_ES',          # ES
                 '@Cabify_Chile',    '@Uber_Chile',       # CL
                 '@cabify_panama',   '@Uber_Panama',      # PA
                 '@Cabify_Do',       '@Uber_DOM',         # DO
                 '@cabify_uruguay',  '@Uber_UY',          # UY
                 '@cabify_portugal', '@uber_portugal'     # PT
                 '@cabifybrasil',    '@Uber_Brasil',      # BR
                 '@Cabify_Peru',     '@Uber_Peru',        # PE
                 'cabify', 'Cabify', 'uber', 'Uber'       # Generic
                ]
class StreamListener(tweepy.StreamListener):
    # This is a class provided by tweepy to access the Twitter Streaming API.

    def on_connect(self):
        # Called initially to connect to the Streaming API
        print("You are now connected to the streaming API.")

    def on_error(self, status_code):
        # On error - if an error occurs, display the error / status code
        print("Error received in kafka producer " + repr(status_code))
        return True # Don't kill the stream

    def on_data(self, data):
        """ This method is called whenever new data arrives from live stream.
        We asynchronously push this data to kafka queue"""
        try:
            producer.send('twittertest', data.encode('utf-8'))
        except Exception as e:
            print(e)
            return False
        return True



    def on_timeout(self):
        return True # Don't kill the stream


# Kafka Configuration
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])


# Create Auth object
auth = tweepy.OAuthHandler(twitter_dev.__API_KEY, twitter_dev.__API_SECRET_KEY)
auth.set_access_token(twitter_dev.__ACCESS_TOKEN, twitter_dev.__ACCESS_SECRET_TOKEN)
api = tweepy.API(auth)

# Set up the listener. The 'wait_on_rate_limit=True' is needed to help with Twitter API rate limiting.
listener = StreamListener(api=tweepy.API(wait_on_rate_limit=True,
                                         wait_on_rate_limit_notify=True,
                                         timeout=1000,
                                         retry_delay=5,
                                         retry_count=10,
                                         retry_errors=set([401, 406, 429, 503])))

""" 
* API Rate Limit: 60 requests per minute *
Retry Errors: 
401 - Unauthorized (HTTP authentication failed due to invalid credentials.)
406 - Not Acceptable
429 - Rate Limit (Exceeded the limit on connection requests.)
503 - Service Unavailable
"""

# Retry Errors: 401 - (Unauthorized),
stream = tweepy.Stream(auth=auth, listener=listener)
print("Tracking: " + str(CABIFY_TOKENS))
stream.filter(track=CABIFY_TOKENS, languages=['es', 'pt', 'en'])
