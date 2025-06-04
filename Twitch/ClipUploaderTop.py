#!/usr/bin/env python3

import mysql.connector
from mysql.connector import Error
import os
import subprocess
import json
import httplib2
import os.path
import glob
import random
import sys
import time
import tweepy
from random import randrange

from apiclient.discovery import build
from apiclient.errors import HttpError
from apiclient.http import MediaFileUpload
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import argparser, run_flow


# Explicitly tell the underlying HTTP transport library not to retry, since
# we are handling retry logic ourselves.
httplib2.RETRIES = 1

# Maximum number of times to retry before giving up.
MAX_RETRIES = 10

# Always retry when these exceptions are raised.
RETRIABLE_EXCEPTIONS = (httplib2.HttpLib2Error, IOError)

# Always retry when an apiclient.errors.HttpError with one of these status
# codes is raised.
RETRIABLE_STATUS_CODES = [500, 502, 503, 504]

# The CLIENT_SECRETS_FILE variable specifies the name of a file that contains
# the OAuth 2.0 information for this application, including its client_id and
# client_secret. You can acquire an OAuth 2.0 client ID and client secret from
# the Google API Console at
# https://console.developers.google.com/.
# Please ensure that you have enabled the YouTube Data API for your project.
# For more information about using OAuth2 to access the YouTube Data API, see:
#   https://developers.google.com/youtube/v3/guides/authentication
# For more information about the client_secrets.json file format, see:
#   https://developers.google.com/api-client-library/python/guide/aaa_client_secrets
CLIENT_SECRETS_FILE = "client_secrets.json"

# This OAuth 2.0 access scope allows an application to upload files to the
# authenticated user's YouTube channel, but doesn't allow other types of access.
YOUTUBE_UPLOAD_SCOPE = "https://www.googleapis.com/auth/youtube.upload"
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

# This variable defines a message to display if the CLIENT_SECRETS_FILE is
# missing.
MISSING_CLIENT_SECRETS_MESSAGE = """
WARNING: Please configure OAuth 2.0

To make this sample run you will need to populate the client_secrets.json file
found at:

   %s

with information from the API Console
https://console.developers.google.com/

For more information about the client_secrets.json file format, please visit:
https://developers.google.com/api-client-library/python/guide/aaa_client_secrets
""" % os.path.abspath(os.path.join(os.path.dirname(__file__),
                                   CLIENT_SECRETS_FILE))

VALID_PRIVACY_STATUSES = ("public", "private", "unlisted")



# Recycled SQL functions
def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection


def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection


# Get videos
def obtain_videos(channelName):
    proc = subprocess.Popen(['/home/pi/Twitch/twitch-dl', 'clips', channelName, '--limit', '100', '--json'], stdout=subprocess.PIPE)
    videos = json.loads(proc.stdout.read())
    
    return videos


# Upload video
def upload_video(channel, title, description, filePath):
    youtube = get_authenticated_service()
    try:
        vidID = initialize_upload(youtube, channel, title, description, filePath)
        return vidID
    except HttpError as e:
        print("An HTTP error %d occurred:\n%s") % (e.resp.status, e.content)


def tweet_video(channelName, title, youtube_url):
    api_key = "D6CxbgK5y2oIonRDT0d6dDqG3"
    api_key_secret = "Sz1tYAF4z1LIFbxDtAcQpx3juFhGyGTLcftf9iHGZyPPDpDWcy"
    access_token = "1502714030045151234-r5k2FV8fs80Un5ys2B7eMnG67KBkV9"
    access_token_secret = "akOjsVuWX1waqyrvJtvextXu1b9FRGU0roA9gM8EskO0G"

    authenticator = tweepy.OAuthHandler(api_key, api_key_secret)
    authenticator.set_access_token(access_token, access_token_secret)

    api = tweepy.API(authenticator)

    tweet = "New " + channelName + " clip '" + title + "' was posted! Check it out: " + youtube_url
    api.update_status(tweet)



def get_authenticated_service():
    flow = flow_from_clientsecrets(CLIENT_SECRETS_FILE, scope=YOUTUBE_UPLOAD_SCOPE, message=MISSING_CLIENT_SECRETS_MESSAGE)

    storage = Storage("/home/pi/Twitch/ClipUploader.py-oauth2.json")
    credentials = storage.get()

    if credentials is None or credentials.invalid:
        credentials = run_flow(flow, storage)

    return build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, credentials=credentials) #http=credentials.authorize(httplib2.Http()))

def initialize_upload(youtube, channel, title, description, filePath):
    body=dict(
        snippet=dict(
            title=title,
            description=description,
            tags=[channel, 'Twitch', 'Clip'],
            categoryId="20"
        ),
        status=dict(
            privacyStatus="private",
            selfDeclaredMadeForKids="False"
        )
    )

    # Call the API's videos.insert method to create and upload the video.
    insert_request = youtube.videos().insert(
            part=",".join(body.keys()),
            body=body,
    # The chunksize parameter specifies the size of each chunk of data, in
    # bytes, that will be uploaded at a time. Set a higher value for
    # reliable connections as fewer chunks lead to faster uploads. Set a lower
    # value for better recovery on less reliable connections.
    #
    # Setting "chunksize" equal to -1 in the code below means that the entire
    # file will be uploaded in a single HTTP request. (If the upload fails,
    # it will still be retried where it left off.) This is usually a best
    # practice, but if you're using Python older than 2.6 or if you're
    # running on App Engine, you should set the chunksize to something like
    # 1024 * 1024 (1 megabyte).
            media_body=MediaFileUpload(filePath, chunksize=-1, resumable=True)
    )

    return resumable_upload(insert_request)


# This method implements an exponential backoff strategy to resume a
# failed upload.
def resumable_upload(insert_request):
    response = None
    error = None
    retry = 0
    while response is None:
        try:
            print("Uploading file...")
            status, response = insert_request.next_chunk()
            if response is not None:
                if 'id' in response:
                    print("Video id '%s' was successfully uploaded." % response['id'])
                    return response['id']
                else:
                    exit("The upload failed with an unexpected response: %s" % response)
        except HttpError as e:
            if e.resp.status in RETRIABLE_STATUS_CODES:
                error = "A retriable HTTP error %d occurred:\n%s" % (e.resp.status, e.content)
            else:
                raise
        except RETRIABLE_EXCEPTIONS as e:
            error = "A retriable error occurred: %s" % e

    if error is not None:
        print(error)
        retry += 1
        if retry > MAX_RETRIES:
            exit("No longer attempting to retry.")

        max_sleep = 2 ** retry
        sleep_seconds = random.random() * max_sleep
        print("Sleeping %f seconds and then retrying...") % sleep_seconds
        time.sleep(sleep_seconds)

    return "0"

if __name__ == '__main__':

    ################ CHANGE HERE ###################
    twitchChannel = "streamerName"
    viewThreshold = 2000
    databaseName = "twitch_videos"
    tableName = twitchChannel + "_top_videos"
    twitchDir = '/home/pi/Twitch/'
    ################################################

    # Connect to server
    connection = create_db_connection("localhost", "root", "Password", databaseName)
    mycursor = connection.cursor()
    mycursor.execute("SET NAMES utf8mb4;")
    mycursor.execute("SET CHARACTER SET utf8mb4;")
    mycursor.execute("SET character_set_connection=utf8mb4")
    mycursor.execute("CREATE TABLE IF NOT EXISTS " + tableName + "(title varchar(255), createdAt varchar(255), durationSeconds int, viewCount int, url varchar(255))")

    # Grab videos from channel
    videoDict = obtain_videos(twitchChannel)

    # For each video
    for i in videoDict:
        # For each video above the view threshold
        if i["viewCount"] > viewThreshold:
            # For each video not already uploaded
            mycursor.execute('SELECT title FROM ' + tableName)
            results = mycursor.fetchall()
            videoTitles = [x[0] for x in results]
            if i["title"].replace("'","") not in videoTitles:
                print("Video is over view threshold (" + str(viewThreshold) + "): " + str(i["viewCount"]))
            
                # Download video
                os.system(twitchDir + 'twitch-dl download -q source --output "{title}.{format}" ' + i["url"])

                print(i["title"])
                # Add new video in table 
                sql = "INSERT INTO " + tableName + " (title, createdAt, durationSeconds, viewCount, url) VALUES ('" + i["title"].replace("'","") + "', '" + i["createdAt"] + "', " + str(i["durationSeconds"]) + ", " + str(i["viewCount"]) + ", '" + i["url"] + "')"
                mycursor.execute(sql)
                connection.commit()
                connection.close()

                # Upload video
                desc = "\n" + str(i["durationSeconds"]) + " second " + twitchChannel + " clip '" + i["title"] + "' from " + i["createdAt"] + "\nFOLLOW ON TWITCH: https://www.twitch.tv/" + twitchChannel + "\n\n" + "Follow Socials:\nMain Channel: https://youtube.com/c/" + twitchChannel + "\n#" + twitchChannel 
                fileName = glob.glob(os.path.join('.', '*.mp4'))[0] # Want to do this another way
                videoID = upload_video(twitchChannel, i["title"], desc, fileName)
                
                # Make announcement on Twitter
                #if videoID != "0":
                #    youtube_url = "https://youtu.be/" + videoID
                #    tweet_video(twitchChannel, i["title"], youtube_url)
 
                # Remove videos
                os.system('rm /home/pi/Twitch/*.mp4')
        
                break

