
from googleapiclient.discovery import build
from pprint import pprint as pp
import pymongo
import mysql.connector
import pandas as pd
from pymongo import MongoClient
import streamlit as st
#import SQLAlchemy


api_service_name = "youtube"
api_version = "v3"
api_key = "AIzaSyCNvNdgRTeG-0sZ63yHD6Fj3-bWHYkpn14"
youtube = build(api_service_name, api_version, developerKey=api_key)


def channel_details(channel_id):
    request = youtube.channels().list(part="snippet,contentDetails,statistics", id=channel_id)
    response = request.execute()
    datach = dict(channel_id=response['items'][0]['id'],
                  channel_name=response['items'][0]['snippet']['title'],
                  channel_description=response['items'][0]['snippet']['description'],
                  channel_published=response['items'][0]['snippet']['publishedAt'],
                  channel_playlist=response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
                  channel_scount=response['items'][0]['statistics']['subscriberCount'],
                  channel_vcount=response['items'][0]['statistics']['videoCount'],
                  channel_viewcount=response['items'][0]['statistics']['viewCount'])

    return datach


def to_getchannel_videos(channel_id):
    video_ids = []
    request = youtube.channels().list(id=channel_id, part='contentDetails')
    response = request.execute()
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None

    while True:
        request = youtube.playlistItems().list(playlistId=playlist_id, part="snippet",
                                               maxResults=50, pageToken=next_page_token)
        response = request.execute()

        for i in range(len(response['items'])):
            video_ids.append(response['items'][i]
                             ['snippet']['resourceId']['videoId'])

            next_page_token = response.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids


def get_video_detailas(videos):
    video_data = []
    for video_id in videos:
        request = youtube.videos().list(part="snippet,contentDetails,statistics",
                                        # id=#','.join(video_ids[i:i+50]))
                                        id=video_id)
        response = request.execute()
        for i in response['items']:
            datav = dict(
                channel_name=response['items'][0]['snippet']['channelTitle'],
                channel_id=response['items'][0]['snippet']['channelId'],
                video_id=response['items'][0]['id'],
                video_name=response['items'][0]['snippet']['title'],
                video_description=response['items'][0]['snippet']['description'],
                tags=','.join(response['items'][0]['snippet'].get('tags', [])),
                published_date=response['items'][0]['snippet']['publishedAt'],
                view_count=response['items'][0]['statistics']['viewCount'],
                like_count=response['items'][0]['statistics']['likeCount'],
                favourite_count=response['items'][0]['statistics']['favoriteCount'],
                comment_count=response['items'][0]['statistics']['commentCount'],
                duration=response['items'][0]['contentDetails']['duration'],
                thumbnail=response['items'][0]['snippet']['thumbnails'],
                caption_status=response['items'][0]['contentDetails']['caption'])
            video_data.append(datav)
    return video_data




def get_comment_info(videos):
    Comment_data = []
    for video_id in videos:
        try:
            request = youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=50
            )
            response = request.execute()

            for item in response['items']:
                data = dict(comment_Id=item['snippet']['topLevelComment']['id'],
                            Video_id=item['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])

                Comment_data.append(data)
        except:
            pass

    return Comment_data



client = pymongo.MongoClient('mongodb://localhost:27017')

db = client['youtube_data']


def main(channel_id):
    videos = to_getchannel_videos(channel_id)
    channels = channel_details(channel_id)
    videodetails = get_video_detailas(videos)
    commentdetails = get_comment_info(videos)
    data = {"channel_info": channels, "video_details": videodetails, "comment_details": commentdetails}

    collection = db['channel_details']
    collection.insert_one(data)

    return 'upload completed successfully'



mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="1234", database="youtube_details")
mycursor = mydb.cursor()
client = MongoClient('mongodb://localhost:27017/')
db = client["youtube_data"]
collection = db['channel_details']

mycursor.execute("CREATE DATABASE if not exists youtube_details")




def channel_tab(selected_channel):
    drop_query = '''drop table if exists channeldata'''
    mycursor.execute(drop_query)
    mycursor.execute('''CREATE TABLE if not exists channeldata(channel_name VARCHAR(100),
                                                                channel_id VARCHAR(100) primary key,
                                                                channel_description TEXT,
                                                                channel_published VARCHAR(100),
                                                                channel_playlist VARCHAR(255),
                                                                channel_scount INT,
                                                                channel_vcount INT,
                                                                channel_viewcount INT)''')
    channels_list = []
    for chdata in collection.find({}, {"_id": 0, "channel_info": 1}):
        channels_list.append(chdata["channel_info"])
    df = pd.DataFrame(channels_list)
    for index, row in df.iterrows():
        insert_query = '''insert into channeldata(channel_name,
                                                    channel_id,
                                                    channel_description,
                                                    channel_published,
                                                    channel_playlist,
                                                    channel_scount,
                                                    channel_vcount,
                                                    channel_viewcount)values(%s,%s,%s,%s,%s,%s,%s,%s)'''
        values = (row['channel_name'],
                  row['channel_id'], 
                  row['channel_description'], 
                  row['channel_published'].replace("T", " ").replace("Z", " "), 
                  row['channel_playlist'],
                  row['channel_scount'], 
                  row['channel_vcount'], 
                  row['channel_viewcount'])
        mycursor.execute(insert_query, values)
    mydb.commit()
    





def video_tab(selected_channel):
    drop_query = '''drop table if exists videodata'''
    mycursor.execute(drop_query)
    mycursor.execute('''CREATE TABLE if not exists videodata(channel_name VARCHAR(100),
                                                                channel_id VARCHAR(100),
                                                                video_id VARCHAR(100) primary key,
                                                                video_name VARCHAR(100),
                                                                tags TEXT,
                                                                video_description TEXT,
                                                                published_date VARCHAR(100),
                                                                view_count BIGINT,
                                                                like_count BIGINT,
                                                                favourite_count int,
                                                                comment_count int,
                                                                duration VARCHAR(100),
                                                                thumbnail VARCHAR(200),
                                                                caption_status VARCHAR(50))''')
    videos_list = []
    for vidata in collection.find({}, {'_id': 0, 'video_details': 1}):
        for i in range(len(vidata['video_details'])):
            videos_list.append(vidata['video_details'][i])
    df2 = pd.DataFrame(videos_list)
    for index, row in df2.iterrows():

        insert_query = '''INSERT INTO videodata(channel_name,
                                                channel_id,
                                                video_id,
                                                video_name,
                                                tags,
                                                video_description,
                                                published_date,
                                                view_count,
                                                like_count,
                                                favourite_count,
                                                comment_count,
                                                duration,
                                                thumbnail,
                                                caption_status)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        values = (row['channel_name'],
                  row['channel_id'],
                  row['video_id'],
                  row['video_name'],
                  row['tags'],
                  row['video_description'],
                  row['published_date'].replace("T", " ").replace("Z", " "),
                  row['view_count'],
                  row['like_count'],
                  row['favourite_count'],
                  row['comment_count'],
                  row['duration'].replace('PT', '').replace('H', ':').replace('M', ':').split('S')[0],
                  row['thumbnail']['default']['url'],
                  row['caption_status'])
        mycursor.execute(insert_query, values)
    mydb.commit()



def comnt_tab():
    drop_query = '''drop table if exists comments'''
    mycursor.execute(drop_query)
    mycursor.execute('''CREATE TABLE if not exists comments(comment_Id varchar(100) primary key,
                                                              Video_id varchar(100),
                                                              Comment_Text text,
                                                              Comment_Author varchar(100),
                                                              Comment_Published VARCHAR(100))''')
    comment_list = []
    for com_data in collection.find({}, {'_id': 0, 'comment_details': 1}):
        for i in range(len(com_data['comment_details'])):
            comment_list.append(com_data['comment_details'][i])
    df3 = pd.DataFrame(comment_list)
    for index, row in df3.iterrows():
        insert_query = '''INSERT INTO comments(comment_Id,
                                                Video_id,
                                                Comment_Text,
                                                Comment_Author,
                                                Comment_Published
                                                )
                                                values(%s,%s,%s,%s,%s)'''
        values = (row['comment_Id'], row['Video_id'], row['Comment_Text'],
                  row['Comment_Author'], row['Comment_Published'].replace("T", " ").replace("Z", " "))

        mycursor.execute(insert_query, values)
    mydb.commit()



def tables(selected_channel):
    channel_tab(selected_channel)
    video_tab(selected_channel)
    comnt_tab()
    return "tables created"
 

st.title(':blue[YouTube Data Harvesting and Warehousing] ' )
channel_id = st.text_input("Enter the channel ID")
   
ch_ids = []
if st.button('collect and store data'):
    db = client['youtube_data']
    collection= db['channel_details']
    for ch_data in collection.find({}, {'_id': 0, 'channel_info': 1}):
        ch_ids.append(ch_data['channel_info']['channel_id'])

    if channel_id in ch_ids:
        st.success('Channel Details of the Given ID Already Exists')

    else:
        insert = channel_details(channel_id)
        st.success(insert)
    
selected_channel = st.selectbox("Select a channel", ch_ids, index=0)


ch_list = []
db = client['youtube_data']
collection = db['channel_details']
for ch_data in collection.find({}, {'_id': 0, 'channel_info': 1}):
    ch_list.append(ch_data['channel_info']['channel_name'])
        
selected_channel =st.multiselect('select a channel  to migrate the details into sql:',ch_list)
st.write(f'You selected: {selected_channel}') 

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="1234", database="youtube_details")
mycursor = mydb.cursor()


if st.button('Migrate to Sql'):
    Table = tables(selected_channel)
    st.success(Table)

show = st.radio('SELECT THE TABLE FOR VIEW', ('CHANNELS', 'VIDEOS', 'COMMENTS'), index=None)

if show == "CHANNELS":
        ch = pd.read_sql_query("select * from channeldata;", mydb)
        st.write(ch)
        
elif show == "VIDEOS":
        vd = pd.read_sql_query("select * from videodata;", mydb)
        st.write(vd)
elif show == "COMMENTS":
        ct = pd.read_sql_query("select * from comments;", mydb)
        st.write(ct)
#ch=pd.read_mysql_query("select * from channeldata",mydb)

Question=st.selectbox("select your Question",('1.What are the names of all the videos and their corresponding channels?',
                                     '2.Which channels have the most number of videos, and how many videos do they have?',
                                     '3.What are the top 10 most viewed videos and their respective channels?',
                                     '4.How many comments were made on each video, and what are their corresponding video names?',
                                     '5.Which videos have the highest number of likes, and what are their corresponding channel names?',
                                     '6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?'
                                     '7.What is the total number of views for each channel, and what are their corresponding channel names?',
                                     '8.What are the names of all the channels that have published videos in the year 2022?',
                                     '9.What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                     '10.Which videos have the highest number of comments, and what are their corresponding channel names?'
                                    ))



import sqlalchemy as sa
import pandas as pd

engine = sa.create_engine('mysql+mysqlconnector://root:1234@localhost/youtube_details')


if Question == '1.What are the names of all the videos and their corresponding channels':
    query1 = '''SELECT video_name, channel_name FROM videodata;'''
    res_df = pd.read_sql_query(query1, con=engine)
    st.write(res_df)

ct=pd.read_sql_query("select * from comments;",mydb)
