from googleapiclient.discovery import build
import pymongo
import mysql.connector
from mysql.connector import errorcode
import pandas as pd
from pymongo import MongoClient
import streamlit as st


api_service_name = "youtube"
api_version = "v3"
api_key = "AIzaSyCNvNdgRTeG-0sZ63yHD6Fj3-bWHYkpn14"
youtube = build(api_service_name, api_version, developerKey=api_key)

# to get channel information


def channel_details(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics", id=channel_id)
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

# to get video id's from playlist id


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

# to get all videos information from all video id's


def get_video_detailas(videos):
    video_data = []
    for video_id in videos:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics", id=video_id)
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
                view_count=response['items'][0]['statistics'].get('viewCount'),
                like_count=response['items'][0]['statistics'].get('likeCount'),
                favourite_count=response['items'][0]['statistics'].get('favoriteCount'),
                comment_count=response['items'][0]['statistics'].get('commentCount'),
                duration=response['items'][0]['contentDetails']['duration'],
                thumbnail=response['items'][0]['snippet']['thumbnails'],
                caption_status=response['items'][0]['contentDetails']['caption'])
            video_data.append(datav)
    return video_data


# to get all comments
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


# MongDB connection
client = pymongo.MongoClient('mongodb://localhost:27017')
db = client['youtube_data']


def main(channel_id):
    videos = to_getchannel_videos(channel_id)
    channels = channel_details(channel_id)
    videodetails = get_video_detailas(videos)
    commentdetails = get_comment_info(videos)
    data = {"channel_info": channels, "video_details": videodetails,
            "comment_details": commentdetails}

    collection = db['channel_details']
    collection.insert_one(data)

    return 'upload completed successfully'


# My SQL connection
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="1234", database="youtube_details")
mycursor = mydb.cursor()
client = MongoClient('mongodb://localhost:27017/')
db = client["youtube_data"]
collection = db['channel_details']

mycursor.execute("CREATE DATABASE if not exists youtube_details")


def channel_tab():
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


def new_channel_tab():
    channels_list = []
    for chdata in collection.find({}, {"_id": 0, "channel_info": 1}):
        channels_list.append(chdata["channel_info"])
    df = pd.DataFrame(channels_list)   
    for index, row in df.iterrows():
        if row['channel_id'] == temp_channel_id:
            # Check if the channel_id already exists in the MySQL table
            check_query = "SELECT COUNT(*) FROM channeldata WHERE channel_id = %s"
            mycursor.execute(check_query, (row['channel_id'],))
            result = mycursor.fetchone()
            if result[0] == 0:
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
                return 'channel data inserted successfully'
            else:
                return "channel data already exist"
                
   


# Creation and insertion values into video table
def video_tab():
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
                  row['duration'].replace('PT', '').replace(
                      'H', ':').replace('M', ':').split('S')[0],
                  row['thumbnail']['default']['url'],
                  row['caption_status'])
        mycursor.execute(insert_query, values)
    mydb.commit()
    
def new_video_tab():
    videos_list = []
    for vidata in collection.find({}, {'_id': 0, 'video_details': 1}):
        for i in range(len(vidata['video_details'])):
            videos_list.append(vidata['video_details'][i])
    df2 = pd.DataFrame(videos_list)
    for index, row in df2.iterrows():
        if row['channel_id'] == temp_channel_id:    
            temp_video_id.append(row['video_id'])
            # Check if the video_id already exists in the MySQL table
            check_query = "SELECT COUNT(*) FROM videodata WHERE video_id = %s"
            mycursor.execute(check_query,
                             (row['video_id'],))
            result = mycursor.fetchone()
            if result[0] == 0:       
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
                        row['duration'].replace('PT', '').replace(
                            'H', ':').replace('M', ':').split('S')[0],
                        row['thumbnail']['default']['url'],
                        row['caption_status'])
                mycursor.execute(insert_query, values)
                abc = " new video data inserted successfully"
            else:
                abc = "video data already exist"
    mydb.commit()
    return abc
               
            
               
# creation and insertion values into comments table
def comnt_tab():
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

def new_comnt_tab():
    comment_list = []
    for com_data in collection.find({}, {'_id': 0, 'comment_details': 1}):
            for i in range(len(com_data['comment_details'])):
                comment_list.append(com_data['comment_details'][i])
    df3 = pd.DataFrame(comment_list)
    for index, row in df3.iterrows():
        if row['Video_id'] in temp_video_id:
            # Check if the comment_Id already exists in the MySQL table
            check_query = "SELECT COUNT(*) FROM comments WHERE comment_Id = %s"
            mycursor.execute(check_query, (row['comment_Id'],))
            result = mycursor.fetchone()
            if result[0] == 0:
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
                abc="new comments inserted"              
            else:
                abc="comments data already exist"
    mydb.commit()
    return abc           
def tables():
    ch=new_channel_tab()
    st.write(ch)
    vd=new_video_tab()
    st.write(vd)
    cmnt=new_comnt_tab()
    st.write(cmnt)
    return 'Move TO SQL Successful'

def show_channel_tab():
    channels_list = []
    db = client["youtube_data"]
    collection = db['channel_details']
    for chdata in collection.find({}, {"_id": 0, "channel_info": 1}):
        channels_list.append(chdata["channel_info"])
    channel_tab = pd.DataFrame(channels_list)
    return channel_tab


def show_video_tab():
    videos_list = []
    db = client["youtube_data"]
    collection = db['channel_details']
    for vidata in collection.find({}, {'_id': 0, 'video_details': 1}):
        for i in range(len(vidata['video_details'])):
            videos_list.append(vidata['video_details'][i])
    video_tab = pd.DataFrame(videos_list)
    return video_tab


def show_comnt_tab():
    comment_list = []
    db = client["youtube_data"]
    collection = db['channel_details']
    for com_data in collection.find({}, {'_id': 0, 'comment_details': 1}):
        for i in range(len(com_data['comment_details'])):
            comment_list.append(com_data['comment_details'][i])
    comnt_tab = pd.DataFrame(comment_list)
    return comnt_tab

# Streamlit environment creation


st.title(':blue[YouTube Data Harvesting and Warehousing] ')
channel_id = st.text_input("Enter the channel ID")
temp_channel_id = channel_id
temp_video_id = []
channels = channel_id.split(',')
channels = [ch.strip() for ch in channels if ch]

if st.button('show channel data'):
    channeldata = channel_details(channel_id)
    st.success(channeldata)

# side bar
with st.sidebar:
    with st.expander(':blue[Skills Used]'):
        st.caption(':blue[Python Scripting]')
        st.caption(':blue[Data Collection]')
        st.caption(':blue[MongoDB]')
        st.caption(':blue[API Intergration]')
        st.caption(':blue[Data Management using MongoDB and SQL]')

col1, col2 = st.columns(2)

with col1:
    if st.button("migrate to MongoDB"):
        for channel_id in channels:
            ch_ids = []
            db = client['youtube_data']
            collection = db['channel_details']
            for ch_data in collection.find({}, {'_id': 0, 'channel_info': 1}):
                ch_ids.append(ch_data['channel_info']['channel_id'])

            if channel_id in ch_ids:
                st.success('Channel Details of the Given ID Already Exists')

            else:
                insert = main(channel_id)
                st.success(insert)

with col2:
    if st.button('Migrate to Sql') and temp_channel_id !="":
        Display = tables()
        st.success(Display)

show = st.radio('SELECT THE TABLE FOR VIEW', ('CHANNELS', 'VIDEOS', 'COMMENTS'))
if temp_channel_id != "":
    if show == "CHANNELS":
        st.write(show_channel_tab())

    elif show == "VIDEOS":
        st.write(show_video_tab())

    elif show == "COMMENTS":
        st.write(show_comnt_tab())

# SQL connection
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="1234", database="youtube_details")
mycursor = mydb.cursor()


Question = st.selectbox("select your Question", (' ',
                                                 '1.What are the names of all the videos and their corresponding channels?',
                                                 '2.Which channels have the most number of videos, and how many videos do they have?',
                                                 '3.What are the top 10 most viewed videos and their respective channels?',
                                                 '4.How many comments were made on each video, and what are their corresponding video names?',
                                                 '5.Which videos have the highest number of likes, and what are their corresponding channel names?',
                                                 '6.What is the total number of likes for each video,and what are their corresponding video names?',
                                                 '7.What is the total number of views for each channel, and what are their corresponding channel names?',
                                                 '8.What are the names of all the channels that have published videos in the year 2022?',
                                                 '9.What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                                 '10.Which videos have the highest number of comments, and what are their corresponding channel names?'
                                                 ))


if Question == '1.What are the names of all the videos and their corresponding channels?':
    query1 = '''SELECT video_name, channel_name FROM videodata;'''
    mycursor.execute(query1)
    t1 = mycursor.fetchall()
    st.write(pd.DataFrame(t1, columns=["video_name", "channel_name"]))

if Question == '2.Which channels have the most number of videos, and how many videos do they have?':
    query2 = '''SELECT channel_name, COUNT(*) AS video_count
                FROM videodata
                GROUP BY channel_name
                ORDER BY video_count DESC;'''
    mycursor.execute(query2)
    t2 = mycursor.fetchall()
    st.write(pd.DataFrame(t2, columns=["channel_name", "video_count"]))


if Question == '3.What are the top 10 most viewed videos and their respective channels?':
    query3 = '''SELECT video_name, channel_name, view_count
                FROM videodata
                ORDER BY view_count DESC
                LIMIT 10;'''
    mycursor.execute(query3)
    t3 = mycursor.fetchall()
    st.write(pd.DataFrame(t3, columns=[
             "video_name", "channel_name", "view_count"]))


if Question == '4.How many comments were made on each video, and what are their corresponding video names?':
    query4 = '''SELECT videodata.video_name,count(comments.comment_Id) AS comment_count
                FROM videodata 
                LEFT JOIN comments  ON videodata.video_id=comments.Video_id
                GROUP BY videodata.video_name;'''
    mycursor.execute(query4)
    t4 = mycursor.fetchall()
    st.write(pd.DataFrame(t4, columns=["video_name", "comment_count"]))

if Question == '5.Which videos have the highest number of likes, and what are their corresponding channel names?':
    query5 = '''SELECT video_name,channel_name,like_count
                FROM videodata
                ORDER BY like_count DESC
                LIMIT 10;'''
    mycursor.execute(query5)
    t5 = mycursor.fetchall()
    st.write(pd.DataFrame(t5, columns=[
             "video_name", "channel_name", "like_count"]))

if Question == '6.What is the total number of likes for each video,and what are their corresponding video names?':
    query6 = '''SELECT v.video_name,SUM(v.like_count) AS total_likes
                FROM videodata v 
                GROUP BY v.video_id, v.video_name;'''
    mycursor.execute(query6)
    t6 = mycursor.fetchall()
    st.write(pd.DataFrame(t6, columns=["video_name", "total_likes"]))

if Question == '7.What is the total number of views for each channel, and what are their corresponding channel names?':
    query7 = '''SELECT channel_name, SUM(view_count) AS total_views
                FROM videodata
                GROUP BY channel_name;'''
    mycursor.execute(query7)
    t7 = mycursor.fetchall()
    st.write(pd.DataFrame(t7, columns=["channel_name", "total_views"]))

if Question == '8.What are the names of all the channels that have published videos in the year 2022?':
    query8 = '''SELECT DISTINCT channel_name
                FROM videodata
                WHERE year(published_date) = '2022';'''
    mycursor.execute(query8)
    t8 = mycursor.fetchall()
    st.write(pd.DataFrame(t8, columns=["channel_name"]))

if Question == '9.What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    query9 = '''SELECT channel_name, AVG(duration) AS average_duration
                FROM videodata
                GROUP BY channel_name;'''
    mycursor.execute(query9)
    t9 = mycursor.fetchall()
    st.write(pd.DataFrame(t9, columns=["channel_name", "average_duration"]))

if Question == '10.Which videos have the highest number of comments, and what are their corresponding channel names?':
    query10 = '''SELECT videodata.video_name, videodata.channel_name, COUNT(comments.comment_id) AS comment_count
                FROM videodata
                LEFT JOIN comments ON videodata.video_id = comments.Video_id
                GROUP BY videodata.video_id, videodata.video_name, videodata.channel_name
                ORDER BY comment_count DESC
                '''
    mycursor.execute(query10)
    t10 = mycursor.fetchall()
    st.write(pd.DataFrame(t10, columns=[
             "video_name", "channel_name", "comment_count"]))

if Question == ' ':
    st.write("Please Select Questions from the Dropdown")
