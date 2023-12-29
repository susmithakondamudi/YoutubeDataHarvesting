#!/usr/bin/env python
# coding: utf-8

# In[1]:


import google.auth
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from pprint import pprint as pp
api_service_name = "youtube"
api_version = "v3"
api_key="AIzaSyCNvNdgRTeG-0sZ63yHD6Fj3-bWHYkpn14"
youtube =build(api_service_name, api_version, developerKey=api_key)


# In[2]:


def channel_details(channel_id):
    request = youtube.channels().list(part="snippet,contentDetails,statistics",id=channel_id)    
    response = request.execute()
    datach=dict(channel_id=response['items'][0]['id'],
           channel_name=response['items'][0]['snippet']['title'],
           channel_description=response['items'][0]['snippet']['description'],
           channel_published=response['items'][0]['snippet']['publishedAt'],
           channel_playlist=response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
           channel_scount=response['items'][0]['statistics']['subscriberCount'],
           channel_vcount=response['items'][0]['statistics']['videoCount'],
           channel_viewcount=response['items'][0]['statistics']['viewCount'])

    return datach



# In[3]:


channel_details('UCwtmXsVJdEjB97fzsgTlcBg')


# In[4]:


def to_getchannel_videos(channel_id):
    video_ids=[]
    request=youtube.channels().list(id=channel_id,part='contentDetails')
    response=request.execute()
    playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token=None
    
    while True:
        request = youtube.playlistItems().list(playlistId=playlist_id,part="snippet",maxResults=50,pageToken=next_page_token)
        response = request.execute()
        
        for i in range(len(response['items'])):
            video_ids.append(response['items'][i]['snippet']['resourceId']['videoId'])
            
            next_page_token=response.get('nextPageToken')
            
        if next_page_token is None:
            break
    return video_ids


# In[5]:


videos=to_getchannel_videos('UCwtmXsVJdEjB97fzsgTlcBg')
pp(videos)


# In[27]:


print(len(videos))


# In[6]:


def get_video_detailas(videos):
    video_data=[]
    for video_id in videos:
        request=youtube.videos().list(part="snippet,contentDetails,statistics",id=video_id)#id=#','.join(video_ids[i:i+50]))
        response=request.execute()
        for i in response['items']:
            datav=dict(
            channel_name=response['items'][0]['snippet']['channelTitle'],
            channel_id=response['items'][0]['snippet']['channelId'],
            video_id=response['items'][0]['id'],
            video_name=response['items'][0]['snippet']['title'],
            video_description=response['items'][0]['snippet']['description'],
            tags=','.join(response['items'][0]['snippet'].get('tags',[])),
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
    


# In[23]:


request=youtube.videos().list(part="snippet,contentDetails,statistics",id="xbjFGU65KAI")#id=#','.join(video_ids[i:i+50]))
response=request.execute()
tags=response['items'][0]['snippet'].get('tags')
print(tags)


# In[54]:


get_video_detailas(videos)


# In[7]:


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


# In[56]:


get_comment_info(videos)


# In[10]:


import pymongo
client = pymongo.MongoClient('mongodb://localhost:27017')

db = client['youtube_data']


def main(channel_id):
    videos=to_getchannel_videos(channel_id)
    channels=channel_details(channel_id)
    videodetails= get_video_detailas(videos)
    commentdetails=get_comment_info(videos)
    data={"channel_info":channels,"video_details":videodetails,"comment_details":commentdetails}
    
    collection = db['channel_details']
    collection.insert_one(data)
    

    return 'upload completed successfully'



# In[67]:


main("UCcQhorXxoL6R7a2PwE7xrFg")


# In[8]:


videos=to_getchannel_videos('UCcQhorXxoL6R7a2PwE7xrFg')
videos
df=pd.DataFrame(videos)
df


# In[9]:


pip install pymysql


# In[30]:


pip install SQLAlchemy


# In[5]:


from sqlalchemy import create_engine


# In[27]:


import mysql.connector
import pandas as pd
from pymongo import MongoClient


# In[28]:


mydb=mysql.connector.connect(
            host="localhost",
            user="root",
            password="1234",database="youtube_details")
mycursor=mydb.cursor()
client = MongoClient('mongodb://localhost:27017/')
db = client["youtube_data"]
collection=db['channel_details']


# In[83]:


mycursor.execute("CREATE DATABASE youtube_datas")


# In[13]:


mycursor.execute("use youtube_details")


# In[72]:


mycursor.execute("DROP TABLE youtube_details.channeldata")


# In[38]:


def channel_tab():
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
    channels_list=[]
    for chdata in collection.find({},{"_id":0,"channel_info":1}):
            channels_list.append(chdata["channel_info"])
    df=pd.DataFrame(channels_list)
    for index,row in df.iterrows():
            insert_query='''insert into channeldata(channel_name,
                                                    channel_id,
                                                    channel_description,
                                                    channel_published,
                                                    channel_playlist,
                                                    channel_scount,
                                                    channel_vcount,
                                                    channel_viewcount)values(%s,%s,%s,%s,%s,%s,%s,%s)'''
            values=(row['channel_name'],row['channel_id'],row['channel_description'],row['channel_published'].replace("T"," ").replace("Z"," "),row['channel_playlist'],row['channel_scount'],row['channel_vcount'],row['channel_viewcount'])
            mycursor.execute(insert_query,values)
    mydb.commit()






# In[74]:


channel_tab()


# In[11]:


channels_list=[]
client = MongoClient('mongodb://localhost:27017/')
db=client["youtube_data"]
collection=db['channel_details']
for chdata in collection.find({},{"_id":0,"channel_info":1}):
        channels_list.append(chdata["channel_info"])
        df=pd.DataFrame(channels_list)


# In[12]:


df


# In[65]:


def video_tab():
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
    videos_list=[]
    for vidata in collection.find({},{'_id':0,'video_details':1}):
        for i in range(len(vidata['video_details'])):
            videos_list.append(vidata['video_details'][i])
    df2=pd.DataFrame(videos_list)
    for index, row in df2.iterrows():
        #print(row['published_date'].replace("T"," ").replace("Z"," "))

        insert_query='''INSERT INTO videodata(channel_name,
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
                    row['published_date'].replace("T"," ").replace("Z"," "),
                    row['view_count'],
                    row['like_count'],
                    row['favourite_count'],
                    row['comment_count'],
                    row['duration'].replace('PT', '').replace('H', ':').replace('M', ':').split('S')[0],
                    row['thumbnail']['default']['url'],
                    row['caption_status'])
        mycursor.execute(insert_query, values)
    mydb.commit()



# In[66]:


video_tab()


# In[40]:


drop_query = '''drop table if exists videodata'''
mycursor.execute(drop_query)
mycursor.execute('''CREATE TABLE if not exists videodata(channel_name VARCHAR(100),channel_id VARCHAR(100),video_id VARCHAR(100) primary key,video_name VARCHAR(100),tags TEXT,video_description TEXT,published_date VARCHAR(100),view_count BIGINT,like_count BIGINT,favourite_count int,comment_count int,duration TIME,thumbnail VARCHAR(200),caption_status VARCHAR(50))''')
videos_list=[]
for vidata in collection.find({},{'_id':0,'video_details':1}):
    for i in range(len(vidata['video_details'])):
        videos_list.append(vidata['video_details'][i])
df2=pd.DataFrame(videos_list)


# In[ ]:


def video_tab():
    drop_query='''drop table if exists videodata'''
    mycursor.execute(drop_query)
    mycursor.execute('''CREATE TABLE if not exists videodata(channel_name VARCHAR(100),
                                                               channel_id VARCHAR(100),
                                                               video_id VARCHAR(100) PRIMARY KEY,
                                                               video_name VARCHAR(200),
                                                               tags TEXT,
                                                               video_description TEXT,
                                                               published_date VARCHAR(100)
                                                               
                                                        
                                                                
                                                                


# In[13]:


videos_list=[]
client = MongoClient('mongodb://localhost:27017/')
db = client["youtube_data"]
collection=db['channel_details']
for vidata in collection.find({},{'_id':0,'video_details':1}):
    for i in range(len(vidata['video_details'])):
        videos_list.append(vidata['video_details'][i])
df2=pd.DataFrame(videos_list)


# In[14]:


df2


# In[63]:


def comnt_tab():
    drop_query = '''drop table if exists comments'''
    mycursor.execute(drop_query)
    mycursor.execute('''CREATE TABLE if not exists comments(comment_Id varchar(100) primary key,
                                                              Video_id varchar(100),
                                                              Comment_Text text,
                                                              Comment_Author varchar(100),
                                                              Comment_Published VARCHAR(100))''')
    comment_list=[]
    for com_data in collection.find({}, {'_id': 0, 'comment_details': 1}):
        for i in range(len(com_data['comment_details'])):
            comment_list.append(com_data['comment_details'][i])
    df3 = pd.DataFrame(comment_list)
    for index, row in df3.iterrows():
        insert_query = '''INSERT INTO comments(comment_Id,
                                                Video_id,
                                                Comment_Text,
                                                Comment_Author,
                                                Comment_Published)values(%s,%s,%s,%s,%s)'''
        values = (row['comment_Id'],row['Video_id'],row['Comment_Text'],row['Comment_Author'],row['Comment_Published'].replace("T"," ").replace("Z"," "))

        mycursor.execute(insert_query, values)
    mydb.commit()


# In[64]:


comnt_tab()


# In[20]:


comment_list=[]
client = MongoClient('mongodb://localhost:27017/')
db = client["youtube_data"]
collection=db['channel_details']
for com_data in collection.find({}, {'_id': 0, 'comment_details': 1}):
    for i in range(len(com_data['comment_details'])):
        comment_list.append(com_data['comment_details'][i])
df3 = pd.DataFrame(comment_list)
df3


# In[ ]:




