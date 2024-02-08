#-------------------------------------------Needed Libraries------------------------------------------------------------
import streamlit as st 
import pandas as pd
import googleapiclient.discovery
from googleapiclient.errors import HttpError
import pymongo
from st_pages import Page,show_pages,add_page_title
#----------------------------------------------MySQL Connection------------------------------------------------------------
import mysql.connector
config = {
       'user': 'root',          'password':'1234', 
       'host':'127.0.0.1',      'database':'youtube'
}
mydb = mysql.connector.connect(**config)
cursor = mydb.cursor()

#---------------------------------------------MongoDB Connection-----------------------------------------------------------
client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client['Youtube']
collection = db['Channel_details']


#-----------------------------------------------API SETUP-------------------------------------------------------------------
api_ = 'YOUR API KEY'
api_service_name = "youtube"
api_version = "v3"

youtube = googleapiclient.discovery.build(
          api_service_name, api_version, developerKey=api_)


#------------------------------------------Getting channel details--------------------------------------------------------
def get_channel_details(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id= channel_id)
    response = request.execute()
    data = {
        "Channel_Name" : response['items'][0]['snippet']['title'],
        "Channel_Id" : response['items'][0]['id'],
        "Subscription_Count" : response['items'][0]['statistics']['subscriberCount'],
        "Channel_Views" : response['items'][0]['statistics']['viewCount'],
        "Channel_Description" : response['items'][0]['snippet']['description'],
        "Playlist_Id" : response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
        "Total_Videos":response['items'][0]['statistics']['videoCount']
    }
    return data


#-------------------------------------------Getting playlist details-----------------------------------------------------
def get_playlist_details(channel_id):
  playlist_details = []
  nextPageToken = None
  while True:
    request = youtube.playlists().list(
        part = 'snippet, contentDetails',
        channelId = channel_id,
        maxResults = 50,
        pageToken = nextPageToken
    )
    response = request.execute()
    for item in response['items']:
      playlist_info = {
          'Playlist_Id':item['id'],
          'Playlist_title':item['snippet']['title'],
          'Channel_id':channel_id,
          'Channnel_name':item['snippet']['channelTitle'],
          'Published_At':item['snippet']['publishedAt'],
          'Video_count':item['contentDetails']['itemCount']
      }
      playlist_details.append(playlist_info)
    nextPageToken = response.get('nextPageToken')
    if nextPageToken is None:
      break
  return playlist_details


#-------------------------------------------Getting video IDs------------------------------------------------------------ 
def get_video_ids(channel_id):
    playlist_id = get_channel_details(channel_id)['Playlist_Id']
    video_ids = []
    nextpageToken = None
    while True:
        request = youtube.playlistItems().list(
          part = "contentDetails",
          playlistId = playlist_id,
          maxResults = 50,
          pageToken = nextpageToken)
        response = request.execute()
        for i in range(0,len(response['items'])):
            video_ids.append(response['items'][i]['contentDetails']['videoId'])
        nextpageToken = response.get('nextPageToken')
        if nextpageToken is None:
            break
    return video_ids


#-------------------------------------------Getting Video Details--------------------------------------------------------
def get_video_details(channel_id):
    video_ids = get_video_ids(channel_id)
    video_details = []
    for video_id in video_ids:
        request = youtube.videos().list(
            part='snippet,statistics,contentDetails',
            id=video_id
            ).execute()
        response =  request['items'][0]
        s = response['contentDetails']['duration']
        l=[]
        f =''
        for i in s:
            if i.isnumeric():
                f = f+i
            else:
                if f:
                    l.append(f)
                    f=''
        if 'H' not in s:
            l.insert(0,'00')
        if 'M' not in s:
            l.insert(1,'00')
        if 'S' not in s:
            l.insert(-1,'00') 
	for i in range(len(l)):
            if len(l[i])==1:
                value='0'+l[i]
                l.remove(l[i])
                l.insert(i,value)
            duration = ':'.join(l) 	 
        if len(duration) <= 8: 
            video_info = {
                "Channel_Name":response['snippet']['channelTitle'],
                "Channel_ID":response['snippet']['channelId'],
                "Video_Id": response['id'],
                "Video_Name": response['snippet']['localized']['title'],
                "Video_Description": response['snippet']['description'],
                "Tags": response['snippet'].get('tags'),
                "PublishedAt": response['snippet']['publishedAt'],
                "View_Count": response['statistics']['viewCount'],
                "Like_Count": response['statistics'].get('likeCount'),
                "Favorite_Count": response['statistics'].get('favoriteCount'),
                "Comment_Count": response['statistics'].get('commentCount'),
                "Duration": duration,
                "Thumbnail": response['snippet']['thumbnails']['default']['url'],
                "Caption_Status": response['contentDetails']['caption']
                }
            video_details.append(video_info)
    return video_details


#------------------------------------------Getting comment Details-------------------------------------------------------
def get_comment_details(channel_id):
  try:
        video_ids = get_video_ids(channel_id)
        comments_details = []
        nextPageToken = None
        for video_id in video_ids:
            while True:
                request = youtube.commentThreads().list(
                    part = "snippet",
                    videoId = video_id,
                    maxResults = 50,
                    pageToken = nextPageToken
                    )
                response = request.execute()
                for items in response['items']:
                    Comment_info = {
                        "Channel_Name":get_channel_details(channel_id)['Channel_Name'],
                        "Channel_Id":items['snippet']['channelId'],
                        "Video_Id":items['snippet']['videoId'],
                        "Comment_Id": items['snippet']['topLevelComment']['id'],
                        "Comment_Text": items['snippet']['topLevelComment']['snippet']['textOriginal'],
                        "Comment_Author": items['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        "Comment_PublishedAt": items['snippet']['topLevelComment']['snippet']['publishedAt']
                        }
                    comments_details.append(Comment_info)
                nextPageToken = response.get('nextPageToken')

                if nextPageToken is None:
                    break
  except HttpError as e:
        if e.resp.status == 403 and b'commentsDisabled' in e.content:
          Comment_info = {}    
  return comments_details


#-------------------------------------------Data storing in MongoDB--------------------------------------------------------
def ch_ids():
    l = []
    for id in collection.find({}):
        l.append(id['Channel Details']['Channel_Id'])
    return l    


def data_extraction(channel_id):
  if channel_id not in ch_ids():
    channel_data = get_channel_details(channel_id)
    playlist_data = get_playlist_details(channel_id)
    video_data = get_video_details(channel_id)
    comment_data = get_comment_details(channel_id)
    youtube_data = {
        'Channel Details':channel_data,
        'Playlist Details':playlist_data,
        'Video Details':video_data,
        'Comment Details':comment_data
    }
    collection.insert_one(youtube_data)
  return st.success("Data has been Successfully Extracted and stored in MongoDB")


#-------------------------------------------MySQL Tables Creation--------------------------------------------------------
def table_creation():
    channel_table_query = """create table if not exists Channel(
          Channel_Name  VARCHAR(100),
          Channel_ID VARCHAR(50) primary key,
          Subscription_Count INT,
          Channel_Views INT,
          Channel_Description TEXT,
          Playlist_Id VARCHAR(50),
          Total_Videos INT
          );""" 
    cursor.execute(channel_table_query)
  

    playlist_table_query = """create table if not exists playlist(
		Playlist_Id varchar(50),
                Playlist_title text,
                Channel_id varchar(50),
                Channnel_name varchar(50),
                Published_At timestamp,
                Video_count int
            );"""
    cursor.execute(playlist_table_query)

    
    video_table_query = """create table if not exists video(
              Channel_Name varchar(50),
              Channel_ID varchar(50),
              Video_Id varchar(50),
              Video_Name varchar(150),
              Video_Description text,
              Tags text,
              PublishedAt timestamp,
              View_Count int,
              Like_Count int,
              Favorite_Count int,
              Comment_Count int,
              Duration varchar(50),
              Thumbnail varchar(200),
              Caption_Status varchar(10)
            );"""
    cursor.execute(video_table_query)
    
    comment_table_query="""create table if not exists comment(
             Channel_Name varchar(50),
             Channel_Id varchar(50),
             Video_Id varchar(30),
             Comment_Id varchar(50),
             Comment_Text text,
             Comment_Author varchar(100),
             Comment_PublishedAt timestamp
            );"""
    cursor.execute(comment_table_query)
    mydb.commit()


#---------------------------------------------Data Migration--------------------------------------------------------------
def data_insert(channel_id):
    # Channel table 
    d = collection.find_one({'Channel Details.Channel_Id':channel_id})
    ch_info = d['Channel Details']
    df = pd.DataFrame(ch_info,index=[0])
    for index, row in df.iterrows():
        channel_insert = """insert into channel values (%s,%s,%s,%s,%s,%s,%s);"""
        value = []
        for column in df.columns:
            value.append(row[column])
        values = tuple(value) 
        cursor.execute(channel_insert,values)
        mydb.commit()


    # Playlist table
    try:
        pl_info =  d['Playlist Details']
        df = pd.DataFrame(pl_info)
        df.Published_At = df.Published_At.str.replace('T',',')
        df.Published_At = df.Published_At.str.replace('Z','')
        for index, row in df.iterrows():
            pl_insert = """ insert into playlist values (%s,%s,%s,%s,%s,%s);"""
            value = []
            for column in df.columns:
                value.append(row[column])
            values = tuple(value) 
            cursor.execute(pl_insert,values)
            mydb.commit()
    except:
        pass 

    # Video Table 
    vi_info = d['Video Details']
    df = pd.DataFrame(vi_info)
    df.PublishedAt = df.PublishedAt.str.replace('T',',')
    df.PublishedAt = df.PublishedAt.str.replace('Z','')
    for index, row in df.iterrows():
        vi_insert = """ insert into video values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        row['Tags'] = str(row['Tags'])
        values = (
            row['Channel_Name'],
            row['Channel_ID'],
            row['Video_Id'],
            row['Video_Name'],
            row['Video_Description'],
            row['Tags'],
            row['PublishedAt'],
            row['View_Count'],
            row['Like_Count'],
            row['Favorite_Count'],
            row['Comment_Count'],
            row['Duration'],
            row['Thumbnail'],
            row['Caption_Status']
        )
        cursor.execute(vi_insert,values)
        mydb.commit() 

    # Comment table 
    com_info = d['Comment Details']
    df = pd.DataFrame(com_info)
    df.Comment_PublishedAt = df.Comment_PublishedAt.str.replace('T',',')
    df.Comment_PublishedAt = df.Comment_PublishedAt.str.replace('Z','')
    for index, row in df.iterrows():
        com_insert = """ insert into comment values(%s,%s,%s,%s,%s,%s,%s)"""
        value = []
        for column in df.columns:
            value.append(row[column])
        values = tuple(value) 
        cursor.execute(com_insert,values)
        mydb.commit()         
    return st.success("Channel Data has been Migrated to SQL")


#---------------------------------------------Useful fuctions for streamlit-----------------------------------------------
def ch_name():
    ch_list = []
    for i in collection.find({}):
        ch_list.append(i['Channel Details']['Channel_Name'])
    return ch_list 

def ch_id(option):
    id = collection.find_one({'Channel Details.Channel_Name':option})
    id = id['Channel Details']['Channel_Id']
    return id

#----------------------------------------------  Streamlit-----------------------------------------------------------------
st.set_page_config(page_title="Data Extraction and Migration",page_icon="🛠")
st.title(':red[Youtube Data Harvesting and Warehousing]')
st.write("## Enter Youtube Channel ID below :")
channel_id = st.text_input('Enter Channel ID')
if st.button('Extract Data'): 
    data_extraction(channel_id)  
st.write('## Select a channel to begin transform to SQL')
option = st.selectbox('Select Channel',ch_name(),index=None,placeholder='Select Channel')
if st.button('Migrate to SQL'):
    try:
        cid = ch_id(option)
        table_creation()
        data_insert(cid)
    except:
        st.success("Duplicate Entry :  Channel already present in the database")   

show_pages(
    [
    Page("main.py","Data Extraction and Migration","🛠"),
    Page("main 2.py","View","👓")
    ])  
           




