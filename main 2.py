import streamlit as st
import mysql.connector
import pandas as pd 

#---------------------------------------------------MySql Connection----------------------------------------------------
config = {
       'user': 'root',          'password':'1234', 
       'host':'127.0.0.1',      'database':'youtube'
}
mydb = mysql.connector.connect(**config)
cursor = mydb.cursor()


#----------------------------------------------------Functions for queries----------------------------------------------
def ch_names():
    query = """select Channel_Name from channel;"""
    #query = "SELECT * from actor;"
    cursor.execute(query)
    data = cursor.fetchall()
    return data


def q1():
    query = """select Channel_Name,Video_Name from video;"""
    cursor.execute(query)
    result = cursor.fetchall()
    df = pd.DataFrame(result,columns=['Channel Name','Video Name'])
    return df

def q2():
    query = """select Channel_Name,Total_Videos from channel order by Total_Videos desc;"""
    cursor.execute(query)
    result = cursor.fetchall()
    df = pd.DataFrame(result,columns=['Channel Name','No.Videos'])
    return df

def q3():
    query = """select Video_Name, Channel_Name,View_Count from video order by View_Count desc limit 10;"""
    cursor.execute(query)
    result = cursor.fetchall()
    df = pd.DataFrame(result,columns=['Video Name','Channel Name','Views'])
    return df

def q4():
    query = """select Video_Name, Comment_Count from video order by Comment_Count desc;"""
    cursor.execute(query)
    result = cursor.fetchall()
    df = pd.DataFrame(result,columns=['Video Name','Comment Count'])
    return df

def q5():
    query = """select Video_Name, Channel_Name, Like_Count from video order by Like_Count desc;"""
    cursor.execute(query)
    result = cursor.fetchall()
    df = pd.DataFrame(result,columns=['Video Name','Channel Name','Like Count'])
    return df

def q6():
    query = """select Video_Name, Like_Count from video order by Like_Count desc;"""
    cursor.execute(query)
    result = cursor.fetchall()
    df = pd.DataFrame(result,columns=['Video Name','Like Count'])
    return df

def q7():
    query = """select Channel_Name, Channel_Views from Channel order by Channel_Views desc;"""
    cursor.execute(query)
    result = cursor.fetchall()
    df = pd.DataFrame(result,columns=['Channel Name','Views'])
    return df

def q8():
    query = """select Channel_Name,Video_Name,date (PublishedAt) from video where year(PublishedAt)=2022;"""
    cursor.execute(query)
    result = cursor.fetchall()
    df = pd.DataFrame(result,columns=['Channel Name','Video Name','Published At'])
    return df

def q9():
    query = """select Channel_Name, sec_to_time(avg(time_to_sec(Duration))) as Avg_Duration from video group by Channel_Name order by Avg_Duration ;"""
    cursor.execute(query)
    result = cursor.fetchall()
    df = pd.DataFrame(result,columns=['Channel Name','Average Duration'])
    return df

def q10():
    query = """select Video_Name,Channel_Name,Comment_Count from video order by Comment_Count desc;"""
    cursor.execute(query)
    result = cursor.fetchall()
    df = pd.DataFrame(result,columns=['Video Name','Channnel Name','Comment Count'])
    return df    

#---------------------------------------------------Streamlit-------------------------------------------------------------
st.set_page_config(page_title="VIEW",page_icon="🥽")
st.title(':red[Data Queries]')
st.write('## Available Channels')
sel = st.checkbox('Show Channels') 
if sel:
    try:
        data = ch_names()
        df = pd.DataFrame(data,columns=['Channel Name'])
        st.dataframe(df)
    except:
        st.error("No Channels Found")    
options = ['1. What are the names of all the videos and their corresponding channels?',
           '2. Which channels have the most number of videos, and how many videos do they have?',
           '3. What are the top 10 most viewed videos and their respective channels?',
           '4. How many comments were made on each video, and what are their corresponding video names?',
           '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
           '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
           '7. What is the total number of views for each channel, and what are their corresponding channel names?',
           '8. What are the names of all the channels that have published videos in the year 2022?',
           '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
           '10. Which videos have the highest number of comments, and what are their corresponding channel names?'
           ]
try:
    select_question = st.selectbox("Select the Question",options,index = None,placeholder='Tap to Select')
    if select_question == '1. What are the names of all the videos and their corresponding channels?':
        st.dataframe(q1())

    if select_question == '2. Which channels have the most number of videos, and how many videos do they have?':
        st.dataframe(q2())

    if select_question == '3. What are the top 10 most viewed videos and their respective channels?':
        st.dataframe(q3())    

    if select_question == '4. How many comments were made on each video, and what are their corresponding video names?':
        st.dataframe(q4())

    if select_question == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        st.dataframe(q5())    

    if select_question == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        st.dataframe(q6())     

    if select_question == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        st.dataframe(q7())     

    if select_question == '8. What are the names of all the channels that have published videos in the year 2022?':
        st.dataframe(q8())    

    if select_question == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        st.dataframe(q9())     

    if select_question == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        st.dataframe(q10())     
except:
    st.success("Please add atleast one channel to MySQL database")        