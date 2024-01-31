# YouTube Data Harvesting and Warehousing using MongoDB, SQL and Streamlit

### Introduction :
This project is to develop a user-friendly Streamlit application that utilizes the Google API to extract information on a YouTube channel, stores it in a MongoDB database, migrates it to a SQL data warehouse. For getting a various insights about youtube Channels. 

### Technologies used :
- Python
- MongoDB
- MySQL
- YouTube Data API
- Streamlit
- Pandas

### Project Overview :
1. Fetch YouTube data (channels, playlists, videos, comments) with Google API client.
2. User-driven channel data extraction via IDs triggers API calls.
3. Store retrieved data in flexible MongoDB data lake because it can handle unstructured/semi-structured data .
4. Migrate/transform data of the multiple channels to structured MySQL data warehouse using pandas DataFrame.
5. Join tables & retrieve specific channel data based on user SQL queries.
6. Gain insights through SQL analysis & display results for user query in Streamlit app.

### Usage
- Enter a YouTube channel ID to retrieve data for that channel.
- Store the retrieved data in the MongoDB data lake.
- Collect and store data for multiple YouTube channels in the data lake.
- Select a channel and migrate its data from the data lake to the SQL data warehouse.
- Search and retrieve data from the SQL database using various user search options.

### References
- [YouTube API Documentation](https://developers.google.com/youtube)
- [MongoDB Documentation](https://docs.mongodb.com/)
- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/)
- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [Python Documentation](https://docs.python.org/)
- [Streamlit Documentation](https://docs.streamlit.io/)
