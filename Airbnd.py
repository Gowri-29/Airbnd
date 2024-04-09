# Technologies
import pymongo
import pandas as pd
import plotly.express as px
import geopandas as gpd
import pymysql
#data collection
client = pymongo.MongoClient("mongodb+srv://gowrisanker963:PZARIlxuFrrrcKm8@cluster1.nwq5fyq.mongodb.net/")
# Access your database and collection
def data_Extraction():
    import pandas as pd
    from decimal import Decimal
    db = client.sample_airbnb  
    collection = db.listingsAndReviews 

    # Extract entire data from MongoDB and convert it to list of dictionaries
    cursor = collection.find()
    data = list(cursor)

    # Create DataFrame
    df = pd.DataFrame(data)

    # Data preprocessing:

    df["weekly_price"].fillna("0", inplace=True)
    df["monthly_price"].fillna("0", inplace=True)
    df["reviews_per_month"].fillna(0, inplace=True)
    df["cleaning_fee"].fillna("0.00", inplace=True)
    df["security_deposit"].fillna("0.00", inplace=True)
    df["bathrooms"].fillna("0.00", inplace=True)
    df["beds"].fillna(0.00, inplace=True)
    df["bedrooms"].fillna(0.00, inplace=True)
    # Convert "first_review" column to datetime dtype
    df["first_review"] = pd.to_datetime(df["first_review"])

    # Calculate the average of the datetime values in the "first_review" column
    average_first_review = df["first_review"].mean()
    df["first_review"].fillna(average_first_review, inplace=True)

    # Convert "first_review" column to datetime dtype
    df["last_review"] = pd.to_datetime(df["last_review"])

    # Calculate the average of the datetime values in the "first_review" column
    average_last_review = df["last_review"].mean()
    df["last_review"].fillna(average_last_review, inplace=True)

    country = df["address"]
    Country = []
    for i in country:
        count = i.get("country")
        Country.append(count)
    df["Country"] = Country

    

    # Assuming df['price'] is your DataFrame column containing Decimal128 values
    # Convert Decimal128 values to strings
    df['price'] = df['price'].astype(str)

    # Then convert the column to float
    df['price'] = df['price'].astype(float)

    df["security_deposit"] = df["security_deposit"].astype(str)
    df["security_deposit"] = df['security_deposit'].astype(float)

    df["cleaning_fee"] = df["cleaning_fee"].astype(str)
    df["cleaning_fee"] = df["cleaning_fee"].astype(float)

    df["Total_price"] = df["price"] + df["security_deposit"] + df["cleaning_fee"]

    latitude = []
    longitude = []
    for index, row in df.iterrows():
        try:
            coord1 = row["address"]["location"]["coordinates"][0]
            longitude.append(coord1)
            coord2 = row["address"]["location"]["coordinates"][1]
            latitude.append(coord2)
        except (KeyError, TypeError):
            pass

    df["latitude"] = latitude
    df["longitude"] = longitude

    rew = df["reviews"]
    Review = []
    for i in rew:
        Rew = len(i)
        Review.append(Rew)
    df["Review"] = Review
    return (df)
Data = data_Extraction()
def uploading_Data(Data):
   df = Data
   updated_DF = df[["name","summary","space","description","neighborhood_overview","transit","access","property_type","room_type","bed_type","minimum_nights","maximum_nights","first_review","last_review","accommodates","bedrooms","weekly_price","monthly_price","Country","Total_price","Review"]]
   #Connecting to data base:
   myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='gowri@2903')
   cur = myconnection.cursor()
   try:
    cur.execute("create database Airbnb_Database")
   except:
    pass 
   myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='gowri@2903',database="Airbnb_Database")
   cur = myconnection.cursor() 
   try:
    # Define table creation SQL statement
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS Data (
        name TEXT,
        summary TEXT,
        space TEXT,
        description TEXT,
        neighborhood_overview TEXT,
        transit TEXT,
        access TEXT,
        property_type TEXT,
        room_type TEXT,
        bed_type TEXT,
        minimum_nights INT,
        maximum_nights INT,
        first_review DATETIME,
        last_review DATETIME,
        accommodates INT,
        bedrooms FLOAT,
        weekly_price FLOAT,
        monthly_price FLOAT,
        Country TEXT,
        Total_price FLOAT,
        Review INT
    )
    """

    # Execute table creation SQL statement
    cur.execute(create_table_sql)

    # Define SQL statement for data insertion
    sql = """
    INSERT INTO Data VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    """

    for i in range(0,len(updated_DF)):
        cur.execute(sql,tuple(updated_DF.iloc[i]))
        myconnection.commit()
   except:
    pass        
 
import pymysql
import streamlit as st

def data_Evaluation1():
    myconnection = pymysql.connect(host='127.0.0.1', user='root', passwd='gowri@2903', database="Airbnb_Database")
    cur = myconnection.cursor()
    
    # Fetching distinct countries from the database
    cur.execute("SELECT DISTINCT Country FROM data")
    result = cur.fetchall()
    country = {}
    for row in result:
        country[row[0]] = row[0]
    
    # Fetching distinct property types from the database
    cur.execute("SELECT DISTINCT property_type FROM data")
    result = cur.fetchall()
    property_type = {}
    for row in result:
        property_type[row[0]] = row[0]
    
    # Fetching distinct room types from the database
    cur.execute("SELECT DISTINCT room_type FROM data")
    result = cur.fetchall()
    room_type = {}
    for row in result:
        room_type[row[0]] = row[0]
    
    selected_country = st.selectbox("View details by country", list(country.keys()))
    selected_property_type = st.selectbox("View details by property type", list(property_type.keys()))
    selected_room_type = st.selectbox("View details by room_type", list(room_type.keys()))

    # Fetching data from the database based on the selected country, property type, and room type
    cur.execute(f"SELECT name, Country, Total_price, property_type, room_type FROM Data WHERE Country='{selected_country}' AND property_type='{selected_property_type}' AND room_type='{selected_room_type}' ORDER BY Total_price DESC")
    table_data = cur.fetchall()


    # Displaying the data in a Streamlit table
    st.table(table_data)


def data_Evaluation2():
    myconnection = pymysql.connect(host='127.0.0.1', user='root', passwd='gowri@2903', database="Airbnb_Database")
    cur = myconnection.cursor()
    
    # Fetching distinct countries from the database
    cur.execute("SELECT DISTINCT Country FROM data")
    result = cur.fetchall()
    country = {}
    for row in result:
        country[row[0]] = row[0]
    
    selected_country = st.selectbox("View details by Review", list(country.keys()))

    # Fetching data from the database based on the selected country and sorting by Review in descending order
    cur.execute(f"SELECT name, Country, summary, space, description, neighborhood_overview, transit, access, first_review, last_review, Review FROM Data WHERE Country='{selected_country}' ORDER BY Review DESC")
    table_data = cur.fetchall()

    # Displaying the data in a Streamlit table
    st.table(table_data)

def data_Evaluation3():
   myconnection = pymysql.connect(host='127.0.0.1', user='root', passwd='gowri@2903', database="Airbnb_Database")
   cur = myconnection.cursor()
    
   # Fetching distinct countries from the database
   cur.execute("SELECT DISTINCT Country FROM data")
   result = cur.fetchall()
   country = {}
   for row in result:
      country[row[0]] = row[0]
    
   selected_country = st.selectbox("View details based on Price", list(country.keys()))
   cur.execute(f"SELECT name, Country, weekly_price , monthly_price , Total_price FROM Data WHERE Country='{selected_country}' ORDER BY Total_price DESC")
   table_data = cur.fetchall()

    # Displaying the data in a Streamlit table
   st.table(table_data)


def Analaysis1(Data):
    # Create GeoDataFrame from DataFrame
    df = Data
    geo_df = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude))

    # Plot the GeoDataFrame
    fig = px.scatter_mapbox(geo_df,
                            lat="latitude",
                            lon="longitude",
                            hover_name = "name",
                            mapbox_style="carto-positron",
                            zoom=2)
    st.plotly_chart(fig)
    
def Analaysis2(Data):
    df = Data
    No_of_property = df["Country"].value_counts()
    import pandas as pd

    # Initialize an empty list to store dictionaries
    data = []

    # Iterate over the items of the No_of_property series
    for country, count in No_of_property.items():
        # Append a dictionary containing the country and its count to the list
        data.append({'Country': country, 'Count': count})

    # Create a DataFrame from the list of dictionaries
    no_of_property_details = pd.DataFrame(data)

    import plotly.express as px
    fig = px.scatter(no_of_property_details, x="Country", y="Count")
    st.plotly_chart(fig)

def Analaysis3(Data):    
   df = Data
   selected_columns = df[["Country", "Total_price", "property_type"]]
   grouped_data = selected_columns.groupby(["Country", "property_type"]).agg({
        "Total_price": ["sum"]})
   grouped_data.reset_index(inplace=True)
   grouped_data.columns = ["Country", "property_type", "Total_price_sum"]
   gf = grouped_data

   fig = px.bar(gf, x="Country", y="Total_price_sum", color="property_type",
             pattern_shape="property_type", pattern_shape_sequence=[".", "x", "+"])
   st.plotly_chart(fig)

def Analaysis4(Data): 
    df = Data   
    # Assuming select_Column is defined correctly
    select_Column = df[["room_type", "Total_price"]]

    # Group by "room_type" and calculate sum of "Total_price"
    grouped_data1 = select_Column.groupby(["room_type"]).agg({
        "Total_price": ["sum"]
    })

    # Reset index to make the grouped columns regular columns
    grouped_data1.reset_index(inplace=True)

    # Rename columns for better readability
    grouped_data1.columns = ["room_type","Total_price_sum"]

    ag = grouped_data1   
    fig = px.pie(ag, values='Total_price_sum', names='room_type')
    st.plotly_chart(fig)

def Analaysis5(Data):
    df = Data
   # Assuming select_Column is defined correctly
    select_Columns = df[["Country", "property_type","Review"]]

    # Group by "room_type" and calculate sum of "Total_price"
    grouped_data1 = select_Columns.groupby(["Country", "property_type"]).agg({
        "Review": ["sum"]
    })

    # Reset index to make the grouped columns regular columns
    grouped_data1.reset_index(inplace=True)

    # Rename columns for better readability
    grouped_data1.columns = ["Country", "property_type","Review_sum"]

    ab = grouped_data1

    fig = px.bar(ab, x="Country", y="Review_sum", color='property_type')
    st.plotly_chart(fig)


# streamlit part

import streamlit as st
st.title('Airbnb Analysis')
col1, col2 = st.columns(2)
with col1:
   if st.checkbox(":open_book: Insights"):
        st.write('''
                    Data Evaluation Functions:
                        These functions are responsible for retrieving data from the database based on certain criteria (e.g., country, property type) 
                        and displaying it in Streamlit tables.
                        Each function retrieves distinct values for certain attributes (e.g., country, property type) from the database and provides a 
                        selection mechanism (e.g., select boxes) for users to choose specific criteria.
                        SQL queries are used to fetch data from the database based on the selected criteria.
                        The retrieved data is then displayed in Streamlit tables using st.table().
                
                    Analysis Functions:
                        These functions are responsible for visualizing the data retrieved from the database using different types of plots.
                        1. plots a scatter map using the latitude and longitude coordinates of the properties.
                        2. plots a scatter plot showing the number of properties in each country.
                        3. plots a bar chart showing the total price of properties grouped by country and property type.
                        4. plots a pie chart showing the distribution of property types based on the total price.
                        5. plots a bar chart showing the total number of reviews grouped by country and property type.
                    
                    Insights:

                        These functions provide a comprehensive analysis of the Airbnb database, allowing users to explore various aspects such as property distribution, pricing trends, and review statistics.
                        Users can interactively select criteria (e.g., country, property type) to customize the displayed data.
                        Visualization techniques such as maps, scatter plots, bar charts, and pie charts are used to present the data effectively and enable better understanding of patterns and trends.
                        Overall, these functions serve as valuable tools for exploring and analyzing the Airbnb database, providing insights that can inform decision-making and strategic planning.''')
            
with col2:
   st.header("Data Visualization")
   tab1, tab2 = st.tabs(["Visualization", "Analysis"])

with tab1:
   st.header("Visualization")
   st.balloons()
   if st.button("scatter map using the latitude and longitude"):
        Analaysis1(Data)
   if st.button("scatter plot showing the number of properties"):
        Analaysis2(Data)
   if st.button("bar chart showing the total price of properties"):
        Analaysis3(Data)
   if st.button("pie chart showing the distribution of property"):
        Analaysis4(Data)
   if st.button("bar chart showing the total number of reviews"):
        Analaysis5(Data)   

with tab2:
   st.header("Analysis")
     
   if st.button("Uploaded Data"):
      uploading_Data(Data)
      st.success("successfully uploaded")
   tab1, tab2,tab3= st.tabs(["TAB 1","TAB 2","TAB 3"])  
   with tab1:
        data_Evaluation1()
   with tab2:
        data_Evaluation2()
   with tab3:
        data_Evaluation3()
  

 

