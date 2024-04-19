# Airbnd
# Technologies
import pymongo
import pandas as pd
import plotly.express as px
import geopandas as gpd
import pymysql
# Data Evaluation Functions:
    `  These functions are responsible for retrieving data from the database based on certain criteria (e.g., country, property type) and displaying it in Streamlit tables.
        Each function retrieves distinct values for certain attributes (e.g., country, property type) from the database and provides a selection mechanism (e.g., select boxes) for users to choose specific criteria.
        SQL queries are used to fetch data from the database based on the selected criteria.The retrieved data is then displayed in Streamlit tables using st.table().
                
# Analysis Functions:
        These functions are responsible for visualizing the data retrieved from the database using different types of plots.
                        1. plots a scatter map using the latitude and longitude coordinates of the properties.
                        2. plots a scatter plot showing the number of properties in each country.
                        3. plots a bar chart showing the total price of properties grouped by country and property type.
                        4. plots a pie chart showing the distribution of property types based on the total price.
                        5. plots a bar chart showing the total number of reviews grouped by country and property type.
                    
