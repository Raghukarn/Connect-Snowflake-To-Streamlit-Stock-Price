# Modified from Johannes Rieke's example code

import streamlit as st
from snowflake.snowpark import Session
import pandas as pd

st.title('❄️ Connect Streamlit to a Snowflake database')

# Establish Snowflake session
@st.cache_resource
def create_session():
    return Session.builder.configs(st.secrets.snowflake).create()

session = create_session()
st.success("Connected to Snowflake!")

# Load data table
@st.cache_data
def load_data(table_name):
    ## Read in data table
    st.write(f"Here's some example data from `{table_name}`:")
    table = session.table(table_name)
    
    ## Do some computation on it
    table = table.limit(100)
    
    ## Collect the results. This will run the query and download the data
    table = table.collect()
    return table

# Select and display data table
table_name = "STOCK_TRACKING_SP_500_STOCK_PRICES_BY_DAY.STOCK.SP500_STOCK_METRICS" #"PETS.PUBLIC.MYTABLE"

## Display data table
with st.expander("See Table"):
    df = load_data(table_name)
    df_pandas = pd.DataFrame(df)
    st.dataframe(df)

# st.write("You selected:", option)

filtered_df = df_pandas['CLOSE']


filtered_df = pd.to_numeric(filtered_df, errors='coerce') #CONVERT THE COLUMN TO NUMERIC TYPE

st.line_chart(filtered_df, x_label = "Companies Closing Value")



option = st.selectbox(
    "Please select a company to see closing value", df_pandas['COMPANY_NAME'])

filtered_df_bar = df_pandas[df_pandas['COMPANY_NAME'] == option]



filtered_df_bar = filtered_df_bar['CLOSE']
filtered_df_bar = pd.to_numeric(filtered_df_bar, errors='coerce') #CONVERT THE COLUMN TO NUMERIC TYPE
st.bar_chart(filtered_df_bar, x_label = option, color = "#FFA500")

## Writing out data
# for row in df:
#     st.write(f"{row[0]} has a :{row[1]}:")