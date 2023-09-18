# %%
import os
import json
import pandas as pd

data_directory = r'C:\Users\sanat\OneDrive\Documents\GitHub\How_To_Combine_Multiple_Json_store_in_mqsql\pulse\data\map\transaction\hover\country\india\state'
def process_data(directory):
    data = {}  # Create an empty dictionary to organize the data
    
    for entry in os.listdir(directory):
        entry_path = os.path.join(directory, entry)
        
        if os.path.isdir(entry_path):  # If it's a directory, process it recursively
            data[entry] = process_data(entry_path)
        elif entry.endswith('.json'):  # If it's a JSON file, read and store the data
            with open(entry_path, 'r') as json_file:
                data[entry] = json.load(json_file)
    
    return data

# %%
organized_data = process_data(data_directory)

json.dumps(organized_data, indent=2)

# %%
dfs = []

# Iterate through the data dictionary
for state, years in organized_data.items():
    for year, quarters in years.items():
        for quarter, data in quarters.items():
            # Check if 'hoverDataList' key exists in the data
            if "hoverDataList" in data["data"]:
                hover_data_list = data["data"]["hoverDataList"]

                # Convert the hoverDataList into a DataFrame
                df = pd.DataFrame(hover_data_list)

                # Add columns for state, year, and quarter
                df["State"] = state
                df["Year"] = year
                df["Quarter"] = quarter

                # Append the DataFrame to the list of DataFrames
                dfs.append(df)
            else:
                print(f"Data for {state}, {year}, Q{quarter}.json does not contain 'hoverDataList' key.")

print("Length of dfs:", len(dfs))

if dfs:
    final_df = pd.concat(dfs, ignore_index=True)
    # Display the resulting DataFrame
    print("Data frame concatenated successfully")
else:
    print("No DataFrames to concatenate.")
        


# %%
final_df['Metric_Type'] = final_df['metric'].apply(lambda x: x[0]['type'])
final_df['Metric_Count'] = final_df['metric'].apply(lambda x: x[0]['count'])
final_df['Metric_Amount'] = final_df['metric'].apply(lambda x: x[0]['amount'])

final_df.drop(columns=['metric'], inplace=True)


final_df.isnull()

# %%
db_url = "mysql://root:Sanath997#@localhost/phonepe_data_visualization"

# %%
from sqlalchemy import create_engine, text

# %%
engine = create_engine(db_url)

# %%
from sqlalchemy.exc import OperationalError

db_url = 'mysql://root:Sanath997#@localhost/phonepe_data_visualization'

try:
    engine = create_engine(db_url)

    with engine.connect():
        print("MySQL URL is valid and the database is reachable.")
except OperationalError as e:
    print(f"Error: {e}")
    print("MySQL URL is invalid or the database is not reachable.")

# %%
connection = engine.connect()

if engine.dialect.has_table(connection, "Map_Transaction_Data_Table"):
    # Drop the table if it exists
    drop_table_statement = text("DROP TABLE Map_Transaction_Data_Table")
    connection.execute(drop_table_statement)

connection.close()

final_df.to_sql("Map_Transaction_Data_Table", engine, if_exists="replace", index=False)

# %%
import mysql.connector

# Define your MySQL database connection parameters
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "Sanath997#",
    "database": "phonepe_data_visualization",
}

data_directory_2 = r'C:\Users\sanat\OneDrive\Documents\GitHub\How_To_Combine_Multiple_Json_store_in_mqsql\pulse\data\aggregated\transaction\country\india\state'

# %%
def process_data_2(directory):
    data = {}  # Create an empty dictionary to organize the data
    
    for entry in os.listdir(directory):
        entry_path = os.path.join(directory, entry)
        
        if os.path.isdir(entry_path):  # If it's a directory, process it recursively
            data[entry] = process_data_2 (entry_path)
        elif entry.endswith('.json'):  # If it's a JSON file, read and store the data
            with open(entry_path, 'r') as json_file:
                data[entry] = json.load(json_file)
    
    return data

# %%
organized_data_2 = process_data_2(data_directory_2)
json.dumps(organized_data_2, indent=2)

# %%
dfs_2 = []

# Iterate through the data dictionary
for state, years in organized_data_2.items():
    for year, quarters in years.items():
        for quarter, data in quarters.items():
            # Check if 'transactionData' key exists in the data
            if "transactionData" in data["data"]:
                transaction_Data_list = data["data"]["transactionData"]

                # Convert the transaction_Data into a DataFrame
                df_2= pd.DataFrame(transaction_Data_list)

                # Add columns for state, year, and quarter
                df_2["State"] = state
                df_2["Year"] = year
                df_2["Quarter"] = quarter

                # Append the DataFrame to the list of DataFrames
                dfs_2.append(df_2)
            else:
                print(f"Data for {state}, {year}, Q{quarter}.json does not contain 'transactionData' key.")

print("Length of dfs_2:", len(dfs_2))

if dfs_2:
    final_df_2 = pd.concat(dfs_2, ignore_index=True)
    # Display the resulting DataFrame
    print("Data frame concatenated successfully")
    print(final_df_2)
else:
    print("No DataFrames to concatenate.")
        
excel_file_path = "output_data_2.xlsx"

final_df_2.to_excel(excel_file_path, index=False)

print(f"Data has been exported to {excel_file_path}")


final_df_2['paymentInstruments_Type'] = final_df_2['paymentInstruments'].apply(lambda x: x[0]['type'])
final_df_2['paymentInstruments_Count'] = final_df_2['paymentInstruments'].apply(lambda x: x[0]['count'])
final_df_2['paymentInstruments_Amount'] = final_df_2['paymentInstruments'].apply(lambda x: x[0]['amount'])

final_df_2.drop(columns=['paymentInstruments'], inplace=True)

connection = engine.connect()

if engine.dialect.has_table(connection, "Map_Transaction_Data_Table_1"):
    drop_table_statement = text("DROP TABLE Map_Transaction_Data_Table_1")
    connection.execute(drop_table_statement)

connection.close()

final_df_2.to_sql("Map_Transaction_Data_Table_1", engine, if_exists="replace", index=False)



import streamlit as st
st.image('./phonepe.jpg', width=150)
st.title('Phonepe Pulse Dashboard')
st.text('This is a web app to explore phonepe Data to get useful insights')

main_tab = st.radio("Select Main Tab:", ["MAP", "DATA INSIGHT"])

if main_tab == "MAP":
    st.subheader("Select Year and quarter to view the transaction data:")
    

    year_select = st.selectbox("Select Year:", ["2018","2019","2020","2021", "2022", "2023"])
    quarter_select = st.selectbox("Select Quarter:", ["1.json", "2.json", "3.json", "4.json"])
    st.write(f"You selected Year {year_select} and Quarter {quarter_select}.")
    # %%
    year = year_select
    quarter = quarter_select

    query = """
        SELECT *
        FROM phonepe_data_visualization.map_transaction_data_table
        WHERE Year = %s
        AND Quarter = %s
    """

    try:
        conn = mysql.connector.connect(**db_config)
        
        # Execute the SQL query with the specified parameters
        df = pd.read_sql_query(query, conn, params=(year, quarter))
        
        # Close the database connection
        conn.close()
        
        # Check if the DataFrame is empty (no matching records)
        if df.empty:
            print("No data found for the specified criteria.")
        else:
            # You now have your DataFrame containing the filtered data
            print(df)

    except mysql.connector.Error as err:
        print(f"Error: {err}")


    # %%
    combined_df = df.groupby('State').agg({
        'Year': 'first',  # Take the first year in the group
        'Quarter': 'first',  # Take the first quarter in the group
        'Metric_Type': 'first',  # Take the first Metric_Type in the group
        'Metric_Count': 'sum',
        'Metric_Amount': 'sum'
    }).reset_index()

    combined_df = combined_df.rename(columns={'Metric_Count': 'NO_OF_TRANSACTION', 'Metric_Amount': 'TRANSACTION_AMOUNT'})

    # %%
    import json
    india_states = json.load(open("states_india.geojson", "r"))
    import pymysql
    import plotly.express as px

    import plotly.offline as pyo
    import plotly.io as pio
    pio.renderers.default = 'firefox'


    #streamlit
    import streamlit as st

    state_id_map={}
    for feature in india_states['features']:
        feature['id'] = feature['properties']['state_code']
        state_id_map[feature['properties']['st_nm']]=feature['id']

    state_id_map = {k.lower(): v for k, v in state_id_map.items()}

    # %%
    state_id_map['dadra-&-nagar-haveli-&-daman-&-diu'] = state_id_map.pop('dadara & nagar havelli')
    state_id_map['andhra-pradesh'] = state_id_map.pop('andhra pradesh')
    state_id_map['andaman-&-nicobar-islands'] = state_id_map.pop('andaman & nicobar island')
    state_id_map['himachal-pradesh'] = state_id_map.pop('himachal pradesh')
    state_id_map['arunachal-pradesh'] = state_id_map.pop('arunanchal pradesh')
    state_id_map['jammu-&-kashmir'] = state_id_map.pop('jammu & kashmir')
    state_id_map['madhya-pradesh'] = state_id_map.pop('madhya pradesh')
    state_id_map['tamil-nadu'] = state_id_map.pop('tamil nadu')  
    state_id_map['uttar-pradesh'] = state_id_map.pop('uttar pradesh')	
    state_id_map['west-bengal'] = state_id_map.pop('west bengal')
    state_id_map['ladakh'] = state_id_map.pop('daman & diu')

    state_id_map['delhi'] = state_id_map.pop('nct of delhi')
    combined_df['id']=combined_df['State'].apply(lambda x: state_id_map[x])

    # %%
    fig = px.choropleth(combined_df, locations='id', geojson=india_states, color='NO_OF_TRANSACTION',hover_name='State',hover_data='TRANSACTION_AMOUNT')
    fig.update_geos(fitbounds='locations',visible=False)
    st.write("Hover over states to view statewise transactions")
    st.plotly_chart(fig)

elif main_tab == "DATA INSIGHT":
    st.subheader("Select Year , Quater & Question :")
    sub_tab_options = ["Year and Quarter"]
    sub_tab_selected = st.radio("", sub_tab_options)
    
    if sub_tab_selected == "Year and Quarter":
        year_select = st.selectbox("Select Year:", ["2018","2019","2020","2021", "2022", "2023"])
        quarter_select = st.selectbox("Select Quarter:", ["1.json", "2.json", "3.json", "4.json"])
        st.write(f"You selected Year {year_select} and Quarter {quarter_select}.")
        options = ["1) What is the average payment amount across individual states?",
            "2) Which is the max payment amount across individual states?",
            "3) At above mentioned year which state has done lowest number of transactions?",
            "4)Which state has the highest total payment amount across all years in the dataset?",
            "5)Position of tamil nadu with respect to other states in payment amount?"]

        Question_select = st.selectbox("Select a question:", options)

    year = year_select
    quarter = quarter_select

    query = """
        SELECT *
        FROM phonepe_data_visualization.map_transaction_data_table_1
        WHERE Year = %s
        AND Quarter = %s
    """
    query_1="""
        SELECT *
        FROM phonepe_data_visualization.map_transaction_data_table_1 """

    try:
        conn = mysql.connector.connect(**db_config)
        df = pd.read_sql_query(query, conn, params=(year, quarter))
        Complete_data_df=pd.read_sql_query(query_1,conn)
        conn.close()
        if df.empty:
            print("No data found for the specified criteria.")
        else:
            if Question_select == "1) What is the average payment amount across individual states?":
                average_payments_by_state = df.groupby('State')['paymentInstruments_Amount'].mean().reset_index()
                st.write(average_payments_by_state) 
            if Question_select == "2) Which is the max payment amount across individual states?":
                average_payments_by_state = df.groupby('State')['paymentInstruments_Amount'].max().reset_index()
                st.write(average_payments_by_state)    
            if Question_select == "3) At above mentioned year which state has done lowest number of transactions?":
                df_2020 = df[df['Year'] == year]
                state_transaction_counts = df_2020['State'].value_counts()
                state_with_lowest_count = state_transaction_counts.idxmin()
                lowest_transaction_count = state_transaction_counts.min()
                st.write(f"In the year {year}, the state with the lowest number of transactions is {state_with_lowest_count} with {lowest_transaction_count} transactions.")   
            if Question_select == "4)Which state has the highest total payment amount across all years in the dataset?":
                state_total_amounts = Complete_data_df.groupby('State')['paymentInstruments_Amount'].sum()
                state_with_highest_amount = state_total_amounts.idxmax()
                highest_total_amount = state_total_amounts.max()
                st.write(f"The state with the highest total payment amount across all years is {state_with_highest_amount} with an amount of {highest_total_amount}.")
            if Question_select == "5)Position of tamil nadu with respect to other states in payment amount?":   
                target_state = 'tamil-nadu'
                state_amounts = Complete_data_df.groupby('State')['paymentInstruments_Amount'].sum().reset_index()
                state_amounts_sorted = state_amounts.sort_values(by='paymentInstruments_Amount', ascending=False)
                target_state_position = state_amounts_sorted[state_amounts_sorted['State'] == target_state].index[0] + 1
                st.write(f"The state '{target_state}' is at position {target_state_position} in terms of total amount sum")
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    