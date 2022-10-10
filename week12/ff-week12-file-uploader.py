import streamlit as st
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# Connect to Snowflake
ctx = snowflake.connector.connect(**st.secrets["snowflake"])
cs = ctx.cursor()

@st.cache # This keeps a cache in place so the query isn't constantly re-run.
def load_schemas():
    q_schemas = """select schema_name
    from frosty_friday.information_schema.schemata
    where schema_name like 'WORLD_BANK%';"""
    cur = ctx.cursor().execute(q_schemas)
    schemas_df = pd.DataFrame.from_records(iter(cur), columns=[x[0] for x in cur.description])
    return schemas_df

def load_tables(p_schema):
    q_tables = """select table_name
from frosty_friday.information_schema.tables
where table_schema = '""" + p_schema + """';"""
    cur = ctx.cursor().execute(q_tables)
    tables_df = pd.DataFrame.from_records(iter(cur), columns=[x[0] for x in cur.description])
    return tables_df
    
def app_creation():
    st.title('Manual CSV to Snowflake Table Uploader')
    st.sidebar.image("logo.png")
    st.sidebar.header("Instructions:")
    st.sidebar.write("•	Select the schema from the available.")
    st.sidebar.write("•	Then select the table which will automatically update to reflect your schema choice.")
    st.sidebar.write("•	Check that the table corresponds to that which you want to ingest into.")
    st.sidebar.write("•	Select the file you want to ingest.")
    st.sidebar.write("•	You should see an upload success message detailing how many rows were ingested.")

    # Get schemas
    schemas_df = load_schemas()
    upload_schema = st.radio(
        "Select schema:",
        schemas_df)
    # For debugging: st.write('You selected ' + upload_schema)

    # Get tables of the selected schema
    tables_df = load_tables(upload_schema)
    upload_table = st.radio(
        "Select table to upload to:",
        tables_df)
    # For debugging: st.write('You selected ' + upload_table)

    # Get the file to be uploaded
    uploaded_file = st.file_uploader("Choose a file")
    if uploaded_file is not None:
        # Read the file into dataframe
        file_df = pd.read_csv(uploaded_file)
        # For debugging: st.write(file_df)
        
        # Write the data from the DataFrame to the table 
        upload_database = 'FROSTY_FRIDAY'
        success, nchunks, nrows, _ = write_pandas( conn=ctx
                                                 , df=file_df
                                                 , table_name=upload_table
                                                 , database=upload_database
                                                 , schema=upload_schema
                                                 , quote_identifiers=False)
        if success:
            st.write('Your upload was a success. You uploaded ' + str(nrows) + ' rows.')

# Invoke the main function
app_creation() 
