import streamlit as st
from datetime import datetime
import time
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
import os

import great_expectations as gx

context = gx.get_context()

def get_snowflake_connection():
    conn = snowflake.connector.connect(
        user=st.secrets["SNOWFLAKE_USER"],
        password=st.secrets["SNOWFLAKE_PASSWORD"],
        account=st.secrets["SNOWFLAKE_ACCOUNT"],
        warehouse=st.secrets["SNOWFLAKE_WAREHOUSE"],
        database=st.secrets["SNOWFLAKE_DATABASE"],
        schema=st.secrets["SNOWFLAKE_SCHEMA"],
    )
    return conn

# Function to run a query and return results
@st.cache_data
def run_query(query):
    # Open a new connection for each query
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
        columns = [col[0] for col in cursor.description]  # Extract column names
        df = pd.DataFrame(data, columns=columns)
    finally:
        cursor.close()
        conn.close()  # Always close the connection
    return df

def validate_data(df):
    data_source = context.data_sources.add_pandas(name='employee')
    data_asset = data_source.add_dataframe_asset(name='employee_asset')
    batch_definition_name = 'employee_batch'
    # Add the Batch Definition
    batch_definition = data_asset.add_batch_definition_whole_dataframe(batch_definition_name)
    # Define the Batch Parameters
    batch_parameters = {"dataframe": df}
    # Retrieve the Batch
    batch = batch_definition.get_batch(batch_parameters=batch_parameters)
    # Create an Expectation Suite
    expectation_suite_name = "employee_suite"
    suite = gx.ExpectationSuite(name=expectation_suite_name)
    # Add Expectations
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="EMPLOYEE_ID")
    )
    # Add the Expectation Suite to the Context
    context.suites.add(suite)
    # Validate the Data Against the Suite
    validation_results = batch.validate(suite)
    return validation_results

# Function to check if an alert was already sent
def is_alert_sent(conn, message):
    query = "SELECT 1 FROM sent_alerts WHERE alert_message = %s"
    cursor = conn.cursor()
    cursor.execute(query, (message,))
    result = cursor.fetchone()
    return result is not None

# Function to log an alert in the Snowflake table
def log_alert(conn, message):
    try:
        conn.cursor().execute("BEGIN")
        conn.cursor().execute("INSERT INTO sent_alerts (alert_message) VALUES (%s)", (message,))
        conn.cursor().execute("COMMIT")
    except snowflake.connector.errors.IntegrityError:
        conn.cursor().execute("ROLLBACK")  # Roll back the transaction if the alert already exists

# Function to send an alert
def send_alert(message):
    conn = get_snowflake_connection()
    if not is_alert_sent(conn, message):
        log_alert(conn, message)
        st.warning(f"Alert: {message}")  # Display in Streamlit
        # Add email or notification logic here if needed
    else:
        st.info("Alert already sent for this issue.")
    conn.close()

query = 'SELECT * FROM "EMPLOYEE"."PUBLIC"."EMPLOYEE" LIMIT 10;'

st.title('Data Anomaly Detection')

# Initialize auto-refresh state
if 'auto_refresh' not in st.session_state:
    st.session_state['auto_refresh'] = False

# Button to toggle auto-refresh
if st.button('Toggle Auto Refresh'):
    st.session_state['auto_refresh'] = not st.session_state['auto_refresh']

# Display current status
st.write(f"Auto-refresh is {'ON' if st.session_state['auto_refresh'] else 'OFF'}")

# Function to display the current time
def display_time():
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    st.write(f"Current time: {current_time}")

# Main logic
if st.session_state['auto_refresh']:
    display_time()
    # Check if the maximum value in the specified column exceeds 6
    try:
        # Fetch cached data
        results_df = run_query(query)
        st.success("Query executed successfully!")
        st.write(f"Results ({len(results_df)} rows):")
        st.dataframe(results_df)  # Display results as a DataFrame

        validation_results = validate_data(results_df)

        st.write(validation_results['success'])

        # Perform anomaly detection
        if not results_df.empty:
            if results_df.select_dtypes(include="number").max().max() > 6:
                send_alert("One or more numeric columns have values greater than 6.")
            else:
                st.write("No anomalies detected.")
    except Exception as e:
        st.error(f"Error running query: {e}")
    # Wait for the specified interval
    time.sleep(10)  # Refresh every 10 seconds
    # Rerun the script
    st.rerun()
else:
    display_time()

# use google cloud scheduler to ping main app
# create email alert if app goes down using Google Cloud Monitoring
