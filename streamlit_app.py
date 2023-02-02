import streamlit as st
import st_connection
import st_connection.snowflake
from snowflake.snowpark.functions import avg as avg_, sum as sum_, col, lit, datediff, dateadd, date_trunc
from snowflake.snowpark.types import StringType, IntegerType, DateType, FloatType
import datetime
import snowflake.snowpark as sp
try:
    st.set_page_config(
        page_title="Monitoring Snowflake with Streamlit",
        page_icon="❄️",
        layout="wide",
        initial_sidebar_state="expanded",
    )
except:
    pass


st.title("❄️ Monitoring Snowflake with Streamlit ❄️")
st.caption("Based on the https://medium.com/snowflake/monitoring-snowflake-with-snowsight-e9990a2898f1")

disclaimer = """Disclaimer: Use at your own discretion. This site does not store your Snowflake credentials and your credentials are only used as a passthrough to connect to your Snowflake account."""

# Things above here will be run before (and after) you log in.
if 'ST_SNOW_SESS' not in st.session_state:
    with st.expander("Login Help", False):
        st.markdown(
        """
***account***: this should be the portion in between "https://" and ".snowflakecomputing.com" - for example https://<account>.snowflakecomputing.com
            
***database***: This should remain ***SNOWFLAKE*** unless you have copied your `query_history` and `warehouse_metering_history` to another location

***schema***: This should remain ***ACCOUNT_USAGE*** unless you have copied your `query_history` and `warehouse_metering_history` to another location

***role***: This should remain ***ACCOUNTADMIN*** unless you have delegated access to `query_history` and `warehouse_metering_history`
""")
    st.caption(disclaimer)

session = st.connection.snowflake.login({
    'account': 'XXX',
    'user': '',
    'password': None,
    'warehouse': 'ADHOC_WH',
    'database': 'SNOWFLAKE',
    'schema': 'ACCOUNT_USAGE',
    'role': 'ACCOUNTADMIN',
}, {
    'ttl': 120
}, 'Snowflake Login')

session.sql_simplifier_enabled = True

# Set up the layout
date_row = st.columns(2)
top_row = st.columns(3)
middle_row = st.columns(2)
bottom_row = st.container()
st.caption(disclaimer + " The metrics shown on this page should be used as information only. Please work with your Snowflake Account team if you have any questions.")
st.caption(f"Streamlit Version: {st.__version__}")
st.caption(f"Snowpark Version: {sp.__version__}")

try:
    with date_row[1]:
        start_date, end_date = st.date_input(
                        'start date  - end date :',
                        value=[datetime.date.today() + datetime.timedelta(days=-31), datetime.date.today()],
                        max_value=datetime.date.today()
                    )
        if start_date < end_date:
            pass
        else:
            st.error('Error: End date must fall after start date.')
except:
    st.error("Please select an end date")
    st.stop()

with top_row[0]:
    # Let's start with SQL
    try:    
        query = f"""SELECT SUM(CREDITS_USED)::FLOAT AS CREDITS FROM METERING_HISTORY WHERE START_TIME BETWEEN '{start_date}' AND '{end_date}'"""
        df = session.sql(query).to_pandas()
        st.metric('Credits Used','{:,.2f}'.format( df['CREDITS'][0]))
    except Exception as e:
        st.warning(e)

with top_row[1]:
    # Now let's use the Snowpark way
    try:
        query_history = session.table('QUERY_HISTORY')
        query_history = query_history.filter(col("START_TIME").between(start_date, end_date))
        query_history = query_history.count()
        st.metric('Total # Jobs Executed', '{:,.0f}'.format(query_history))
    except Exception as e:
        st.warning(e)

with top_row[2]:
    try:
        storage_usage = session.table('STORAGE_USAGE')
        storage_usage = storage_usage.select(avg_((col('STORAGE_BYTES')+col('STAGE_BYTES')+col('FAILSAFE_BYTES'))/(1024*1024*1024*1024)).alias('BILLABLE_TB'))
        storage_usage = storage_usage.filter(col("USAGE_DATE").between(start_date, end_date))
        storage_usage = storage_usage.to_pandas()
        st.metric('Current Storage (TB)', '{:,.3f}'.format(storage_usage['BILLABLE_TB'][0]))
    except Exception as e:
        st.warning(e)

with middle_row[0]:
    try:
        st.header('Credit Usage by Warehouse')
        warehouse_metering_history = session.table('WAREHOUSE_METERING_HISTORY').select(col('WAREHOUSE_NAME'),col('CREDITS_USED') ).filter(col("START_TIME").between(start_date, end_date)).group_by("WAREHOUSE_NAME").agg(sum_('CREDITS_USED').cast(FloatType()).alias('TOTAL_CREDITS_USED')).sort('TOTAL_CREDITS_USED', ascending=False).to_pandas()    
        st.vega_lite_chart(warehouse_metering_history,{
            'mark': 'bar',
            'encoding': {
                'x': { "aggregate": "sum", 'field': 'TOTAL_CREDITS_USED'},
                'y': {'field': 'WAREHOUSE_NAME', "sort": "-x"}
            },
        }, use_container_width=True)
    except Exception as e:
        st.warning(e)


with bottom_row:
    try:
        st.header('Credit Usage Overtime')
        warehouse_metering_history = session.table('WAREHOUSE_METERING_HISTORY')
        warehouse_metering_history = warehouse_metering_history.select(col('WAREHOUSE_NAME'),col('CREDITS_USED'), col('START_TIME').cast(DateType()).alias('USAGE_DATE') )
        warehouse_metering_history = warehouse_metering_history.filter(col('USAGE_DATE').between(start_date, end_date))
        warehouse_metering_history = warehouse_metering_history.group_by(['USAGE_DATE','WAREHOUSE_NAME']).agg(sum_('CREDITS_USED').cast(FloatType()).alias('TOTAL_CREDITS_USED'))
        warehouse_metering_history = warehouse_metering_history.sort('TOTAL_CREDITS_USED', ascending=False)
        warehouse_metering_history = warehouse_metering_history.to_pandas()   
        st.vega_lite_chart(warehouse_metering_history,{
            'mark': 'bar',
            'encoding': {
                'x': { "timeUnit": "utcyearmonthdate", "field": "USAGE_DATE", "type": "temporal"},
                'y': {"aggregate": "sum", 'field': 'TOTAL_CREDITS_USED'},
                "color": {      "field": "WAREHOUSE_NAME"    }
            },
        }, use_container_width=True)
    except Exception as e:
        st.warning(e)

    # Let's add another chart to this section
    try:
        st.header('Warehouse Usage Greater than 7 Day Average')
        seven_day_average = session.sql(f"""
        SELECT
        WAREHOUSE_NAME,
        DATE(START_TIME) AS DATE,
        SUM(CREDITS_USED)::FLOAT AS CREDITS_USED,
        AVG(SUM(CREDITS_USED)) OVER (
            PARTITION BY WAREHOUSE_NAME
            ORDER BY
                DATE ROWS 7 PRECEDING
        )::FLOAT AS CREDITS_USED_7_DAY_AVG,
        ((SUM(CREDITS_USED)/CREDITS_USED_7_DAY_AVG)-1)::float AS VARIANCE_TO_7_DAY_AVERAGE
    FROM
        WAREHOUSE_METERING_HISTORY
    WHERE
        START_TIME BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY
        DATE,
        WAREHOUSE_NAME
    ORDER BY
        DATE DESC    
        """).to_pandas()    
        st.vega_lite_chart(seven_day_average,{
            'mark': 'bar',
            'encoding': {
                'x': { "timeUnit": "utcyearmonthdate", "field": "DATE", "type": "temporal"},
                'y': {
                    "type": "quantitative", 
                    'field': 'VARIANCE_TO_7_DAY_AVERAGE', 
                    "axis": {
                        "format": ".0%",
                    }
                },
                "color": {      "field": "WAREHOUSE_NAME"    }
            },
        }, use_container_width=True)
    except Exception as e:
            st.warning(e)

with middle_row[1]:
    try:
        st.header('Execution Time by Query Type (Avg Seconds)')
        execution_time = session.table('QUERY_HISTORY')
        execution_time = execution_time.select(col('QUERY_TYPE'),col('WAREHOUSE_SIZE'),col('EXECUTION_TIME') )
        execution_time = execution_time.filter(col("START_TIME").between(start_date, end_date))
        execution_time = execution_time.group_by(["WAREHOUSE_SIZE", 'QUERY_TYPE']).agg((avg_('EXECUTION_TIME')/1000).cast(FloatType()).alias('AVERAGE_EXECUTION_TIME'))
        execution_time = execution_time.sort('AVERAGE_EXECUTION_TIME', ascending=False).to_pandas()    
        st.vega_lite_chart(execution_time,{
            'mark': 'bar',
            'encoding': {
                'x': { "aggregate": "sum", 'field': 'AVERAGE_EXECUTION_TIME'},
                'y': {'field': 'QUERY_TYPE', "sort": "-x"}
            },
        }, use_container_width=True)
    except Exception as e:
        st.warning(e)


with bottom_row:
    try:
        st.header('Top 25 Longest Queries')
        execution_time = session.table('QUERY_HISTORY')
        execution_time = execution_time.select(col('QUERY_ID'),col('QUERY_TEXT'),(col('EXECUTION_TIME')/1000).cast(FloatType()).alias('EXEC_TIME') )
        execution_time = execution_time.filter(col("START_TIME").between(start_date, end_date)).filter(col('EXECUTION_STATUS')=='SUCCESS')
        # execution_time = execution_time.group_by(["WAREHOUSE_SIZE", 'QUERY_TYPE']).agg((avg_('EXECUTION_TIME')/1000).cast(FloatType()).alias('AVERAGE_EXECUTION_TIME'))
        execution_time = execution_time.sort('EXEC_TIME', ascending=False).limit(25).to_pandas()        
        cols = st.columns(2)
        with cols[0]:
            st.vega_lite_chart(execution_time,{
                'mark': 'bar',
                'encoding': {
                    'x': {'field': 'EXEC_TIME',"type": "quantitative", },
                    'y': {'field': 'QUERY_TEXT', "sort": "-x"}
                },
            }, use_container_width=True)
        with cols[1]:
            st.write(execution_time)
    except Exception as e:
        st.warning(e)

with bottom_row:
    try:
        st.header('Total Execution Time by Repeated Queries')
        execution_time = session.table('QUERY_HISTORY')
        execution_time = execution_time.select(col('QUERY_TEXT'),col('EXECUTION_TIME'))
        execution_time = execution_time.filter(col("START_TIME").between(start_date, end_date)).filter(col('EXECUTION_STATUS')=='SUCCESS')
        execution_time = execution_time.group_by("QUERY_TEXT").agg((sum_('EXECUTION_TIME')/60000).cast(FloatType()).alias('EXEC_TIME'))
        execution_time = execution_time.sort('EXEC_TIME', ascending=False).limit(25).to_pandas()        
        cols = st.columns(2)
        with cols[0]:
            st.vega_lite_chart(execution_time,{
                'mark': 'bar',
                'encoding': {
                    'x': {'field': 'EXEC_TIME',"type": "quantitative", },
                    'y': {'field': 'QUERY_TEXT', "sort": "-x"}
                },
            }, use_container_width=True)
        with cols[1]:
            st.write(execution_time)
    except Exception as e:
        st.warning(e)

with bottom_row:
    try:
        st.header('Credits Billed by Month')
        st.caption('Not seeing multiple months? Try changing your date range at the top')
        df = session.table('METERING_DAILY_HISTORY')
        df = df.select(date_trunc('MONTH', 'USAGE_DATE').alias('USAGE_MONTH'),col('CREDITS_BILLED').cast(FloatType()).alias('CREDITS_BILLED'),col('USAGE_DATE') )
        df = df.filter(col('USAGE_DATE').between(start_date, end_date))
        df = df.group_by('USAGE_MONTH').agg(sum_('CREDITS_BILLED').cast(FloatType()).alias('CREDITS_BILLED'))
        # df = df.sort('TOTAL_CREDITS_USED', ascending=False)
        df = df.to_pandas()   
        st.vega_lite_chart(df,{
            'mark': 'bar',
            'encoding': {
                'x': { "timeUnit": "utcyearmonth", "field": "USAGE_MONTH", "type": "temporal"},
                'y': {"aggregate": "sum", 'field': 'CREDITS_BILLED'}
            },
        }, use_container_width=True)
    except Exception as e:
        st.warning(e)

with bottom_row:
    try:
        st.header('Average Query Execution Time (By User)')
        df = session.table('QUERY_HISTORY')
        df = df.select(col('USER_NAME'),col('EXECUTION_TIME'))
        df = df.filter(col('START_TIME').between(start_date, end_date))
        df = df.group_by("USER_NAME").agg((sum_('EXECUTION_TIME')/1000).cast(FloatType()).alias('AVERAGE_EXECUTION_TIME'))
        df = df.sort('AVERAGE_EXECUTION_TIME', ascending=False)    
        df = df.to_pandas()   
        st.bar_chart(df, x='USER_NAME', y='AVERAGE_EXECUTION_TIME')
    except Exception as e:
        st.warning(e)

# There's a few more sample queries that you can try out on your own!
# https://medium.com/snowflake/monitoring-snowflake-with-snowsight-e9990a2898f1