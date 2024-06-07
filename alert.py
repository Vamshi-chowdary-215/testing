import pandas as pd
import snowflake.connector
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders
from snowflake.connector.pandas_tools import write_pandas
import os
import matplotlib.pyplot as plt

user = 'TRILOKVARMA'
password = 'Aha123'
account = "cs74371.ap-south-1.aws"
database = "AHA_ADHOC"
schema = "MISC"
warehouse = "AHA_WH"
role = "AHA_TEAM_ADMIN"
authenticator = "snowflake"

def fetch_table():
    try:
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            schema=schema,
            database=database,
            warehouse=warehouse,
            role=role
        )
        query = "SELECT * FROM aha_adhoc.misc.TABLE_DAU_SUBS_HOUR_KPI WHERE DAU_DATE between '2024-01-01' and DATEADD(DAY, -1, GETDATE())"
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        print(f"Error fetching table: {e}")
        return pd.DataFrame()

def df_to_html(df):
    return df.to_html(index=False, border=0, justify='center')

def send_email(subject, body, html_body=None):
    from_email = 'aditya.krishna@arhamedia.com'
    to_email = 'trilok.mudunuri@arhamedia.com'
    password = 'cholebhature@1503'

    if not from_email or not to_email or not password:
        print("Error: Email environment variables are not set properly.")
        return

    try:
        msg = MIMEMultipart('alternative')
        msg['From'] = from_email
        msg['To'] = to_email
        msg['Subject'] = subject

        # Attach the plain text body
        msg.attach(MIMEText(body, 'plain'))

        # Attach the HTML body if provided
        if html_body:
            msg.attach(MIMEText(html_body, 'html'))

        server = smtplib.SMTP('smtp.outlook.com', 587)
        server.starttls()
        server.login(from_email, password)
        text = msg.as_string()
        server.sendmail(from_email, to_email, text)
        server.quit()
    except Exception as e:
        print(f"Error sending email: {e}")

def calculate_percentage_drop(value, lower_bound):
    return ((lower_bound - value) / lower_bound) * 100

def check_and_alert(subs_df, dau_df, time_per_user_df, days):
    breaches = []
    
    recent_subs = subs_df.tail(days).reset_index()
    recent_dau = dau_df.tail(days).reset_index()
    recent_time_per_user = time_per_user_df.tail(days).reset_index()

    for index, row in recent_subs.iterrows():
        if row['SUBS'] < row['SUBS_LOWER_BOUND']:
            drop_percentage = calculate_percentage_drop(row['SUBS'], row['SUBS_LOWER_BOUND'])
            breaches.append((row['DAU_DATE'], 'SUBS', row['SUBS'], row['SUBS_LOWER_BOUND'], drop_percentage))

    for index, row in recent_dau.iterrows():
        if row['DAU'] < row['DAU_LOWER_BOUND']:
            drop_percentage = calculate_percentage_drop(row['DAU'], row['DAU_LOWER_BOUND'])
            breaches.append((row['DAU_DATE'], 'DAU', row['DAU'], row['DAU_LOWER_BOUND'], drop_percentage))

    for index, row in recent_time_per_user.iterrows():
        if row['AVG_TIME_PER_USER'] < row['TIME_PER_USER_LOWER_BOUND']:
            drop_percentage = calculate_percentage_drop(row['AVG_TIME_PER_USER'], row['TIME_PER_USER_LOWER_BOUND'])
            breaches.append((row['DAU_DATE'], 'AVG_TIME_PER_USER', row['AVG_TIME_PER_USER'], row['TIME_PER_USER_LOWER_BOUND'], drop_percentage))

    if breaches:
        subject = "Alert: Metric Breach Detected"
        body = "The following metrics have breached their lower bounds:\n\n"
        body += "Date\t\tMetric\t\tValue\t\tLower Bound\t\tDrop (%)\n"
        body += "-"*80 + "\n"
        for breach in breaches:
            body += f"{breach[0]}\t{breach[1]}\t{breach[2]:.2f}\t{breach[3]:.2f}\t{breach[4]:.2f}%\n"
        send_email(subject, body)

df = fetch_table()

if not df.empty:
    df = df.drop(columns=['TRAFFIC_HOUR','TRAFFIC_FLAG','SUBS_BY_HR','DAU_TRAFFIC'])
    df = df.drop_duplicates()
    df = df.sort_values(by='DAU_DATE', ascending=True, ignore_index=True)
    df = df[['DAU_DATE','SUBS','AVG_SUBS_LAST_15DAYS','LAST_WEEK_SAMEDAY_SUBS','DAU','AVG_DAU_LAST_15DAYS','LAST_WEEK_SAMEDAY_DAU','STREAMING_DAU','STREAMING_MINS','AVG_TIME_PER_USER','AVG_TIME_PER_USER_LAST_15DAYS','LAST_WEEK_SAMEDAY_AVG_TIME_PER_USER']]
    df['DAU_DATE'] = pd.to_datetime(df['DAU_DATE'])  # Ensure DAU_DATE is in datetime format
    print(df)

    # Helper function to calculate the rolling average for the same day of the week over the past 14 weeks
    def calculate_rolling_avg(df, column):
        df['Weekday'] = df.index.dayofweek
        rolling_avg = df.groupby('Weekday')[column].transform(lambda x: x.rolling(window=14, min_periods=1).mean())
        return rolling_avg

    # Create time series for SUBS and AVG_SUBS_LAST_15DAYS
    subs_time_series = df[['DAU_DATE', 'SUBS', 'AVG_SUBS_LAST_15DAYS','LAST_WEEK_SAMEDAY_SUBS']].set_index('DAU_DATE')
    subs_time_series['AVG_SUBS_SAME_DAY_14W'] = calculate_rolling_avg(subs_time_series, 'SUBS')
    print(subs_time_series)

    # Calculate standard deviation for the columns AVG_*_LAST_15DAYS and AVG_*_SAME_DAY_14W
    subs_time_series['STD_SUBS_LAST_15DAYS'] = subs_time_series['AVG_SUBS_LAST_15DAYS'].rolling(window=14, min_periods=1).std()
    subs_time_series['STD_SUBS_SAME_DAY_14W'] = subs_time_series['AVG_SUBS_SAME_DAY_14W'].rolling(window=14, min_periods=1).std()

    # Create time series for DAU and AVG_DAU_LAST_15DAYS
    dau_time_series = df[['DAU_DATE', 'DAU', 'AVG_DAU_LAST_15DAYS','LAST_WEEK_SAMEDAY_DAU']].set_index('DAU_DATE')
    dau_time_series['AVG_DAU_SAME_DAY_14W'] = calculate_rolling_avg(dau_time_series, 'DAU')
    print(dau_time_series)

    # Calculate standard deviation for the columns AVG_*_LAST_15DAYS and AVG_*_SAME_DAY_14W
    dau_time_series['STD_DAU_LAST_15DAYS'] = dau_time_series['AVG_DAU_LAST_15DAYS'].rolling(window=14, min_periods=1).std()
    dau_time_series['STD_DAU_SAME_DAY_14W'] = dau_time_series['AVG_DAU_SAME_DAY_14W'].rolling(window=14, min_periods=1).std()

    # Create time series for AVG_TIME_PER_USER and AVG_TIME_PER_USER_LAST_15DAYS
    time_per_user_series = df[['DAU_DATE', 'AVG_TIME_PER_USER', 'AVG_TIME_PER_USER_LAST_15DAYS','LAST_WEEK_SAMEDAY_AVG_TIME_PER_USER']].set_index('DAU_DATE')
    time_per_user_series['AVG_TIME_PER_USER_SAME_DAY_14W'] = calculate_rolling_avg(time_per_user_series, 'AVG_TIME_PER_USER')
    print(time_per_user_series)

    # Calculate standard deviation for the columns AVG_*_LAST_15DAYS and AVG_*_SAME_DAY_14W
    time_per_user_series['STD_TIME_PER_USER_LAST_15DAYS'] = time_per_user_series['AVG_TIME_PER_USER_LAST_15DAYS'].rolling(window=14, min_periods=1).std()
    time_per_user_series['STD_TIME_PER_USER_SAME_DAY_14W'] = time_per_user_series['AVG_TIME_PER_USER_SAME_DAY_14W'].rolling(window=14, min_periods=1).std()

    print(subs_time_series)
    print(dau_time_series)
    print(time_per_user_series)

    # Filter data for the past 2 weeks
    two_weeks_ago = pd.Timestamp.now() - pd.Timedelta(weeks=2)
    subs_time_series_last_2_weeks = subs_time_series[subs_time_series.index >= two_weeks_ago]
    dau_time_series_last_2_weeks = dau_time_series[dau_time_series.index >= two_weeks_ago]
    time_per_user_series_last_2_weeks = time_per_user_series[time_per_user_series.index >= two_weeks_ago]

    # Calculate AVG - 1*SD for the respective columns
    subs_time_series['SUBS_LOWER_BOUND'] = subs_time_series['AVG_SUBS_SAME_DAY_14W'] - subs_time_series['STD_SUBS_SAME_DAY_14W']
    dau_time_series['DAU_LOWER_BOUND'] = dau_time_series['AVG_DAU_SAME_DAY_14W'] - dau_time_series['STD_DAU_SAME_DAY_14W']
    time_per_user_series['TIME_PER_USER_LOWER_BOUND'] = time_per_user_series['AVG_TIME_PER_USER_SAME_DAY_14W'] - time_per_user_series['STD_TIME_PER_USER_SAME_DAY_14W']

    # Check and send alert if necessary
    check_and_alert(subs_time_series, dau_time_series, time_per_user_series, 60)

    # Plotting the time series for the past 2 weeks
    plt.figure(figsize=(14, 8))

    # Plot for SUBS and AVG_SUBS_SAME_DAY_14W
    plt.subplot(3, 1, 1)
    plt.plot(subs_time_series.index, subs_time_series['SUBS'], label='SUBS')
    plt.plot(subs_time_series.index, subs_time_series['AVG_SUBS_LAST_15DAYS'], label='AVG_SUBS_LAST_15DAYS')
    plt.plot(subs_time_series.index, subs_time_series['AVG_SUBS_SAME_DAY_14W'], label='AVG_SUBS_SAME_DAY_14W', linestyle='--')
    plt.plot(subs_time_series.index, subs_time_series['SUBS_LOWER_BOUND'], label='SUBS_LOWER_BOUND', linestyle=':')
    plt.title('SUBS and AVG_SUBS_SAME_DAY_14W Over Time')
    plt.legend()

    # Plot for DAU and AVG_DAU_SAME_DAY_14W
    plt.subplot(3, 1, 2)
    plt.plot(dau_time_series.index, dau_time_series['DAU'], label='DAU')
    plt.plot(dau_time_series.index, dau_time_series['AVG_DAU_LAST_15DAYS'], label='AVG_DAU_LAST_15DAYS')
    plt.plot(dau_time_series.index, dau_time_series['AVG_DAU_SAME_DAY_14W'], label='AVG_DAU_SAME_DAY_14W', linestyle='--')
    plt.plot(dau_time_series.index, dau_time_series['DAU_LOWER_BOUND'], label='DAU_LOWER_BOUND', linestyle=':')
    plt.title('DAU and AVG_DAU_SAME_DAY_14W Over Time')
    plt.legend()

    # Plot for AVG_TIME_PER_USER and AVG_TIME_PER_USER_SAME_DAY_14W
    plt.subplot(3, 1, 3)
    plt.plot(time_per_user_series.index, time_per_user_series['AVG_TIME_PER_USER'], label='AVG_TIME_PER_USER')
    plt.plot(time_per_user_series.index, time_per_user_series['AVG_TIME_PER_USER_LAST_15DAYS'], label='AVG_TIME_PER_USER_LAST_15DAYS')
    plt.plot(time_per_user_series.index, time_per_user_series['AVG_TIME_PER_USER_SAME_DAY_14W'], label='AVG_TIME_PER_USER_SAME_DAY_14W', linestyle='--')
    plt.plot(time_per_user_series.index, time_per_user_series['TIME_PER_USER_LOWER_BOUND'], label='TIME_PER_USER_LOWER_BOUND', linestyle=':')
    plt.title('AVG_TIME_PER_USER and AVG_TIME_PER_USER_SAME_DAY_14W Over Time')
    plt.legend()

    plt.tight_layout()
    plt.show()
