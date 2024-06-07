from flask import Flask, render_template, request, redirect, url_for, flash, session
import pandas as pd
import snowflake.connector
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from snowflake.connector.pandas_tools import write_pandas
import os

app = Flask(__name__)
app.secret_key = 'your_secret_key'

user = 'TRILOKVARMA'
password = 'Aha123'
account = "cs74371.ap-south-1.aws"
database = "AHA_ADHOC"
schema = "MISC"
warehouse = "AHA_WH"
role = "AHA_TEAM_ADMIN"
authenticator = "snowflake"

def fetch_titles():
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        schema=schema,
        database=database,
        warehouse=warehouse,
        role=role
    )
    query = "SELECT DISTINCT title_plink FROM prod_aha.tableau_analytics.view_videos"
    df = pd.read_sql(query, conn)
    conn.close()
    return df['TITLE_PLINK'].tolist()

def run_query_and_send_email(target_name, selected_titles, no_of_titles, email):
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        database=database,
        warehouse=warehouse,
        role=role
    )
    
    try:
        titles_str = "', '".join(selected_titles)
        query = f"""
        WITH churns AS (
            SELECT 
                user_id,
                pack_type,
                pack_language,
                sku,
                MAX(DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', subscription_end_date))) AS max_sub_end_date
            FROM aha_adhoc.misc.ev_dim_subscriptions_concise
            GROUP BY 1,2,3,4
        ),
        content_watched AS (
            SELECT 
                userid,
                COUNT(DISTINCT title_plink) AS no_of_title_watched
            FROM aha_adhoc.misc.dim_watch_history_subscriptions_content_concise
            WHERE title_plink IN ('{titles_str}')
            GROUP BY userid
        )
        SELECT 
            DISTINCT profile_id,
            '{target_name}' AS targetgroup,
            b.user_id as user_id,
            max_sub_end_date as churn_date,
            sku,
            pack_type,
            pack_language,
            date(convert_timezone('America/Los_Angeles','Asia/Kolkata',current_timestamp())) as fetch_date
        FROM content_watched a
        LEFT JOIN churns b ON a.userid = b.user_id
        LEFT JOIN prod_aha.tableau_analytics.ev_dim_profiles edp ON a.userid = edp.user_id
        WHERE 
            no_of_title_watched >= {no_of_titles}
            AND LOWER(profile_type) NOT IN ('kids', 'family')
            AND max_sub_end_date BETWEEN 
            DATEADD(day, -30, date(convert_timezone('America/Los_Angeles','Asia/Kolkata',current_timestamp()))) AND date(convert_timezone('America/Los_Angeles','Asia/Kolkata',current_timestamp()))
        GROUP BY 1,2,3,4,5,6,7,8;
        """
        
        df = pd.read_sql(query, conn)
        print(df)
        
        # Save dataframe to CSV
        csv_file = 'result.csv'
        df[['PROFILE_ID','TARGETGROUP']].to_csv(csv_file, index=False)
        df_table = df[['TARGETGROUP','USER_ID','PACK_TYPE','CHURN_DATE','SKU','PACK_LANGUAGE','FETCH_DATE']]

        # Send email with CSV attachment
        try:
            msg = MIMEMultipart()
            msg['From'] = 'aditya.krishna@arhamedia.com'
            msg['To'] = email
            msg['Subject'] = 'Query Result'
            
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(open(csv_file, 'rb').read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename="{os.path.basename(csv_file)}"')
            msg.attach(part)
            
            server = smtplib.SMTP('smtp.outlook.com', 587)
            server.starttls()
            server.login('aditya.krishna@arhamedia.com', 'cholebhature@1503')
            server.sendmail('aditya.krishna@arhamedia.com', email, msg.as_string())
            server.quit()
            print("Email sent successfully")
        except Exception as e:
            print(f"Failed to send email: {e}")
        
        # Create table if not exists
        try:
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {schema}.Match_audience_retention_v1 (
                target_group STRING,
                user_id STRING,
                pack_type STRING,
                churn_date DATE,
                sku STRING,
                pack_language STRING, 
                fetch_date DATE
            )
            """
            conn.cursor().execute(create_table_query)
            print("Table checked/created successfully.")
        except Exception as e:
            print(f"Failed to create table: {e}")

        # Insert data into Snowflake table
        try:
            insert_query = f"""
            INSERT INTO {schema}.Match_audience_retention_v1 (target_group,user_id, pack_type,churn_date,sku, pack_language, fetch_date)
            VALUES (%s ,%s, %s, %s, %s, %s, %s)
            """
            data_to_insert = df_table.values.tolist()
            cursor = conn.cursor()
            cursor.executemany(insert_query, data_to_insert)
            conn.commit()
            print(f"Successfully inserted {len(data_to_insert)} rows into Match_audience_retention table.")
        except Exception as e:
            print(f"Failed to insert data: {e}")
    
    finally:
        conn.close()
        print("Connection closed.")

@app.route('/')
def home():
    return redirect(url_for('select_team'))

@app.route('/select_team', methods=['GET', 'POST'])
def select_team():
    if request.method == 'POST':
        team = request.form.get('team')
        if team == 'retention':
            return redirect(url_for('index'))
        else:
            flash("Only Retention Team is supported at the moment.", "error")
            return redirect(url_for('select_team'))
    return render_template('select_team.html')

@app.route('/index', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        target_name = request.form.get('target_name')
        selected_titles = request.form.getlist('titles[]')
        no_of_titles = request.form.get('no_of_titles')
        email = request.form.get('email')
        
        # Debug statements to check form data
        print(f"target_name: {target_name}")
        print(f"selected_titles: {selected_titles}")
        print(f"no_of_titles: {no_of_titles}")
        print(f"email: {email}")
        
        if not target_name or not selected_titles or not no_of_titles or not email:
            flash("All fields are required", "error")
            return redirect(url_for('index'))
        
        run_query_and_send_email(target_name, selected_titles, int(no_of_titles), email)
        flash("Query executed and email sent successfully", "success")
        return redirect(url_for('index'))
    
    titles = fetch_titles()
    return render_template('index.html', titles=titles)

if __name__ == '__main__':
    app.run(debug=True)

