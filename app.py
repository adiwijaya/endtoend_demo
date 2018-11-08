from flask import Flask, render_template, request,session
#from flask_sqlalchemy import SQLAlchemy
#from send_email import send_email
#from sqlalchemy.sql import func

app=Flask(__name__)
app.secret_key = 'A0Zr98j/3yX R~XHH!jmN]LWX/,?RT'
#app.config['SQLALCHEMY_DATABASE_URI']='postgresql://user_guest:password@localhost/height_collector'
#db=SQLAlchemy(app)


@app.route("/")
def index():
    return render_template("index.html")

@app.route("/success", methods=['POST'])
def success():
    if request.method=='POST':
        height_name=int(request.form["height_name"])
        weight_name=int(request.form["weight_name"])
        print(height_name, weight_name)

        bmi =  weight_name/height_name

        return render_template('index.html', text="This is calculated using Python : Your BMI is %s"%bmi)
    return render_template('index.html', text="Seems like we got something from that email once!")

@app.route("/get_table", methods=['POST'])
def get_table():
    if request.method=='POST':
        import requests
        import json
        url = "http://209.97.172.240:50111/templeton/v1/ddl/database/default/table?user.name=mapr"
        text_result = requests.get(url).text
        data = json.loads(text_result)
        result = data['tables']
        session['list_tables'] = result
        print(data)
        return render_template('index.html', text="This result taken from hcat hive. The result is %s"%result)

@app.route("/get_columns", methods=['POST'])
def get_columns():
    if request.method=='POST':
        import requests
        table_name=str(request.form["table_name"])
        url = "http://209.97.172.240:50111/templeton/v1/ddl/database/default/table/%s?user.name=mapr"%table_name
        data = requests.get(url).text
        print(data)
        return render_template('index.html', text=data)

@app.route("/query_hive", methods=['POST'])
def query_hive():
    if request.method=='POST':
        from pyhive import hive
        import pandas as pd

        conn = hive.Connection(host="209.97.172.240", port=10000, auth="NONE",username="mapr")
        df = pd.read_sql("SELECT * FROM bank LIMIT 10", conn)

        df.show()
        return render_template('index.html', text=data)

import findspark
findspark.init()
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
@app.route("/query_sparksql", methods=['POST'])
def query_sparksql():
    if request.method=='POST':
        from spark import get_spark
        spark_session = get_spark()
        sc = spark_session.sparkContext
        array = session.get('list_tables', None)

        rdd = sc.parallelize(array)
        count = str(rdd.count())

        return render_template('index.html', text="This is calculated using Spark. Result is : %s"%count)



if __name__ == '__main__':
    app.debug=True
    app.run(port=9998)
