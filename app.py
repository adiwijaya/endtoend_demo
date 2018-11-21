from flask import Flask, render_template, request,session
#from predict import predict
from flask_sqlalchemy import SQLAlchemy
#from send_email import send_email
from sqlalchemy.sql import func
from util import get_random_email, get_timestamp,get_random_amount
from settings import *

app=Flask(__name__)
app.secret_key = 'A0Zr98j/3yX R~XHH!jmN]LWX/,?RT'
app.config['SQLALCHEMY_DATABASE_URI']='postgresql://postgres:DataLabsP%40ssw0rd@209.97.167.105/default_db'
db=SQLAlchemy(app)

class Data(db.Model):
    __tablename__="height_collector"
    id=db.Column(db.Integer,primary_key=True)
    email_ = db.Column(db.String(120),unique=True)
    height_ = db.Column(db.Integer)

    def __init__(self,email_,height_):
        self.email_ = email_
        self.height_ = height_

class TransactionData(db.Model):
    __tablename__="transaction_data_dummy"
    tx_id=db.Column(db.Integer,primary_key=True)
    email = db.Column(db.String)
    time_ = db.Column(db.TIMESTAMP)
    amount_ = db.Column(db.Integer)

    def __init__(self,email,time_,amount_):
        self.email = email
        self.time_ = time_
        self.amount_ = amount_




@app.route("/")
def index():
    return render_template("index.html")

@app.route("/success", methods=['POST'])
def success():
    if request.method=='POST':
        height=int(request.form["height_name"])
        weight=int(request.form["weight_name"])
        print(height, weight)

        bmi =  weight/height
        email = get_random_email()

        data = Data(email, height)
        db.session.add(data)
        db.session.commit()

        return render_template('index.html', text="This is calculated using Python : Your BMI is %s"%bmi)
    return render_template('index.html', text="Seems like we got something from that email once!")

@app.route("/get_table", methods=['POST'])
def get_table():
    if request.method=='POST':
        import requests
        import json
        url = host_http_hive+":50111/templeton/v1/ddl/database/default/table?user.name=%s"%username_hive
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
        url = host_http_hive+":50111/templeton/v1/ddl/database/default/table/%s?user.name=%s"%(table_name,username_hive)
        data = requests.get(url).text
        print(data)
        return render_template('index.html', text=data)

@app.route("/query_hive", methods=['POST'])
def query_hive():
    if request.method=='POST':
        from pyhive import hive
        conn = hive.Connection(host=host_hive, port=10000, auth="NOSASL",username="adi",database="default")
        #cursor = conn.cursor()
        #cursor.execute("SELECT * FROM bankclasssample")
        #for result in cursor.fetchall():
        #    use_result(result)

        #import pandas as pd
        conn = hive.Connection(host=host_hive, port=10000, auth="NOSASL",username="adi")
        df = pd.read_sql("SELECT * FROM default.bank LIMIT 10", conn)

        #df.show()
        return render_template('index.html', text=data)

import findspark
findspark.init()
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
@app.route("/spark_process", methods=['POST'])
def spark_process():
    if request.method=='POST':
        from spark import get_spark
        spark_session = get_spark()
        sc = spark_session.sparkContext
        array = session.get('list_tables', None)

        rdd = sc.parallelize(array)
        count = str(rdd.count())

        return render_template('index.html', text="This is calculated using Spark. Result is : %s"%count)

@app.route("/spark_query", methods=['POST'])
def spark_query():
     if request.method=='POST':
        query=str(request.form["query_text"])

        from spark import get_spark
        spark_session = get_spark()
        sc = spark_session.sparkContext
        df = spark_session.sql(query)
        pandas_df = df.toPandas()

        return render_template('index.html', text=pandas_df.to_html())

@app.route("/spark_api", methods=['POST'])
def spark_api():
     if request.method=='POST':
        word=str(request.form["word"])
        import requests
        import json


        url = spark_host+'/spark'

        data = {"word": word}
        params = {"word": word}
        result = requests.post(url, params=params, json=data)
        #json_data = json.loads(result.text)
        return render_template('index.html', text=result.text)

@app.route("/spark_hive_api", methods=['POST'])
def spark_hive_api():
     if request.method=='POST':
        table_name=str(request.form["table_name"])
        import requests
        import json

        url = spark_host+ '/query'

        data = {"table_name": table_name}
        params = {"table_name": table_name}
        result = requests.post(url, params=params, json=data)
        json_data = json.loads(result.text)
        return render_template('index.html', text=json_data)

@app.route("/predict_ml", methods=['POST'])
def predict_ml():
     if request.method=='POST':
         sex=str(request.form["sex_name"])
         age=str(request.form["age_name"])
         prediction = predict(sex,age)
         return render_template('index.html', text="This is predicted using sklearn. Result is : %s"%prediction)

@app.route("/build_model_ml_spark_api", methods=['POST'])
def build_model_ml_spark_api():
     if request.method=='POST':
         table_name=str(request.form["table_name"])
         target_col=str(request.form["target_col"])
         import requests
         import json

         url = spark_host+'/build_auto_ml'

         data = {"table_name": table_name,"target_col":target_col}
         params = {"table_name": table_name,"target_col":target_col}
         result = requests.post(url, params=params, json=data)
         return render_template('index.html', text=result.text)

@app.route("/predict_ml_spark_api", methods=['POST'])
def predict_ml_spark_api():
     if request.method=='POST':
         table_name=str(request.form["table_name"])
         import requests
         import json

         url = spark_host+'/predict_auto_ml'

         data = {"table_name": table_name}
         params = {"table_name": table_name}
         result = requests.post(url, params=params, json=data)
         return render_template('index.html', text=result.text)



@app.route("/generate_random_transaction", methods=['POST'])
def generate_random_transaction():
    if request.method=='POST':

        email = get_random_email()
        time = get_timestamp()
        amount = get_random_amount()

        tx_data = TransactionData(email, time , amount)
        db.session.add(tx_data)
        db.session.commit()

        return render_template('index.html', text="Random Transaction Data Generated. Email %s , Time %s, Amount %s"%(email,str(time),str(amount)))

@app.route("/postgres_to_hive", methods=['POST'])
def postgres_to_hive():
    if request.method=='POST':
        import os
        sqoopcom="sqoop import --connect jdbc:postgresql://%s/default_db --username postgres -password DataLabsP@ssw0rd --table transaction_data_dummy --hive-import --target-dir /user/hive/warehouse/transaction_data_dummy  --delete-target-dir --direct"%postgres_host
        os.system(sqoopcom)

        return render_template('index.html', text="Success Transfer data from postgres to Hive")


if __name__ == '__main__':
    app.run(host=HOST,debug=DEBUG,port=PORT)
