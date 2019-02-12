from flask import Flask, render_template, request,session
#from predict import predict
from flask_sqlalchemy import SQLAlchemy
#from send_email import send_email
from sqlalchemy.sql import func
from util import *
from settings import *
from predict import predict

app=Flask(__name__)
app.secret_key = 'A0Zr98j/3yX R~XHH!jmN]LWX/,?RT'

#PostgreSQL
#app.config['SQLALCHEMY_DATABASE_URI']='postgresql://postgres:DataLabsP%40ssw0rd@209.97.167.105/default_db'

#MYSQL
app.config['SQLALCHEMY_DATABASE_URI']='mysql://adi:AdiPassw0rd@209.97.172.254/demo'
db=SQLAlchemy(app)

class Data(db.Model):
    __tablename__="height_collector"
    id=db.Column(db.Integer,primary_key=True)
    email_ = db.Column(db.String(120),unique=True)
    height_ = db.Column(db.Integer)

    def __init__(self,email_,height_):
        self.email_ = email_
        self.height_ = height_

class DimensionData(db.Model):
    __tablename__="customer"
    id=db.Column(db.Integer,primary_key=True)
    name = db.Column(db.String)
    age = db.Column(db.Integer)
    time_inputed = db.Column(db.TIMESTAMP)

    def __init__(self,name,age,time_inputed):
        self.name = name
        self.age = age
        self.time_inputed = time_inputed

class TransactionData(db.Model):
    __tablename__="cust_trx"
    trx_id=db.Column(db.Integer,primary_key=True)
    cust_id = db.Column(db.String)
    value = db.Column(db.Integer)
    time = db.Column(db.TIMESTAMP)


    def __init__(self,cust_id,value,time):
        self.cust_id = cust_id
        self.value = value
        self.time = time




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
        #email = get_random_email()

        #data = Data(email, height)
        #db.session.add(data)
        #db.session.commit()

        return render_template('index.html', text="This is calculated using Python : Your BMI is %s \n This is a simple logic calculated in Python, Python is a multipurpose programming language"%bmi)
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
        return render_template('index.html', text="This result taken from hcat hive, on hadoop cluster.\n The result is %s"%result)

@app.route("/get_columns", methods=['POST'])
def get_columns():
    if request.method=='POST':
        import requests
        table_name=str(request.form["table_name"])
        url = host_http_hive+":50111/templeton/v1/ddl/database/default/table/%s?user.name=%s"%(table_name,username_hive)
        data = requests.get(url).text
        print(data)
        return render_template('index.html', text=data)

# @app.route("/query_hive", methods=['POST'])
# def query_hive():
#     if request.method=='POST':
#         from pyhive import hive
#         conn = hive.Connection(host=host_hive, port=10000, auth="NOSASL",username="adi",database="default")
#         #cursor = conn.cursor()
#         #cursor.execute("SELECT * FROM bankclasssample")
#         #for result in cursor.fetchall():
#         #    use_result(result)
#
#         #import pandas as pd
#         conn = hive.Connection(host=host_hive, port=10000, auth="NOSASL",username="adi")
#         df = pd.read_sql("SELECT * FROM default.bank LIMIT 10", conn)
#
#         #df.show()
#         return render_template('index.html', text=data)

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

        return render_template('index.html', text="This is calculated using Spark, its counting number of tables results from 'Get Table' Features.\n Result is : %s"%count)

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
        return render_template('index.html', text="This is a process running from other Spark Service through API, its calculating length of word inputted. \n Result : %s"%result.text)

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
         return render_template('index.html', text="SUCCESS, now try Predict API")

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
         return render_template('index.html', text="SUCCESS, this process generate a prediction CSV in linux directory and hive table named model_predict_result")

@app.route("/generate_random_dimension", methods=['POST'])
def generate_random_dimension():
    if request.method=='POST':

        name = get_random_name()
        time = get_timestamp()
        age = get_random_age()

        dim_data = DimensionData(name, age , time)
        db.session.add(dim_data)
        db.session.commit()

        return render_template('index.html', text="This generate Dimension Data to Database, its an illustration for Customer Data. \n Random Customer Data Generated. Email %s , Time %s, Age %s"%(name,str(time),str(age)))


@app.route("/generate_random_transaction", methods=['POST'])
def generate_random_transaction():
    if request.method=='POST':

        cust_id = get_random_cust_id()
        time = get_timestamp()
        amount = get_random_amount()

        tx_data = TransactionData(cust_id, amount,time)
        db.session.add(tx_data)
        db.session.commit()

        return render_template('index.html', text="This generate data to PostgreSQL Database, its an illustration for Transaction Apps. \n Random Transaction Data Generated. Cust Id %s , Amount %s, Time %s"%(cust_id,str(amount),str(time)))

@app.route("/update_data_dimension", methods=['POST'])
def update_data_dimension():
    if request.method=='POST':

        name = get_random_name()

        row = DimensionData.query.filter_by(id=1).first()
        row.name = name
        row_2 = DimensionData.query.filter_by(id=2).first()
        row_2.age = get_random_age()
        db.session.commit()

        return render_template('index.html', text="This update Dimension Data to Database")


@app.route("/postgres_to_hive", methods=['POST'])
def postgres_to_hive():
    if request.method=='POST':
        import os
        sqoopcom="sudo -u mapr sqoop import --connect jdbc:mysql://datalabs-mapr-02:3306/demo --driver com.mysql.jdbc.Driver --table customer --username mapr --password DataLabsMapRP@ssW%% --hive-import --hive-overwrite --hive-table sandbox.customer_tmp1 --delete-target-dir"
        os.system(sqoopcom)

        return render_template('index.html', text="This is simulation for batch ETL Process using sqoop. \n Success Transfer data from postgres to Hive, please check transaction_data_dummy in hive for access")


if __name__ == '__main__':
    app.run(host=HOST,debug=DEBUG,port=PORT)
