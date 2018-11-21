import numpy as np
import pandas as pd
from sklearn.externals import joblib
import os.path

def predict(sex,age):
    # Load Model
    my_path = os.path.abspath(os.path.dirname(__file__))
    path = os.path.join(my_path, "../ml_model/model_risk.pkl")

    model_loaded = joblib.load(path)

    # Static Variables
    sex_female_flag=0
    sex_male_flag=1
    final_prediction = "SAFE"

    # Data Engineering
    if sex=="Female":
        sex_female_flag=1

    if sex=="Male":
        sex_male_flag=1

    df = pd.DataFrame({'AGE':age,'SEX_Female':sex_female_flag,'SEX_Male':sex_male_flag}, index=[0])
    query = pd.get_dummies(df)

    # Predict
    prediction = int(model_loaded.predict(query))

    # Labeling
    if prediction == 0:
        final_prediction == "RISKY"

    return final_prediction
