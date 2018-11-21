import numpy as np
import pandas as pd
from sklearn.externals import joblib

def predict(sex,age):
    # Load Model
    model_loaded = joblib.load('ml_model/model_risk.pkl')

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
