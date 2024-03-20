import pickle
import pandas as pd
model=pickle.load(open('model.pkl','rb'))
encoder=pickle.load(open('encoder.pkl','rb'))
TfIdf_model=pickle.load(open('TfIdf_model.pkl','rb'))

df2=pd.read_csv("csv_path")
for index,row in df2.iterrows():
    arr=TfIdf_model.transform([row["cleaned_comment"]])
    sentiment=model.predict(arr.A)
    df2.at[index,"sentiment_pred_on_comments_model/op"]=encoder.inverse_transform(sentiment)[0]