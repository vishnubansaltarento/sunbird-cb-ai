from flask import Flask, request, jsonify
import numpy as np
import time
from functions import *
from config import languages

app = Flask(__name__)

global refined_df

@app.route('/process_data', methods=['POST'])


def process_data_route():
    json_files=fetch_github_json_names()
    print(json_files)
    for file in json_files: 
        current_json=fetch_github_json(file)
        print("##############")
        print(current_json)
                  
        
        if current_json:
            result_df=create_dataframe_from_json(file, current_json)
            print("##############")
            print(result_df)
            df=read_google_sheet()
            print("@@@@@@@@@@@@@@@@@@@@@@@@")
            print(df)
            refined_df = result_df[~result_df["en_value (current)"].isin(df["en_value (current)"])]
            ##
            if not refined_df.empty:
                
           
                for lang in languages:
                    refined_df[f"{lang}_translated"] = parallel_api_calls(refined_df, "translation", f"{lang}", max_workers=10)         ##max_threads
                    time.sleep(5)
                    refined_df[f"{lang}_transliterated"] = parallel_api_calls(refined_df, "transliteration", f"{lang}", max_workers=10)
                    # time.sleep(5) 
                    refined_df[f"{lang}_curated"]=refined_df.fillna('')       ###np.nan not valid in google spread
                refined_df.reset_index(inplace=True, drop=True)
                merged_df = merge_labels_for_approval(df, refined_df)
                print("/////////////////////////////////")
                print(merged_df)
                merged_df.to_excel("previously updated_df.xlsx")
                update_google_sheet(merged_df)
            else:
                print("no new data")

            for language in languages:

                create_Json(language, merged_df, current_json, file)
            
        else:
            print("Json is not loaded")

    return "Process completed"

@app.route('/get_active_api', methods=['POST'])
def get_active_api_route():
    return jsonify(get_api(request.json))

if __name__ == '__main__':
    app.run(debug=True)
