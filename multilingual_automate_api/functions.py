import requests
import os
import json
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from config import active_api, userID, ulcaApiKey, key_location, repo_url, sheet_name, sheet_id, folder_path

# Function to fetch JSON files from GitHub
def fetch_github_json_names():
    
    global m_files, w_files
    repo_url_parts = repo_url.split('/')

    parent_repo = repo_url_parts[3]
    repo_name = repo_url_parts[4]
    

    contents_url = f"https://api.github.com/repos/{parent_repo}/{repo_name}/contents/{folder_path}"
    
    response = requests.get(contents_url)
    if response.status_code == 200:
        json_files = [file['name'] for file in response.json() if file['name'].endswith('.json')]
        m_files=[i for i in json_files if i.startswith("mobile")]
        w_files=[i for i in json_files if i.startswith("web")]
        return json_files
    else:
        print(f"Failed to fetch directory contents from GitHub. Status code: {response.status_code}")
        print(f"Response text: {response.text}")  

def fetch_github_json(file):
    # Replace 'blob' with 'raw' in the URL to get the raw content
    file_path = os.path.join(folder_path, file)
    file_path = file_path.replace("\\", "/")

    raw_url = f"{repo_url}/{file_path}".replace('blob', 'raw')
    response = requests.get(raw_url)
    print(response.status_code)
    if response.status_code == 200:        
        return response.json()
        
    else:
        print("Failed to fetch JSON from GitHub")
        return None

# Function to fetch data from Google Sheets
def read_google_sheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(key_location, scope)
    client = gspread.authorize(creds)

    sheet = client.open_by_key(sheet_id).worksheet(sheet_name)
    data = sheet.get_all_records()
    df = pd.DataFrame(data)
    return df

def update_google_sheet(merged_df):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(key_location, scope)
    client = gspread.authorize(creds)

    sheet = client.open_by_key(sheet_id).worksheet(sheet_name)
    
    # Clear the existing data in the Google Sheet
    sheet.clear()

    # Convert DataFrame to list of lists for updating
    data = [merged_df.columns.tolist()] + merged_df.astype(str).values.tolist()

    # Update Google Sheet with the new data
    sheet.update("A1", data)

    return "Google Sheet updated successfully"



# Function to create DataFrame from JSON files
def create_dataframe_from_json(file_name, data):
    dfs = []  
    try:
        dict_json = {f"{file_name[0]}.{k}.{key}": string for k, v in data.items() for key, string in v.items()}
    except:
        print("json is having only one dict")
        dict_json = {f"{file_name[0]}.{k}": v for k, v in data.items()}  # for only one dict in json

    df = pd.DataFrame.from_dict(dict_json, orient='index').reset_index()
    df.rename(columns={"index": "languagekey", 0: "en_value (current)"}, inplace=True)
    dfs.append(df)
    
    result_df = pd.concat(dfs, ignore_index=True)
    result_df = result_df.groupby('en_value (current)').first()
    result_df.reset_index(inplace=True)

    return result_df

# Function to fetch active API
def get_active_api(taskType):
    url = "https://meity-auth.ulcacontrib.org/ulca/apis/v0/model/getModelsPipeline"
    payload = json.dumps({
      "pipelineTasks": [
        {
          "taskType": taskType,
          "config": {
            "language": {
              "sourceLanguage": "en"
            }
          }
        }
      ],
      "pipelineRequestConfig": {
        "pipelineId": "64392f96daac500b55c543cd"
      }
    })
    headers = {
      'userID': userID,
      'ulcaApiKey': ulcaApiKey,
      'Content-Type': 'application/json'
    }
    response = requests.post(url, headers=headers, data=payload)

    config_translation = response.text
    config_translation_data = json.loads(config_translation)
    api_translation = config_translation_data['pipelineResponseConfig'][0]['config'][0]['serviceId']
    try:
        with open("bhashini_api.txt", "r") as f:
            existing_content = f.readlines()
    except FileNotFoundError:
        existing_content = []
    
    with open("bhashini_api.txt", "w") as f:
        if existing_content:
            f.writelines(existing_content)  
            f.write(api_translation + "\n") 
        else:
            f.write(api_translation + "\n") 
    
    return api_translation

# Function to call Bhashini API
def bhashini_api_call(task, target_lang, active_api, string):
    if task == "translation":
        api = active_api[0]
    else:
        api = active_api[1]

    url = "https://dhruva-api.bhashini.gov.in/services/inference/pipeline"
    payload = json.dumps({
        "pipelineTasks": [
            {
                "taskType": task,
                "config": {
                    "serviceId": api,
                    "language": {
                        "sourceLanguage": "en",
                        "targetLanguage": target_lang
                    },
                    "isSentence": True
                },
                "controlConfig": {
                    "dataTracking": True
                }
            }
        ],
        "inputData": {
            "input": [
                {
                    "source": string
                }
            ]
        }
    })
    headers = {
        'Accept': '*/*',
        'Authorization': '9uAUqhCxaept0FGxeOUkyJ1XQSZtp9GWHy5XLriwyBsS-sovl9RkTe2Gkthwrx2F',
        'Content-Type': 'application/json'
    }

    response = requests.post(url, headers=headers, data=payload)
    translation_json = response.text
    print("||||||||||||||||||||||||||||||||||||||||||||||||| translated json")
    print(translation_json)
    translated_data = json.loads(translation_json)

    if task == "translation":
        try:
            return translated_data['pipelineResponse'][0]['output'][0]['target']
        except KeyError as e:
            print(f"KeyError: {e}")
            return "Translation not available"
    else:
        try:
            return translated_data['pipelineResponse'][0]['output'][0]['target'][0]
        except KeyError as e:
            print(f"KeyError: {e}")
            return "Output not available"

# Function to process a single row for API calls
def process_row(row, task, target_lang):
    index, data = row
    return bhashini_api_call(task, target_lang, active_api, data["en_value (current)"])

# Function to make parallel API calls
def parallel_api_calls(df, task, target_lang, max_workers=5):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # results = list(executor.map(lambda row: process_row(row, task, target_lang), df.iterrows()), total=len(df))
        results = list(executor.map(lambda row: process_row(row, task, target_lang), df.iterrows()))


    return results

# Function to merge approved and new labels for approval
def merge_labels_for_approval(approved_df, new_df):
    added_new_labels = pd.concat([approved_df, new_df])
    added_new_labels.reset_index(inplace=True, drop=True)
    return added_new_labels

# Function to get active API
def get_api(data):
    taskType = data.get('taskType')
    api = get_active_api(taskType)
    return {"active_api": api}

def create_Json(u_in, dataframe, file, file_n):
    df=dataframe
    print(file)
    final_dict={}
    l1=[]      
            
    for k,v in file.items():
        temp_dict={}

        for tag, value in v.items():

            try:
                if value in df[f"{u_in}_value(curated)"].values:
                    value_df=df[f"{u_in}_value(curated)"][df["en_value (current)"]==value].values[0]
                elif value in df["en_value (current)"].values:
                    value_df=df[f"{u_in}_translated"][df["en_value (current)"]==value].values[0]
                elif value=="NA":
                    value_df="NA"
                else:
                    l1.append(value)
                temp_dict[tag]=value_df

            except:                   
                print("in except block")
                print(f"no value found for {tag} : {value}")
        final_dict[k]=temp_dict 
        
            

        if file.startswith("mobile"):

            with open(f"temp_output/{file_n}_translated_output_{u_in}", 'w', encoding='utf-8') as json_file:
                json.dump(final_dict["mobile"], json_file, indent=2, ensure_ascii=False)

        else:

            with open(f"temp_output/{file_n}_translated_output_{u_in}", 'w', encoding='utf-8') as json_file:
                json.dump(final_dict, json_file, indent=2, ensure_ascii=False)
    print(f"there are {len(set(l1))} unique labels added from {file_n}") 
    
    return None

