import pandas as pd
import json

path_excel=r"data/faq.xlsx"
path_priority=r"data/temp_PRIORITY.xlsx"


# Creating class api
class api():
    
    # while calling class API, we must pass lang=Language("EN", "HI", etc) and 
    # p_cat=parent category("IN": information, "IS": issue)

    # while calling class API, we must pass lang=Language("EN", "HI", etc) and 
    # p_cat=parent category("IN": information, "IS": issue)

    def __init__(self, lang, p_cat):
        self.lang = lang
        self.p_cat = p_cat
        self.load_paths()
        self.load_dataframes()
        self.load_all_maps()
        ####################################################
        
    def load_paths(self):
        # Define file paths based on language and parent category
        base_path = r"data"
        self.path_recommend = f"{base_path}/{self.p_cat}_recommendation_{self.lang}.json"
        self.path_cat_map = f"{base_path}/{self.p_cat}_cat_map_{self.lang}.json"
        self.path_que_map = f"{base_path}/{self.p_cat}_qa_{self.lang}.json"
        self.path_excel = r"data/faq.xlsx"
        self.path_priority = r"data/temp_PRIORITY.xlsx"

        ####################################################
        
    def load_dataframes(self):
        # as all the data is in on excel file we need to fetch specific sheet
        sheet_name=f"{self.p_cat}{self.lang}"
        self.df = pd.read_excel(self.path_excel, sheet_name=sheet_name)
        ####################################################
        
    def load_all_maps(self):
        with open(self.path_recommend, "r", encoding="utf-8") as rec_file:
            rec_data = json.load(rec_file)
            self.recommend1 = rec_data["faqBot"]

        with open(self.path_cat_map, "r", encoding="utf-8") as cat_file:
            cat_data = json.load(cat_file)
            self.cat_map = cat_data

 

        with open(self.path_que_map, "r", encoding="utf-8") as que_file:
            self.que_map = json.load(que_file)
        ####################################################
        
    def generate_priorit_recommend(self, p_data=0): #method for giving priority for existing recommendation_map
        global data

        
        config=[]
        
        data=self.df
        faqBot=self.recommend1     
        lang=self.lang
        ####################################################
        
        dict_container = {}                # Create a dictionary to hold other dictionaries
        dict_container[f"cat_clicks"] = {}
        for i in range(1,len(data["Category"].unique())+1):#for loop to auto generate dictionaary for no. of categories 
            dict_name = f"cat{i}_L1_clicks" # Create the dynamic dictionary name based on category and only L1 clicks
            dict_container[dict_name] = {}  # Create an empty dictionary and store it in the container
            dict_name = f"cat{i}_L2_clicks"  # Create the dynamic dictionary name based on category and only L2 clicks
            dict_container[dict_name] = {}  

        priority_rank={} #initiating dict to store ids and respective priority rank
        ##################################################

        #Iterating over existing recommendation map and setting priority based on priority rank dictionary 
        if type(p_data)!=int:# checking if priority dataframe is passed otherwise p_data is taken as 0 by default
            num=1 # num variable is used for incrementation
            
            for index,row in p_data.iterrows():#for loop for iterating over priority dataframe
                if index.startswith(f"{self.p_cat}{self.lang}C") and 0<len(index)<=8:
                    dict_container[f"cat{p}_clicks"][index]=row["clicks"]# creating cat clicks dict in parent dict 

                for i in range(1,len(data["Category"])):# creating L1 and L2 clicks dict in parent dict
                    if index.startswith(f"{self.p_cat}{self.lang}C10{i}") and 8<len(index)==14:
                        dict_container[f"cat{i}_L1_clicks"][index]=row['clicks']

                    elif index.startswith(f"{self.p_cat}{self.lang}C10{i}") and 14<len(index)==20:
                        dict_container[f"cat{i}_L2_clicks"][index]=row['clicks']
            num+=1
            ####################################################
            
            for k,v in dict_container.items():# for loop for reverse sorting of all dicts in parent dict
                dict_container[k]={k:v for k,v in (sorted(dict_container[k].items(), key=lambda x:x[1],reverse=True))}          
            count=1 #count variable is initiated to set priority rank for sorted ids from 1 to length of that dict
            
            for key,value in dict_container.items():# for loop for iterating over each dictionary in parent dict

                for k,v in dict_container[key].items():# for loop for iterating over key i.e dicts in parent dict 
                    priority_rank[k]=count                             # and value i.e dict having ids and clicks
                    count+=1
                count=1 
            count=1
#             print(dict_container)


            ####################################################
#             print(priority_rank)
            #Iterating over existing faqBot json and changing priorities based on clicks received in priority dataframe
            #if there are no clicks on any questions then default value for them will be set to 20 so while sorting it is at last
            seq=1
            for l in range(len(faqBot)):
                if faqBot[l]["catId"] in priority_rank:
                    faqBot[l]["priority"]=priority_rank[faqBot[l]["catId"]]
                else:
                    faqBot[l]["priority"]=seq
                seq=1

                for q in range(len(faqBot[l]["recommendedQues"])):

                    if faqBot[l]["recommendedQues"][q]["quesID"] in priority_rank:
                        faqBot[l]["recommendedQues"][q]["priority"]=priority_rank[faqBot[l]["recommendedQues"][q]["quesID"]]
                    else:
                        faqBot[l]["recommendedQues"][q]["priority"]=seq
                    seq=1
                    for q2 in range(len(faqBot[l]["recommendedQues"][q]["recommendedQues"])):
                        if faqBot[l]["recommendedQues"][q]["recommendedQues"][q2]["quesID"] in priority_rank:
                            faqBot[l]["recommendedQues"][q]["recommendedQues"][q2]["priority"]=priority_rank[faqBot[l]["recommendedQues"][q]["recommendedQues"][q2]["quesID"]]
                        else:
                            faqBot[l]["recommendedQues"][q]["recommendedQues"][q2]["priority"]=seq
                    seq+=1
                seq+=1
            seq+=1
            
        elif p_data==0:          

            for l in range(len(faqBot)):
                if faqBot[l]["catId"] in priority_rank:
                    faqBot[l]["priority"]=priority_rank[faqBot[l]["catId"]]
                else:
                    faqBot[l]["priority"]=1

                for q in range(len(faqBot[l]["recommendedQues"])):

                    if faqBot[l]["recommendedQues"][q]["quesID"] in priority_rank:
                        faqBot[l]["recommendedQues"][q]["priority"]=priority_rank[faqBot[l]["recommendedQues"][q]["quesID"]]
                    else:
                        faqBot[l]["recommendedQues"][q]["priority"]=1

                    for q2 in range(len(faqBot[l]["recommendedQues"][q]["recommendedQues"])):
                        if faqBot[l]["recommendedQues"][q]["recommendedQues"][q2]["quesID"] in priority_rank:
                            faqBot[l]["recommendedQues"][q]["recommendedQues"][q2]["priority"]=priority_rank[faqBot[l]["recommendedQues"][q]["recommendedQues"][q2]["quesID"]]
                        else:
                            faqBot[l]["recommendedQues"][q]["recommendedQues"][q2]["priority"]=1          
        config.append(faqBot)          # config empty list will capture recommendation maps of Issue and information
            ##################################################
            
        def sort_by_priority(item):        # Method for sorting of recommendation map 
            if 'recommendedQues' in item:
                item['recommendedQues'] = sorted(item['recommendedQues'], key=lambda x: x['priority'])
                for sub_item in item['recommendedQues']:
                    sort_by_priority(sub_item)
        all_faqBot={}                      # empty dict to capture sorted recommendation maps
        
        data_sorted = sorted(config[0], key=lambda x: x['priority'])
        
        for item in data_sorted:
            sort_by_priority(item)
        
        all_faqBot["recommendationMap"]=data_sorted
        ##################################################
        
        all_json_dump={}
        all_json_dump.update(self.cat_map)
        all_json_dump.update(all_faqBot)
        all_json_dump.update(self.que_map)      
        data_output = {"config": all_json_dump}
        
        ##################################################


        return data_output
    
# Create an instance of the API class
s = API("HI", "IS")
# Load priority data
df1 = pd.read_excel(s.path_priority)
df1.set_index("qid", inplace=True)
# Generate priorities and recommendations
s.generate_priorit_recommend()