import json
import os
import csv


# load json file
def load_json(file_path):
    with open(file_path,"r",encoding="UTF-8") as file:
        data=json.load(file)
    return data

# store json file
def store_json(data,file_path):
    if not os.path.exists(os.path.dirname(file_path)):
        os.mkdir(os.path.dirname(file_path))
    with open(file_path, "w", encoding="UTF-8") as file:
        json.dump(data, file)

# load json file 1 line
def load_json_1_line(file_path):
    data=[]
    with open(file_path,"r",encoding="UTF-8") as file:
        for line in file:
            entry=json.loads(line.strip())
            data.append(entry)
    return data

def load_txt(file_path):
    data=[]
    with open(file_path, "r",encoding="UTF-8") as f:
        for index, i in enumerate(f.readlines()):
            data.append(i.strip())
    return data

def load_csv(file_path):
    data=[]
    with open(file_path, "r",encoding="UTF-8") as f:
        reader=csv.reader(f)
        for row in reader:
            data.append(row)
    return data
