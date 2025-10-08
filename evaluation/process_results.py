import os
import sys
import json
import re
import argparse

path = ".."
abs_path = os.path.abspath(path)
sys.path.append(abs_path)
from utils.json_utils import load_json, store_json, load_json_1_line

def append_golden(predict_data,golden_data):
    for i in range(len(predict_data)):
        gold_index=int(predict_data[i]["index"])
        predict_data[i]["golden_answer"]=list(golden_data[gold_index]["answer_entities"].values())
    return predict_data

def refine_answers(data):
    for i in range(len(data)):
        refined_answers=[]
        for j in range(len(data[i]["searched_answers"])):
            refined_answers+=data[i]["searched_answers"][j]
        data[i]["refined_answers"]=list(set(refined_answers))
    for i in range(len(data)):
        filtered_answers=[]
        for j in range(len(data[i]["refined_answers"])):
            if "m." not in data[i]["refined_answers"][j] and "g." not in data[i]["refined_answers"][j]:
                filtered_answers.append(data[i]["refined_answers"][j])
        data[i]["refined_answers"]=filtered_answers
    return data

if __name__ == '__main__':
    # add args
    parser = argparse.ArgumentParser(description="Process searched results for the final evaluation.")
    parser.add_argument('--dataset_type', type=str, default="WebQSP", required=True,
                        help="Type of dataset (e.g., 'WebQSP').")
    args = parser.parse_args()

    # load results
    path="../main/output/"+args.dataset_type+"/"+args.dataset_type+"_searched.jsonl"
    path=os.path.abspath(path)
    data=load_json_1_line(path)

    # load golden data
    golden_path="../data/processed/"+args.dataset_type+"/"+args.dataset_type+"_test_extracted.json"
    golden_path=os.path.abspath(golden_path)
    golden_data=load_json(golden_path)

    # process
    predict_data=append_golden(data,golden_data)
    refined_data=refine_answers(predict_data)

    # output
    output_path="../main/output/"+args.dataset_type+"/"+args.dataset_type+"_final.json"
    output_path=os.path.abspath(output_path)
    store_json(refined_data,output_path)