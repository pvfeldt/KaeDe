import os
import sys
path = ".."
abs_path = os.path.abspath(path)
sys.path.append(abs_path)
import re
import sqlite3
import random
import numpy as np
import time
import torch
import json
import pandas as pd
import argparse
from utils.json_utils import load_json, store_json
from utils.convert_sparql import convert_sparql_to_s_expression, convert_s_expression_to_logical_form
from utils.sparql_execution import get_label_with_odbc

# dataset_type= "WQSP" or "CWQ"
# split="train" or "test"
def extract_information(data, dataset_type, split):
    output_entries = []
    if dataset_type == 'WebQSP':
        data = data["Questions"]
        for i in range(len(data)):
            print("i=", i)
            question = data[i]["ProcessedQuestion"]
            # add topic entities
            topic_entity_label = data[i]["Parses"][0]["TopicEntityName"]
            topic_entity_id = data[i]["Parses"][0]["TopicEntityMid"]
            if topic_entity_label==None or topic_entity_id==None:
                continue
            golden_entities = {}
            golden_entities[topic_entity_id] = topic_entity_label
            if data[i]["Parses"][0]["Constraints"] != []:
                for j in range(len(data[i]["Parses"][0]["Constraints"])):
                    if data[i]["Parses"][0]["Constraints"][j]["ArgumentType"] == "Entity":
                        constraint_entity_id = data[i]["Parses"][0]["Constraints"][j]["Argument"]
                        constraint_entity_label = data[i]["Parses"][0]["Constraints"][j]["EntityName"]
                        golden_entities[constraint_entity_id] = constraint_entity_label
            # add answer entities
            answer_entities = {}
            for j in range(len(data[i]["Parses"][0]["Answers"])):
                if data[i]["Parses"][0]["Answers"][j]["AnswerType"] == "Entity":
                    answer_entities[data[i]["Parses"][0]["Answers"][j]["AnswerArgument"]] = \
                        data[i]["Parses"][0]["Answers"][j]["EntityName"]
                else:
                    answer_entities["value"] = data[i]["Parses"][0]["Answers"][j]["AnswerArgument"]
            sparql_query = data[i]["Parses"][0]["Sparql"]
            ns_golden_entities = {}
            golden_keys = golden_entities.keys()
            for key in golden_keys:
                ns_golden_entities["ns:" + key] = golden_entities[key]
            try:
                s_expression = convert_sparql_to_s_expression(sparql_query, ns_golden_entities)
            except Exception as e:
                print("Error:", e)
                if split == "train":
                    continue
                elif split == "test":
                    s_expression = None
            if s_expression == None:
                logical_form = ""
                if split == "train":
                    continue
            elif s_expression != None:
                logical_form, _ = convert_s_expression_to_logical_form(s_expression, {})
            entry = {"question": question, "sparql": sparql_query, "logical_form": logical_form,
                     "golden_entities": golden_entities,"answer_entities": answer_entities}
            if split=="train":
                if data[i]["Parses"][0]["InferentialChain"] != None and data[i]["Parses"][0]["Answers"] != None:
                    output_entries.append(entry)
            elif split=="test":
                output_entries.append(entry)

    elif dataset_type == 'CWQ':
        for i in range(len(data)):
            print("i=", i)
            question = data[i]["question"]
            # sparql=sparql query
            sparql_query = data[i]["sparql"]
            pattern_str_1 = r'ns:m\.0\w*'
            pattern_str_2 = r'ns:g\.\w*'
            mid_list_1 = [mid.strip() for mid in re.findall(pattern_str_1, sparql_query)]
            mid_list_2 = [mid.strip() for mid in re.findall(pattern_str_2, sparql_query)]
            mid_list = mid_list_1 + mid_list_2
            # add topic entities
            golden_entities = {}
            for ent_id in mid_list:
                ent_id = ent_id.replace("ns:", "")
                ent_label = get_label_with_odbc(ent_id)
                golden_entities[ent_id] = ent_label
            # add answer entities
            answer_entities = {}
            if split == "train" or split == "dev":
                for j in range(len(data[i]["answers"])):
                    answer_entities[data[i]["answers"][j]["answer_id"]] = data[i]["answers"][j]["answer"]
            elif split == "test":
                for ans_id in data[i]["answer"]:
                    ans_label = get_label_with_odbc(ans_id)
                    answer_entities[ans_id] = ans_label
            logical_form = ""
            try:
                s_expression = convert_sparql_to_s_expression(sparql_query, mid_list)
            except Exception as e:
                print("Error:", e)
                if split == "train":
                    continue
                elif split == "test":
                    s_expression = None
            if s_expression == None:
                if split == "train":
                    continue
            elif s_expression != None:
                logical_form, _ = convert_s_expression_to_logical_form(s_expression, golden_entities)
            entry = {"question": question, "sparql": sparql_query, "logical_form": logical_form,
                     "golden_entities": golden_entities, "answer_entities": answer_entities}
            output_entries.append(entry)
    return output_entries

############## extract reasoning information from SPARQL ####################

def process_sparql(sparql):
    inference_lines = []
    split_sparql = sparql.split("\n")
    for line in split_sparql:
        if line.startswith("?") or line.startswith("ns"):
            inference_lines.append(line)
    return inference_lines

# line_list=sparql_lines
def split_paths(line_list):
    split_lines = []
    initial_tmp_line = []
    # initial split
    for line in line_list:
        if "?sk" in line:
            continue
        split_line = line.split(" ")
        initial_tmp_line.append(line)
        # split by "?x" as end entity or by "ns:m." as end entity
        if split_line[2] == "?x" or split_line[2].startswith("ns:"):
            split_lines.append(initial_tmp_line)
            initial_tmp_line = []
    # adjust split
    new_split_lines = []
    new_path = []
    flag_path = []
    for i in range(len(split_lines)):
        path_line = len(split_lines)
        if flag_path == split_lines[i]:
            continue
        if i < path_line - 1:
            # compare next path
            # if relation in [,,relation,], flag=1
            flag = 0
            next_path = split_lines[i + 1]
            new_tmp_line = ""
            for line in next_path:
                split_line = line.split(" ")
                if split_line[0].startswith("ns:"):
                    if not split_line[0].startswith("ns:m.") and not split_line[0].startswith("ns:g."):
                        flag = 1
                elif split_line[2].startswith("ns:"):
                    if not split_line[2].startswith("ns:m.") and not split_line[2].startswith("ns:g."):
                        flag = 1
                new_tmp_line += line
            # combine complete paths
            if "ns:m." not in new_tmp_line and "ns:g." not in new_tmp_line and flag == 0:
                new_path += split_lines[i] + split_lines[i + 1]
                flag_path = next_path
            else:
                new_path = split_lines[i]
            new_split_lines.append(new_path)
        else:
            new_split_lines.append(split_lines[i])
    return new_split_lines

# path=single path(multiple paths in one sparql)
def reorganize_line_order(paths):
    tmp_line = ""
    new_paths = []
    split_first_line = paths[0].split(" ")
    # if relation in [,,relation,], flag=1
    flag = 0
    if split_first_line[0].startswith("ns:"):
        if not split_first_line[0].startswith("ns:m.") and not split_first_line[0].startswith("ns:g."):
            flag = 1
    elif split_first_line[2].startswith("ns:"):
        if not split_first_line[2].startswith("ns:m.") and not split_first_line[2].startswith("ns:g."):
            flag = 1
    if "ns:m." in paths[0] or "ns:g." in paths[0] or flag == 1:
        new_paths = paths
    else:
        for line in paths:
            flag_tmp = 0
            split_line = line.split(" ")
            if split_line[0].startswith("ns:"):
                if not split_line[0].startswith("ns:m.") and not split_line[0].startswith("ns:g."):
                    flag_tmp = 1
            elif split_line[2].startswith("ns:"):
                if not split_line[2].startswith("ns:m.") and not split_line[2].startswith("ns:g."):
                    flag_tmp = 1
            if "ns:m." in line or "ns:g." in line or flag_tmp == 1:
                tmp_line = line
                new_paths.append(line)
                break
        for line in paths:
            if tmp_line == line:
                continue
            else:
                new_paths.append(line)
    return new_paths

# path=single path(multiple paths in one sparql)
def extract_reasoning_info(paths):
    entity = ""
    variable = ""
    direction = []
    relation = []
    # extract first search
    split_first_line = paths[0].split(" ")
    relation.append(split_first_line[1][3:])
    if "ns:" in split_first_line[0]:
        entity = split_first_line[0][3:]
        variable = split_first_line[2]
        direction.append("forward")
    elif "ns:" in split_first_line[2]:
        entity = split_first_line[2][3:]
        variable = split_first_line[0]
        direction.append("backward")
    for i in range(1, len(paths)):
        split_line = paths[i].split(" ")
        relation.append(split_line[1][3:])
        if split_line[0] == variable:
            variable = split_line[2]
            direction.append("forward")
        elif split_line[2] == variable:
            variable = split_line[0]
            direction.append("backward")
    info_dict = {"entity": entity, "relation": relation, "direction": direction}
    return info_dict

def process_meta_lf(entity, relation, direction):
    output_logical_forms = []
    lf = "[ " + entity + " ]"
    for i in range(len(direction)):
        new_relation = relation[i].replace(".", " , ")
        new_relation = new_relation.replace("_", " ")
        if direction[i] == "forward":
            lf = "( JOIN ( R [ " + new_relation + " ] ) " + lf + " )"
        elif direction[i] == "backward":
            lf = "( JOIN [ " + new_relation + " ] " + lf + " )"
        output_logical_forms.append(lf)
    if output_logical_forms!=[]:
        return output_logical_forms[-1]
    else:
        return ""

# generate intermediate logical forms
def process_intermediate_lf(reasoning_information, topic_entities):
    for i in range(len(reasoning_information)):
        entity_label=""
        if reasoning_information[i]["entity"] in topic_entities:
            entity_label = topic_entities[reasoning_information[i]["entity"]]
        else:
            if "." in reasoning_information[i]["entity"]:
                entity_label=reasoning_information[i]["entity"].replace("."," , ")
        intermediate_lfs_one_entity = process_meta_lf(entity_label, reasoning_information[i]["relation"],
                                                      reasoning_information[i]["direction"])
        reasoning_information[i]["path_LF"] = intermediate_lfs_one_entity
    return reasoning_information

def process_sparql_all(data):
    for i in range(len(data)):
        print("i=", i)
        inference_lines = process_sparql(data[i]["sparql"])
        splitted_paths = split_paths(inference_lines)
        reasoning_information = []
        for path in splitted_paths:
            new_path = reorganize_line_order(path)
            info_dict = extract_reasoning_info(new_path)
            reasoning_information.append(info_dict)
        topic_entities=data[i]["golden_entities"]
        updated_reasoning_information = process_intermediate_lf(reasoning_information, topic_entities)
        data[i]["reasoning_information"] = updated_reasoning_information
    return data

if __name__ == '__main__':
    # add args
    parser = argparse.ArgumentParser(description="Process dataset.")
    parser.add_argument('--dataset_type', type=str, default="WebQSP",required=True, help="Type of dataset (e.g., 'WebQSP').")
    parser.add_argument('--split', type=str, default="train",required=True, choices=['train', 'test'],
                        help="Dataset split (train or test).")
    args = parser.parse_args()

    # load data
    if args.dataset_type == "WebQSP":
        path="../data/original/"+args.dataset_type+"/"+args.dataset_type+"."+args.split+".json"
        path=os.path.abspath(path)
    elif args.dataset_type == "CWQ":
        path = "../data/original/" + args.dataset_type + "/ComplexWebQuestions_" + args.split + ".json"
        path = os.path.abspath(path)
    data=load_json(path)

    # extract data
    extracted_data = extract_information(data, args.dataset_type, args.split)

    # add reasoning information
    reasoned_data = process_sparql_all(extracted_data)

    # output data
    output_path="../data/processed/"+args.dataset_type+"/"+args.dataset_type+"_"+args.split+"_extracted.json"
    output_path=os.path.abspath(output_path)
    store_json(reasoned_data, output_path)
