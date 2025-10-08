import os
import sys
import json
import re
import random
import argparse

path = ".."
abs_path = os.path.abspath(path)
sys.path.append(abs_path)
from utils.json_utils import load_json, store_json


def generate_prompt(data):
    ## strategy : (1) parse simple questions for all entities->(2) generate path-level logical forms based on the simple questions->(3) assemble the final complete logical form
    ## step 1: parse simple question for each entity {**question**}
    prompt_template_STR_STEP1 = "You are an expert in KBQA. Given an original question {**question**}. Please parse the original question into the corresponding simple questions or explanations based on each topic entity."
    ## step 2: generate path-level logical forms based on the simple question {**question**,**simple questions**}
    prompt_template_STR_STEP2 = "You are an expert in KBQA. Given an original question {**question**} and the corresponding simple questions or explanations {**simple questions**}. Please generate the corresponding path-level logical forms."
    ## step 3: assemble the final complete logical form {**question**,**logical forms**}
    prompt_template_STR_STEP3 = "You are an expert in KBQA. Given an original question {**question**} and all path-level logical forms {**logical forms**}. Please generate the final complete logical form for the original question."

    for i in range(len(data)):
        print("i=", i)
        simple_questions = []
        for j in range(len(data[i]["reasoning_information"])):
            simple_questions.append(data[i]["reasoning_information"][j]["decomposed"])
        # step 1
        prompt_STR_STEP1 = prompt_template_STR_STEP1.replace("**question**", data[i]["question"])
        output_STR_STEP1 = "\n".join(simple_questions)
        entry = {"prompt": prompt_STR_STEP1, "output": output_STR_STEP1}
        data[i]["step1"] = entry
        # step 2
        prompt_STR_STEP2 = prompt_template_STR_STEP2.replace("**question**", data[i]["question"])
        input_simple_question_set = output_STR_STEP1.replace("\n", "#")
        prompt_STR_STEP2 = prompt_STR_STEP2.replace("**simple questions**", input_simple_question_set)
        path_level_LFs = []
        for j in range(len(data[i]["reasoning_information"])):
            path_level_LFs.append(data[i]["reasoning_information"][j]["path_LF"])
        output_STR_STEP2 = "\n".join(path_level_LFs)
        entry = {"prompt": prompt_STR_STEP2, "output": output_STR_STEP2}
        data[i]["step2"] = entry
        # step 3
        prompt_STR_STEP3 = prompt_template_STR_STEP3.replace("**question**", data[i]["question"])
        input_path_level_LF_set = output_STR_STEP2.replace("\n", ",")
        prompt_STR_STEP3 = prompt_STR_STEP3.replace("**logical forms**", input_path_level_LF_set)
        output_STR_STEP3 = data[i]["logical_form"]
        entry = {"prompt": prompt_STR_STEP3, "output": output_STR_STEP3}
        data[i]["step3"] = entry

    return data


def generate_train_dataset(data):
    output_entries = []
    instruction = "Please follow the instructions.\n"
    for i in range(len(data)):
        ## step 1
        print("i=", i)
        entry = {"instruction": instruction, "input": data[i]["step1"]["prompt"],
                 "output": data[i]["step1"]["output"], "history": []}
        output_entries.append(entry)
        ## step 2
        entry = {"instruction": instruction, "input": data[i]["step2"]["prompt"],
                 "output": data[i]["step2"]["output"], "history": []}
        output_entries.append(entry)
        ## step 3
        entry = {"instruction": instruction, "input": data[i]["step3"]["prompt"],
                 "output": data[i]["step3"]["output"], "history": []}
        output_entries.append(entry)
    random.shuffle(output_entries)
    return output_entries


def generate_test_dataset(data):
    output_entries = []
    ## strategy: (1) parse simple questions for all entities->(2) generate path-level logical forms based on the simple questions->(3) assemble the final complete logical form
    ## step 1: parse simple question for each entity {**question**}
    prompt_template_STR_STEP1 = "You are an expert in KBQA. Given an original question {**question**}. Please parse the original question into the corresponding simple questions or explanations based on each topic entity."
    ## step 2: generate path-level logical forms based on the simple question {**question**,**simple questions**}
    prompt_template_STR_STEP2 = "You are an expert in KBQA. Given an original question {**question**} and the corresponding simple questions or explanations {**simple questions**}. Please generate the corresponding path-level logical forms."
    ## step 3: assemble the final complete logical form {**question**,**logical forms**}
    prompt_template_STR_STEP3 = "You are an expert in KBQA. Given an original question {**question**} and all path-level logical forms {**logical forms**}. Please generate the final complete logical form for the original question."

    for i in range(len(data)):
        question = data[i]["question"]
        logical_form = data[i]["logical_form"]
        # step 1
        prompt_STR_STEP1 = prompt_template_STR_STEP1.replace("**question**", question)
        # step 2
        prompt_STR_STEP2 = prompt_template_STR_STEP2.replace("**question**", question)
        # step 3
        prompt_STR_STEP3 = prompt_template_STR_STEP3.replace("**question**", question)
        entry = {"question": question, "logical_form": logical_form, "step1": prompt_STR_STEP1,
                 "step2": prompt_STR_STEP2, "step3": prompt_STR_STEP3}
        output_entries.append(entry)
    return output_entries


if __name__ == '__main__':
    # add args
    parser = argparse.ArgumentParser(description="Generate dataset entries.")
    parser.add_argument('--dataset_type', type=str, default="WebQSP", required=True,
                        help="Type of dataset (e.g., 'WebQSP').")
    parser.add_argument('--split', type=str, default="train", required=True, choices=['train', 'test'],
                        help="Dataset split (train or test).")
    args = parser.parse_args()

    # load data
    path = "../data/processed/" + args.dataset_type + "/" + args.dataset_type + "_" + args.split + "_extracted.json"
    path = os.path.abspath(path)
    data = load_json(path)

    # process dataset
    generated_data = []
    # train
    if args.split == "train":
        # generate prompt
        prompt_data = generate_prompt(data)
        # generate data
        generated_data = generate_train_dataset(prompt_data)
    # test
    else:
        generated_data = generate_test_dataset(data)

    # output data
    output_path = "../main/input/" + args.dataset_type + "/" + args.split + "_data/" + args.split + "_data_entries.json"
    output_path = os.path.abspath(output_path)
    store_json(generated_data, output_path)
