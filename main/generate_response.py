import os
import sys
import json
import re
import itertools
import random
import time
import argparse
path = ".."
abs_path = os.path.abspath(path)
sys.path.append(abs_path)
from utils.json_utils import load_json, store_json
from llamafactory.chat.chat_model import ChatModel


def wrap_prompt(prompt):
    wrapped_prompt = [{"role": "user", "content": prompt}]
    return wrapped_prompt

# response vector=[[res1,score1],[res2,score2],...],responses=[res1,res2]
def extract_response(response_vector):
    responses = []
    for i in range(len(response_vector)):
        responses.append(response_vector[i][0])
    return responses

# generate
def generate_response(data, output_path):
    with open(output_path, "a", encoding="UTF-8") as file:
        instruction = "Please follow the instructions.\n"
        for i in range(len(data)):
            print("i=", i, "/", len(data))
            # step 1: parse simple questions for all entities
            STEP1_wrapped_prompt = wrap_prompt(instruction + data[i]["step1"])
            print("step 1 prompt:", STEP1_wrapped_prompt)
            STEP1_response_vector = chat_model.chat(STEP1_wrapped_prompt)
            STEP1_response = extract_response(STEP1_response_vector)
            simple_questions = []
            for simple_question in STEP1_response:
                simple_question = simple_question.replace("\n", "#")
                simple_questions.append(simple_question)
                print("simple questions:", simple_question)
            predicted_logical_forms = []
            for j in range(len(simple_questions)):
                STEP2_replaced_prompt = data[i]["step2"].replace("**simple questions**", simple_questions[j])
                STEP2_wrapped_prompt = wrap_prompt(instruction + STEP2_replaced_prompt)
                print("step 2 prompt:", STEP2_wrapped_prompt)
                STEP2_response_vector = chat_model.chat(STEP2_wrapped_prompt)
                STEP2_response = extract_response(STEP2_response_vector)
                path_LFs = []
                for lf in STEP2_response:
                    lf = lf.replace("\n", ",")
                    path_LFs.append(lf)
                    print("path level LFs:", lf)
                    STEP3_replaced_prompt = data[i]["step3"].replace("**logical forms**", lf)
                    STEP3_wrapped_prompt = wrap_prompt(instruction + STEP3_replaced_prompt)
                    print("step 3 prompt:", STEP3_wrapped_prompt)
                    STEP3_response_vector = chat_model.chat(STEP3_wrapped_prompt)
                    STEP3_response = extract_response(STEP3_response_vector)
                    predicted_logical_forms += STEP3_response
            predicted_logical_forms = list(set(predicted_logical_forms))
            entry = {"index": i, "question": data[i]["question"], "predicted_logical_forms": predicted_logical_forms,
                     "label_logical_form": data[i]["logical_form"]}
            json.dump(entry, file)
            file.write("\n")
            file.flush()


if __name__ == '__main__':
    # add args
    parser = argparse.ArgumentParser(description="Generate logical forms with fine-tuned LLM.")
    parser.add_argument('--dataset_type', type=str, default="WebQSP", required=True,
                        help="Type of dataset (e.g., 'WebQSP').")
    parser.add_argument('--model_name_or_path', type=str, required=True,
                        help="Path to LLM model.")
    parser.add_argument('--template', type=str, required=True,
                        help="Prompt template (e.g., 'llama2').")
    parser.add_argument('--adapter_name_or_path', type=str, required=True,
                        help="Path to LLM checkpoint.")
    parser.add_argument('--num_beams', type=int, required=True,
                        help="Beam size.")
    args = parser.parse_args()

    # load files and models
    path = "../main/input/" + args.dataset_type + "/test_data/test_data_entries.json"
    path = os.path.abspath(path)
    data = load_json(path)
    params = {"model_name_or_path": args.model_name_or_path, "template": args.template,
              "adapter_name_or_path": args.adapter_name_or_path, "num_beams": args.num_beams}
    chat_model = ChatModel(params)

    # generate response
    output_pathl = "../main/output/" + args.dataset_type + "/" + args.dataset_type + "_result.jsonl"
    output_pathl = os.path.abspath(output_pathl)
    generate_response(data, output_pathl)
