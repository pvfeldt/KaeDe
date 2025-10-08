import os
import sys
import json
import re
from itertools import count
import argparse

path = ".."
abs_path = os.path.abspath(path)
sys.path.append(abs_path)
from utils.json_utils import load_json, store_json, load_json_1_line, load_txt


def FindstrInList(entry, elist):
    for item in elist:
        if entry in item:
            return True
    return False


def Find_P_R_F_HIT(fp, tp, fn):
    precision = tp / (tp + fp)
    recall = tp / (tp + fn)

    f1 = (2 * precision * recall) / (precision + recall)

    if tp > 1e-40:
        hit = 1
    else:
        hit = 0

    return [precision, recall, f1, hit]


def ans_acc(predict, gold):
    tp = 1e-40  # numerical trick
    fp = 0.0
    fn = 0.0
    for x in gold:
        if FindstrInList(x, predict):
            tp += 1
        else:
            # print(x)
            fn += 1

    for x in predict:
        x = x.strip()
        if not FindstrInList(x, gold):
            # print(x)
            fp += 1

    precision, recall, f1, hit = Find_P_R_F_HIT(fp, tp, fn)

    return [precision, recall, f1, hit]

def evaluate_results_logical_form(data):
    p_mean = 0
    r_mean = 0
    f1_mean = 0
    hit1_mean = 0
    for i in range(len(data)):
        predict_data = data[i]["refined_answers"]
        golden_data = data[i]["golden_answer"]
        if None in golden_data:
            golden_data = ["None"]
        p, r, f1, hit1 = ans_acc(predict_data, golden_data)
        p_mean += p
        r_mean += r
        f1_mean += f1
        hit1_mean += hit1
    print("count:", len(data))
    print("p_mean:", p_mean, p_mean / len(data))
    print("r_mean:", r_mean, r_mean / len(data))
    print("f1 mean:", f1_mean, f1_mean / len(data))
    print("hit1 mean:", hit1_mean, hit1_mean / len(data))
    return

def get_unavailable_data(data):
    count_unavailable=0
    available_data=[]
    for i in range(len(data)):
        # golden answer missing in the label
        if data[i]["golden_answer"] == []:
            count_unavailable += 1
        else:
            if data[i]["label_logical_form"]=="":
                count_unavailable+=1
            else:
                available_data.append(data[i])
    print("unavailable (golden answer missing or label logical form error):",count_unavailable)
    return available_data


def calculate_non_ex(data):
    count = 0
    non_ex_list = []
    for i in range(len(data)):
        searched_answers = []
        for j in range(len(data[i]["searched_answers"])):
            searched_answers += data[i]["searched_answers"][j]
        # searched_answers=data[i]["refined_answers"]
        if searched_answers == []:
            count += 1
            non_ex_list.append(i)
    print("non executable:", count)
    print("non executable rate:", count / len(data))


def exact_match(data):
    count_beam = 0
    for i in range(len(data)):
        for j in range(len(data[i]["predict_logical_form"])):
            if data[i]["predict_logical_form"][j].lower() == data[i]["label_logical_form"].lower():
                count_beam += 1
    print("beam match:", count_beam / len(data))


if __name__ == '__main__':
    # add args
    parser = argparse.ArgumentParser(description="Evaluate.")
    parser.add_argument('--dataset_type', type=str, default="WebQSP", required=True,
                        help="Type of dataset (e.g., 'WebQSP').")
    args = parser.parse_args()

    # load data
    path = "../main/output/" + args.dataset_type + "/" + args.dataset_type + "_final.json"
    path = os.path.abspath(path)
    data = load_json(path)

    new_data=[]
    for i in range(len(data)):
        if data[i]["label_logical_form"]!='':
            new_data.append(data[i])

    # calculate result
    print("Evaluate all:")
    calculate_non_ex(data)
    evaluate_results_logical_form(data)
    exact_match(data)

    print("")

    print("Evaluate available:")
    available_data=get_unavailable_data(data)
    calculate_non_ex(available_data)
    evaluate_results_logical_form(available_data)
    exact_match(available_data)



