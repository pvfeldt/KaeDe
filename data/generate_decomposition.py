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

# relation: last relation, category: the last two relation
def further_extract_relations(relations):
    updated_relations = []
    categories = []
    for rel in relations:
        new_rel = rel.split(".")[-1]
        category = ",".join(rel.split(".")[-2:-1])
        new_rel = new_rel.replace("_", " ")
        category = category.replace("_", " ")
        updated_relations.append(new_rel)
        categories.append(category)
    return updated_relations, categories


def human_check(relation):
    result = False
    human_list = ["sibling", "married to", "spouse", "official", "holder", "parent", "children", "member", "actor",
                  "representatives", "president", "participant", "leader", "owner", "manager", "coach", "ruler",
                  "player", "winner", "author", "composer", "employee", "politician", "founder", "student"]
    for human in human_list:
        if relation in human or human in relation:
            result = True
            break
    return result

# define "what" or "who" to start the question
def define_question_start(question, relation, hop_type):
    question = question.lower()
    split_question_first = question.split(" ")[0]
    if hop_type == "final":
        if question.startswith("what year"):
            question_start = "What year"
        elif question.startswith("what years"):
            question_start = "What years"
        elif question.startswith("what time") and not question.startswith("what time zone") and not question.startswith(
                "what timezone"):
            question_start = "What time"
        elif split_question_first == "when":
            question_start = "When"
        elif human_check(relation) == True or split_question_first == "who":
            question_start = "Who"
        else:
            question_start = "What"
    else:
        if human_check(relation) == True:
            question_start = "Who"
        else:
            question_start = "What"
    return question_start


# generate question for a single hop
def generate_single_question(question, entity, relation, category, direction, hop, hop_type):
    prepositions = ["by", "at", "from", "to", "for", "as", "contains", " in", "of"]
    output_question = ""
    # consider directions
    ## backward->constraint statements
    if hop == 0:
        if direction == "backward":
            output_question = entity + " is the " + relation + " of the " + category + "."
        ## forward->questions
        elif direction == "forward":
            ## define question start
            question_start = define_question_start(question, relation, hop_type)
            ## consider prepositions, with prepositions->passive voice
            for symbol in prepositions:
                if relation.endswith(symbol):
                    output_question = question_start + " is " + category + " " + entity + " " + relation + "?"
                    break
            ## if no preposition
            if output_question == "":
                output_question = question_start + " is the " + relation + " of " + category + " " + entity + "?"
    elif hop > 0:
        if direction == "backward":
            if define_question_start(question, relation, hop_type) == "who" or human_check(relation) == True:
                output_question = "He/She is the " + relation + " of the " + category + "."
            else:
                output_question = "It is the " + relation + " of the " + category + "."
        elif direction == "forward":
            question_start = define_question_start(question, relation, hop_type)
            for symbol in prepositions:
                if relation.endswith(symbol):
                    output_question = question_start + " is the " + category + " " + relation + "?"
                    break
            if output_question == "":
                output_question = question_start + " is the " + relation + " of the " + category + "?"

    return output_question


# generate questions for path-level logical forms
def generate_path_level_questions(question, entity, relations, categories, directions):
    questions = []
    for i in range(len(relations)):
        hop_type = "not final"
        if i == len(relations) - 1:
            hop_type = "final"
        output_question = generate_single_question(question, entity, relations[i], categories[i], directions[i], i,
                                                   hop_type)
        questions.append(output_question)
    simple_questions = " ".join(questions)
    return simple_questions


def generate_entries(data):
    for i in range(len(data)):
        print("i=", i)
        if data[i]["logical_form"]=="":
            continue
        question=data[i]["question"]
        reasoning_information=data[i]["reasoning_information"]
        for j in range(len(reasoning_information)):
            relations=reasoning_information[j]["relation"]
            entity_id=reasoning_information[j]["entity"]
            if entity_id.startswith("m.") or entity.startswith("g."):
                entity=data[i]["golden_entities"][entity_id]
            else:
                entity=entity.replace("."," , ")
            directions=reasoning_information[j]["direction"]
            updated_relations, categories = further_extract_relations(relations)
            questions = generate_path_level_questions(question, entity, updated_relations, categories,directions)
            reasoning_information[j]["decomposed"]=questions
    return data


if __name__ == '__main__':
    # add args
    parser = argparse.ArgumentParser(description="Generate questions.")
    parser.add_argument('--dataset_type', type=str, default="WebQSP", required=True,
                        help="Type of dataset (e.g., 'WebQSP').")
    parser.add_argument('--split', type=str, default="train", required=True, choices=['train', 'test'],
                        help="Dataset split (train or test).")
    args = parser.parse_args()

    # load data
    path="../data/processed/"+args.dataset_type+"/"+args.dataset_type+"_"+args.split+"_extracted.json"
    path=os.path.abspath(path)
    data=load_json(path)

    # generate decomposed simple questions or statements
    generated_data=generate_entries(data)

    # output data
    output_path = "../data/processed/"+args.dataset_type+"/"+args.dataset_type+"_"+args.split+"_extracted.json"
    output_path = os.path.abspath(output_path)
    store_json(generated_data, output_path)
