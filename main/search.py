import re
import os
import sys
from tqdm import tqdm
from h5py.h5ds import get_label
from sqlalchemy.sql.operators import truediv
import time
import argparse

path = ".."
abs_path = os.path.abspath(path)
sys.path.append(abs_path)
from utils.sparql_execution import execute_query_with_odbc, get_label_with_odbc, get_2hop_relations_with_odbc_wo_filter
from utils.logic_form_util import lisp_to_sparql
from entity_retrieval import surface_index_memory
import difflib
import itertools
import shutil
import json
from simcse import SimCSE
from utils.json_utils import load_json, load_json_1_line

model = SimCSE("princeton-nlp/unsup-simcse-roberta-large")


# os.environ["HF_ENDPOINT"] = "https://hf-mirror.com"

def is_number(t):
    t = t.replace(" , ", ".")
    t = t.replace(", ", ".")
    t = t.replace(" ,", ".")
    try:
        float(t)
        return True
    except ValueError:
        pass
    try:
        import unicodedata  # handle ascii
        unicodedata.numeric(t)  # string of number --> float
        return True
    except (TypeError, ValueError):
        pass
    return False


def type_checker(token: str):
    """Check the type of a token, e.g. Integer, Float or date.
       Return original token if no type is detected."""

    pattern_year = r"^\d{4}$"
    pattern_year_month = r"^\d{4}-\d{2}$"
    pattern_year_month_date = r"^\d{4}-\d{2}-\d{2}$"
    if re.match(pattern_year, token):
        if int(token) < 3000:  # >= 3000: low possibility to be a year
            token = token + "^^http://www.w3.org/2001/XMLSchema#dateTime"
    elif re.match(pattern_year_month, token):
        token = token + "^^http://www.w3.org/2001/XMLSchema#dateTime"
    elif re.match(pattern_year_month_date, token):
        token = token + "^^http://www.w3.org/2001/XMLSchema#dateTime"
    else:
        return token

    return token


def date_post_process(date_string):
    """
    When quering KB, (our) KB tends to autoComplete a date
    e.g.
        - 1996 --> 1996-01-01
        - 1906-04-18 --> 1906-04-18 05:12:00
    """
    pattern_year_month_date = r"^\d{4}-\d{2}-\d{2}$"
    pattern_year_month_date_moment = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"

    if re.match(pattern_year_month_date_moment, date_string):
        if date_string.endswith('05:12:00'):
            date_string = date_string.replace('05:12:00', '').strip()
    elif re.match(pattern_year_month_date, date_string):
        if date_string.endswith('-01-01'):
            date_string = date_string.replace('-01-01', '').strip()
    return date_string


def denormalize_s_expr_new(normed_expr,
                           entity_label_map,
                           type_label_map,
                           surface_index):
    expr = normed_expr

    convert_map = {
        '( greater equal': '( ge',
        '( greater than': '( gt',
        '( less equal': '( le',
        '( less than': '( lt'
    }

    for k in convert_map:
        expr = expr.replace(k, convert_map[k])
        expr = expr.replace(k.upper(), convert_map[k])

    # expr = expr.replace(', ',' , ')
    tokens = expr.split(' ')

    segments = []
    prev_left_bracket = False
    prev_left_par = False
    cur_seg = ''
    numt = 0
    for t in tokens:
        numt = numt + 1
        if t == '[':
            prev_left_bracket = True
            if cur_seg:
                segments.append(cur_seg)
        elif t == ']':
            prev_left_bracket = False
            cur_seg = cur_seg.strip()

            # find in linear origin map
            processed = False

            if not processed:
                if cur_seg.lower() in type_label_map:  # type
                    cur_seg = type_label_map[cur_seg.lower()]
                    processed = True
                else:  # relation or unlinked entity
                    if ' , ' in cur_seg:
                        if is_number(cur_seg):
                            # check if it is a number
                            cur_seg = cur_seg.replace(" , ", ".")
                            cur_seg = cur_seg.replace(" ,", ".")
                            cur_seg = cur_seg.replace(", ", ".")
                        else:
                            # view as relation
                            cur_seg = cur_seg.replace(' , ', ',')
                            cur_seg = cur_seg.replace(',', '.')
                            cur_seg = cur_seg.replace(' ', '_')
                        processed = True
                    else:
                        search = True
                        if is_number(cur_seg):
                            search = False
                            cur_seg = cur_seg.replace(" , ", ".")
                            cur_seg = cur_seg.replace(" ,", ".")
                            cur_seg = cur_seg.replace(", ", ".")
                            cur_seg = cur_seg.replace(",", "")
                        elif len(entity_label_map.keys()) != 0:
                            search = False
                            if cur_seg.lower() in entity_label_map:
                                cur_seg = entity_label_map[cur_seg.lower()]
                            else:
                                similarities = model.similarity([cur_seg.lower()], list(entity_label_map.keys()))
                                merged_list = list(zip([v for _, v in entity_label_map.items()], similarities[0]))
                                sorted_list = sorted(merged_list, key=lambda x: x[1], reverse=True)[0]
                                if sorted_list[1] > 0.2:
                                    cur_seg = sorted_list[0]
                                else:

                                    search = True
                        if search:
                            facc1_cand_entities = surface_index.get_indexrange_entity_el_pro_one_mention(cur_seg,
                                                                                                         top_k=50)
                            if facc1_cand_entities:
                                temp = []
                                for key in list(facc1_cand_entities.keys())[1:]:
                                    if facc1_cand_entities[key] >= 0.001:
                                        temp.append(key)
                                if len(temp) > 0:
                                    cur_seg = [list(facc1_cand_entities.keys())[0]] + temp
                                else:
                                    cur_seg = list(facc1_cand_entities.keys())[0]

            segments.append(cur_seg)
            cur_seg = ''
        else:
            if prev_left_bracket:
                # in a bracket
                cur_seg = cur_seg + ' ' + t
            else:
                if t == '(':
                    prev_left_par = True
                    segments.append(t)
                else:
                    if prev_left_par:
                        if t in ['ge', 'gt', 'le', 'lt']:  # [ge, gt, le, lt] lowercase
                            segments.append(t)
                        else:
                            segments.append(t.upper())  # [and, join, r, argmax, count] upper case
                        prev_left_par = False
                    else:
                        if t != ')':
                            if t.lower() in entity_label_map:
                                t = entity_label_map[t.lower()]
                            else:
                                t = type_checker(t)  # number
                        segments.append(t)
    combinations = [list(comb) for comb in itertools.islice(
        itertools.product(*[item if isinstance(item, list) else [item] for item in segments]), 10000)]

    exprs = [" ".join(s) for s in combinations]

    return exprs


def execute_normed_s_expr_from_label_maps(normed_expr,
                                          entity_label_map,
                                          type_label_map,
                                          surface_index
                                          ):
    try:
        denorm_sexprs = denormalize_s_expr_new(normed_expr,
                                               entity_label_map,
                                               type_label_map,
                                               surface_index
                                               )
    except:
        return 'null', []

    query_exprs = [d.replace('( ', '(').replace(' )', ')') for d in denorm_sexprs]
    for query_expr in query_exprs[:500]:
        try:
            # invalid sexprs, may leads to infinite loops
            if 'OR' in query_expr or 'WITH' in query_expr or 'PLUS' in query_expr:
                denotation = []
            else:
                sparql_query = lisp_to_sparql(query_expr)
                denotation = execute_query_with_odbc(sparql_query)
                denotation = [res.replace("http://rdf.freebase.com/ns/", '') for res in denotation]
                if len(denotation) == 0:
                    ents = set()
                    for item in sparql_query.replace('(', ' ( ').replace(')', ' ) ').split(' '):
                        if item.startswith("ns:m."):
                            ents.add(item)
                    addline = []
                    for i, ent in enumerate(list(ents)):
                        addline.append(f'{ent} rdfs:label ?en{i} . ')
                        addline.append(f'?ei{i} rdfs:label ?en{i} . ')
                        addline.append(f'FILTER (langMatches( lang(?en{i}), "EN" ) )')
                        sparql_query = sparql_query.replace(ent, f'?ei{i}')
                    clauses = sparql_query.split('\n')
                    for i, line in enumerate(clauses):
                        if line == "FILTER (!isLiteral(?x) OR lang(?x) = '' OR langMatches(lang(?x), 'en'))":
                            clauses = clauses[:i + 1] + addline + clauses[i + 1:]
                            break
                    sparql_query = '\n'.join(clauses)
                    denotation = execute_query_with_odbc(sparql_query)
                    denotation = [res.replace("http://rdf.freebase.com/ns/", '') for res in denotation]
        except:
            denotation = []
        if len(denotation) != 0:
            break
    if len(denotation) == 0:
        query_expr = query_exprs[0]
    return query_expr, denotation


def execute_normed_s_expr_from_label_maps_rel(normed_expr,
                                              entity_label_map,
                                              type_label_map,
                                              surface_index
                                              ):
    try:
        denorm_sexprs = denormalize_s_expr_new(normed_expr,
                                               entity_label_map,
                                               type_label_map,
                                               surface_index
                                               )
    except:
        return 'null', []
    query_exprs = [d.replace('( ', '(').replace(' )', ')') for d in denorm_sexprs]

    for d in tqdm(denorm_sexprs[:50]):
        query_expr, denotation = try_relation(d)
        if len(denotation) != 0:
            break

    if len(denotation) == 0:
        query_expr = query_exprs[0]

    return query_expr, denotation


def try_relation(d):
    ent_list = set()
    rel_list = set()
    denorm_sexpr = d.split(' ')
    for item in denorm_sexpr:
        if item.startswith('m.'):
            ent_list.add(item)
        elif '.' in item:
            rel_list.add(item)
    ent_list = list(ent_list)
    rel_list = list(rel_list)
    cand_rels = set()
    for ent in ent_list:
        in_rels, out_rels, _ = get_2hop_relations_with_odbc_wo_filter(ent)
        cand_rels = cand_rels | set(in_rels) | set(out_rels)
    cand_rels = list(cand_rels)
    if len(cand_rels) == 0 or len(rel_list) == 0:
        return d.replace('( ', '(').replace(' )', ')'), []
    similarities = model.similarity(rel_list, cand_rels)
    change = dict()
    for i, rel in enumerate(rel_list):
        merged_list = list(zip(cand_rels, similarities[i]))
        sorted_list = sorted(merged_list, key=lambda x: x[1], reverse=True)
        change_rel = []
        for s in sorted_list:
            if s[1] > 0.01:
                change_rel.append(s[0])
        change[rel] = change_rel[:15]
    for i, item in enumerate(denorm_sexpr):
        if item in rel_list:
            denorm_sexpr[i] = change[item]
    combinations = [list(comb) for comb in itertools.islice(
        itertools.product(*[item if isinstance(item, list) else [item] for item in denorm_sexpr]), 10000)]
    exprs = [" ".join(s) for s in combinations][:4000]
    query_exprs = [d.replace('( ', '(').replace(' )', ')') for d in exprs]
    for query_expr in query_exprs:
        try:
            # invalid sexprs, may leads to infinite loops
            if 'OR' in query_expr or 'WITH' in query_expr or 'PLUS' in query_expr:
                denotation = []
            else:
                sparql_query = lisp_to_sparql(query_expr)
                denotation = execute_query_with_odbc(sparql_query)
                denotation = [res.replace("http://rdf.freebase.com/ns/", '') for res in denotation]
                if len(denotation) == 0:

                    ents = set()

                    for item in sparql_query.replace('(', ' ( ').replace(')', ' ) ').split(' '):
                        if item.startswith("ns:m."):
                            ents.add(item)
                    addline = []
                    for i, ent in enumerate(list(ents)):
                        addline.append(f'{ent} rdfs:label ?en{i} . ')
                        addline.append(f'?ei{i} rdfs:label ?en{i} . ')
                        addline.append(f'FILTER (langMatches( lang(?en{i}), "EN" ) )')
                        sparql_query = sparql_query.replace(ent, f'?ei{i}')
                    clauses = sparql_query.split('\n')
                    for i, line in enumerate(clauses):
                        if line == "FILTER (!isLiteral(?x) OR lang(?x) = '' OR langMatches(lang(?x), 'en'))":
                            clauses = clauses[:i + 1] + addline + clauses[i + 1:]
                            break
                    sparql_query = '\n'.join(clauses)
                    denotation = execute_query_with_odbc(sparql_query)
                    denotation = [res.replace("http://rdf.freebase.com/ns/", '') for res in denotation]
        except:
            denotation = []
        if len(denotation) != 0:
            break
    if len(denotation) == 0:
        query_expr = query_exprs[0]
    return query_expr, denotation


# if all ids, return true
# if not all ids, return true
def check_ids(answers):
    count = 0
    for ans in answers:
        if ans.startswith("m.") or ans.startswith("g."):
            count += 1
    if count == len(answers):
        return True
    else:
        return False


def search_in_freebase(data, golden_data, golden, output_path):
    with open(output_path, "a", encoding="utf-8") as file:
        for i in range(0, len(data)):
            print("searching i=", i, "/", len(data))
            if golden:
                reversed_dict = {v: k for k, v in golden_data[i]["golden_entities"].items()}
                entity_label_map = reversed_dict
            else:
                entity_label_map = {}
            logical_forms = data[i]["predicted_logical_forms"]
            if logical_forms == []:
                entry = {"index": data[i]["index"], "question": data[i]["question"],
                         "label_logical_form": data[i]["label_logical_form"], "predict_logical_form": logical_forms,
                         "searched_answers": []}
                json.dump(entry, file)
                file.write("\n")
                file.flush()
            else:
                answers = []
                combined_answers = []
                # entity retrieval
                for j in range(len(logical_forms)):
                    if ", " in logical_forms[j] and " , " not in logical_forms[j]:
                        # llama 3 8b
                        logical_forms[j] = logical_forms[j].replace(", ", " , ")
                    count = 0
                    try:
                        _, answer_ids = execute_normed_s_expr_from_label_maps(logical_forms[j], entity_label_map, {},
                                                                              surface_index)
                        answer = []
                        for ans_id in answer_ids:
                            ans_label = ""
                            if ans_id.startswith("m.") or ans_id.startswith("g."):
                                ans_label = get_label_with_odbc(ans_id)
                            else:
                                answer.append(ans_id)
                            if ans_label == None:
                                answer.append(ans_id)
                            else:
                                answer.append(ans_label)
                        answers.append(answer)
                        print("answer ent:", answers)
                        combined_answers += answer
                    except Exception as e:
                        print("Error:", e)
                        pass
                # relation retrieval
                answers_rel = []
                if combined_answers == [] or check_ids(combined_answers):
                    for j in range(len(logical_forms)):
                        if ", " in logical_forms[j] and " , " not in logical_forms[j]:
                            # llama 3 8b
                            logical_forms[j] = logical_forms[j].replace(", ", " , ")
                        count = 0
                        try:
                            _, answer_ids = execute_normed_s_expr_from_label_maps_rel(logical_forms[j],
                                                                                      entity_label_map, {},
                                                                                      surface_index)
                            answer = []
                            for ans_id in answer_ids:
                                ans_label = ""
                                if ans_id.startswith("m.") or ans_id.startswith("g."):
                                    ans_label = get_label_with_odbc(ans_id)
                                else:
                                    answer.append(ans_id)
                                if ans_label == None:
                                    answer.append(ans_id)
                                else:
                                    answer.append(ans_label)
                            answers.append(answer)
                            if check_ids(answer) == False and answer != []:
                                answers_rel.append(answer)
                            # max iteration=3 if answer obtained
                            if len(answers_rel) == 2:
                                break
                            print("answer rel:", answers)
                        except Exception as e:
                            print("Error:", e)
                            pass
                entry = {"index": data[i]["index"], "question": data[i]["question"],
                         "label_logical_form": data[i]["label_logical_form"], "predict_logical_form": logical_forms,
                         "searched_answers": answers}
                json.dump(entry, file)
                file.write("\n")
                file.flush()


if __name__ == '__main__':
    # add args
    parser = argparse.ArgumentParser(description="Execute and perform unsupervised retrieval.")
    parser.add_argument('--dataset_type', type=str, default="WebQSP", required=True,
                        help="Type of dataset (e.g., 'WebQSP').")
    parser.add_argument('--golden', type=bool, required=True,
                        help="Whether use golden entity.")
    parser.add_argument('--facc1_path', type=str, required=True,
                        help="Path to FACC1 annotation.", default="../entity_retrieval/facc1/")
    args = parser.parse_args()

    # load data
    path = "../main/output/" + args.dataset_type + "/" + args.dataset_type + "_result.jsonl"
    path = os.path.abspath(path)
    data = load_json_1_line(path)

    # load FACC1
    surface_index = surface_index_memory.EntitySurfaceIndexMemory(
        args.facc1_path + "/entity_list_file_freebase_complete_all_mention",
        args.facc1_path + "/surface_map_file_freebase_complete_all_mention",
        args.facc1_path + "/freebase_complete_all_mention")

    # load golden entity path
    golden_path="../data/processed/"+args.dataset_type+"/"+args.dataset_type+"_test_extracted.json"
    golden_path=os.path.abspath(golden_path)
    golden_data=load_json(golden_path)
    golden=args.golden

    # search
    output_path="../main/output/"+args.dataset_type+"/"+args.dataset_type+"_searched.jsonl"
    output_path=os.path.abspath(output_path)
    search_in_freebase(data,golden_data,golden,output_path)
