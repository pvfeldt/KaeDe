# KaeDe

The repository of paper **KaeDe: Progressive Generation of Logical Forms via Knowledge-Aware Question Decomposition for Improved KBQA [EMNLP 25 Findings]**.

## 0 Setup

### 0.1 Knowledge Base Setup

Freebase serves as the knowledge base background for the two datasets (WebQSP and CWQ) used in this study. Detailed instructions for downloading and setting it up in Virtuoso are available [here](https://github.com/dki-lab/Freebase-Setup). 

Clone the setup repository and start up the service with the following instruction.

```
cd Freebase-Setup
python3 virtuoso.py start 3001 -d [/path/to/virtuoso/db/files]
```

Close the service as follows.

```
python3 virtuoso.py stop 3001
```

### 0.2 FACC1 Annotation Download

FACC1 annotation serves the function of mapping generated entities to existing entities in Freebase, which can be downloaded [here](https://github.com/HXX97/GMT-KBQA/tree/main/data/common_data/facc1).

### 0.3 Environment Setup

The versions specified in the requirements are primarily intended to ensure compatibility with the latest version of Llamafactory for further LLM fine-tuning and generation within this framework.

```
conda create -n KaeDe python=3.10
conda activate KaeDe
pip install -r requirements.txt
```

### 0.4 Dataset and LLM Backbone

The datasets used in this work include WebQSP and CWQ.

| Dataset | Download Link                                                |
| ------- | ------------------------------------------------------------ |
| WebQSP  | [WebQSP Link](https://aka.ms/WebQSP)                         |
| CWQ     | [CWQ Link](https://www.dropbox.com/scl/fo/nqujvpg2gc4y0ozkw3wgr/AOzjVEsdUhv2Fx2pamfJlSw?rlkey=746t7xehfqxf1zr867nxiq8aq&e=1&st=n9e0fa7f) |

The LLM backbones include Llama-based and DeepSeek-based models. 

| LLM Model                    | Download Link                                                |
| ---------------------------- | ------------------------------------------------------------ |
| Llama 2 7B                   | [Llama 2 7B Link](https://huggingface.co/meta-llama/Llama-2-7b-chat) |
| Llama 2 13B                  | [Llama 2 13B Link](https://huggingface.co/meta-llama/Llama-2-13b-chat) |
| DeepSeek LLM 7B              | [DeepSeek LLM 7B Link](https://huggingface.co/deepseek-ai/deepseek-llm-7b-chat) |
| DeepSeek R1 Distill Llama 8B | [DeepSeek R1 Distill Llama 8B Link](https://huggingface.co/deepseek-ai/DeepSeek-R1-Distill-Llama-8B) |

## 1 Dataset Processing

Run process.sh to process the original [WebQSP](https://aka.ms/WebQSP) and [CWQ](https://www.dropbox.com/scl/fo/nqujvpg2gc4y0ozkw3wgr/AOzjVEsdUhv2Fx2pamfJlSw?rlkey=746t7xehfqxf1zr867nxiq8aq&e=1&st=n9e0fa7f) datasets.

```
cd data
bash process.sh
```

For separate steps in process.sh, please follow:

Initially extract information from the original datasets and convert the LFs (path-level and graph-level LFs). 

```
python process_dataset.py --dataset_type [dataset] -- split [split]

# [dataset]="WebQSP" or "CWQ"
# [split]="train" or "dev" or "test"
```

Generate intermediate expressions (sub-questions and statements) for each question.

```
python generate_decomposition.py --dataset_type [dataset] --split [split]

# [dataset]="WebQSP" or "CWQ"
# [split]="train" or "dev" or "test"
```

Wrap the intermediate expressions (sub-questions and statements) and LFs in template to generate prompts.

```
python generate_entries.py --dataset_type [dataset] --split [split]

# [dataset]="WebQSP" or "CWQ"
# [split]="train" or "dev" or "test"
```

## 2 LLM Fine-Tuning

Run train.sh to perform fine-tuning.

```
cd main
bash train.sh
```

An example is demonstrated in the train.sh. To specify other relevant hyperparameters, please vary the contents in the box with the following instructions.

```
CUDA_VISIBLE_DEVICES=0 nohup python -u train_bash.py \
--stage sft \
--model_name_or_path [/path/to/LLM] \
--do_train  \
--dataset_dir input/[dataset] \
--dataset train_data \
--template [template] \
--finetuning_type lora \
--lora_target [PEFT modules] \
--output_dir [/path/to/output/checkpoint/]  \
--overwrite_cache \
--per_device_train_batch_size [train batch size] \
--gradient_accumulation_steps 4  \
--lr_scheduler_type cosine \
--logging_steps 10 \
--save_steps 1000 \
--learning_rate [learning rate]  \
--num_train_epochs [epoch] \
--plot_loss >> [/path/to/log/file]

# [dataset]="WebQSP" or "CWQ"
# [template]="llama2" (for Llama 2 models) or "deepseek" (for DeepSeek LLM 7B model) or "deepseek3" (for DeepSeek R1 Distill Llama 8B model)
# [PEFT modules]="all" (for all linear layers) or "q_proj,v_proj" (for query and value projection layers)
# Note: The lora rank is 8 by default. To change the rank, please add --lora_rank [rank] to specify the hyperparameter.
```

## 3 Logical Form Generation

Run response.sh to perform logical form generation.

```
cd main
bash response.sh
```

An example is demonstrated in the response.sh. To specify other relevant hyperparameters, please vary the contents in the box with the following instructions.

```
CUDA_VISIBLE_DEVICES=0 nohup python -u  generate_response.py \
--dataset_type [dataset] \
--model_name_or_path [/path/to/LLM] \
--template [template] \
--adapter_name_or_path [/path/to/stored/checkpoint/] \
--num_beams [beam size] >> [/path/to/log/file]

# [dataset]="WebQSP" or "CWQ"
# [template]="llama2" (for Llama 2 models) or "deepseek" (for DeepSeek LLM 7B model) or "deepseek3" (for DeepSeek R1 Distill Llama 8B model)
# [beam size]={3,5,8} (8 for WebQSP dataset, 5 for CWQ dataset)
```

## 4 Execution & Unsupervised Retrieval

Run search.sh to perform the logical form execution (as well as  the unsupervised retrieval).

```
cd main
bash search.sh
```

 To change the setting, please edit:

```
CUDA_VISIBLE_DEVICES=1 nohup python search.py \
--dataset_type [dataset] \
--golden [whether golden] \
--facc1_path [/path/to/downloaded/FACC1/annotation] >> [/path/to/log/file]

# [dataset]="WebQSP" or "CWQ"
# [golden]=True or False
```

## 5 Evaluation

Run evaluate.sh to perform the final evaluation.

```
cd evaluation
bash evaluate.sh
```

For separate steps in evaluate.sh, please follow:

Append golden answers.

```
python process_results.py --dataset_type [dataset]

# [dataset]="WebQSP" or "CWQ"
```

Final evaluate.

```
python evaluate.py --dataset_type [dataset]

# [dataset]="WebQSP" or "CWQ"
```

## Acknowledgements

This work benefits from [LLaMA-Factory](https://github.com/hiyouga/LLaMA-Factory), [ChatKBQA](https://github.com/LHRLAB/ChatKBQA), [SimCSE](https://github.com/princeton-nlp/SimCSE) and [Freebase-Setup](https://github.com/dki-lab/Freebase-Setup). The authors would like to express their gratitude for the resources provided.