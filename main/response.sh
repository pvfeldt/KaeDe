CUDA_VISIBLE_DEVICES=1 nohup python -u  generate_response.py \
--dataset_type CWQ \
--model_name_or_path ../llm_model/llama2-7b-chat \
--template llama2  \
--adapter_name_or_path ../checkpoint/CWQ/llama2-7b \
--num_beams 5 >>response_CWQ.log