CUDA_VISIBLE_DEVICES=0 nohup python search.py \
--dataset_type WebQSP \
--golden True \
--facc1_path /data/brr/ChatKBQA/data/common_data/facc1/ >> search_WebQSP.log

CUDA_VISIBLE_DEVICES=1 nohup python search.py \
--dataset_type CWQ \
--golden True \
--facc1_path /data/brr/ChatKBQA/data/common_data/facc1/ >> search_CWQ.log