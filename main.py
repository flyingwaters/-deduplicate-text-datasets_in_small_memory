import argparse
from find_overlap import *
parser = argparse.ArgumentParser(description='depulicate')
parser.add_argument('--query_file_pth', type=str,help ='query file with lines')
parser.add_argument('--large_dataset_pth', type=str,help='dataset file that waits to be queried')    
parser.add_argument('--batch_size', type=int,default=10 ,help="the batch num of large dataset")
args = parser.parse_args()

process = findOverlap(dataset_dir=args.query_file_pth,batch_size = args.batch_size)
query_files, c4_batched_list = process.prepare_data(input_dir=args.query_file_pth)
# # input is right
start_init=False
if start_init:
    process.make_suffix_batch(query_files)
    current_wd = os.path.abspath(__file__)
project_path = "/".join(current_wd.split("/")[:-1])
cache_dir = "%s/compare/{}"%(project_path)
print(query_files)
result = process.query_between_two_datasets(query_files, cache_dir, c4_batched_list, 100)

import json 
import os

#print(project_path)
result_save_pth = "%s/final_result.json"%(project_path)
#print(result_save_pth)
with open(result_save_pth, "w") as f:
    json.dump(result, f, ensure_ascii=False, indent=2)
