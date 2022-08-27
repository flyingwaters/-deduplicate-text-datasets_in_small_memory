# coding=utf-8
from call_rust import make_suffix, merge_dicts
import os
from typing import List
from tqdm import tqdm
from acrossfiles import pipeline

class findOverlap:
    def __init__(self, dataset_dir:str="/raid/yiptmp/huggingface-models/c4-en-textlines/loaded",
                        batch_size:int =100):
        self.dataset_dir = dataset_dir
        self.batch_size = batch_size 
        print("batch_len:", self.batch_size)

    def list_files(self, dataset_dir,split="fly")->list:
        "return batch_files list ready to make suffix and find_overlap "
        path_list = []
        count_n = 1
        tmp_batch=[]
        print(dataset_dir)
        for _,_,files in os.walk(dataset_dir):
           
            files.sort()
            for file_name in files:
                if file_name.endswith(split):
                    path =os.path.join(dataset_dir, file_name)
                    tmp_batch.append(path)
                    if count_n%self.batch_size==0: 
                        path_list.append(tmp_batch)
                        tmp_batch=[]
                    count_n+=1
            # last batch append
            path_list.append(tmp_batch)    
        return path_list
    
    # tested
    def make_suffix_batch(self,files_list:List[str]):
        "make some files suffix "
        for path in files_list:
            make_suffix(path)
        
        
    def delete_batchsuffix(self):
        "delete the suffix process generated files"
        import os
        os.system("rm -rf {}/*part*".format(self.dataset_dir))
        os.system("rm -rf {}/*table.bin".format(self.dataset_dir))

    def query_between_two_datasets(self, 
                                   query_files_list:List[str],
                                   cache_dir:str,
                                   batched_files_list:List[list], length_bytes):
        '''
        some list of lines text files to be taken as query files
        some loaded  files to be taken as suffix file list
        (1) iter_search every bacth in the batched list
        (2) suffix the batch files in one batch
        (3) concurrence find all query deplicated sentence num between
            query files sentence and batched_files_list in this batch
        (4) delete the suffixed files at this batch 
        '''
        query_result = {}
        
        
        for idx,batch in enumerate(batched_files_list):
            # make suffix
            
            self.make_suffix_batch(batch)
            # qyery
            print("total {} epochs, {} epoch:".format(len(batched_files_list), idx))
            for file_b in batch:
                for file_a in query_files_list:
                    batch_result = pipeline(file_a, file_b, cache_dir=cache_dir.format(idx),length_bytes=length_bytes)
                    if file_a in query_result.keys():
                        query_result[file_a] = merge_dicts(batch_result, query_result[file_a])
                    else:
                        query_result[file_a] = batch_result

            print("{} epoch result is:".format(idx),query_result)
            # clear the batch
            self.delete_batchsuffix()
            
        return query_result

    # tested
    def prepare_data(self, input_dir:str="/tmp/blurb"):
        '''
        customize function to provide input_files_list
        and provide part of loaded c4_files_list
        '''
        # batch_size =100
        blurb_train = self.list_files(input_dir, split="train")
        blurb_test = self.list_files(input_dir, split="test")
        c4_list = self.list_files(self.dataset_dir, split="fly")
        all_query_files = []
        for i in blurb_test:
            all_query_files.extend(i)
        for j in blurb_train:
            all_query_files.extend(j)

        return all_query_files, c4_list

if __name__ =="__main__":
    # set the batch_size 
    process = findOverlap(batch_size = 10)
    query_files, c4_batched_list = process.prepare_data()
    # # input is right
    start_init=False
    if start_init:
        process.make_suffix_batch(query_files)
    cache_dir = "/raid/zyftest/project/deduplicate-text-datasets/compare/{}"
    print(query_files)
    result = process.query_between_two_datasets(query_files, cache_dir, c4_batched_list, 100)

    import json 
    import os
    current_wd = os.path.abspath(__file__)
    project_path = "/".join(current_wd.split("/")[:-1])
    #print(project_path)
    result_save_pth = "%s/final_result.json"%(project_path)
    #print(result_save_pth)
    with open(result_save_pth, "w") as f:
         json.dump(result, f, ensure_ascii=False, indent=2)
    
    
    
    
        