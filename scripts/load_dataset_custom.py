
#!/usr/bin/env python
#encoding:utf-8
#Author:Fenglongyu
#Date:2022/05/14 12:56
#Description:

# Copyright 2021 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import tensorflow as tf
import os
import struct
import numpy as np
from transformers import GPT2Tokenizer, T5Tokenizer
import multiprocessing as mp
# import argparse
pre_sep = b"\xff\xff"
post_sep = b""
args_tokenize=False
args_tokenizer='t5'
if args_tokenize:
        if args_tokenizer == 'gpt2':
            tokenizer = GPT2Tokenizer.from_pretrained('gpt2')
        elif args_tokenizer == 't5':
            print("t5")
            tokenizer = T5Tokenizer.from_pretrained('t5-small')
        else:
            raise
UID = 0
def sep():
    global UID
    UID += 1
    return pre_sep+struct.pack("<I", UID)+post_sep

def tok(x):
    if args_tokenize:
        out = tokenizer.encode(x.decode("utf8"))
        out = np.array(out, dtype=np.uint16).view(np.uint8).tobytes()
    else:
        out = x
    return out

def load_custom(dataset_info, save_dir,split):
    data_pth, dataset_name = dataset_info
# parser = argparse.ArgumentParser(description='Load a dataset.')
# parser.add_argument('--data_dir', type=str, default="")
# parser.add_argument('--save_dir', type=str, default="/tmp/c4/")
# parser.add_argument('--name', type=str, default="C4")
# parser.add_argument('--split', type=str, default="example")
# parser.add_argument('--tokenize', action='store_true',default=False)
# parser.add_argument('--tokenizer', type=str, default="t5")
# parser.add_argument('--pre_sep', type=bytes, default=b"\xff\xff")
# parser.add_argument('--post_sep', type=bytes, default=b"")
# args = parser.parse_args()
    ds = tf.data.TextLineDataset([data_pth])
    assert isinstance(ds, tf.data.Dataset)
    if not os.path.exists(save_dir):
        os.mkdir(save_dir)
    fout = open(os.path.join(save_dir, dataset_name+"."+split), "wb")

    # with mp.get_context("fork").Pool(mp.cpu_count()) as p:
    sizes = [0]
        # text = p.map(tok,ds1)
    for x in ds:
        next_line = sep()+x.numpy()
        fout.write(next_line)
        sizes.append(sizes[-1]+len(next_line))

    open(os.path.join(save_dir,dataset_name+"."+split+".size"), "wb").write(np.array(sizes,dtype=np.uint64).tobytes())
    print("finish: ", dataset_name)
    
def load_blurb():
    blurb_pth ="/raid/zyftest/project/contamination_scifive/blurb_dataset" 
    save_dir = '/tmp/blurb'
    import os
    for _,_,file_names in os.walk(blurb_pth):
        for file_name in file_names:
            split_name = file_name.split('_')[1]
            dataset_name = file_name.split('_')[0]
            data_pth = os.path.join(blurb_pth, file_name)
            load_custom((data_pth,dataset_name),save_dir,split_name)
def load_c4():
    c4_pth = "/raid/yiptmp/huggingface-models/c4-en-textlines/c4/en"
    result_dir = '/raid/yiptmp/huggingface-models/c4-en-textlines/loaded'
    # fly: take care of the consume of aiming dir, whether it is adequate for processing result
    #   
    import tqdm
    import functools
    import multiprocessing 
    pool = multiprocessing.Pool(20)
    load_c4 = functools.partial(load_custom, save_dir=result_dir, split="fly")

    all_params = []
    for _,_,file_names in os.walk(c4_pth):
        
        for file_name in file_names:
            dataset_name = file_name
            data_pth = os.path.join(c4_pth, file_name)
            all_params.append((data_pth, dataset_name)) 
    
    print(all_params)
    print(len(all_params))
    print("start!#####################################")
    list(tqdm.tqdm(pool.imap(load_c4, all_params),total=1032))
    pool.close()
    pool.join()

if __name__ =="__main__":
   load_c4()


