#encoding:utf-8
#Author:Fenglongyu
#Date:2022/05/16 22:28
#Description: call rust Api to get duplicate number between two datasets 
import os
import time

def project_abspath():
    this_file_absolute_path = os.path.abspath(__file__)
    project_path = "/".join(this_file_absolute_path.split("/")[:-1])
    return project_path

def make_suffix(path):
    project_pth = project_abspath()
    start = time.time()
    return_n = os.system("python3 {}/scripts/make_suffix_array.py {}".format(project_pth,path))
    end = time.time()
    if return_n==0:
        print("suffixing end! {} consuming {} s".format(path, end-start))
    else:
        print("fail to make suffix, {}".format(path))
        
def find_overlap(A, query_str):
    project_pth = project_abspath()
    query_str = query_str.replace("\"", "")
    cmd_order = "python3 %s/scripts/count_occurrences.py --suffix %s --query \"%s\" "%(project_pth, A, query_str)
    cmd_p = os.popen(cmd_order)
    tmp = cmd_p.read()
    result = tmp.split()
    return (" ".join(result[:-5]), int(result[-1]), result, A, tmp)

def lookup_filelist(query,file_list):
    '''
    # tested
    query string in C4 or some other dataset consist of quite a few files 
    return {query: 'times'}
    '''
    result = {}
    # too short return directly
    if len(query.split())<5:
        return result

    for file_path in file_list:
        tmp = find_overlap(file_path, query)
        if tmp[1] != 0:
            # 
            if tmp[0] not in list(result.keys()):
                result[tmp[0]] = int(tmp[1])
            else:
                result[tmp[0]]+=int(tmp[1])
            
    if result!={}:
        print("query: {} lookup in {} \n result is {}: ".format(query, file_list,  result))
    return result

def split_sentence(line, num):
    "according to num, split the sentence into subsentences"
    result =set()
    if len(line.strip().split()) <= num:
        return [line.strip()]
    else:
        can = line.strip().split()
        for start in range(0,num):

            itert = start
            while itert+num <= len(can):
                tmp = " ".join(can[itert: itert+num])
                result.add(tmp)
                itert+=num
        return list(result)

def merge_dicts(dict_a, dict_b):
    "merge a and b dict, the same key will be added the old one return dict_b"
    for key in dict_a.keys():
        if key in dict_b.keys():
            dict_b[key]+=dict_a[key]
        else:
            dict_b[key]=dict_a[key]
    return dict_b



if __name__ =="__main__":
    # brief unit test
    # print(merge_dicts({"A":4, "B":4}, {"A":3,"C":4}))
    # print(project_abspath())
    # print(find_overlap("/raid/yiptmp/huggingface-models/c4-en-textlines/loaded/c4-train.00134-of-01024.fly", "hello world"))
    result = lookup_filelist(", 8 , 11 , 14 - eicosatetraynoic acid ( ETYA , Ro 3 - 1428", ["/raid/yiptmp/huggingface-models/c4-en-textlines/loaded/c4-train.00134-of-01024.fly", "/raid/yiptmp/huggingface-models/c4-en-textlines/loaded/c4-train.00135-of-01024.fly"])
    print("result", result)
