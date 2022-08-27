import os

def across_similar(file_a:str, file_b:str, cache_dir:str, length_bytes=100)-> int:
    '''
     cargo run across-similar --data-file-1 [dataset1] --data-file-2 [dataset2]
      --length-threshold [num_bytes] --cache-dir [where/to/save] --num-threads [N]
      return the total count num of subsentence of length_bytes in file_a that also emerge in file_b 
    '''
    # file_a filter 
    # file_b filter 
    # try:
    cmd_order = '''
                cargo run across-similar --data-file-1 {} --data-file-2 {} --length-threshold {} --cache-dir {} --num-threads 30
                '''.format(file_a, file_b, length_bytes, cache_dir)
    return_content = os.popen(cmd_order).read()
    return int(return_content.split()[-1])
    

def duplicate_subsentence(file_a, file_b, cache_dir, length_bytes=100):
    cmd_order_2 = '''
                  cargo run collect --data-file {} --length-threshold {} --cache-dir {} 
                  '''.format(file_a, length_bytes, cache_dir)
    result = os.popen(cmd_order_2).read().split()
    try:
        split_sign = result.index('out')
    except:
        print(result)
    index_set = result[split_sign+1:]
    if len(index_set) % 2!=0:
        raise ValueError("some mistakes happen in collect or across-similar")
    else:
        start_list =  index_set[0::2] 
        end_list = index_set[1::2]
        result = {}
        data = open(file_a, "rb").read()
        data_2 = open(file_b, "rb").read()
        
        for start,end in zip(start_list, end_list):
            print(len(data[int(start):int(end)]))
            print("num: ", data_2.count(data[int(start):int(end)]))
            result[data[int(start):int(end)]] = data_2.count(data[int(start):int(end)])
        
        return result
                        
        
def pipeline(file_a, file_b, cache_dir, length_bytes=300):

    # 0--> no duplicate 
    dup_num = across_similar(file_a, file_b, cache_dir, length_bytes)
    print("num of duplicates: {}".format(dup_num))
    if dup_num == 0:
        return {}
    else:
        result_1 = duplicate_subsentence(file_a,file_b,cache_dir, length_bytes)
        return result_1


if __name__ == "__main__":
    
    cache_dir = "/raid/zyftest/project/deduplicate-text-datasets/compare"
    file_a = "/tmp/blurb/BC2GM.test"
    file_c = "/tmp/blurb"
    file_b = "/raid/yiptmp/huggingface-models/c4-en-textlines/loaded/c4-train.00109-of-01024.fly"
    process = findOverlap(1)
    process.make_suffix_batch(["/raid/yiptmp/huggingface-models/c4-en-textlines/loaded/c4-train.00109-of-01024.fly"])
    # across_similar(file_a, file_b, cache_dir)
    pipeline(file_a, file_b, cache_dir, length_bytes=100)
    