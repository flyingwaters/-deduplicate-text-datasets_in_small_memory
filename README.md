## 在超大文本数据中,搜索定长的重复子串

这是bigscience T0 paper 的数据污染分析,所用的算法.具体可以搜索相关论文.
源头来源于以下paper
["Deduplicating Training Data Makes Language Models Better"](https://arxiv.org/abs/2107.06499) by Katherine Lee, Daphne Ippolito, Andrew Nystrom, Chiyuan Zhang, Douglas Eck, Chris Callison-Burch and Nicholas Carlini.

本文基于以下项目修改而来.用于较小的内存cpu,使用.

```
@inproceedings{lee2021deduplicating,
      title={Deduplicating Training Data Makes Language Models Better}, 
      author={Katherine Lee and Daphne Ippolito and Andrew Nystrom and Chiyuan Zhang and Douglas Eck and Chris Callison-Burch and Nicholas Carlini},
    booktitle = "Proceedings of the 60th Annual Meeting of the Association for Computational Linguistics",
    year = "2022",
    publisher = "Association for Computational Linguistics"
}
```
(1) 所有待搜索数据集, 应保存在一个目录下,并且分为多个文件.

(2) query file, 应是lines file. 
以上的文件,都应该按照Deduplicating Training Data Makes Language Models Better安装环境和python包,并且对待搜索和被搜索数据处理, 进行-- make suffix, 具体可看origin_depulicate.


-------
-------
### 用法

完成(1),(2) 后
使用
```
python main.py --query_file_pth 需要搜索的文件path --large_dataset_pth 待搜索的大数据文件目录地址 --batch_size 设置大数据文件目录的batch_num --subseq_len 需要搜索的subseqlen
``` 
结果在final_result.json中, 本项目只是batched
"Deduplicating Training Data Makes Language Models Better"

(1)删除重复 

(2) 查找等功能,需要依照原项目执行.




wechat: 343123814