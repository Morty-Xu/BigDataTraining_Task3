from pyspark import SparkContext, SparkConf
import pandas as pd

# 统计标签字段出现的次数，并以 DataFrames的形式输出到 highFreWord.csv文件中

conf = SparkConf().setAppName('highFreWord').setMaster('local[*]')
sc = SparkContext(conf=conf)

data = pd.read_csv('..\\..\\..\\bilibili.csv', usecols=['标签'])

rdd = sc.parallelize(data['标签'])

flatMapRDD = rdd.flatMap(
    lambda row: str(row).replace(',', ' ').replace('"', ' ').split(' '))

filterRDD = flatMapRDD.filter(lambda x: x != '')

output = filterRDD.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], False).collect()

df = pd.DataFrame(output, columns=['标签名', '出现次数'])

df.to_csv('highFreWord.csv', sep=',', header=True, index=True)
