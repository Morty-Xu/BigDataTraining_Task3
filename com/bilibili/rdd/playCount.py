from pyspark import SparkContext, SparkConf
import pandas as pd

conf = SparkConf().setAppName('playCount').setMaster('local[*]')
sc = SparkContext(conf=conf)

file = pd.read_csv('..\\..\\..\\bilibili-test.csv')

data = []

for row in file.itertuples():
    t = (getattr(row, 'up主'), getattr(row, '视频链接'), getattr(row, '三连量'), getattr(row, '播放量'), getattr(row, '标签'), getattr(row, '标题'))
    data.append(t)

rdd = sc.parallelize(data)
rdd_play = sc.parallelize(file['播放量'])

mean = int(rdd_play.mean())

sumText = '本月视频播放总量为：' + str(rdd_play.sum())
maxText = '本月视频播放量最高值为：' + str(rdd_play.max())
minText = '本月视频播放量最低值为：' + str(rdd_play.min())
meanText = '本月视频播放量平均值为：' + str(mean)

f = open('playCount.txt', 'w')
f.write(sumText + '\n')
f.write(maxText + '\n')
f.write(minText + '\n')
f.write(meanText + '\n')
f.write('以下是播放量大于平均值的视频' + '\n')
f.close()

filterRDD = rdd.filter(lambda x: int(x[3]) > mean)

output = filterRDD.sortBy(lambda x: x[3], False).collect()

df = pd.DataFrame(output, columns=['up主', '视频链接', '三连量', '播放量', '标签', '标题'])

df.to_csv('playCount.txt', mode='a', sep=',', header=True)