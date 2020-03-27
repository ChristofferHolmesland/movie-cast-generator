current = graph.filter(graph.node == leonardo)
next = current.select(F.explode(current.edges))
next = next.withColumn("distance", F.lit(1))
next = next.join(graph, next.key == graph.node).drop(graph.node)
next2 = next.select("key", "value", F.col("distance") + 1, F.explode(next.edges))


import time
def bfs():
    start_time = time.time()
    current = graph.filter(graph.node == leonardo)
    next = current.select(F.col("node").alias("start"), F.explode(current.edges).alias("step1", "value1"))
    next = next.join(graph, next.step1 == graph.node).drop(graph.node)
    next2 = next.select("*", F.explode(next.edges).alias("step2", "value2")).drop(next.edges)
    next2 = next2.join(graph, next2.step2 == graph.node).drop(graph.node)
    next3 = next2.select("*", F.explode(next2.edges).alias("step3", "value3")).drop(next2.edges)
    next3 = next3.join(graph, next3.step3 == graph.node).drop(graph.node)
    next4 = next3.select("*", F.explode(next3.edges).alias("step4", "value4")).drop(next3.edges)
    #next4 = next4.join(graph, next4.step4 == graph.node).drop(graph.node)
    res = next4.count()
    end_time = time.time()
    print("Time spent: " + str(end_time - start_time))
    return res

next5 = next4.select("*", F.explode(next4.edges).alias("step5", "value5")).drop(next4.edges)
next5 = next5.join(graph, next5.step5 == graph.node).drop(graph.node)

cond = (next4.step1 == christian) | (next4.step2 == christian) | (next4.step3 == christian) | (next4.step4 == christian)



next.count() == 350
cond = (next.step1 == christian)
next.filter(cond).count() == 1

next2.count() == 58 339
cond = (next4.step1 == christian) | (next4.step2 == christian)
next2.filter(cond).count() == 372

next3.count() == 8 901 575
cond = (next4.step1 == christian) | (next4.step2 == christian) | (next4.step3 == christian)
next3.filter(cond).count() == 77 087

next4.count() == 1 354 994 561
cond = (next4.step1 == christian) | (next4.step2 == christian) | (next4.step3 == christian) | (next4.step4 == christian)
next4.filter(cond).count() == 12 798 876



import pyspark.sql.types as _type


def bfs(start, goal, graph, n):
    def __calc_score(row):
        # columns 1,3,5... are actor ids
        # columns 2,4,6... are the values
        
    
    _calc_score = F.udf(__calc_score, _type.FloatType)

    current = graph.filter(graph.node == start)
    current = current.select(F.col("node").alias("start"), F.explode(current.edges).alias("step1", "value1"))
    current = current.join(graph, current.step1 == graph.node).drop(graph.node)
    for i in range(1, n):
        current = current.select("*", F.explode(current.edges).alias("step{}".format(i+1), "value{}".format(i+1))).drop(current.edges)
        current = current.join(graph, current["step{}".format(i+1)] == graph.node).drop(graph.node)
    
    columns = F.struct([current[x] for x in current.columns])
    current = current.withColumn("score", _calc_score(columns))
    
    return current

s = time.time()
res = bfs(leonardo, christian, graph, 3)
s1 = time.time()
res.count()
s2 = time.time()
