from random import randint, random
from math import log, inf
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import sys
from collections import defaultdict
import threading

def update_true_count(item, true_count):
    '''
    Updates the exact count of an item in the data stream.

    Input:
    item:       The current item (integer) from the stream
    true_count: Dictionary tracking the exact frequency of each item

    Output:
    None (the dictionary is modified in place)
    '''
    true_count[item] +=1

def update_sticky_sampling(item, F_ss,p):
    '''
    Processes an item using the Sticky Sampling algorithm.
    If the item is already being tracked, its count is incremented.
    Otherwise, it is added to the tracked items with probability p.

    Input:
    item: The current item (integer) from the stream
    F_ss: Dictionary tracking the sampled items and their estimated frequencies
    p:    The sampling probability (r/n)

    Output:
    None (the dictionary is modified in place)
    '''
    if item in F_ss:
        F_ss[item] += 1
    else: 
        if random()<=p:
            F_ss[item] = 1


def update_count_min_sketch(item, a_list, b_list, sketch_table, w, d, threshold, F_cm):
    '''
    Processes an item using the Count-Min Sketch algorithm.
    Updates the sketch table using d independent hash functions and 
    adds the item to the frequent items set if its estimated frequency 
    exceeds the given threshold.

    Input:
    item:         The current item (integer) from the stream
    a_list:       List of 'a' parameters for the hash functions
    b_list:       List of 'b' parameters for the hash functions
    sketch_table: Matrix (d x w) representing the count-min sketch table
    w:            Number of columns in the sketch table
    d:            Number of rows (hash functions) in the sketch table
    threshold:    Frequency threshold to consider an item frequent (phi * n)
    F_cm:         Set of items identified as frequent by Count-Min Sketch

    Output:
    None (the sketch_table and F_cm are modified in place)
    '''

    hash_values = [((a_list[index]*item+b_list[index])%8191)%w for index in range(d)]
    min_val = inf

    for i,hash_value in enumerate(hash_values):
        sketch_table[i][hash_value]+= 1
        if sketch_table[i][hash_value] < min_val:
            min_val = sketch_table[i][hash_value]

    if min_val >= threshold: 
        F_cm.add(item)


def main():
    '''
    1. Input parsing and data structures initialization
    2. Streaming context management
    3. Execution of the algorithms on the streaming batches
    4. Results computation and prints
    
    Note:
    We defined the "process_batch" function inside "main()" to make tracking data easier. 
    By doing so, the function can directly see and update our main data structures 
    (like the "true_count" dictionary, "F_ss", "F_cm", and the "processed_items" list) 
    by exploiting types' mutability in Python. In particular, this allowed us to keep the 
    update functions outside of the "main()".
    '''

    assert len(sys.argv) == 8, "Usage: python pyspark-streaming-items.py <n> <phi> <epsilon> <delta> <d> <w> <portExp>"

    conf = SparkConf().setMaster("local[*]").setAppName('pyspark-streaming-items')
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 1)
    stopping_condition = threading.Event()

    n = int(sys.argv[1])
    phi = float(sys.argv[2])
    epsilon = float(sys.argv[3])
    delta = float(sys.argv[4])
    d = int(sys.argv[5])
    w = int(sys.argv[6])
    portExp = int(sys.argv[7])

    true_count = defaultdict(int)
    F_ss = {}
    F_cm = set()

    processed_items = [0]
    r = log(1/(delta*phi))/epsilon
    p = r/n
    threshold = phi*n

    a_list = [randint(1,8190) for _ in range(d)]
    b_list = [randint(0,8190) for _ in range(d)]
    sketch_table = [[0] * w for _ in range(d)]


    def process_batch(batch_rdd):
        '''
        Processes a single batch from the stream.
        Extracts items, updates their exact counts, and applies both
        Sticky Sampling and Count-Min Sketch algorithms.

        Input:
        batch_rdd: An RDD containing items as strings from the current time window

        Output:
        None
        '''

        if processed_items[0] >=n:
            return

        batch_items_strings = batch_rdd.collect()

        for item_str in batch_items_strings:
            if processed_items[0] >= n:
                break
                
            item = int(item_str)
            processed_items[0] += 1

            update_true_count(item, true_count)
            update_sticky_sampling(item, F_ss, p)
            update_count_min_sketch(item, a_list, b_list, sketch_table, w, d, threshold, F_cm)


        if processed_items[0] >= n:
            stopping_condition.set()

    stream = ssc.socketTextStream("algo.dei.unipd.it", portExp)
    stream.foreachRDD(process_batch)   

    ssc.start()

    stopping_condition.wait()
    ssc.stop(False, False)

    print("INPUT PARAMETERS")
    print(f"n = {n}")
    print(f"phi = {phi}")
    print(f"epsilon = {epsilon}")
    print(f"delta = {delta}")
    print(f"d = {d}")
    print(f"w = {w}")
    print(f"port = {portExp}")
    print()

    print("TRUE FREQUENT ITEMS")

    true_frequent = sorted([item for item, count in true_count.items() if count >= threshold])
    for item in true_frequent:
        print(f"Item = {item} True Freq = {true_count[item]}")
    print()

    print("STICKY SAMPLING")
    print(f"Size of dictionary = {len(F_ss)}")
    F_ss = sorted([key for key, value in F_ss.items() if value >= (phi-epsilon)*n])
    for item in F_ss:
        print(f"Item = {item} True Freq = {true_count[item]}")
    print()

    print("COUNT-MIN SKETCH")
    print(f"Size of F_CM = {len(F_cm)}")
    F_cm = sorted(list(F_cm))
    for item in F_cm:
        print(f"Item = {item} True Freq = {true_count[item]}")


if __name__ == "__main__":
    main()