from pyspark import SparkContext, StorageLevel, SparkConf
import sys
import json
import csv
import itertools
from time import time
import math

start = time()
def process(entry):
    newe= entry[0].replace('\'', '').split(',')
    return (newe[0],newe[1])
#reading arguments
if len(sys.argv) != 5:
    print("Usage is incorrect")
    exit(-1)
else:
    filterthreshold = int(sys.argv[1])
    supp_threshold = int(sys.argv[2])
    input_file_path = sys.argv[3]
    output_file_path = sys.argv[4]
'''
#sample input
filterthreshold = 70
supp_threshold = 50
input_file_path = r"ub.csv"
output_file_path = r"output_task2.csv"
'''
con = SparkConf().setAll([('spark.executor.memory', '8g'), ('master','local'),('appName','new')])
sc = SparkContext(conf=con)
#start = time()
smallRdd = sc.textFile(input_file_path).map(lambda entry: entry.split('\n'))
#converting list into sets
small1Rdd=smallRdd.map(lambda entry: process(entry))
headers = small1Rdd.take(1)
#print(headers)
finalRdd = small1Rdd.filter(lambda entry: entry[0] != headers[0][0])
print(finalRdd.take(1))
#Writing frequentset output
#def writing_ouput(f_set):
def check_and_generate_frequent_klength_candidates(current_candidate_set, partition_support_threshold,original_baskets):

    current_k_frequent_candidates = {}
    current_k_frequents = set()

    for key, values in original_baskets.items():
        # print("inside for loop:")
        # print(type(values))
        # basket_value_set = frozenset([values])
        # print("current basket item: " + str(values))
        for candidate in current_candidate_set:
            # print("current frequent candidate: " + str(candidate))
            if candidate.issubset(values):
                # print("isSubset: " + "true")
                if candidate in current_k_frequent_candidates.keys():
                    current_k_frequent_candidates[candidate] += 1
                else:
                    current_k_frequent_candidates.update({candidate:1})
                    # current_k_frequent_candidates.update({1:candidate})

    # print(":::::::::::::::: frequent candidates :::::::::::::::::::::::::::")
    for key, value in current_k_frequent_candidates.items():
        # print(key)
        # print(value)
        if value >= partition_support_threshold:
            current_k_frequents.add(key)
    # print("::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::")
    current_k_frequents = frozenset(sorted(current_k_frequents))
    return current_k_frequents




def generate_klength_candidates(current_candidate_set,k):

    new_klength_candidate_set = set()

    # print("Current Candidate set in loop:")
    # print(current_candidate_set)
    current_candidate_list_flattened = frozenset(itertools.chain.from_iterable(current_candidate_set))
    # print("Flattened set:")
    # print(str(current_candidate_list_flattened))

    new_candidate = frozenset()
    for old_candidate in current_candidate_set:
        # print("old candidate: " + str(old_candidate))
        for single_item in current_candidate_list_flattened:
            # print("new single item: " + str(single_item))
            if single_item not in old_candidate:
                new_candidate = frozenset(sorted(old_candidate.union(frozenset([single_item]))))
                # print("new candidate: " + str(new_candidate))
                if len(new_candidate) == k:
                    k_minus_one_subsets = itertools.combinations(new_candidate, k-1)
                    # print("k-1 subsets: ")
                    # for subset in k_minus_one_subsets:
                    #     print(str(subset))
                    is_valid_candidate = True

                    for subset in k_minus_one_subsets:
                        subset_frozen = frozenset(subset)
                        # print("current subset: " + str(subset_frozen))
                        # print(subset_frozen in current_candidate_set)
                        if not subset_frozen in current_candidate_set:
                            # print("invalid")
                            is_valid_candidate = False
                            break

                    if is_valid_candidate:
                        new_klength_candidate_set.add(new_candidate)

    # print("Current Candidates in loop:")
    # for cd in new_k_candidate_set:
    #     print(cd)
    new_k_candidate_set = frozenset(sorted(new_klength_candidate_set))
    return new_k_candidate_set




def apriori(s_threshold,es, total_baskets_count):
    # print("********************************************************")
    # for key, values in baskets:
    #     print(values)
    # print("********************************************************")
    # Calculate singletons
    original_baskets = {}
    for k, v in es:
        original_baskets.update({k:frozenset(v)})
    print("total_baskets_count",total_baskets_count)
    partition_support_threshold = math.ceil((float(len(original_baskets))/total_baskets_count) * s_threshold)

    # print("Current parition threshold: " + str(partition_support_threshold))
    # print(original_baskets)
    all_frequent_items_set ={}
    # calculate singletons
    single_frequentitems_candidates = {}
    single_frequent_items = set()
    for k, v in original_baskets.items():
        # print("inside for loop for baskets: ")
        # print(values)
        for val in v:
            if val in single_frequentitems_candidates.keys():
                single_frequentitems_candidates[val] += 1
            else:
                single_frequentitems_candidates.update({val:1})

    for key, value in single_frequentitems_candidates.items():
        if value >= partition_support_threshold:
            single_frequent_items.add(frozenset([key]))

    single_frequent_items = frozenset(sorted(single_frequent_items))
    # print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$0$$$$$$$$$$$$$")
    # for fi in single_frequent_items:
    #     print(fi)

    all_frequent_items_set.update({1:single_frequent_items})

    # print(type(all_frequent_items_set))
    # print(all_frequent_items_set)

    # current_candidate_set =[]
    current_candidate_set = single_frequent_items
    current_frequent_items = set()
    # calculate all other frequent_item_sets starting from k=2

    k=2
    # print(current_candidate_list)
    while len(current_candidate_set) != 0 :
        # print("inside while")
        # print("Current k: " + str(k))
        current_candidate_set = generate_klength_candidates(current_candidate_set,k)
        # print("Current Candidate set: ")
        # print(str(current_candidate_set))
        # print("Current candidate length: " + str(len(current_candidate_set)))
        # for i in current_candidate_list:
        #     print(i)
        current_frequent_items = check_and_generate_frequent_klength_candidates(current_candidate_set, partition_support_threshold,original_baskets)
        # print("Current Frequent set: ")
        # print(str(current_frequent_items))
        # print("Current frequent length: " + str(len(current_frequent_items)))

        if len(current_frequent_items) != 0:
            all_frequent_items_set.update({k:current_frequent_items})

        k += 1
        current_candidate_set = current_frequent_items

    # print(all_frequent_items_set)

    # print(":::::::::::::::::::::::::::::::::::::::::")
    # print(type(all_frequent_items_set.values()))
    # print(list(all_frequent_items_set.values()))

    with open(output_file_path, "w+") as op:
        op.write("Candidates: " + '\n\n')
        # print(type(all_frequent_items_set.values()))
        for key, itemset in all_frequent_items_set.items():
            # print(type(itemset))
            # print(itemset)
            values = sorted([tuple(sorted(i)) for i in itemset])
            length = len(values)
            for index, tuple_to_write in enumerate(values):
                tuple_to_write_string = ''
                # tuple_to_write = tuple(sorted(value))
                if key == 1:
                    tuple_to_write_string = '(\'' + tuple_to_write[0] + '\')'
                else:
                    tuple_to_write_string = str(tuple_to_write)

                if index != length - 1:
                    tuple_to_write_string += ','

                op.write(tuple_to_write_string)

            op.write("\n\n")

    return list(all_frequent_items_set.values())


def count_frequent_candidates(basket, candidate_list):

    frequent_candidate_counts = []
    
    for candidate in candidate_list:
        # print(type(candidate))
        # print(candidate)
        if candidate.issubset(basket):
            # print("valid")
            frequent_candidate_counts.append((candidate, 1))

    return frequent_candidate_counts

#SON implementation
def son(fRdd, s_threshold, total_num_baskets):
    map1_task = fRdd \
        .mapPartitions(lambda es: apriori(s_threshold,es,total_num_baskets)) \
        .map(lambda r: (r, 1))
    # print(map_task_1)
    reduce1_task = map1_task.reduceByKey(lambda x1, y1: x1 + y1) \
        .map(lambda e: e[0])

    task1_candidates = reduce1_task.collect()

    task1_candidates_broadcasted = sc.broadcast(task1_candidates).value
    task1_candidates_broadcasted = frozenset(itertools.chain.from_iterable(task1_candidates_broadcasted))
    map2_task = fRdd.flatMap(lambda e: count_frequent_candidates(e[1], task1_candidates_broadcasted))
    # print(map_task_2.collect())
    reduce2_task = map2_task.reduceByKey(lambda x1, y1: x1 + y1)
    # print(reduce_task_2.collect())
    frequentItemsets = reduce2_task.filter(lambda entry: entry[1] >= supp_threshold) \
        .map(lambda entry: (len(entry[0]), frozenset([entry[0]]))) \
        .reduceByKey(lambda set1, set2: set1.union(set2)).sortByKey().collect() \
        # .flatMap(lambda entry: entry[1])\
    # .collect()
    # print("Final Frequent Itemsets: ")
    # print(frequent_itemsets)
    # print(type(frequent_itemsets[1]))

    with open(output_file_path, "a+") as op:
        op.write("Frequent Itemsets: " + '\n\n')
        # print(type(frequent_itemsets))
        for item_set in frequentItemsets:
            # print(itemset)
            # print(type(itemset[0]))
            values = sorted([tuple(sorted(i)) for i in item_set[1]])
            length = len(values)
            for index, tuple_to_write in enumerate(values):
                tuple_to_write_string = ''
                # print(itemset[0])
                # tuple_to_write = tuple(sorted(value))
                if item_set[0] == 1:
                    tuple_to_write_string = '(\'' + tuple_to_write[0] + '\')'
                else:
                    tuple_to_write_string = str(tuple_to_write)

                if index != length - 1:
                    tuple_to_write_string += ','

                op.write(tuple_to_write_string)

            op.write("\n\n")


    return frequentItemsets

finalRdd = finalRdd.map(lambda e: (e[0], e[1]))\
    .groupByKey()\
    .mapValues(lambda e: set(e)).filter(lambda e: len(e[1]) > filterthreshold).persist()    

total_number_baskets = finalRdd.count()
print(total_number_baskets)
print("apply son")
#applying SON
#supp_threshold=4
Resultset=son(finalRdd, supp_threshold, total_number_baskets)
end = time()
print("Duration: " + str(end-start))
