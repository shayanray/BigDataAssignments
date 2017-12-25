from pyspark import SparkConf, SparkContext
from random import random

def mymapper(a_set):
    #print(" line in mymapper ----------------------------------------  ", a_set)
    map_values = []
    set_key = a_set[0]
    set_values = a_set[1]

    for aValue in set_values:
        map_values.append((aValue, set_key),)

    #print(" output from mymapper ----------------------------------------  ", map_values)
    return map_values

def myreducer(values):
    #print(" line in myreducer ----------------------------------------  ", (values))
    reduce_values =[]
    set_key = values[0]
    set_values = values[1]

    if len(set_values) == 1 and set_values == 'R': # R - S
        reduce_values.append(('R-S', set_key),)

    return reduce_values


def formatSetDifferenceResult(set_diff):
    """
    formats the final output for set difference
    :return:
    """
    if set_diff[0] is not None and set_diff[0][1] == 'R':
        formattedResult = list()
        for atupl in set_diff:
            formattedResult.append(atupl[0])
        return formattedResult

    return set_diff
if  __name__ == "__main__":

    #create Spark context with Spark Configuration
    sconf = SparkConf().setAppName("Pyspark wordcount and set-difference")
    sc = SparkContext(conf=sconf)
    """
            -------------------------Start of WordCount implementation below ----------------------------------
    """
    word_count_data = [(1, "The horse raced past the barn fell"),
            (2, "The complex houses married and single soldiers and their families"),
            (3, "There is nothing either good or bad, but thinking makes it so"),
            (4, "I burn, I pine, I perish"),
            (5, "Come what come may, time and the hour runs through the roughest day"),
            (6, "Be a yardstick of quality."),
            (7, "A horse is the projection of peoples' dreams about themselves - strong, powerful, beautiful"),
            (8,
             "I believe that at the end of the century the use of words and general educated opinion will have altered so much that one will be able to speak of machines thinking without expecting to be contradicted."),
            (9, "The car raced past the finish line just in time."),
            (10, "Car engines purred and the tires burned.")]

    # your code here:
    word_count_rdd = sc.parallelize(word_count_data)

    #get wordcounts after map and reduce
    wordCounts = word_count_rdd.flatMap(lambda line:line[1].split(" ")).map(lambda word: (word.lower(),1)).reduceByKey(lambda v1, v2: v1 + v2).collect()


    #list = wordCounts.collect()
    print(" ***************** WORD COUNT RESULT ************ ")
    print(sorted(wordCounts))
    print(" ************************************************ ")


    """
        -------------------------End of WordCount implementation below ----------------------------------
    """

    """
       -------------------------Start of Set Difference implementation below ----------------------------------
    """
    # your code here:
    setdiff_data1 = [('R', ['apple', 'orange', 'pear', 'blueberry']),
                     ('S', ['pear', 'orange', 'strawberry', 'fig', 'tangerine'])]

    setdiff_data2 = [('R', [x for x in range(50) if random() > 0.5]),
                     ('S', [x for x in range(50) if random() > 0.75])]

    set_diff_rdd1 = sc.parallelize(setdiff_data1)

    set_diff_result1 = set_diff_rdd1.flatMap(mymapper).reduceByKey(lambda v1, v2 : v1 + v2).filter(lambda settype: settype[1] == 'R').distinct().collect()
    print(" ***************** SET DIFFERENCE #1 ************ ")
    print(formatSetDifferenceResult(set_diff_result1))
    print(" ************************************************ ")

    set_diff_rdd2 = sc.parallelize(setdiff_data2)

    set_diff_counts2 = set_diff_rdd2.flatMap(mymapper).reduceByKey(
        lambda v1, v2: v1 + v2).filter(lambda settype: settype[1] == 'R').distinct()
    set_diff_result2 = set_diff_counts2.collect()
    print(" ***************** SET DIFFERENCE #2 ************ ")
    print(formatSetDifferenceResult(set_diff_result2))
    print(" ************************************************ ")

    """
        -------------------------End of Set Difference implementation below ----------------------------------
    """
