########################################
## Template Code for Big Data Analytics
## assignment 1 - part I, at Stony Brook Univeristy
## Fall 2017


import sys
from abc import ABCMeta, abstractmethod
from multiprocessing import Process, Manager
from pprint import pprint
import numpy as np
from random import random


##########################################################################
##########################################################################
# PART I. MapReduce

class MyMapReduce:  # [TODO]
    __metaclass__ = ABCMeta

    def __init__(self, data, num_map_tasks=5, num_reduce_tasks=3):  # [DONE]
        self.data = data  # the "file": list of all key value pairs
        self.num_map_tasks = num_map_tasks  # how many processes to spawn as map tasks
        self.num_reduce_tasks = num_reduce_tasks  # " " " as reduce tasks

    ###########################################################
    # programmer methods (to be overridden by inheriting class)

    @abstractmethod
    def map(self, k, v):  # [DONE]
        print("Need to override map")

    @abstractmethod
    def reduce(self, k, vs):  # [DONE]
        print("Need to override reduce")

    ###########################################################
    # System Code: What the map reduce backend handles

    def mapTask(self, data_chunk, namenode_m2r):  # [DONE]
        # runs the mappers and assigns each k,v to a reduce task
        for (k, v) in data_chunk:
            # run mappers:
            mapped_kvs = self.map(k, v)
            # assign each kv pair to a reducer task
            for (k, v) in mapped_kvs:
                namenode_m2r.append((self.partitionFunction(k), (k, v)))

    def partitionFunction(self, k):
        # [TODO]
        # given a key returns the reduce task to send it
        if type(k) == type(1):
            return k % self.num_reduce_tasks
        else:
            return len(k) % self.num_reduce_tasks

    def reduceTask(self, kvs, namenode_fromR):
        # [TODO]
        # sort all values for each key (can use a list of dictionary)
        # call reducers on each key with a list of values
        # and append the result for each key to reduced_tuples
        # [TODO]
        sortedbykey = sorted(kvs, key=lambda tupl: tupl[0])

        kv_dict = dict()
        reduced_kv_dict = dict()

        #add the key and list of counts per key to a dictionary
        for key,value in sortedbykey:
            if key in kv_dict:
                kv_dict[key].append(value)
            else:
                kv_dict[key] = [value]

        #for each item run the reduce task and append the values to the final namenode list
        for key, value in kv_dict.items():
            reduce_kvs = self.reduce(key, value)
            if reduce_kvs is not None:
                namenode_fromR.append(reduce_kvs)

    def runSystem(self):  # [TODO]
        # runs the full map-reduce system processes on mrObject
        # the following two lists are shared by all processes
        # in order to simul0. ate the communication
        # [DONE]
        namenode_m2r = Manager().list()     # stores the reducer task assignment and
                                            # each key-value pair returned from mappers
                                            # in the form: [(reduce_task_num, (k, v)), ...]
        namenode_fromR = Manager().list()   # stores key-value pairs returned from reducers
                                            # in the form [(k, v), ...]

        # divide up the data into chunks accord to num_map_tasks, launch a new process
        # for each map task, passing the chunk of data to it.
        # hint: if chunk contains the data going to a given maptask then the following
        #      starts a process
        #      p = Process(target=self.mapTask, args=(chunk,namenode_m2r))
        #      p.start()
        #  (it might be useful to keep the processes in a list)
        # [TODO]
        map_processes_List = []

        num_chunks = self.num_map_tasks

        for chunk in self.getChunks():
            mapp = Process(target=self.mapTask, args=(chunk, namenode_m2r))
            mapp.start()
            # join map task processes back
            # [TODO]
            mapp.join()
            map_processes_List.append(mapp)


        # print output from map tasks
        # [DONE]
        print("namenode_m2r after map tasks complete:")
        pprint(sorted(list(namenode_m2r)))

        # "send" each key-value pair to its assigned reducer by placing each
        # into a list of lists, where to_reduce_task[task_num] = [list of kv pairs]
        #to_reduce_task = [[] for i in range(self.num_reduce_tasks)]
        # [TODO]
        to_reduce_task = [[arow[1] for arow in list(namenode_m2r) if arow[0] == i ] for i in range(self.num_reduce_tasks)] #nested list comprehension to extract the list of kv pairs as mentioned above

        # launch the reduce tasks as a new process for each.
        # [TODO]
        reduce_processes_list = []
        for i in range(self.num_reduce_tasks):
            reducep = Process(target=self.reduceTask, args=(to_reduce_task[i],namenode_fromR))
            reducep.start()

        # join the reduce tasks back
        # [TODO]
            reducep.join()
            reduce_processes_list.append(reducep)
        # print output from reducer tasks
        # [DONE]
        print(" ***************** RESULTS ********************** ")
        print("namenode_m2r after reduce tasks complete:")
        print(" ************************************************ ")
        namenode_fromR = self.formatSetDifferenceResult(namenode_fromR)
        print(sorted(list(namenode_fromR)))

        # return all key-value pairs:
        # [DONE]
        return namenode_fromR

    def getChunks(self):
        """
        based on number of map tasks divide data into chunks
        :return: data chunk
        """
        for achunkposition in range(0, len(self.data), self.num_map_tasks):
            yield self.data[achunkposition: achunkposition + self.num_map_tasks]

    def formatSetDifferenceResult(self, set_diff):
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

##########################################################################
##########################################################################
##Map Reducers:

class WordCountMR(MyMapReduce):  # [DONE]
    # the mapper and reducer for word count
    def map(self, k, v):  # [DONE]
        counts = dict()
        for w in v.split():
            w = w.lower()  # makes the word-count case-insensitive
            try:  # try/except KeyError is just a faster way to check if w is in counts:
                counts[w] += 1
            except KeyError:
                counts[w] = 1
        return counts.items()

    def reduce(self, k, vs):  # [DONE]
        return (k, np.sum(vs))


class SetDifferenceMR(MyMapReduce):  # [TODO]
    # contains the map and reduce function for set difference
    # Assume that the mapper receives the "set" as a list of any primitives or comparable objects
    def map(self, k, v):
        counts = dict()
        for word in v:
            counts[word] = k

        return counts.items()

    def reduce(self, k, vs):
        """

        :param k: key
        :param vs: list of values
        :return: only values which are in R
        """
        if len(vs) == 1 and vs[0] == 'R':
            return (k,'R')
        else:
            return None


##########################################################################
##########################################################################


if __name__ == "__main__":  # [DONE: Uncomment peices to test]
    ###################
    ##run WordCount:
    print
    "\n\n*****************\n Word Count \n*****************\n"
    data = [(1, "The horse raced past the barn fell"),
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
    mrObject = WordCountMR(data, 4, 3)
    mrObject.runSystem()

    ####################
    ##run SetDifference
    # (TODO: uncomment when ready to test)
    print
    "\n\n*****************\n Set Difference\n*****************\n"
    data1 = [('R', ['apple', 'orange', 'pear', 'blueberry']),
     		 ('S', ['pear', 'orange', 'strawberry', 'fig', 'tangerine'])]

    data2 = [('R', [x for x in range(50) if random() > 0.5]),
    		 ('S', [x for x in range(50) if random() > 0.75])]

    print("random generated set data #2 >> \n", data2)

    mrObject = SetDifferenceMR(data1, 2, 2)
    mrObject.runSystem()

    mrObject = SetDifferenceMR(data2, 2, 2)
    mrObject.runSystem()

