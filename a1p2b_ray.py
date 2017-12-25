"""
Assignment 2b, blogs
"""

from pyspark import SparkContext, SparkConf
import os
import re
from time import time


def dataByIndustry(fullFileTuple):
    """
    read every file at a time, each entry is a (filename, filecontents) tuple
    use regex to clean-up whitespace
    then use regex to accept only \w(alphanum and _) and "<" ">" "," "/"

    for every match(date-post) in the regex, check if any of the industries occur. If occurence found,
    put to a dictionary of industry type and occurence in each post
    each item in the dictionary is a key of the industry and value is a tuple of tuples

    Now use the combiner strategy to merge any similar yyyy-mon values within an industry inside the map function
    before sending it to reducer

    if line contains <post> or not check if matching industry
    :param fullFile:
    :return:
    """

    yyyy_mon = None

    decodedFile = fullFileTuple[1]

    filterPattern1 = re.compile("\s\s+");
    filterPattern2 = re.compile("[^\w<>/,]+");
    date_pattern = re.compile("<date>[\s\S]*?</date>");
    date_post_pattern = re.compile("<date>[\s\S]*?<\/post>")

    intOutput = re.sub(filterPattern1, " ", decodedFile) # convert multi-spaces to single space
    output = re.sub(filterPattern2, " ", intOutput)  # accept these character  ,<>/ and alpha numeric words and _


    matchList = re.findall(date_post_pattern, output.lower()) # get everything between <date>...</post> each post data comprises of <date></date> and <post></post>
    industry_dict = dict()
    for match in matchList:

        lastDate = re.findall( date_pattern, match)
        yyyy_mon = str(lastDate).split(",")[2][:4] + "-" + str(lastDate).split(",")[1]

        # need not extract post as match should be in <date></post>
        for aIndustry in industries.value:
            countPerIndPerPost = len(re.findall("\W"+aIndustry+"\W", match, re.IGNORECASE))
            if aIndustry in industry_dict and countPerIndPerPost > 0:
                oldVal = industry_dict.get(aIndustry)
                newVal = oldVal + ((yyyy_mon, countPerIndPerPost),)
                industry_dict[aIndustry] = newVal
            elif countPerIndPerPost > 0:
                industry_dict[aIndustry] = ((yyyy_mon, countPerIndPerPost),)

    if len(industry_dict) == 0:
        return tuple()
    else:
        return combiner(industry_dict)

def combiner(industryDct):
    """
    merges any similar yyyy-mon values within an industry before sending it to reducer
    :param industryDct: each item has an industry as key and tuple of tuples as values
    :return:
    """
    result_tpl = tuple()
    if len(industryDct.items()) != 0:
        for key, values in industryDct.items():
            yymmdict = dict()
            aIndustryList = list()
            for aTupl in values:
                if aTupl[0] in yymmdict:
                    existingVal = yymmdict[aTupl[0]]
                    yymmdict[aTupl[0]] = existingVal + aTupl[1]
                else:
                    yymmdict[aTupl[0]] = aTupl[1]

            for kc, vc in yymmdict.items():
                aIndustryList.append((kc, vc), )
            if key is not None:
                result_tpl = result_tpl + ((key, tuple(aIndustryList)),)

    if len(result_tpl) > 0:
        return result_tpl


def myreducer(oldTuple, newTuple):
    """
    check if there are any common yyyy-mon groups then merge else set to dict and pass.
    across map outputs merge the industry-wise yyyy-mon group counts

    :param oldTuple:
    :param newTuple:
    :return: industry-wise merged counts
    """
    reducedDict = dict()
    for v1 in oldTuple:
        reducedDict[v1[0]] = v1[1]

    for v2 in newTuple:
        if v2[0] in reducedDict:
            oldval = reducedDict.get(v2[0])
            newValue = oldval + v2[1]
            reducedDict[v2[0]] = newValue
        else:
            reducedDict[v2[0]] = v2[1]

    return tuple(reducedDict.items())

if __name__ == "__main__":

    sconf = SparkConf().setAppName("2b. blog analysis ")
    #sconf.setMaster("local[8]")
    sc = SparkContext(conf=sconf)
    fullPathToFiles = "/blogs-sample"


    # ------ start of solution i -------------------
    fileNames = os.listdir(fullPathToFiles)
    fileNames_fullpath = [ fullPathToFiles + fileName for fileName in fileNames]

    filesRDD = sc.parallelize(fileNames)

    distinct_industries = set(filesRDD.map(lambda file:file.split(".")).map(lambda filepart: filepart[3]).distinct().collect())
    industries = sc.broadcast(distinct_industries)

    print("distinct industries 2b(i) >>>>>>>>>>>>>>>>>>>>>>>>>>> ",distinct_industries)
    # ------ end of solution i -------------------

    # ------ start of solution ii -------------------
    t0 = time()
    all_file_linesRDD = sc.wholeTextFiles(fullPathToFiles)
    answer = all_file_linesRDD.flatMap(dataByIndustry).reduceByKey(myreducer).collect()
    print(" \n\n final answer 2b(ii) >>>>>>>>>>>>>>>>>>>>>>>>>>> ", answer)

    print("time taken to completed 2b for filecount ",len(fileNames)," is >>>>>>>>>>>>>>>>>>>>>>>>>> ", (time() - t0) , " seconds.")
    # ------ end of solution ii -------------------
