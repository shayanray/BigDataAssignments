import io
import numpy as np
import zipfile
from PIL import Image #this is an image reading library
from numpy.linalg import svd
from numpy.linalg import norm
from pyspark import SparkContext, SparkConf
from tifffile import TiffFile
from pprint import pprint
import hashlib

def getImgsEucDist(filename, alowDImg):
    """
    Calculate the euclidean distance between the 2 images and their corresponding list of images
    :param filename:
    :param alowDImg:
    :return:
    """
    #getthe 3b imagenames to be printed
    a3blowDDict = lowDfor3bimgsGlobalVar.value
    
    #get all the matching files whose distance is to be calculated
    similarFileMappingDict = img2DictGlobalVar.value
    
    #store the euclidean sistance for each similar image mapping
    sortedEucDistance = list()
    for key, values in similarFileMappingDict.items():
        #print(" For File Name ...", key)
        for eachSimilarImg in values:
            #print("\n eachSimilarImg ,,, ", eachSimilarImg)

            if eachSimilarImg == filename:
                sortedEucDistance.append((key + ">>" + filename, norm(a3blowDDict[key] - alowDImg)), )

    return sortedEucDistance

def svdTransform(fileTuples):
    global Vtb,mub, stdb
    """
    calculate the Vt, my and std for a large sample and re-use it for PCA/SVD of all the other sub-images found
    :param fileTuples: file tuple consists of filename and image vector
    
    :return:
    """
    if fileTuples is not None:
        
        #store img vector separately to tranform it with SVD
        imgMatrix = list()
        fileNames = list()
        for afileTuple in fileTuples:
            
            
            
            imgMatrix.append(afileTuple[1])
            fileNames.append(afileTuple[0])


        
        #calculate mean and standard deviation to standardize SVD of all images

        mu, std = np.mean(imgMatrix, axis=0), np.std(imgMatrix, axis=0)
        #find the zscore
        imgMatrix_zs = (imgMatrix - mu) / std
        
        # run singular value decomposition.
        
        
        U, s, Vh = svd(imgMatrix_zs, full_matrices=1)

        # need the transposed V for matrix multiplication later
        Vt = np.matrix.transpose(Vh[:10,:]) # get first 10 rows/dimensions of V matrix and transpose 
        
        
        # low_dim_p = 10 # reduce to 10 dimensions
        # img_diffs_zs_lowdim = U[:, 0:low_dim_p] #only features truncated
        #
        # #since each image is in order return a tuple of the parent image-subimage# and lowered dimensions
        # # need this to group by
        # idx = 0
        # lowDImgs = list()
        # for eachImg in img_diffs_zs_lowdim:
        #     lowDImgs.append((fileNames[idx]+"-"+str(idx), eachImg),)
        #     idx += 1

        #print("\n---------------------------------------------",(Vt,mu, std))

        #broadcast the variables Vt, mu, std to be used later
        Vtb = sc.broadcast(Vt)
        mub = sc.broadcast(mu)
        stdb = sc.broadcast(std)
        return (Vtb.value,mub.value, stdb.value), #[:10,:]


def transformEach(filename, filecontent):
    """
    use mu and std to standardize every image 
    calculate zscore for each image
    find out the corresponding low dimensional vector for each image 

    :param filename: get the original file name
    :param filecontent: original image vector
    :return: filename, lowDvector
    """
    Vtbs = Vtb.value #Vt, Mu, Std tuple
    mubs = mub.value
    stdbs = stdb.value
    #print("\n\n.... Vtbs >>>> ", Vtbs)
    #print("\n\n.... mubs >>>> ", mubs)
    #print("\n\n.... stdbs >>>> ", stdbs)
    imgMatrix_zs = (filecontent - mubs) / stdbs #
    lowD = np.matmul(imgMatrix_zs, Vtbs)
    #print("lowD after matmul >>>>>> ", lowD)
    return (filename, lowD),


def getSimilarFiles(files):
    """
    the list of similar files  
    3b - match the 4 files in the broadcasted variable and send those files back as a tuple of filename, file contents
    :param files: the list of similar files corresponding to a hashcode
    :return: return those files that are required in the answer
    """
    ans3bFiles = answer3b.value
    # files to be used as per question

    similarFiles = list()
    for aFile in ans3bFiles:
        if aFile in files:
            similarFiles.append((aFile, files),)

    
    
    return similarFiles

def performLSH(filename, signature):
    """
    3b for every file, split into bands and rows, hash on the entire band, put to respective buckets for similarity match
    :return:
    """

    """
    
    (small sample):
    -----------------
    bands = 14
    rows(hashChunkSize) = int(128/14) = 9

    (large sample AWS): 
    -----------------
    bands = 12
    rows(hashChunkSize) = int(128/12) = 10
    
    """

    bands = 12 
    rowsize = int(len(signature)/bands) #represents total contents of band
    #print("\n rowsize ... >>> ", rowsize)

    # in steps of bandsize get the chunk to hash
    bandNum = 0
    hashedList = list()
    for aByteStartPosn in range(0,len(signature), (rowsize)):
        aRow = signature[aByteStartPosn : aByteStartPosn + (rowsize)]
        m = hashlib.md5()
        aRowStr = ''.join(str(e) for e in aRow)
        aRowBytes = aRowStr.encode('utf-8')
        m.update(aRowBytes)

        # adding a band number to end of hexdigest essentially ensures that the hashedkey has a bandnumber tied to it
        # hence documents are matched at every band bucket.
        hashedValue = m.hexdigest() + str(bandNum)
        #print("str(bandNum) >>> ", str(bandNum))
        bandNum += 1
        hashedList.append((hashedValue, filename),)

    
    #return the list of hashed keys for a particular file 
    return hashedList


def getSignature(filename, featureVector):
    """
    #3 a
    generate the signature for every filename given its feature vector
    :return:
    """
    signature = list()
        
    #remainder = int(len(featureVector) % 128) #36 bits remaining
    chunksize = int(len(featureVector) / 128)  #38
    # print("chunksize >>>> ", chunksize)
    chunks = list()
    start = 0
    end = start+chunksize

    #first 38 - add to chunks
    for i in range(0,128):
        chunks.append((featureVector[start:end]).tolist())
        # print("chunk length .... ",(end-start))
        start += chunksize
        end += chunksize

    end -= chunksize

    #now work with remainder 36
    
    remainder = featureVector[end:]
    
    #add each of the 36 bits one by one to the chunks
    i = 0
    for aChunk in chunks:
        #aChunk = np.insert(aChunk, 0 ,remainder[i])
        if i < len(remainder):
            aChunk.append(remainder[i])
            # print("new chunk length >>>> ", len(aChunk))
            i+=1  #next remainder bit
        else:
            break

    #for each chunk - convert to hex - then binary and append to signature list
    for aChunk in chunks:
        m = hashlib.md5()
        # print("achunk before signature>>>>>>>>>> ",aChunk)
        chunkStrRep = ''.join(str(e) for e in aChunk)

        # print("chunkStrRep>>>>>>>>>> ", chunkStrRep)
        m.update(chunkStrRep.encode('utf-8'))
        hex = m.hexdigest()
        binaryRepr = bin(int(hex, 16))[2:]
        # print("binaryRepr>>>>>>>>>> ", binaryRepr)
        # print("binaryRepr 1st digit >>>>>>>>>> ", binaryRepr[0])
        signature.append(int(binaryRepr[3])) #take first bit into signature

    # final 128 bit signature - need to convert to string
    # print(" final signature for file >>>> ", filename, " >>> ", signature)
    return performLSH(filename, signature)

def rowcoldiff(arr):
    """
    to form the signature need to perform this tranformation and flattening
    50 X 50 - regular for 2b
    100 X 100 - for 3d.

    :param arr:
    :return:
    """
    factor1 = 50 # 100 for 3d, 50 for 2b
    factor2 = 49  # 99 for 3d, 49 for 2b
    rowdiff = np.zeros((factor1, factor2))  # , dtype=np.int8
    coldiff = np.zeros((factor2, factor1))  # , dtype=np.int8
    for row in range(0,factor1):
        rowdiff[row,:] = np.diff(arr[row, :])

    #use numpy syntax to set the intensity bit values
    rowdiff[rowdiff > 1] = 1
    rowdiff[rowdiff < -1] = -1
    rowdiff[np.logical_and(rowdiff > -1, rowdiff < 1)] = 0

    for col in range(0,factor1):
        coldiff[:, col] = np.diff(arr[:, col])

    # use numpy syntax to set the intensity bit values
    coldiff[coldiff > 1] = 1
    coldiff[coldiff < -1] = -1
    coldiff[np.logical_and(coldiff > -1, coldiff < 1)] = 0

    #print("rowdiff >>>>>>>>> ", rowdiff.shape)
    # flatten the rows and columns

    rowflat = rowdiff.flatten()
    colflat = coldiff.flatten()

    #flattened and concatenated row and cols
    flatfeature = np.concatenate([rowflat, colflat])
    

    return flatfeature 

def reduceResolution(arr):
    """
    Ans 2b. 3d
    reduce the resolution of the images
    :param arr:
    :return:
    """

    #print("reduceresolution shape of input >>>>>>>>>>> ", arr.shape)
    factorizedVal = 50  # 2b 50 , 3d 100

    finalList = list()
    rows = factorizedVal # change to 100 for 3d, 50 for 2b
    cols = factorizedVal # change to 100 for 3d, 50 for 2b
    irow = 0
    icol = 0

    newarr = np.zeros((factorizedVal, factorizedVal)) #, dtype=np.int8, # change to 100 for 3d, 50 for 2b
    #vertical array or rows (varr), harr - horizontal array (harr)
    varr = np.vsplit(arr, rows)  # row split
    
    for avarr in varr:
        harr = np.hsplit(avarr, cols)  # column split
        #print("reduceresolution shape of harr >>>>>>>>>>> ", np.shape(harr))
        for aharr in harr:
            newarr[irow][icol] = np.mean(aharr)  # row split , took the mean intensity of 10x10 sub-matrices.
            icol +=1
        irow +=1
        icol = 0

    #now call row column difference method with this reduced image
    return rowcoldiff(newarr)


def splitImage(filename, arr, factor=5):
    """
    Ans 1c.
    split each image into multiple files based on the factor given

    :param filename:
    :param arr:
    :param factor:
    :return:
    """
    finalList = list()
    rows = factor
    cols = factor
    hcntr = 0
    vcntr = 0

    varr = np.vsplit(arr, rows) #row split

    for avarr in varr:
        harr = np.hsplit(avarr, cols) # column split
        for aharr in harr:
            num = hcntr + vcntr
            finalList.append((filename+"-"+str(num), aharr ),) #row split
            hcntr +=1
        hcntr -= 1 # one extra jump reduced
        vcntr +=1

    
    #return the final list of split images    

    return finalList



def getOrthoTif(filename, zfBytes):
    """
    Ans 1b.
    :param filename:
    :param zfBytes:
    :return:
    """
    #given a zipfile as bytes (i.e. from reading from a binary file),
    # return a np array of rgbx values for each pixel
    bytesio = io.BytesIO(zfBytes)
    zfiles = zipfile.ZipFile(bytesio, "r")
    #find tif:
    for fn in zfiles.namelist():
        if fn[-4:] == '.tif':#found it, turn into array:
            tif = TiffFile(io.BytesIO(zfiles.open(fn).read()))

    return splitImage(filename.rsplit("/")[-1], tif.asarray(), 5)


def convertToIntensity(filename, arr):
    """
    Ans 1.
    :param filename:
    :param arr: use the intensity calculation formula and find the intensity for each bit
    :return:
    """

    newarr = np.zeros((500,500)) #, dtype=np.int8
    intensities = list()
    
    for row in range(0,500):
        for column in range(0,500):

            #print("\n convertToIntensity val2 .................", arr[row][column], "..\n")
            rgbi = arr[row][column]
            rgb_mean = (int(rgbi[0]) + int(rgbi[1]) + int(rgbi[2]))/3
            intensity = int(rgb_mean * (rgbi[3]/100)) # removed int
            
            newarr[row][column] = intensity #set intensity

    #call reduceresolution on the intensity matrix          
    return (filename, reduceResolution(newarr)),


if __name__ == "__main__":
    sconf = SparkConf().setAppName("Pyspark-LSH and PCA")
    
    sc = SparkContext(conf=sconf)
    #--------- location of images ----------------------
    # read the correct folder with images
    #imagesRDD = sc.binaryFiles('hdfs:/data/small_sample') #small samples
    imagesRDD = sc.binaryFiles('hdfs:/data/large_sample') #large samples
    #rdd.map(lambda x: x[0]).collect()
    #imagesRDD = sc.binaryFiles("/data/a2_small_sample") #local
    #--------------location end

    filenames = imagesRDD.map(lambda x:x[0]).collect()
    #print("\n\n Zip filenames >>>>>>>>>>>>>>>>>>>>>>>>>> ", filenames, "\n\n")

    #get the tiff RDD
    tiffsRDD = imagesRDD.flatMap(lambda x: getOrthoTif(x[0], x[1]))
    tiffsRDD.persist()

    # filter the files required to print
    tiffs = tiffsRDD.filter(lambda x:(r"3677454_2025195.zip" in x[0])).collect()
    
    #convert to intensity
    tiffIntensityRDD = tiffsRDD.flatMap(lambda x:convertToIntensity(x[0],x[1]))
    #tiffIntensityRDD.persist()

    # filter files required for answer 2
    answer2 = tiffIntensityRDD.filter(lambda x:(r"3677454_2025195.zip" in x[0])).collect()

    # ************************************* Answer #1 *****************************************
    print("\n --------------Answer 1------------------------- \n ")

    for aTiff in tiffs:
        #print("\nfilenames >>>>>>>> ",aTiff[0])
        
        if r"3677454_2025195.zip-0" in aTiff[0] and (aTiff[0])[-2:] == '-0':
            print(aTiff[0], ">>", aTiff[1][0][0])
        if r"3677454_2025195.zip-1" in aTiff[0] and (aTiff[0])[-2:] == '-1':
            print(aTiff[0], ">>", aTiff[1][0][0])
        if r"3677454_2025195.zip-18" in aTiff[0] and (aTiff[0])[-2:] == '18':
            print(aTiff[0], ">>", aTiff[1][0][0])
        if r"3677454_2025195.zip-19" in aTiff[0] and (aTiff[0])[-2:] == '19':
            print(aTiff[0], ">>", aTiff[1][0][0])

    # ************************************* end Answer #1 *****************************************


    
    # ************************************* Answer #2 *****************************************
    print("\n --------------Answer 2 ------------------------- \n ")
    for aTiff in answer2:
        # print(aTiff[0])
        # print(aTiff[1])
        if r"3677454_2025195.zip-1" in aTiff[0] and (aTiff[0])[-2:] == '-1':
            print(aTiff[0], ">>", aTiff[1])

        if r"3677454_2025195.zip-18" in aTiff[0] and (aTiff[0])[-2:] == '18':
            print(aTiff[0], ">>", aTiff[1])


    # ************************************* end Answer #2 *****************************************

    # ************************************* Answer #3b *****************************************

    # 2 lists - one to print and other to additionally compute
    answer3bListPrint = [r'3677454_2025195.zip-1',r'3677454_2025195.zip-18']
    answer3bList = [r'3677454_2025195.zip-0', r'3677454_2025195.zip-1', r'3677454_2025195.zip-18', r'3677454_2025195.zip-19']

    #broadcast the bigger list
    answer3b = sc.broadcast(answer3bList)

    # get the signatures for the files in question
    signatureRDD = tiffIntensityRDD.flatMap(lambda x: getSignature(x[0], x[1])).groupByKey().flatMap(lambda x: getSimilarFiles(x[1])).groupByKey()#.values().distinct()
    lshList = signatureRDD.collect()
    #print("lshList >> ", lshList)

    img2Dict = dict() # store the filenames and corresponding similar files for 3c.
    filteredSet3b =  set()
    print("\n --------------Answer 3b ------------------------- \n ")
    for aTuple in lshList:
        flattened = set([val for sublist in aTuple[1] for val in sublist]) # get unique values
        flattened.remove(aTuple[0]) # remove the key if found again - its the source image to compare against, needs to be removed from destination
        if aTuple[0] in answer3bListPrint:
            print(" key >> ", aTuple[0], " files >> ", flattened)
            img2Dict[aTuple[0]] = flattened
            filteredSet3b.update(flattened)
            filteredSet3b.add(aTuple[0]) # need this filtered list for 3c


    
    # --------------------------------------------3c ----------------------------------------------
    # sample 100 images and find the V, mu, std
    sample = tiffIntensityRDD.take(100)
    VtMuStd = svdTransform(sample)
    #print("VtMuStd >>>>>>>> ", VtMuStd)

    #VtMuStdBcst = sc.broadcast(VtMuStd)

    # sample 100 images and find the V, mu, std
    svdRDD = tiffIntensityRDD.flatMap(lambda x: transformEach(x[0], x[1])) #.groupByKey()
    lowDImgsRDD = svdRDD.filter(lambda x: x[0] in list(filteredSet3b)) # get only files in question
    lowDImgsRDD.persist()

    lowDfor3bimgs = lowDImgsRDD.filter(lambda x: x[0] in list(answer3bListPrint)).collect()  # remove collect flatMap(lambda x: getImgsEucDist(x[0], x[1]))
    lowDfor3bimgsGlobalDict = dict() # sotres low dimensional vector for files in question

    lowDNotIn3bimgsRDD = lowDImgsRDD.filter(lambda x: x[0] not in list(answer3bListPrint)) # not in list

    #broadcast the lowD values for the answer keys in answer3bListPrint
    for alowDImg in lowDfor3bimgs:
        #print("lowDImgs >>> ", alowDImg)
        lowDfor3bimgsGlobalDict[alowDImg[0]] = alowDImg[1]
    lowDfor3bimgsGlobalVar = sc.broadcast(lowDfor3bimgsGlobalDict) # broadcast this for lookup 
    img2DictGlobalVar = sc.broadcast(img2Dict) #similar image mappings

    output3c = lowDNotIn3bimgsRDD.flatMap(lambda x: getImgsEucDist(x[0], x[1])).collect()
    #print("\n 3c ... ", output3c)

    q3c1 = dict() # for image 1
    q3c2 = dict() # for image 2
    for aTuple in output3c:
        if r"3677454_2025195.zip-18" in aTuple[0]:  #  image 2 will be complete match so used it for comparison
            q3c2[aTuple[0]] = aTuple[1]
        else:
            q3c1[aTuple[0]] = aTuple[1]

    print("\n --------------Answer 3c ------------------------- \n ")
    print("\n -----------For file---------------  ", answer3bListPrint[0])
    print("\n ---------------SORTED---------------------------------- \n ")
    print("Sorted from Least to Greatest .. Euclidean Distance  between \n")
    print("------------------------------------------------- ")
    pprint(sorted(q3c1.items(), key=lambda x: x[1]))

    print("\n -----------For file----------------  ", answer3bListPrint[1])
    print("\n ---------------SORTED---------------------------------- \n ")
    print("Sorted from Least to Greatest .. Euclidean Distance  between \n")
    print("------------------------------------------------- ")
    pprint(sorted(q3c2.items(), key=lambda x: x[1]))
    print("\n --------------End ------------------------- \n ")

# ************************************* Answer #3 *****************************************

