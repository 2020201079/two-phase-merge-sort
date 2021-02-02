import os.path
import sys
import heapq
from collections import defaultdict 
from os import path
from datetime import datetime
import threading
import linecache
startTime = datetime.now()

delSize = 2 # gensort is giving 2 spaces

def printError(error):
    print(error)
    exit()

def getFileHandlers(numOfSublist):
    ans = []
    for i in range(1,numOfSublist+1):
        fileName = "subList"+str(i)+".txt"
        if not path.exists(fileName):
            printError("sublist is not created "+fileName)
        f = open(fileName,'r')
        ans.append(f)
    return ans

def readFromSubList(filehandle,maxTuples,colDetails):
    currSubList = []
    for i in range(maxTuples):
        words = filehandle.readline()
        if not words:
            filehandle.close()
            return currSubList
        else:
            outputTuple = parseLineRead(words,colDetails)
            currSubList.append(outputTuple)
    return currSubList

def newOrder(x,colOrder,colDetails):
    ans = []
    for c in colOrder:
        i = getIndex(c,colDetails)
        ans.append(x[i])
    return tuple(ans)

class comparableMin: # using class for custom comparator in heap
    def __init__(self,tup,fileNo,colOrder,colDetails):
        self.tup = tup
        self.fileNo = fileNo
        self.colOrder = colOrder
        self.colDetails = colDetails

    def __lt__(self,other):#[x1,x2,x3] -> [x3,x2,x1] #[y1,y2,y3]->[y3,y2,y1] ## [y3,y2,y1] < [x3,x2,x1]
         newTuple1 = newOrder(self.tup,self.colOrder,self.colDetails)
         newTuple2 = newOrder(other.tup,other.colOrder,other.colDetails)
         return newTuple1 < newTuple2

class comparableMax: # using class for custom comparator in heap
    def __init__(self,tup,fileNo,colOrder,colDetails):
        self.tup = tup
        self.fileNo = fileNo
        self.colOrder = colOrder
        self.colDetails = colDetails

    def __lt__(self,other):
         newTuple1 = newOrder(self.tup,self.colOrder,self.colDetails)
         newTuple2 = newOrder(other.tup,other.colOrder,other.colDetails)
         return newTuple1 > newTuple2

def removeLastNewLine(outputfileHandler):
    outputfileHandler.seek(0,os.SEEK_END)
    pos = outputfileHandler.tell() - 1
    while pos > 0 and outputfileHandler.read(1) != "\n":
        pos -= 1
        outputfileHandler.seek(pos, os.SEEK_SET)
    if pos > 0:
        outputfileHandler.seek(pos, os.SEEK_SET)
        outputfileHandler.truncate()
    outputfileHandler.close()

def initializeDataDic(data,fileHandlers,phase2MaxNumberOfTuples,colDetails):
    noOfFiles = len(fileHandlers)
    for i in range(noOfFiles):
        if(len(data[i]) == 0 and not fileHandlers[i].closed):
            data[i] = readFromSubList(fileHandlers[i],phase2MaxNumberOfTuples,colDetails)
    return data

def makeCompObject(t,i,order,colOrder,colDetails):
    if(order.lower() == "asc"):
        ans = comparableMin(t,i,colOrder,colDetails)
    else:
        ans = comparableMax(t,i,colOrder,colDetails)
    return ans

def initializeHeapList(heap,data,sortedOrder,colOrder,colDetails):
    for i in range(len(data)):
        heapObject = makeCompObject(data[i][0],i,sortedOrder,colOrder,colDetails) # list of tuples
        data[i].pop(0) # already present in heap so we can remove it
        heap.append(heapObject)
    heapq.heapify(heap)
    return heap

def deleteInterMediateFiles(numOfSublist):
    for i in range(1,numOfSublist+1):
        os.remove("subList"+str(i)+".txt")

def saveAsFileFileHandler(currSubList,filehandle):
    #currSubList has list of tuples
    for t in currSubList:
        for s in t:
            if(s != t[len(t)-1]):
                filehandle.write('%s  ' % s)
            else:
                filehandle.write('%s' % s)
        filehandle.write('\n')

def getMinFromAllSublist(data,fileHandlers,phase2MaxNumberOfTuples,colDetails,colOrder,sortingOrder,heap):
    if not heap:
        return []
    ansObject = heapq.heappop(heap)
    ans = ansObject.tup
    filePoppedFrom = ansObject.fileNo
    if(len(data[filePoppedFrom])>0):
        currTup = data[filePoppedFrom][0]
        tupleObj = makeCompObject(currTup,filePoppedFrom,sortingOrder,colOrder,colDetails)
        heapq.heappush(heap,tupleObj)
        data[filePoppedFrom].pop(0)
    elif(len(data[filePoppedFrom]) == 0 and not fileHandlers[filePoppedFrom].closed):
        data[filePoppedFrom] = readFromSubList(fileHandlers[filePoppedFrom],phase2MaxNumberOfTuples,colDetails)
        if data[filePoppedFrom]:
            currTup = data[filePoppedFrom][0]
            tupleObj = makeCompObject(currTup,filePoppedFrom,sortingOrder,colOrder,colDetails)
            heapq.heappush(heap,tupleObj)
            data[filePoppedFrom].pop(0)
    return ans

def checkIfAllColsExist(colOrder,colDetails):
    colList = [c[0] for c in colDetails]
    for c in colOrder:
        if c not in colList:
            printError("Col not present in metadata : "+c)

def sortSubList(subList,colOrder,colDetails,sortingOrder):
    if(len(colOrder)==0):
        subList.sort()
        return subList
    def newOrder(x):
        ans = []
        for c in colOrder:
            i = getIndex(c,colDetails)
            ans.append(x[i])
        return tuple(ans)
    if(sortingOrder.lower() == "asc"):
        subList.sort(key = newOrder)
    else:
        subList.sort(key = newOrder,reverse=True)
    return subList

def saveAsFile(currSubList,path):
    #currSubList has list of tuples
    with open(path,'w') as filehandle:
        for t in currSubList:
            for s in t:
                if(s != t[len(t)-1]):
                    filehandle.write('%s  ' % s)
                else:
                    filehandle.write('%s' % s)
            filehandle.write('\n')

def getIndex(colName,colDetails):
    for i in range(len(colDetails)):
        if (colDetails[i][0] == colName):
            return i
    printError("col name not in metadata "+colName)
def parseMetadataFile(path):
    colDetails = [] #list of tuples (colname,size)
    if not os.path.exists(path):
        printError("metadata file does not exists")
    else:
        metaFile = open(path,'r')
        while True:
            line = metaFile.readline().strip()
            if not line:
                break
            data = line.split(',')
            if(len(data) != 2):
                printError("Formating error in metadata file")
            colDetails.append((data[0],int(data[1])))
        return colDetails

def checkIfAllColsExist(colOrder,colDetails):
    colList = [c[0] for c in colDetails]
    for c in colOrder:
        if c not in colList:
            printError("Col not present in metadata : "+c)

def getTupleSize(colDetails):
    ans = 0
    for t in colDetails:
        ans = ans + t[1]
    return ans

def getNoOfLines(fileToSort,colDetails):
    sizeOfTuple = getTupleSize(colDetails)
    sizeOfTupleWithDel = sizeOfTuple + len(colDetails)*delSize
    fileSize = os.stat(fileToSort).st_size
    print("file size : ",fileSize)
    print("size of each line : ", sizeOfTupleWithDel)
    if fileSize%sizeOfTupleWithDel != 0:
        printError(" file size should be multiple of the tuple may be the delimeter is not 2 chars or there is extra line break etc.. please stick with gensort output as it is")
    noOfLines = (os.stat(fileToSort).st_size)/sizeOfTupleWithDel
    return noOfLines

class threadDetails:
    def __init__(self,id,start,end,lineLimit):
        self.id = id
        self.start = start
        self.end = end
        self.data = []
        self.currPos = start
        self.lineLimit = lineLimit # can't read more than these lines at once
    def getData(self):
        return self.data
    def clearData(self):
        self.data.clear()
    def needToStart(self):
        if(self.currPos>=self.end):
            return False
        return True

def threadingFunc(td,inputFile,colDetails): 
    #print("thread started id : ",td.id)
    while(td.currPos <= td.end and len(td.data) < td.lineLimit): # should read end line too
        #print("line no : ",td.currPos)
        words = linecache.getline(inputFile,td.currPos)
        #print(words)
        outputTuple = parseLineRead(words,colDetails)
        td.data.append(outputTuple)
        td.currPos += 1
 
def parseLineRead(words,colDetails): #return the record as python tuple as per metadata
    startIndex = 0
    output = []
    for c in colDetails:
        if(startIndex+c[1]>=len(words)):
            printError("Metadata format and record do not match : "+ words)
        currVal = words[startIndex:startIndex+c[1]]
        output.append(currVal)
        startIndex += c[1]+2 # 2 spaces after each col
    outputTuple = tuple(output)
    return outputTuple

def startThreads(threadList):
    for t in threadList:
        t.start()

def anyThreadNeedToStart(threadList):
    for t in threadList:
        if t.needToStart():
            return True
    return False

def makeThreads(threadDetailsList,fileToSort,colDetails):
    ans = []
    for t in threadDetailsList:
        if t.needToStart():
            ans.append(threading.Thread(target=threadingFunc,args=(t,fileToSort,colDetails,)))
    return ans

def main():
    if(len(sys.argv) < 2):
        printError("Input not provided")
    query = sys.argv[1]
    print(query)
    data = query.split(' ')
    data = [t.strip() for t in data]
    if(len(data)<5):
        printError("Query is missing some data")
    
    fileToSort = data[0]
    if(not path.exists(fileToSort)):
        printError("file to sort does not exist "+fileToSort)
    outputPath = data[1]
    if( not data[2].isdigit()):
        printError("Memory should be a digit : "+data[2])
    memLimit = int(data[2])*1000*1000
    
    if( not data[3].isdigit()):
        printError("Number of threads should be a digit : "+data[3])
    noOfThreads = int(data[3])

    if(data[4].lower() != "asc" and data[4].lower() != "desc"):
        printError("order must be mentioned either asc or desc")
    sortingOrder = data[4].lower()

    colOrder = []
    for k in range(5,len(data)):
        colOrder.append(data[k])
    print("order by col : ",colOrder)

    metaDataFile = "Metadata.txt"
    colDetails = parseMetadataFile(metaDataFile)

    if not colOrder:
        for c in colDetails:
            colOrder.append(c[0])
    
    checkIfAllColsExist(colOrder,colDetails)

    noOfLinesInFile = getNoOfLines(fileToSort,colDetails)
    print("no Of Lines in file : ",noOfLinesInFile)

    print("no of threads : ",noOfThreads)
    memForEachThread = int(memLimit/noOfThreads)

    sizeOfTuple = getTupleSize(colDetails)
    lineSize = sizeOfTuple + len(colDetails)*delSize

    noOfTuplesInSubList = int(memLimit/lineSize)
    if(noOfTuplesInSubList == 0):
        printError("Mem size is less than one tuple also mem : "+str(memLimit)+" tuple size "+str( sizeOfTuple))
    print("Number of tuples in a sublist : ",noOfTuplesInSubList)

    tuplesInAThreadMem = int(memForEachThread/lineSize)
    if(tuplesInAThreadMem == 0):
        printError("Mem for each thread is less than size of one tuple")
    #print("Number of tuples handled by each thread : ",tuplesInAThread)

    numOfSublist = noOfLinesInFile/noOfTuplesInSubList

    threadDetailsList = [] # contains whihc thread should read from which line
    start = 1
    for i in range(1,noOfThreads+1):
        if(i!=noOfThreads):
            end = start + int(noOfLinesInFile/noOfThreads)
            threadDetailsList.append(threadDetails(i,start,end,tuplesInAThreadMem))
            start = end+1
        else:
            end = noOfLinesInFile
            threadDetailsList.append(threadDetails(i,start,end,tuplesInAThreadMem))
    for td in threadDetailsList:
        print("start : ",td.start," stop : ",td.end," limit : ",td.lineLimit," curr pos :",td.currPos)
    sublistIndex = 0
    while anyThreadNeedToStart(threadDetailsList):
        currThreads = makeThreads(threadDetailsList,fileToSort,colDetails)
        startThreads(currThreads)
        for x in currThreads:
            x.join()
        for td in threadDetailsList:
            print("start : ",td.start," stop : ",td.end," limit : ",td.lineLimit," curr pos :",td.currPos," stop : ",td.needToStart())
        currSubList = []
        for td in threadDetailsList:
            currSubList += td.data
            td.data.clear()
        sublistIndex = sublistIndex +1
        print("sorting sublist : ",str(sublistIndex))
        currSubList = sortSubList(currSubList,colOrder,colDetails,sortingOrder)
        print("writing to disk : ",str(sublistIndex))
        saveAsFile(currSubList,"subList"+str(sublistIndex)+".txt")
        currSubList.clear()


    numOfSublist = sublistIndex
    if(numOfSublist == 1):
        #rename subList1.txt to output and close this func
        print("whole file fits in mem at once so sorting at once writing to disk")
        os.rename("subList1.txt",outputPath)
        return
    print("number of sublist : ",numOfSublist)
    print("## running phase - 2")
    sizeForEachSubFile = int(memLimit/(sublistIndex+1))
    print("size available for each subfile :",str(sizeForEachSubFile))
    print("running ... ")
    if(sizeForEachSubFile < sizeOfTuple or memLimit < (numOfSublist+1) * sizeOfTuple):
        printError("size of file is too big for two phase merge sort. Increase mem or decrease file size")
    phase2MaxNumberOfTuples = int(sizeForEachSubFile/sizeOfTuple)

    #dic of list of tuples
    fileHandlers = getFileHandlers(numOfSublist)

    if os.path.exists(outputPath):
        os.remove(outputPath)
    outputfileHandler=open(outputPath, "a+")
    data = defaultdict(list)
    data = initializeDataDic(data,fileHandlers,phase2MaxNumberOfTuples,colDetails)
    heap = []
    initializeHeapList(heap,data,sortingOrder,colOrder,colDetails) # heapify is applied
    outputBuffer = []
    while True:
        if(len(outputBuffer) >= phase2MaxNumberOfTuples):
            saveAsFileFileHandler(outputBuffer,outputfileHandler)
            outputBuffer.clear()
        #printDefaultDicSizes(data)
        currMin = getMinFromAllSublist(data,fileHandlers,phase2MaxNumberOfTuples,colDetails,colOrder,sortingOrder,heap)
        if(currMin == []):
            break
        else:
            outputBuffer.append(currMin)
    if(len(outputBuffer) != 0):
        #saveAsFile(outputBuffer,outputPath)
        saveAsFileFileHandler(outputBuffer,outputfileHandler)
    removeLastNewLine(outputfileHandler)
    deleteInterMediateFiles(numOfSublist)

if __name__ == '__main__':
    main()
    print("time taken : ",str(datetime.now() - startTime))