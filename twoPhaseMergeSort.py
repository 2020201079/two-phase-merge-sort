import os.path
import sys
from collections import defaultdict 
from os import path

def printError(error):
    print(error)
    exit()

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

def getTupleSize(colDetails):
    ans = 0
    for t in colDetails:
        ans = ans + t[1]
    return ans

def getIndex(colName,colDetails):
    for i in range(len(colDetails)):
        if (colDetails[i][0] == colName):
            return i
    printError("col name not in metadata "+colName)

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

def saveAsFileFileHandler(currSubList,filehandle):
    #currSubList has list of tuples
    for t in currSubList:
        for s in t:
            if(s != t[len(t)-1]):
                filehandle.write('%s  ' % s)
            else:
                filehandle.write('%s' % s)
        filehandle.write('\n')

def checkIfAllColsExist(colOrder,colDetails):
    colList = [c[0] for c in colDetails]
    for c in colOrder:
        if c not in colList:
            printError("Col not present in metadata : "+c)

def parseLineRead(words,colDetails): #return the record as python tuple as per metadata
    startIndex = 0
    output = []
    for c in colDetails:
        if(startIndex+c[1]>=len(words)):
            printError("Metadata format and record do not match "+ words)
        currVal = words[startIndex:startIndex+c[1]]
        output.append(currVal)
        startIndex += c[1]+2 # 2 spaces after each col
    outputTuple = tuple(output)
    return outputTuple

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

def getMinFromAllSublist(data,fileHandlers,phase2MaxNumberOfTuples,colDetails,colOrder,sortingOrder):
    noOfFiles = len(fileHandlers)
    currMinValues = []
    for i in range(noOfFiles):
        if(len(data[i]) == 0 and not fileHandlers[i].closed):
            data[i] = readFromSubList(fileHandlers[i],phase2MaxNumberOfTuples,colDetails)
            if(data[i] != []):
                currMinValues.append(data[i][0])
        else:
            if(len(data[i])>0):
                currMinValues.append(data[i][0])
    sortedSubList = sortSubList(currMinValues,colOrder,colDetails,sortingOrder)
    if(sortedSubList == []):
        return []
    for i in range(len(data)):
        if data[i]:
            if data[i][0] == sortedSubList[0]:
                data[i].pop(0)
    return sortedSubList[0]

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

def printDefaultDicSizes(data):
    for i in range(len(data)):
        print("size "+str(i)+" : "+str(len(data[i])))
def main():
    if(len(sys.argv) < 2):
        printError("Input not provided")
    query = sys.argv[1]
    print(query)
    data = query.split(' ')
    data = [t.strip() for t in data]
    if(len(data)<4):
        printError("Query is missing some data")

    fileToSort = data[0]
    if(not path.exists(fileToSort)):
        printError("file to sort does not exist "+fileToSort)
    outputPath = data[1]
    if( not data[2].isdigit()):
        printError("Memory should be a digit : "+data[2])
    memLimit = int(data[2]) # now taking as bytes later enter as MB
    if(data[3].lower() != "asc" and data[3].lower() != "desc"):
        printError("order must be mentioned either asc or desc")
    sortingOrder = data[3].lower()

    colOrder = []
    for k in range(4,len(data)):
        colOrder.append(data[k])
    print("order by col : ",colOrder)

    metaDataFile = "Metadata.txt"
    colDetails = parseMetadataFile(metaDataFile)

    checkIfAllColsExist(colOrder,colDetails)

    sizeOfTuple = getTupleSize(colDetails)
    print("size of each tuple is : ",sizeOfTuple)
    noOfTuplesInSubList = int(memLimit/sizeOfTuple)
    if(noOfTuplesInSubList == 0):
        printError("Mem size is less than one tuple also mem : "+str(memLimit)+" tuple size "+str( sizeOfTuple))
    print("Number of tuples in a sublist : ",noOfTuplesInSubList)
    print(" ###start execution ")
    print(" ## running Phase-1")
    inputFile = open(fileToSort,'r')
    currSubList = []
    sublistIndex = 0
    while True:
        if(len(currSubList)==noOfTuplesInSubList):
            sublistIndex = sublistIndex +1
            print("sorting sublist : ",str(sublistIndex))
            currSubList = sortSubList(currSubList,colOrder,colDetails,sortingOrder)
            print("writing to disk : ",str(sublistIndex))
            saveAsFile(currSubList,"subList"+str(sublistIndex)+".txt")
            currSubList.clear()
        words = inputFile.readline()
        if not words:
            break
        outputTuple = parseLineRead(words,colDetails)
        currSubList.append(outputTuple)
    
    if currSubList:
        sublistIndex = sublistIndex +1
        print("sorting sublist : ",str(sublistIndex))
        currSubList = sortSubList(currSubList,colOrder,colDetails,sortingOrder)
        print("writing to disk : ",str(sublistIndex))
        saveAsFile(currSubList,"subList"+str(sublistIndex)+".txt")
        currSubList.clear()

    inputFile.close()
    numOfSublist = sublistIndex
    if(numOfSublist == 1):
        #rename subList1.txt to output and close this func
        print("whole file fits in mem at once so sorting at once writing to disk")
        os.rename("subList1.txt",outputPath)
        exit()
    print("number of sublist : ",numOfSublist)
    print("## running phase - 2")
    sizeForEachSubFile = int(memLimit/(sublistIndex+1))
    print("size available for each subfile :",str(sizeForEachSubFile))
    if(sizeForEachSubFile < sizeOfTuple or memLimit < (numOfSublist+1) * sizeOfTuple):
        printError("size of file is too big for two phase merge sort. Increase mem or decrease file size")
    phase2MaxNumberOfTuples = int(sizeForEachSubFile/sizeOfTuple)

    #dic of list of tuples
    fileHandlers = getFileHandlers(numOfSublist)

    if os.path.exists(outputPath):
        os.remove(outputPath)
    outputfileHandler=open(outputPath, "a+")
    data = defaultdict(list)
    outputBuffer = []
    while True:
        if(len(outputBuffer) >= phase2MaxNumberOfTuples):
            saveAsFileFileHandler(outputBuffer,outputfileHandler)
            outputBuffer.clear()
        currMin = getMinFromAllSublist(data,fileHandlers,phase2MaxNumberOfTuples,colDetails,colOrder,sortingOrder)
        if(currMin == []):
            break
        else:
            outputBuffer.append(currMin)
    if(len(outputBuffer) != 0):
        #saveAsFile(outputBuffer,outputPath)
        saveAsFileFileHandler(outputBuffer,outputfileHandler)
    removeLastNewLine(outputfileHandler)
if __name__ == '__main__':
    main()