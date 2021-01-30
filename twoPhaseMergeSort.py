import os.path
import sys
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

def sortSubList(subList,colOrder,colDetails):
    if(len(colOrder)==0):
        subList.sort()
        return subList
    #implement sort here for now just doing nothing
    def newOrder(x):
        ans = []
        for c in colOrder:
            i = getIndex(c,colDetails)
            ans.append(x[i])
        return tuple(ans)
    #subList.sort(key = lambda x :(x[0],x[1]))
    subList.sort(key = newOrder)
    return subList

def saveAsFile(currSubList,path):
    #currSubList has list of tuples
    with open(path,'w') as filehandle:
        for t in currSubList:
            for s in t:
                filehandle.write('%s  ' % s)
            filehandle.write('\n')

def checkIfAllColsExist(colOrder,colDetails):
    colList = [c[0] for c in colDetails]
    for c in colOrder:
        if c not in colList:
            printError("Col not present in metadata : "+c)

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
    print("Number of tuples in a sublist : ",noOfTuplesInSubList)
    
    inputFile = open(fileToSort,'r')
    currSubList = []
    sublistIndex = 1
    while True:
        if(len(currSubList)==noOfTuplesInSubList):
            #sort the sublist
            currSubList = sortSubList(currSubList,colOrder,colDetails)
            #save the current sublist as sublist+sublistIndex
            saveAsFile(currSubList,"subList"+str(sublistIndex)+".txt")
            sublistIndex = sublistIndex +1
            currSubList.clear()
        words = inputFile.readline().strip()
        if not words:
            break # reached end of file here
        output = [s.strip() for s in words.split('  ') if s]
        if(len(output) != len(colDetails)):
            printError("Number of cols do not match for : "+str(output))
        outputTuple = tuple(output)
        currSubList.append(outputTuple)

if __name__ == '__main__':
    main()