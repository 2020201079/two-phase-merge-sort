import os.path
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

def sortSubList(subList):
    #implement sort here for now just doing nothing
    return subList

def saveAsFile(currSubList,path):
    #currSubList has list of tuples
    with open(path,'w') as filehandle:
        for t in currSubList:
            filehandle.write('%s\n' % str(t))


def main():
    memLimit = 500 # this value is in MB
    #memLimit = 5*1024*1024 #converting to bytes
    fileToSort = "input50.txt"
    if(not path.exists(fileToSort)):
        printError("file to sort does not exist "+fileToSort)
    metaDataFile = "Metadata.txt"
    colDetails = parseMetadataFile(metaDataFile)
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
            currSubList = sortSubList(currSubList)
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