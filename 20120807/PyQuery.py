#coding=utf-8
import heapq,time,random
from collections import defaultdict
from operator import itemgetter
from itertools import ifilter
from itertools import groupby
import re


class PyQuery:
	
	def __init__(self):
		#DataInList,type is list(list):store every line's data
		self.DataInList = []
		#DataInDict,type is dict(key:list):store data after groupby
		self.DataInDict = defaultdict(list)
		#ClumnRemain,type is list:current clumns
		self.ClumnRemain = []
		#Data type of each clumn
		self.ClumnDataType = []
		#ClumnGrouped,type is list:the grouped clumns
		self.ClumnGrouped = []
		#DataInListLength:total line number
		self.DataInListLength=0
	
	def inputWithSelect(self,inputFileName,splitFlag,clumnName,clumnNameSelect):
		now=time.time()
		
		self.ClumnRemain=[oneClumn for oneClumn in clumnName]
		selectedClumnIndex=[self.ClumnRemain.index(oneSelectedClumn) for oneSelectedClumn in clumnNameSelect]
		clumnNum=len(self.ClumnRemain)
		
		tempDataInTuple=(tuple(oneLine.rstrip().split(splitFlag)) for oneLine in open(inputFileName) if clumnNum-oneLine.count(splitFlag)==1)
		self.DataInList=[[oneLine[idx].strip() for idx in selectedClumnIndex] for oneLine in tempDataInTuple]
		
		self.DataInListLength=len(self.DataInList)
		self.ClumnRemain=[oneClumn for oneClumn in clumnNameSelect]
		
		#Detect data type of each clumn
		for index,oneClumnData in enumerate(self.DataInList[0]):
			if oneClumnData.isdigit():
				self.ClumnDataType.append('int')
			elif re.match('^\d+.\d+$',oneClumnData):
				self.ClumnDataType.append('float')
			else:
				self.ClumnDataType.append('string')
		
		print 'inputWithSelect '+str(time.time()-now)+' seconds'
	
	
	def select(self,*ClumnSelected):
		now=time.time()
		
		for oneSelected in ClumnSelected:
			assert oneSelected in self.ClumnRemain,oneSelected+' not in '+str(self.ClumnRemain)
		
		selectedClumnIndex=[self.ClumnRemain.index(oneSelectedClumn) for oneSelectedClumn in ClumnSelected]
		for lineNum in xrange(self.DataInListLength):
			self.DataInList[lineNum]=[self.DataInList[lineNum][oneClumnIndex] for oneClumnIndex in selectedClumnIndex]
		
		self.ClumnRemain=list(ClumnSelected)
		self.ClumnDataType=[self.ClumnDataType[oneSelectedIdx] for oneSelectedIdx in selectedClumnIndex]
		
		print 'select '+str(time.time()-now)+' seconds'
		
	
	def filter(self,filterClumn,filterOper,compareValue):
		now=time.time()
		
		filterClumnIndex=self.ClumnRemain.index(filterClumn)
		compileStr='self.DataInList=list(ifilter(lambda x: x[%s] %s \'%s\',self.DataInList))' % (str(filterClumnIndex),filterOper,compareValue)
		exec compile(compileStr,'','exec')
		self.DataInListLength=len(self.DataInList)
		
		print 'filter '+str(time.time()-now)+' seconds'
		
		
	def translate(self,transFunction,transClumn):
		now=time.time()
		
		transClumnIndex=self.ClumnRemain.index(transClumn)
		for lineNum in xrange(self.DataInListLength):
			self.DataInList[lineNum][transClumnIndex]=transFunction(self.DataInList[lineNum][transClumnIndex])
		
		print 'translate '+str(time.time()-now)+' seconds'

		
	def groupBy(self,*groupClumn):
		now=time.time()
		
		for oneGroupByClumn in groupClumn:
			assert oneGroupByClumn in self.ClumnRemain,oneGroupByClumn+' not in '+str(self.ClumnRemain)
		
		groupClumnIndex=[self.ClumnRemain.index(oneGroupClumn) for oneGroupClumn in groupClumn]
		groupedKey=['\t'.join([oneLine[oneGroupIndex] for oneGroupIndex in groupClumnIndex]) for oneLine in self.DataInList]
		for lineNum in xrange(self.DataInListLength):
			self.DataInDict[groupedKey[lineNum]].append(self.DataInList[lineNum])
		
		self.DataInList=[]
		self.DataInListLength=len(self.DataInDict)
		self.ClumnGrouped=list(groupClumn)
		
		print 'groupBy '+str(time.time()-now)+' seconds'
	
	
	def countEach(self,countClumn,resultClumn):
		now=time.time()
		
		countClumnIndex=self.ClumnRemain.index(countClumn)
		for oneGroupedKey in self.DataInDict:
			self.DataInDict[oneGroupedKey]=str(len(self.DataInDict[oneGroupedKey]))

		for k,v in self.DataInDict.items():
			self.DataInList.append(k.split('\t')+[v])
		
		self.ClumnDataType=[self.ClumnDataType[self.ClumnRemain.index(oneGrouped)] for oneGrouped in self.ClumnGrouped]
		self.ClumnDataType.append('int')
		self.ClumnRemain=self.ClumnGrouped+[resultClumn]
		self.DataInDict=defaultdict(list)
		
		print 'countEach '+str(time.time()-now)+' seconds'
	
	
	def sumEach(self,sumClumn,resultClumn):	
		now=time.time()
		
		sumClumnIndex=self.ClumnRemain.index(sumClumn)
		for oneGroupedKey in self.DataInDict:
			self.DataInDict[oneGroupedKey]=str(sum([float(oneRecord[sumClumnIndex]) for oneRecord in self.DataInDict[oneGroupedKey]]))
		
		for k,v in self.DataInDict.items():
			self.DataInList.append(k.split()+[v])
		
		self.ClumnDataType=[self.ClumnDataType[self.ClumnRemain.index(oneGrouped)] for oneGrouped in self.ClumnGrouped]
		self.ClumnDataType.append('float')
		self.ClumnRemain=self.ClumnGrouped+[resultClumn]
		self.DataInDict=defaultdict(list)
		
		print 'sumEach '+str(time.time()-now)+' seconds'
	
	
	def averageEach(self,averageClumn,resultClumn):
		now=time.time()
		
		averageClumnIndex=self.ClumnRemain.index(averageClumn)
		for oneGroupedKey in self.DataInDict:
			self.DataInDict[oneGroupedKey]=str(sum([float(oneRecord[averageClumnIndex]) for oneRecord in self.DataInDict[oneGroupedKey]])/len(self.DataInDict[oneGroupedKey]))
		
		for k,v in self.DataInDict.items():
			self.DataInList.append(k.split()+[v])
		
		self.ClumnDataType=[self.ClumnDataType[self.ClumnRemain.index(oneGrouped)] for oneGrouped in self.ClumnGrouped]
		self.ClumnDataType.append('float')
		self.ClumnRemain=self.ClumnGrouped+[resultClumn]
		self.DataInDict=defaultdict(list)
		
		print 'averageEach '+str(time.time()-now)+' seconds'
	
	
	def topEach(self,topClumn,nTop):
		now=time.time()
		
		topClumnIndex=self.ClumnRemain.index(topClumn)
		topClumnDataType=self.ClumnDataType[topClumnIndex]
		
		if topClumnDataType=='int':
			self.transToIntInDict(topClumnIndex)
		elif topClumnDataType=='float':
			self.transToFloatInDict(topClumnIndex)
			
		for oneGroupedKey in self.DataInDict:
			compileStr='oneGroupedValue=sorted(self.DataInDict[\''+oneGroupedKey+'\'],key=itemgetter('+str(topClumnIndex)+'),reverse=True)'
			exec compile(compileStr,'','exec')
			nTop=min(len(oneGroupedValue),nTop)
			self.DataInDict[oneGroupedKey]=oneGroupedValue[:nTop]
		
		for records in self.DataInDict.values():
			self.DataInList.extend(records)
		
		if topClumnDataType in ['int','float']:
			self.transToStrInList(topClumnIndex)
		
		self.DataInListLength=len(self.DataInList)
		self.DataInDict=defaultdict(list)
		
		print 'topEach '+str(time.time()-now)+' seconds'
		
	
	def top(self,topClumn,nTop):
		now=time.time()
		
		assert topClumn in self.ClumnRemain,topClumn+' not in '+str(self.ClumnRemain)
		
		# change clumn order,make topClumnIndex=0, so can sort by the first clumn
		tempClumnRemain=self.ClumnRemain[:]
		tempClumnRemain.remove(topClumn)
		tempClumnRemain.insert(0,topClumn)
		self.select(*tuple(tempClumnRemain))
		topClumnIndex=0
		
		topClumnDataType=self.ClumnDataType[topClumnIndex]
		if topClumnDataType=='int':
			self.transToIntInList(topClumnIndex)
		elif topClumnDataType=='float':
			self.transToFloatInList(topClumnIndex)
			
		# maintain a heapq with nTop elements
		if(self.DataInListLength<=nTop):
			self.DataInList=sorted(self.DataInList,key=lambda X:X[topClumnIndex],reverse=True)
		else:
			nTopList=self.DataInList[0:nTop]
			heapq.heapify(nTopList)
			for index in xrange(nTop,self.DataInListLength):
				if self.DataInList[index][topClumnIndex]>nTopList[0][topClumnIndex]:
					heapq.heapreplace(nTopList,self.DataInList[index])
			
			self.DataInListLength=nTop
			self.DataInList=sorted(nTopList,key=lambda X:X[topClumnIndex],reverse=True)
		
		if topClumnDataType in ['int','float']:
			self.transToStrInList(topClumnIndex)
		
		print 'top '+str(time.time()-now)+' seconds'

	
	def uniq(self,*uniqClumn):
		now=time.time()
		
		for oneuniqClumn in uniqClumn:
			assert oneuniqClumn in self.ClumnRemain,oneuniqClumn+' not in '+str(self.ClumnRemain)
		
		uniqClumnIndex=[self.ClumnRemain.index(oneuniqClumn) for oneuniqClumn in uniqClumn]
		uniqedKeys=['\t'.join([oneLine[oneUniqIndex] for oneUniqIndex in uniqClumnIndex]) for oneLine in self.DataInList]
		
		UniqedDataInDict=defaultdict(list)
		for lineNum in xrange(self.DataInListLength):
			oneKey=uniqedKeys[lineNum]
			if oneKey not in UniqedDataInDict:
				UniqedDataInDict[oneKey]=self.DataInList[lineNum]
		
		self.DataInList=UniqedDataInDict.values()
		self.DataInListLength=len(self.DataInList)
		
		print 'uniq '+str(time.time()-now)+' seconds'

	
	def SortBy(self,sortType,*sortByClumns):
		now=time.time()
		
		assert sortType in ('asc','desc'),sortType+' not in (asc,desc)'
		for sortClumn in sortByClumns:
			assert sortClumn in self.ClumnRemain,sortClumn+' not in '+str(self.ClumnRemain)
		
		
		sortByClumnIndex=[self.ClumnRemain.index(oneSortByClumn) for oneSortByClumn in sortByClumns]
		intTypeClumnIndex=[oneClumnIndex for oneClumnIndex in sortByClumnIndex if self.ClumnDataType[oneClumnIndex]=='int']
		floatTypeClumnIndex=[oneClumnIndex for oneClumnIndex in sortByClumnIndex if self.ClumnDataType[oneClumnIndex]=='float']
		for oneIndex in intTypeClumnIndex:
			self.transToIntInList(oneIndex)
		for oneIndex in floatTypeClumnIndex:
			self.transToFloatInList(oneIndex)
		
		sortType='False' if sortType=='asc' else 'True'
		sortByClumnIndexStr=','.join([str(oneClumnIndex) for oneClumnIndex in sortByClumnIndex])
		compileStr='self.DataInList=sorted(self.DataInList,key=itemgetter('+sortByClumnIndexStr+'),reverse='+sortType+')'
		exec compile(compileStr,'','exec')
		
		# after sort change its type from int to str
		for oneIndex in intTypeClumnIndex+floatTypeClumnIndex:
			self.transToStrInList(oneIndex)
		
		print 'SortBy '+str(time.time()-now)+' seconds'
	
	
	def leftJoin(self,leftClumn,rightPyQuery,rightClumn):
		now=time.time()
		
		assert type(rightPyQuery) == type(self),'leftJoin must on two PyQuery'
		assert leftClumn in self.ClumnRemain,leftClumn+' not in '+str(self.ClumnRemain)
		assert rightClumn in rightPyQuery.ClumnRemain,rightClumn+' not in '+str(rightPyQuery.ClumnRemain)
		
		leftClumnIndex=self.ClumnRemain.index(leftClumn)
		rightClumnIndex=rightPyQuery.ClumnRemain.index(rightClumn)
		
		rightPyQuery.groupBy(rightClumn)
		
		joinedDataInList=[]
		for lineNum in xrange(self.DataInListLength):
			oneLeftLine=self.DataInList[lineNum]
			joinKey=self.DataInList[lineNum][leftClumnIndex]
			tempDataInList=[oneLeftLine+oneRightLine for oneRightLine in rightPyQuery.DataInDict[joinKey]]
			joinedDataInList.extend(tempDataInList)
		
		self.DataInList=joinedDataInList
		self.DataInListLength=len(self.DataInList)
		self.ClumnRemain+=rightPyQuery.ClumnRemain
		
		tempClumnRemain=self.ClumnRemain[:]
		tempClumnRemain.remove(rightClumn)
		self.ClumnDataType+=rightPyQuery.ClumnDataType
		self.select(*tuple(tempClumnRemain))
		
		rightPyQuery.clean()
		
		print 'leftJoin '+str(time.time()-now)+' seconds'
	
	
	def head(self,nHead):
		now=time.time()
		
		if(self.DataInListLength<=nHead):
			pass
		else:
			self.DataInList=[self.DataInList[index] for index in xrange(nHead)]
			self.DataInListLength=nHead
		
		print 'head '+str(time.time()-now)+' seconds'
	
	
	def sample(self,nSample):
		now=time.time()
		
		if(self.DataInListLength<=nSample):
			pass
		else:
			nSampleIndex=random.sample(xrange(self.DataInListLength),nSample)
			self.DataInList=[self.DataInList[index] for index in nSampleIndex]
			self.DataInListLength=nSample
		
		print 'sample '+str(time.time()-now)+' seconds'
	
	
	def tail(self,nTail):
		now=time.time()
		
		if(self.DataInListLength<=nTail):
			pass
		else:
			self.DataInList=[self.DataInList[self.DataInListLength-index] for index in xrange(nTail,0,-1)]
			self.DataInListLength=nTail
			
		print 'tail '+str(time.time()-now)+' seconds'
	
	
	def outputAsFile(self,resultFilename):
		now=time.time()
		
		for lineNum in xrange(self.DataInListLength):
			self.DataInList[lineNum]='\t'.join(self.DataInList[lineNum])+'\n'
		
		resultFile=open(resultFilename,'w+')
		resultFile.write('\t'.join(self.ClumnRemain)+'\n')
		resultFile.writelines(self.DataInList)
		resultFile.close()
		
		print 'outputAsFile '+str(time.time()-now)+' seconds'
	
	
	def clean(self):
		self.DataInList = []
		self.DataInDict = defaultdict(list)
		self.ClumnRemain = []
		self.ClumnGrouped = []
		self.DataInListLength=0
	
	
	def transToStrInList(self,clumnIndex):
		for oneRecord in self.DataInList:
			oneRecord[clumnIndex]=str(oneRecord[clumnIndex])
	
	def transToFloatInList(self,clumnIndex):
		for oneRecord in self.DataInList:
			oneRecord[clumnIndex]=float(oneRecord[clumnIndex])
	
	def transToIntInList(self,clumnIndex):
		for oneRecord in self.DataInList:
			oneRecord[clumnIndex]=int(oneRecord[clumnIndex])
			
	def transToStrInDict(self,clumnIndex):
		for oneGrouped in self.DataInDict.values():
			for oneRecord in oneGrouped:
				oneRecord[clumnIndex]=str(oneRecord[clumnIndex])
	
	def transToFloatInDict(self,clumnIndex):
		for oneGrouped in self.DataInDict.values():
			for oneRecord in oneGrouped:
				oneRecord[clumnIndex]=float(oneRecord[clumnIndex])
	
	def transToIntInDict(self,clumnIndex):
		for oneGrouped in self.DataInDict.values():
			for oneRecord in oneGrouped:
				oneRecord[clumnIndex]=int(oneRecord[clumnIndex])