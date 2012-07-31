#coding=utf-8
import heapq,time,random
from collections import defaultdict
from operator import itemgetter

class PyQuery:
	
	def __init__(self):
		self.inputFileName=''
		self.splitFlag=''
		#DataInTuple,type is tuple(tuple):store every line's data before select
		self.DataInTuple = ()
		#DataInList,type is list(list):store every line's data
		self.DataInList = []
		#DataInDict,type is dict(key:list):store data after groupby
		self.DataInDict = defaultdict(list)
		#ClumnRemain,type is list:current clumns
		self.ClumnRemain = []
		#ClumnGrouped,type is list:the grouped clumns
		self.ClumnGrouped = []
		#DataInListLength:total line number
		self.DataInListLength=0
	
	
	def input(self,inputFileName,splitFlag,clumnName):
		self.ClumnRemain=[oneClumn for oneClumn in clumnName]
		self.inputFileName=inputFileName
		self.splitFlag=splitFlag
	
	
	def select(self,*ClumnSelected):
		now=time.time()
		
		for oneSelected in ClumnSelected:
			assert oneSelected in self.ClumnRemain,oneSelected+' not in '+str(self.ClumnRemain)
		
		selectedClumnIndex=[self.ClumnRemain.index(oneSelectedClumn) for oneSelectedClumn in ClumnSelected]
		
		clumnNum=len(self.ClumnRemain)
		self.DataInTuple=(tuple(oneLine.rstrip().split(self.splitFlag)) for oneLine in open(self.inputFileName))
		self.DataInList=[[oneLine[idx].strip() for idx in selectedClumnIndex] for oneLine in self.DataInTuple if len(oneLine)==clumnNum]
		
		self.DataInListLength=len(self.DataInList)
		self.ClumnRemain=list(ClumnSelected)
		
		print 'input '+str(self.DataInListLength)+' lines'
		print 'select '+str(time.time()-now)+' seconds'
		

	def filter(self,filterClumn,filterOper,compareValue):
		now=time.time()
		
		filterClumnIndex=self.ClumnRemain.index(filterClumn)
		compileStr='self.DataInList=[self.DataInList[lineNum] for lineNum in xrange(self.DataInListLength) if self.DataInList[lineNum][%s] %s \'%s\']' % (str(filterClumnIndex),filterOper,compareValue)
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
		
		for lineNum in xrange(self.DataInListLength):
			oneGroupedKey='\t'.join([self.DataInList[lineNum][oneGroupIndex] for oneGroupIndex in groupClumnIndex])
			self.DataInDict[oneGroupedKey].append(self.DataInList[lineNum])
		
		self.DataInList=[]
		self.DataInListLength=len(self.DataInDict)
		self.ClumnGrouped=list(groupClumn)
		
		print 'groupBy '+str(time.time()-now)+' seconds'
	
	
	def countEach(self,countClumn,resultClumn):
		now=time.time()
		
		countClumnIndex=self.ClumnRemain.index(countClumn)
		for oneGroupedKey in self.DataInDict.keys():
			self.DataInDict[oneGroupedKey]=str(len(self.DataInDict[oneGroupedKey]))

		for k,v in self.DataInDict.items():
			self.DataInList.append(k.split('\t')+[v])
		
		self.ClumnRemain=self.ClumnGrouped+[resultClumn]
		
		print 'countEach '+str(time.time()-now)+' seconds'
	
	
	def sumEach(self,sumClumn,resultClumn):	
		now=time.time()
		
		sumClumnIndex=self.ClumnRemain.index(sumClumn)
		for oneGroupedKey in self.DataInDict.keys():
			self.DataInDict[oneGroupedKey]=str(sum([int(oneRecord[sumClumnIndex]) for oneRecord in self.DataInDict[oneGroupedKey]]))
		
		for k,v in self.DataInDict.items():
			self.DataInList.append(k.split()+[v])
		
		self.ClumnRemain=self.ClumnGrouped+[resultClumn]
		print 'sumEach '+str(time.time()-now)+' seconds'
	
	
	def averageEach(self,averageClumn,resultClumn):
		now=time.time()
		
		averageClumnIndex=self.ClumnRemain.index(averageClumn)
		for oneGroupedKey in self.DataInDict.keys():
			self.DataInDict[oneGroupedKey]=str(sum([int(oneRecord[averageClumnIndex]) for oneRecord in self.DataInDict[oneGroupedKey]])/len(self.DataInDict[oneGroupedKey]))
		
		for k,v in self.DataInDict.items():
			self.DataInList.append(k.split()+[v])
			
		self.ClumnRemain=self.ClumnGrouped+[resultClumn]
		
		print 'averageEach '+str(time.time()-now)+' seconds'
	
	
	def topEach(self,topClumn,nTop):
		now=time.time()
		
		topClumnIndex=self.ClumnRemain.index(topClumn)
		for oneGroupedValue in self.DataInDict.values():
			for oneRecord in oneGroupedValue:
				oneRecord[topClumnIndex]=int(oneRecord[topClumnIndex])
		
		for oneGroupedKey in self.DataInDict.keys():
			compileStr='oneGroupedValue=sorted(self.DataInDict[\''+oneGroupedKey+'\'],key=itemgetter('+str(topClumnIndex)+'),reverse=True)'
			exec compile(compileStr,'','exec')
			nTop=min(len(oneGroupedValue),nTop)
			self.DataInDict[oneGroupedKey]=oneGroupedValue[:nTop]
		
		for oneGroupedValue in self.DataInDict.values():
			for oneRecord in oneGroupedValue:
				oneRecord[topClumnIndex]=str(oneRecord[topClumnIndex])
				self.DataInList.append(oneRecord)
		self.DataInListLength=len(self.DataInList)
		
		print 'topEach '+str(time.time()-now)+' seconds'
		
	
	def sortEachBy(self,sortType,*sortByClumns):
		now=time.time()
		
		assert sortType in ('asc','desc'),sortType+' not in (asc,desc)'
		for sortClumn in sortByClumns:
			assert sortClumn in self.ClumnRemain,sortClumn+' not in '+str(self.ClumnRemain)
		
		
		# if one sort clumn's isdigit,sort this clumn by its int value
		sortByClumnIndex=[self.ClumnRemain.index(oneSortByClumn) for oneSortByClumn in sortByClumns]
		intTypeClumnIndex=[oneClumnIndex for oneClumnIndex in sortByClumnIndex if self.DataInDict.values()[0][0][oneClumnIndex].isdigit()]
		for oneGroupedValue in self.DataInDict.values():
			for oneRecord in oneGroupedValue:
				for oneIndex in intTypeClumnIndex:
					oneRecord[oneIndex]=int(oneRecord[oneIndex])
		
		sortByClumnIndexStr=','.join([str(oneClumnIndex) for oneClumnIndex in sortByClumnIndex])
		sortType='False' if sortType=='asc' else 'True'
		for oneGroupedKey in self.DataInDict.keys():
			compileStr='self.DataInDict[\''+oneGroupedKey+'\']=sorted(self.DataInDict[\''+oneGroupedKey+'\'],key=itemgetter('+sortByClumnIndexStr+'),reverse='+sortType+')'
			exec compile(compileStr,'','exec')
		
		for oneGroupedValue in self.DataInDict.values():
			for oneRecord in oneGroupedValue:
				for oneIndex in intTypeClumnIndex:
					oneRecord[oneIndex]=str(oneRecord[oneIndex])
				self.DataInList.append(oneRecord)
		self.DataInListLength=len(self.DataInList)
		
		print 'sortEachBy '+str(time.time()-now)+' seconds'
		
	
	def changeClumnOrder(self,*clumnOrder):
		for oneSelected in clumnOrder:
			assert oneSelected in self.ClumnRemain,oneSelected+' not in '+str(self.ClumnRemain)
		
		selectedClumnIndex=[self.ClumnRemain.index(oneSelectedClumn) for oneSelectedClumn in clumnOrder]
		for lineNum in xrange(self.DataInListLength):
			self.DataInList[lineNum]=[self.DataInList[lineNum][oneClumnIndex] for oneClumnIndex in selectedClumnIndex]
		
		self.ClumnRemain=list(clumnOrder)
	
	
	def top(self,topClumn,nTop):
		now=time.time()
		
		assert topClumn in self.ClumnRemain,topClumn+' not in '+str(self.ClumnRemain)
		
		# change clumn order,make topClumnIndex=0, so can sort by the first clumn
		tempClumnRemain=self.ClumnRemain[:]
		tempClumnRemain.remove(topClumn)
		tempClumnRemain.insert(0,topClumn)
		
		self.changeClumnOrder(*tuple(tempClumnRemain))
		
		# the value of top clumn must be isdigit,compare this clumn by its int value
		for index in xrange(self.DataInListLength):
			self.DataInList[index][0]=int(self.DataInList[index][0])
		
		# maintain a heapq with nTop elements
		if(self.DataInListLength<=nTop):
			self.DataInList=sorted(self.DataInList,key=lambda X:X[0],reverse=True)
		else:
			nTopList=self.DataInList[0:nTop]
			heapq.heapify(nTopList)
			for index in xrange(nTop,self.DataInListLength):
				if self.DataInList[index][0]>nTopList[0][0]:
					heapq.heapreplace(nTopList,self.DataInList[index])
			
			self.DataInListLength=nTop
			self.DataInList=sorted(nTopList,key=lambda X:X[0],reverse=True)
			
		for index in xrange(self.DataInListLength):
			self.DataInList[index][0]=str(self.DataInList[index][0])
		
		print 'top '+str(time.time()-now)+' seconds'

	
	def uniq(self,*uniqClumn):
		now=time.time()
		
		UniqedDataInDict=defaultdict(list)
		uniqClumnIndex=[self.ClumnRemain.index(oneuniqClumn) for oneuniqClumn in uniqClumn]
		for lineNum in xrange(self.DataInListLength):
			oneUniqedValue='\t'.join([self.DataInList[lineNum][oneUniqIndex] for oneUniqIndex in uniqClumnIndex])
			if oneUniqedValue not in UniqedDataInDict:
				UniqedDataInDict[oneUniqedValue]=self.DataInList[lineNum]
		
		self.DataInList=UniqedDataInDict.values()
		print self.DataInList
		self.DataInListLength=len(self.DataInList)
		
		print 'uniq '+str(time.time()-now)+' seconds'

	
	def SortBy(self,sortType,*sortByClumns):
		now=time.time()
		
		assert sortType in ('asc','desc'),sortType+' not in (asc,desc)'
		for sortClumn in sortByClumns:
			assert sortClumn in self.ClumnRemain,sortClumn+' not in '+str(self.ClumnRemain)
		
		sortByClumnIndex=[self.ClumnRemain.index(oneSortByClumn) for oneSortByClumn in sortByClumns]
		intTypeClumnIndex=[oneClumnIndex for oneClumnIndex in sortByClumnIndex if self.DataInList[0][oneClumnIndex].isdigit()]
		
		# if one sort clumn's isdigit,sort this clumn by its int value
		for oneClumnIndex in intTypeClumnIndex:
			for index in xrange(self.DataInListLength):
				self.DataInList[index][oneClumnIndex]=int(self.DataInList[index][oneClumnIndex])
		
		sortType='False' if sortType=='asc' else 'True'
		sortByClumnIndexStr=','.join([str(oneClumnIndex) for oneClumnIndex in sortByClumnIndex])
		compileStr='self.DataInList=sorted(self.DataInList,key=itemgetter('+sortByClumnIndexStr+'),reverse='+sortType+')'
		exec compile(compileStr,'','exec')
		
		# after sort change its type from int to str
		for oneClumnIndex in intTypeClumnIndex:
			for index in xrange(self.DataInListLength):
				self.DataInList[index][oneClumnIndex]=str(self.DataInList[index][oneClumnIndex])
		
		print 'SortBy '+str(time.time()-now)+' seconds'
	
	
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
			print nSampleIndex
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
		resultFile.writelines(self.DataInList)
		resultFile.close()
		
		print 'outputAsFile '+str(time.time()-now)+' seconds'
	
	
	def clean(self):
		self.DataInList = []
		self.DataInDict = defaultdict(list)
		self.ClumnRemain = []
		self.ClumnGrouped = []