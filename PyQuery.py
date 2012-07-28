#coding=utf-8
import heapq,time
from collections import defaultdict
from operator import itemgetter

class PyQuery:
	
	def __init__(self):
		#DataInList,数据结构为list(list),存储每行的数据
		self.DataInList = []
		#DataInDict,数据结构为dict(key:list),存储group之后的中间数据
		self.DataInDict = defaultdict(list)
		#ClumnRemain,数据结构为list,存储当前数据包含的列名
		self.ClumnRemain = []
		#ClumnGrouped,数据结构为list,存储group by的列名
		self.ClumnGrouped = []
		#DataInListLength,当前数据的行数
		self.DataInListLength=0
	
	
	def input(self,inputFileName,splitFlag,clumnName):
		now=time.time()
		
		self.ClumnRemain=[oneClumn for oneClumn in clumnName]
		splitnum=len(clumnName)
		for oneRecord in open(inputFileName,'rb'):
			# 过滤特殊符号'\
			if oneRecord.find('\\')!=-1:
				oneRecord=oneRecord.replace('\\','\\\\')
			if oneRecord.find('\'')!=-1:
				oneRecord=oneRecord.replace('\'','\\\'')
			
			oneLineItems=[oneItem.strip() for oneItem in oneRecord.strip().split(splitFlag)]
			if len(oneLineItems)==splitnum:
				self.DataInList.append(oneLineItems)
				self.DataInListLength+=1
		
		print 'input '+str(time.time()-now)+' seconds('+str(self.DataInListLength)+' lines)'
		
	
	def select(self,*ClumnSelected):
		now=time.time()
		
		for oneSelected in ClumnSelected:
			assert oneSelected in self.ClumnRemain,oneSelected+' not in '+str(self.ClumnRemain)
		
		selectedClumnIndex=[self.ClumnRemain.index(oneSelectedClumn) for oneSelectedClumn in ClumnSelected]
		for lineNum in xrange(self.DataInListLength):
			self.DataInList[lineNum]=[self.DataInList[lineNum][oneClumnIndex] for oneClumnIndex in selectedClumnIndex]
		
		self.ClumnRemain=list(ClumnSelected)
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
			oneGroupedValue='\t'.join([self.DataInList[lineNum][oneGroupIndex] for oneGroupIndex in groupClumnIndex])
			self.DataInDict[oneGroupedValue].append(self.DataInList[lineNum])
		
		self.DataInList=[]
		self.DataInListLength=len(self.DataInDict)
		self.ClumnGrouped=list(groupClumn)
		
		print 'groupBy '+str(time.time()-now)+' seconds'
	
	
	def countEach(self,countClumn,resultClumn):
		now=time.time()
		
		countClumnIndex=self.ClumnRemain.index(countClumn)
		for oneGroupedValue in self.DataInDict.keys():
			self.DataInDict[oneGroupedValue]=str(len(self.DataInDict[oneGroupedValue]))

		for k,v in self.DataInDict.items():
			self.DataInList.append(k.split('\t')+[v])
		
		self.ClumnRemain=self.ClumnGrouped+[resultClumn]
		
		print 'countEach '+str(time.time()-now)+' seconds'
	
	
	def sumEach(self,sumClumn,resultClumn):	
		now=time.time()
		
		sumClumnIndex=self.ClumnRemain.index(sumClumn)
		for oneGroupedValue in self.DataInDict.keys():
			self.DataInDict[oneGroupedValue]=str(sum([int(oneRecord[sumClumnIndex]) for oneRecord in self.DataInDict[oneGroupedValue]]))
		
		for k,v in self.DataInDict.items():
			self.DataInList.append(k.split()+[v])
		
		self.ClumnRemain=self.ClumnGrouped+[resultClumn]
		print 'sumEach '+str(time.time()-now)+' seconds'
	
	
	def averageEach(self,averageClumn,resultClumn):
		now=time.time()
		
		averageClumnIndex=self.ClumnRemain.index(averageClumn)
		for oneGroupedValue in self.DataInDict.keys():
			self.DataInDict[oneGroupedValue]=str(sum([int(oneRecord[averageClumnIndex]) for oneRecord in self.DataInDict[oneGroupedValue]])/len(self.DataInDict[oneGroupedValue]))
		
		for k,v in self.DataInDict.items():
			self.DataInList.append(k.split()+[v])
			
		self.ClumnRemain=self.ClumnGrouped+[resultClumn]
		
		print 'averageEach '+str(time.time()-now)+' seconds'
	
	
	def top(self,topClumn,nTop):
		now=time.time()
		
		assert topClumn in self.ClumnRemain,topClumn+' not in '+str(self.ClumnRemain)
		
		# change clumn order,make topClumnIndex=0, so can sort by the first clumn
		tempClumnRemain=self.ClumnRemain[:]
		tempClumnRemain.remove(topClumn)
		tempClumnRemain.insert(0,topClumn)
		
		self.select(*tuple(tempClumnRemain))
		
		# make the type of sort clumn is int
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
		
		# if one sortByClumn isdigit,before sort its type should change from str to int
		for oneClumnIndex in intTypeClumnIndex:
			for index in xrange(self.DataInListLength):
				self.DataInList[index][oneClumnIndex]=int(self.DataInList[index][oneClumnIndex])
		
		sortType='False' if sortType=='asc' else 'True'
		sortByClumnIndexStr=','.join([str(oneClumnIndex) for oneClumnIndex in sortByClumnIndex])
		compileStr='SortedDataInList=sorted(self.DataInList,key=itemgetter('+sortByClumnIndexStr+'),reverse='+sortType+')'
		exec compile(compileStr,'','exec')
		self.DataInList=SortedDataInList
		
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
		resultFile=open(resultFilename,'w+')
		for lineNum in xrange(self.DataInListLength):
			resultFile.write('%s%s' % ('\t'.join(self.DataInList[lineNum]),'\n'))
		resultFile.close()
		print 'outputAsFile '+str(time.time()-now)+' seconds'
	
	
	def clean(self):
		self.DataInList = []
		self.DataInDict = defaultdict(list)
		self.ClumnRemain = []
		self.ClumnGrouped = []