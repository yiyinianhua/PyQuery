#coding=utf-8
import heapq,time,random
from collections import defaultdict
from operator import itemgetter
from itertools import ifilter
import re,os


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
	
	def InputAndSelect(self,inputFileName,splitFlag,clumnName,clumnNameSelect):
		now=time.time()
		
		for oneSelected in clumnNameSelect:
			assert oneSelected in clumnName,oneSelected+' not in '+str(clumnName)
		
		self.ClumnRemain=[oneClumn for oneClumn in clumnName]
		selectedClumnIdx=[self.ClumnRemain.index(oneSelectedClumn) for oneSelectedClumn in clumnNameSelect]
		
		clumnNum=len(self.ClumnRemain)
		
		if len(clumnName)/2>=len(clumnNameSelect):
			outSideFile=open('PyQuery.OutSideFile','w+')
			for oneLine in open(inputFileName):
				if clumnNum-oneLine.count(splitFlag)==1:
					oneLineItems=oneLine.rstrip().split(splitFlag)
					selectedItem=[oneLineItems[idx].strip() for idx in selectedClumnIdx]
					outSideFile.write('\t'.join(selectedItem)+'\n')
			outSideFile.close()
			self.DataInList=[oneLine.rstrip().split('\t') for oneLine in open('PyQuery.OutSideFile')]
		else:
			tempDataInTuple=(tuple(oneLine.rstrip().split(splitFlag)) for oneLine in open(inputFileName) if clumnNum-oneLine.count(splitFlag)==1)
			self.DataInList=[[oneLine[idx].strip() for idx in selectedClumnIdx] for oneLine in tempDataInTuple]
		
		self.DataInListLength=len(self.DataInList)
		self.ClumnRemain=clumnNameSelect
		
		#Detect data type of each clumn
		for oneClumnData in self.DataInList[10]:
			if oneClumnData.isdigit():
				self.ClumnDataType.append('int')
			elif re.match('^\d+.\d+$',oneClumnData):
				self.ClumnDataType.append('float')
			else:
				self.ClumnDataType.append('string')
		
		print 'InputAndSelect: '+str(time.time()-now)+' seconds'
	
	
	def Select(self,ClumnSelected):
		now=time.time()
		
		for oneSelected in ClumnSelected:
			assert oneSelected in self.ClumnRemain,oneSelected+' not in '+str(self.ClumnRemain)
		
		selectedClumnIdx=[self.ClumnRemain.index(oneSelectedClumn) for oneSelectedClumn in ClumnSelected]
		for lineNum in xrange(self.DataInListLength):
			self.DataInList[lineNum]=[self.DataInList[lineNum][oneClumnIndex] for oneClumnIndex in selectedClumnIdx]
		
		self.ClumnRemain=list(ClumnSelected)
		self.ClumnDataType=[self.ClumnDataType[oneSelectedIdx] for oneSelectedIdx in selectedClumnIdx]
		
		print 'Select: '+str(time.time()-now)+' seconds'
	
	
	def Delect(self,clumnDelected):
		now=time.time()
		
		assert len(clumnDelected)==1 and clumnDelected[0] in self.ClumnRemain,'Delect only dealwith one clumn and must be in '+str(self.ClumnRemain)
		
		delectedClumnIdx=self.ClumnRemain.index(clumnDelected[0])
		for oneRecord in self.DataInList:
			del oneRecord[delectedClumnIdx]
		
		del self.ClumnRemain[delectedClumnIdx]
		del self.ClumnDataType[delectedClumnIdx]
		
		print 'Delect: '+str(time.time()-now)+' seconds'
		
	
	def Filter(self,filterClumn,filterOper,compareValue):
		now=time.time()
		
		assert len(filterClumn)==1 and filterClumn[0] in self.ClumnRemain,'Filter only dealwith one clumn and must be in '+str(self.ClumnRemain)
		
		filterClumnIndex=self.ClumnRemain.index(filterClumn[0])
		filterClumnDataType=self.ClumnDataType[filterClumnIndex]
		if filterOper in ['<','>','<=','>='] and filterClumnDataType in ['int','float']:
			if filterClumnDataType=='int':
				self.TransToIntInList(filterClumnIndex)
			elif filterClumnDataType=='float':
				self.TransToFloatInList(filterClumnIndex)
		
		if type(compareValue)==type(1) or type(compareValue)==type(1.0):
			compileStr='self.DataInList=list(ifilter(lambda x: x[%s] %s %s,self.DataInList))' % (str(filterClumnIndex),filterOper,str(compareValue))
		elif type(compareValue)==type(''):
			compileStr='self.DataInList=list(ifilter(lambda x: x[%s] %s \'%s\',self.DataInList))' % (str(filterClumnIndex),filterOper,compareValue)
		exec compile(compileStr,'','exec')
		
		if filterOper in ['<','>','<=','>='] and filterClumnDataType in ['int','float']:
			self.TransToStrInList(filterClumnIndex)
			
		self.DataInListLength=len(self.DataInList)
		
		print 'Filter: '+str(time.time()-now)+' seconds'
		
		
	def Translate(self,transClumn,transFunction):
		now=time.time()
		
		assert len(transClumn)==1 and transClumn[0] in self.ClumnRemain,'Translate only dealwith one clumn and must be in '+str(self.ClumnRemain)
		
		transClumnIndex=self.ClumnRemain.index(transClumn[0])
		for lineNum in xrange(self.DataInListLength):
			self.DataInList[lineNum][transClumnIndex]=transFunction(self.DataInList[lineNum][transClumnIndex])
		
		print 'Translate '+str(time.time()-now)+' seconds'

		
	def GroupBy(self,groupClumn):
		now=time.time()
		
		for oneGroupByClumn in groupClumn:
			assert oneGroupByClumn in self.ClumnRemain,'GroupBy: '+oneGroupByClumn+' not in '+str(self.ClumnRemain)
		
		groupClumnIndex=[self.ClumnRemain.index(oneGroupClumn) for oneGroupClumn in groupClumn]
		groupedKey=['\t'.join([oneLine[oneGroupIndex] for oneGroupIndex in groupClumnIndex]) for oneLine in self.DataInList]
		for lineNum in xrange(self.DataInListLength):
			if groupedKey[lineNum]:
				self.DataInDict[groupedKey[lineNum]].append(self.DataInList[lineNum])
		
		self.DataInList=[]
		self.DataInListLength=len(self.DataInDict)
		self.ClumnGrouped=groupClumn
		
		print 'GroupBy '+str(time.time()-now)+' seconds'
	
	
	def CountEach(self,resultClumn):
		now=time.time()
		
		for oneGroupedKey in self.DataInDict:
				self.DataInList.append(oneGroupedKey.split('\t')+[str(len(self.DataInDict[oneGroupedKey]))])

		self.DataInDict=defaultdict(list)
		self.ClumnDataType=[self.ClumnDataType[self.ClumnRemain.index(oneGrouped)] for oneGrouped in self.ClumnGrouped]
		self.ClumnDataType.append('int')
		self.ClumnRemain=self.ClumnGrouped+[resultClumn]
		
		print 'CountEach: '+str(time.time()-now)+' seconds'
	
	
	def SumEach(self,sumClumn,resultClumn):	
		now=time.time()
		
		assert len(sumClumn)==1 and sumClumn[0] in self.ClumnRemain,'SumEach only dealwith one clumn and must be in '+str(self.ClumnRemain)
		
		sumClumnIndex=self.ClumnRemain.index(sumClumn[0])
		for oneGroupedKey in self.DataInDict:
			self.DataInList.append(oneGroupedKey.split('\t')+[str(sum([float(oneRecord[sumClumnIndex]) for oneRecord in self.DataInDict[oneGroupedKey]]))])
		
		self.DataInDict=defaultdict(list)
		self.ClumnDataType=[self.ClumnDataType[self.ClumnRemain.index(oneGrouped)] for oneGrouped in self.ClumnGrouped]
		self.ClumnDataType.append('float')
		self.ClumnRemain=self.ClumnGrouped+[resultClumn]
		
		print 'SumEach: '+str(time.time()-now)+' seconds'
	
	
	def AverageEach(self,averageClumn,resultClumn):
		now=time.time()
		
		assert len(averageClumn)==1 and averageClumn[0] in self.ClumnRemain,'AverageEach only dealwith one clumn and must be in '+str(self.ClumnRemain)

		averageClumnIndex=self.ClumnRemain.index(averageClumn[0])
		for oneGroupedKey in self.DataInDict:
			self.DataInList.append(oneGroupedKey.split('\t')+[str(sum([float(oneRecord[averageClumnIndex]) for oneRecord in self.DataInDict[oneGroupedKey]])/len(self.DataInDict[oneGroupedKey]))])
		
		self.DataInDict=defaultdict(list)
		self.ClumnDataType=[self.ClumnDataType[self.ClumnRemain.index(oneGrouped)] for oneGrouped in self.ClumnGrouped]
		self.ClumnDataType.append('float')
		self.ClumnRemain=self.ClumnGrouped+[resultClumn]
		
		print 'AverageEach: '+str(time.time()-now)+' seconds'
	
	
	def TopEach(self,topClumn,nTop):
		now=time.time()
		
		assert len(topClumn)==1 and topClumn[0] in self.ClumnRemain,'TopEach only dealwith one clumn and must be in '+str(self.ClumnRemain)
		
		topClumnIndex=self.ClumnRemain.index(topClumn[0])
		topClumnDataType=self.ClumnDataType[topClumnIndex]
		
		if topClumnDataType=='int':
			self.TransToIntInDict(topClumnIndex)
		elif topClumnDataType=='float':
			self.TransToFloatInDict(topClumnIndex)
			
		for oneGroupedKey in self.DataInDict:
			self.DataInList.extend(heapq.nlargest(nTop,self.DataInDict[oneGroupedKey],key=lambda x:x[topClumnIndex]))
		
		self.DataInDict=defaultdict(list)
		
		if topClumnDataType in ['int','float']:
			self.TransToStrInList(topClumnIndex)
		
		self.DataInListLength=len(self.DataInList)
		
		print 'TopEach: '+str(time.time()-now)+' seconds'
		
	
	def BottomEach(self,bottomClumn,nBottom):
		now=time.time()
		
		assert len(bottomClumn)==1 and bottomClumn[0] in self.ClumnRemain,'BottomEach only dealwith one clumn and must be in '+str(self.ClumnRemain)
		
		bottomClumnIndex=self.ClumnRemain.index(bottomClumn[0])
		bottomClumnDataType=self.ClumnDataType[bottomClumnIndex]
		
		if bottomClumnDataType=='int':
			self.TransToIntInDict(bottomClumnIndex)
		elif bottomClumnDataType=='float':
			self.TransToFloatInDict(bottomClumnIndex)
			
		for oneGroupedKey in self.DataInDict:
			self.DataInList.extend(heapq.nsmallest(nBottom,self.DataInDict[oneGroupedKey],key=lambda x:x[bottomClumnIndex]))
		
		self.DataInDict=defaultdict(list)
		
		if bottomClumnDataType in ['int','float']:
			self.TransToStrInList(bottomClumnIndex)
		
		self.DataInListLength=len(self.DataInList)
		
		print 'BottomEach: '+str(time.time()-now)+' seconds'
		
	
	def Count(self,resultClumn):
		now=time.time()
		
		self.DataInList=[[str(self.DataInListLength)]]

		self.ClumnDataType=['int']
		self.ClumnRemain=[resultClumn]
		self.DataInListLength=1
		
		print 'Count: '+str(time.time()-now)+' seconds'
	
	
	def Sum(self,sumClumn,resultClumn):	
		now=time.time()
		
		assert len(sumClumn)==1 and sumClumn[0] in self.ClumnRemain,'Sum only dealwith one clumn and must be in '+str(self.ClumnRemain)
		
		sumClumnIndex=self.ClumnRemain.index(sumClumn[0])
		self.DataInList=[[str(sum([float(oneRecord[sumClumnIndex]) for oneRecord in self.DataInList]))]]
		
		self.ClumnDataType=['float']
		self.ClumnRemain=[resultClumn]
		self.DataInListLength=1
		
		print 'Sum: '+str(time.time()-now)+' seconds'
	
	
	def Average(self,averageClumn,resultClumn):
		now=time.time()
		
		assert len(averageClumn)==1 and averageClumn[0] in self.ClumnRemain,'Average only dealwith one clumn and must be in '+str(self.ClumnRemain)

		averageClumnIndex=self.ClumnRemain.index(averageClumn[0])
		self.DataInList=[[str(sum([float(oneRecord[averageClumnIndex]) for oneRecord in self.DataInList])/self.DataInListLength)]]
		
		self.ClumnDataType=['float']
		self.ClumnRemain=[resultClumn]
		self.DataInListLength=1
		
		print 'Average: '+str(time.time()-now)+' seconds'
		
	
	def Top(self,topClumn,nTop):
		now=time.time()
		
		assert len(topClumn)==1 and topClumn[0] in self.ClumnRemain,'Top only dealwith one clumn and must be in '+str(self.ClumnRemain)
		
		topClumnIndex=self.ClumnRemain.index(topClumn[0])
		topClumnDataType=self.ClumnDataType[topClumnIndex]
		if topClumnDataType=='int':
			self.TransToIntInList(topClumnIndex)
		elif topClumnDataType=='float':
			self.TransToFloatInList(topClumnIndex)
			
		self.DataInList=heapq.nlargest(nTop,self.DataInList,key=lambda x:x[topClumnIndex])
		self.DataInListLength=nTop
		
		if topClumnDataType in ['int','float']:
			self.TransToStrInList(topClumnIndex)
		
		print 'Top: '+str(time.time()-now)+' seconds'

	
	def Bottom(self,bottomClumn,nBottom):
		now=time.time()
		
		assert len(bottomClumn)==1 and bottomClumn[0] in self.ClumnRemain,'Bottom only dealwith one clumn and must be in '+str(self.ClumnRemain)
		
		bottomClumnIndex=self.ClumnRemain.index(bottomClumn[0])
		bottomClumnDataType=self.ClumnDataType[bottomClumnIndex]
		if bottomClumnDataType=='int':
			self.TransToIntInList(bottomClumnIndex)
		elif bottomClumnDataType=='float':
			self.TransToFloatInList(bottomClumnIndex)
			
		self.DataInList=heapq.nsmallest(nBottom,self.DataInList,key=lambda x:x[bottomClumnIndex])
		self.DataInListLength=nBottom
		
		if bottomClumnDataType in ['int','float']:
			self.TransToStrInList(bottomClumnIndex)
		
		print 'Bottom: '+str(time.time()-now)+' seconds'
		
	
	def Uniq(self,uniqClumn):
		now=time.time()
		
		for oneuniqClumn in uniqClumn:
			assert oneuniqClumn in self.ClumnRemain,'Uniq: '+oneuniqClumn+' not in '+str(self.ClumnRemain)
		
		uniqClumnIndex=[self.ClumnRemain.index(oneuniqClumn) for oneuniqClumn in uniqClumn]
		
		uniqedKeys=['\t'.join([oneLine[oneUniqIndex] for oneUniqIndex in uniqClumnIndex]) for oneLine in self.DataInList]
		UniqedDataInDict=defaultdict(list)
		for lineNum in xrange(self.DataInListLength):
			oneKey=uniqedKeys[lineNum]
			if oneKey not in UniqedDataInDict:
				UniqedDataInDict[oneKey]=self.DataInList[lineNum]
		
		self.DataInList=UniqedDataInDict.values()
		print self.DataInList[:10]
		self.DataInListLength=len(self.DataInList)
		
		print 'Uniq: '+str(time.time()-now)+' seconds'

	
	def SortBy(self,sortByClumns,sortType):
		now=time.time()
		
		assert sortType in ('asc','desc'),'SortBy: '+sortType+' not in (asc,desc)'
		for sortClumn in sortByClumns:
			assert sortClumn in self.ClumnRemain,'SortBy: '+sortClumn+' not in '+str(self.ClumnRemain)
		
		
		sortByClumnIndex=[self.ClumnRemain.index(oneSortByClumn) for oneSortByClumn in sortByClumns]
		intTypeClumnIndex=[oneClumnIndex for oneClumnIndex in sortByClumnIndex if self.ClumnDataType[oneClumnIndex]=='int']
		floatTypeClumnIndex=[oneClumnIndex for oneClumnIndex in sortByClumnIndex if self.ClumnDataType[oneClumnIndex]=='float']
		for oneIndex in intTypeClumnIndex:
			self.TransToIntInList(oneIndex)
		for oneIndex in floatTypeClumnIndex:
			self.TransToFloatInList(oneIndex)
		
		sortType='False' if sortType=='asc' else 'True'
		sortByClumnIndexStr=','.join([str(oneClumnIndex) for oneClumnIndex in sortByClumnIndex])
		compileStr='self.DataInList=sorted(self.DataInList,key=itemgetter('+sortByClumnIndexStr+'),reverse='+sortType+')'
		exec compile(compileStr,'','exec')
		
		# after sort change its type from int to str
		for oneIndex in intTypeClumnIndex+floatTypeClumnIndex:
			self.TransToStrInList(oneIndex)
		
		print 'SortBy: '+str(time.time()-now)+' seconds'
	
	
	def LeftJoin(self,leftClumn,rightPyQuery,rightClumn):
		now=time.time()
		
		assert type(rightPyQuery) == type(self),'LeftJoin: must be two PyQuery Instance'
		assert len(leftClumn)==1 and leftClumn[0] in self.ClumnRemain,'LeftJoin: '+leftClumn+' not in '+str(self.ClumnRemain)
		assert len(rightClumn)==1 and rightClumn[0] in rightPyQuery.ClumnRemain,'LeftJoin: '+rightClumn+' not in '+str(rightPyQuery.ClumnRemain)
		
		leftClumnIndex=self.ClumnRemain.index(leftClumn[0])
		rightClumnIndex=rightPyQuery.ClumnRemain.index(rightClumn[0])
		emptyRightLine=len(rightPyQuery.ClumnRemain)*[' ']
		
		rightPyQuery.GroupBy(rightClumn)
		
		joinedDataInList=[]
		for lineNum in xrange(self.DataInListLength):
			oneLeftLine=self.DataInList[lineNum]
			joinKey=self.DataInList[lineNum][leftClumnIndex]
			if joinKey in rightPyQuery.DataInDict:
				tempDataInList=[oneLeftLine+oneRightLine for oneRightLine in rightPyQuery.DataInDict[joinKey]]
			else:
				tempDataInList=[oneLeftLine+emptyRightLine]
			joinedDataInList.extend(tempDataInList)
		
		self.DataInList=joinedDataInList
		del joinedDataInList
		self.DataInListLength=len(self.DataInList)
		self.ClumnRemain+=rightPyQuery.ClumnRemain
		
		self.ClumnDataType+=rightPyQuery.ClumnDataType
		self.Delect(rightClumn)
		
		rightPyQuery.Clean()
		
		print 'LeftJoin: '+str(time.time()-now)+' seconds'
	
	
	def Head(self,nHead):
		now=time.time()
		
		assert type(nHead) == type(1),'Head: must give a int'
		
		if(self.DataInListLength<=nHead):
			pass
		else:
			self.DataInList=[self.DataInList[index] for index in xrange(nHead)]
			self.DataInListLength=nHead
		
		print 'Head: '+str(time.time()-now)+' seconds'
	
	
	def Sample(self,nSample):
		now=time.time()
		
		assert type(nSample) == type(1),'Sample: must give a int'
		
		if(self.DataInListLength<=nSample):
			pass
		else:
			nSampleIndex=random.sample(xrange(self.DataInListLength),nSample)
			self.DataInList=[self.DataInList[index] for index in nSampleIndex]
			self.DataInListLength=nSample
		
		print 'Sample: '+str(time.time()-now)+' seconds'
	
	
	def Tail(self,nTail):
		now=time.time()
		
		assert type(nTail) == type(1),'Tail: must give a int'
		
		if(self.DataInListLength<=nTail):
			pass
		else:
			self.DataInList=[self.DataInList[self.DataInListLength-index] for index in xrange(nTail,0,-1)]
			self.DataInListLength=nTail
			
		print 'Tail: '+str(time.time()-now)+' seconds'
	
	
	def OutputAsFile(self,resultFilename):
		now=time.time()
		
		for lineNum in xrange(self.DataInListLength):
			self.DataInList[lineNum]='\t'.join(self.DataInList[lineNum])+'\n'
		
		resultFile=open(resultFilename,'w+')
		resultFile.write('\t'.join(self.ClumnRemain)+'\n')
		resultFile.writelines(self.DataInList)
		resultFile.close()
		
		print 'OutputAsFile '+str(time.time()-now)+' seconds'
	
	
	def Clean(self):
		self.DataInList = []
		self.DataInDict = defaultdict(list)
		self.ClumnRemain = []
		self.ClumnGrouped = []
		self.ClumnDataType = []
		self.DataInListLength=0
		os.remove('PyQuery.OutSideFile')
	
	
	def TransToStrInList(self,clumnIndex):
		for oneRecord in self.DataInList:
			oneRecord[clumnIndex]=str(oneRecord[clumnIndex])
	
	def TransToFloatInList(self,clumnIndex):
		for oneRecord in self.DataInList:
			oneRecord[clumnIndex]=float(oneRecord[clumnIndex])
	
	def TransToIntInList(self,clumnIndex):
		for oneRecord in self.DataInList:
			oneRecord[clumnIndex]=int(oneRecord[clumnIndex])
			
	def TransToStrInDict(self,clumnIndex):
		for oneGrouped in self.DataInDict.values():
			for oneRecord in oneGrouped:
				oneRecord[clumnIndex]=str(oneRecord[clumnIndex])
	
	def TransToFloatInDict(self,clumnIndex):
		for oneGrouped in self.DataInDict.values():
			for oneRecord in oneGrouped:
				oneRecord[clumnIndex]=float(oneRecord[clumnIndex])
	
	def TransToIntInDict(self,clumnIndex):
		for oneGrouped in self.DataInDict.values():
			for oneRecord in oneGrouped:
				oneRecord[clumnIndex]=int(oneRecord[clumnIndex])