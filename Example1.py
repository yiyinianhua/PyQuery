# -*- coding: utf-8 -*-
from PyQuery import PyQuery
import time


# 文本csdnpwd每行的格式为:用户名#密码#邮箱,例如:
# zdg # 123 # zdg@csdn.net
# LaoZheng # 670207 # chengming_zheng@163.com
# fstao # 123 # fstao@tom.com
# ......


def getMailHost(oneEmailAddr):
	return oneEmailAddr[oneEmailAddr.rfind('@')+1:].strip().lower()
	
	
if __name__ == "__main__":
	now=time.time()
	
	#Top 100 emailhost used by csdn's user
	clumnName=['username','password','email']
	clumnNameSelect=['email']
	myPyQuery=PyQuery()
	myPyQuery.InputAndSelect('C:\\Users\\Administrator\\Desktop\\ITFiles\\csdnpwd','#',clumnName,clumnNameSelect)
	myPyQuery.Translate(['email'],getMailHost)
	myPyQuery.GroupBy(['email'])
	myPyQuery.CountEach('email_count')
	myPyQuery.Top(['email_count'],100)
	myPyQuery.OutputAsFile('C:\\Users\\Administrator\\Desktop\\topemailhost')
	myPyQuery.Clean()
	
	print 'total '+str(time.time()-now)+' seconds'