# -*- coding: utf-8 -*-
from PyQuery import PyQuery
import time


# 文本cite75_99 700百万条记录,每行为一条引用记录,某论文\t该论文引用的论文
# 3858241,956203
# 3858241,1324234
# 3858241,3634889
# 3858242,1515701
# 3858242,3319261
# ......

# 文本apat63_99.txt 300百万条记录,每行为一片论文的详细信息，我们左联结查看论文的年份与国家
# 3070801,1963,1096,,"BE","",,1,,269,6,69,,1,,0,,,,,,,
# 3070802,1963,1096,,"US","TX",,1,,2,6,63,,0,,,,,,,,,
# 3070803,1963,1096,,"US","IL",,1,,2,6,63,,9,,0.3704,,,,,,,
# 3070804,1963,1096,,"US","OH",,1,,2,6,63,,3,,0.6667,,,,,,,
# ......

	
if __name__ == "__main__":
	now=time.time()
	
	# Top 100 Paper Cited Most
	clumnNameL=['CITING','CITED']
	clumnNameSelectL=['CITED']
	myPyQueryLeft=PyQuery()
	myPyQueryLeft.InputAndSelect('D:\\Desktop\\ITFiles\\cite75_99_7000000.txt','\t',clumnNameL,clumnNameSelectL)
	myPyQueryLeft.GroupBy(['CITED'])
	myPyQueryLeft.CountEach('CITED_COUNT')
	myPyQueryLeft.Top(['CITED_COUNT'],100)
	
	clumnNameR=['PATENT','GYEAR','GDATE','APPYEAR','COUNTRY','POSTATE','ASSIGNEE','ASSCODE','CLAIMS','NCLASS','CAT','SUBCAT','CMADE','CRECEIVE','RATIOCIT','GENERAL','ORIGINAL','FWDAPLAG','BCKGTLAG','SELFCTUB','SELFCTLB','SECDUPBD','SECDLWBD']
	clumnNameSelectR=['PATENT','GYEAR','COUNTRY']
	myPyQueryRight=PyQuery()
	myPyQueryRight.InputAndSelect('C:\\Users\\Administrator\\Desktop\\ITFiles\\apat63_99.txt',',',clumnNameR,clumnNameSelectR)
	
	myPyQueryLeft.LeftJoin(['CITED'],myPyQueryRight,['PATENT'])
	myPyQueryLeft.OutputAsFile('C:\\Users\\Administrator\\Desktop\\cite75_99_top')
	
	print 'total '+str(time.time()-now)+' seconds'