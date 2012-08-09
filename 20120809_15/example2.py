# -*- coding: utf-8 -*-
from PyQuery import PyQuery
import time


# �ı�travelagent5��������¼,ÿ��Ϊһ����������ĳ���е�һ�����Ѽ�¼,������\t��\t���Ѷ�
# 204     21608   0.43
# 225     151     1.06
# 225     151     2.62
# 225     221049  2.53


if __name__ == "__main__":
	now=time.time()
	
	#Top 3 province of each travelagency which they spend most
	clumnName=['travelagency','town','price_d']
	clumnNameSelect=['travelagency','town','price_d']
	myPyQuery=PyQuery()
	myPyQuery.InputAndSelect('C:\\Users\\Administrator\\Desktop\\ITFiles\\2011_travelagent_5000000','\t',clumnName,clumnNameSelect)
	myPyQuery.GroupBy(['travelagency','town'])
	myPyQuery.SumEach(['price_d'],'price_sum')
	myPyQuery.GroupBy(['travelagency'])
	myPyQuery.TopEach(['price_sum'],3)
	myPyQuery.OutputAsFile('C:\\Users\\Administrator\\Desktop\\travelagent')
	myPyQuery.Clean()
	
	print 'total '+str(time.time()-now)+' seconds'