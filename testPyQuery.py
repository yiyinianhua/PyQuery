from PyQuery import PyQueryimport timedef getMailHost(oneEmailAddr):	return oneEmailAddr[oneEmailAddr.rfind('@')+1:].strip().lower()	if __name__ == "__main__":	now=time.time()		#Top 100 password used in csdn	clumnName=['username','password','email']	clumnNameSelect=['email']	myPyQuery=PyQuery()	myPyQuery.inputWithSelect('C:\\Users\\Administrator\\Desktop\\ITFiles\\csdnpwd','#',clumnName,clumnNameSelect)	myPyQuery.translate(getMailHost,'email')	myPyQuery.groupBy('email')	myPyQuery.countEach('email','email_count')	myPyQuery.top('email_count',100)	myPyQuery.SortBy('desc','email_count')	myPyQuery.outputAsFile('C:\\Users\\Administrator\\Desktop\\pws')	myPyQuery.clean()				# clumnName=['userid','cmatch','cntnid','ipb','prov','rank','price_d']	# clumnNameSelect=['cmatch','prov','price_d']	# myPyQuery=PyQuery()	# myPyQuery.inputWithSelect('C:\\Users\\Administrator\\Desktop\\ITFiles\\20120710_clickcharge_1000000','\t',clumnName,clumnNameSelect)	# myPyQuery.groupBy('cmatch','prov')	# myPyQuery.sumEach('price_d','price_sum')	# myPyQuery.groupBy('cmatch')	# myPyQuery.topEach('price_sum',2)	# myPyQuery.outputAsFile('C:\\Users\\Administrator\\Desktop\\clickcharge')	# myPyQuery.clean()			# Most CITED Top 100 paper info using join	# clumnNameL=['CITING','CITED']	# clumnNameSelectL=['CITED']	# myPyQueryLeft=PyQuery()	# myPyQueryLeft.inputWithSelect('C:\\Users\\Administrator\\Desktop\\ITFiles\\cite75_99_12000000.txt','\t',clumnNameL,clumnNameSelectL)	# myPyQueryLeft.groupBy('CITED')	# myPyQueryLeft.countEach('CITED','CITED_COUNT')	# myPyQueryLeft.top('CITED_COUNT',100)		# clumnNameR=['PATENT','GYEAR','GDATE','APPYEAR','COUNTRY','POSTATE','ASSIGNEE','ASSCODE','CLAIMS','NCLASS','CAT','SUBCAT','CMADE','CRECEIVE','RATIOCIT','GENERAL','ORIGINAL','FWDAPLAG','BCKGTLAG','SELFCTUB','SELFCTLB','SECDUPBD','SECDLWBD']	# clumnNameSelectR=['PATENT','GYEAR','COUNTRY']	# myPyQueryRight=PyQuery()	# myPyQueryRight.inputWithSelect('C:\\Users\\Administrator\\Desktop\\ITFiles\\apat63_99.txt',',',clumnNameR,clumnNameSelectR)		# myPyQueryLeft.leftJoin('CITED',myPyQueryRight,'PATENT')	# myPyQueryLeft.outputAsFile('C:\\Users\\Administrator\\Desktop\\cite75_99_top')		print 'total '+str(time.time()-now)+' seconds'