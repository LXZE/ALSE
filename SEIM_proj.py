#!/usr/bin/env python
from collections import defaultdict as ddict
from collections import namedtuple as ntuple
import collections as coll
import re
import math
import operator
import sys

def next(t,cur):
	if len(d[t]) == 0 or d[t][len(d[t])-1]<=cur:
		return 'inf'
	if d[t][0] > cur:
		return d[t][0]
	return d[t][binSearch(t,0,len(d[t]),cur,'next')]

def prev(t,cur):
	if len(d[t]) == 0 or d[t][0]>cur:
		return '-inf'
	if d[t][len(d[t])-1] < cur:
		return d[t][len(d[t])-1]
	return d[t][binSearch(t,0,len(d[t]),cur,'prev')]

def binSearch(t,low,high,cur,type):
	while high - low > 1:
		mid = int(math.floor((low+high)/2))
		if d[t][mid] <= cur:
			low = mid
		else: high = mid
	return high if type == 'next' else low

def nextPhrase(cur,args):
	v = cur
	for term in args:
		v = next(term,v)
	if v == 'inf':
		return ['-inf','inf']
	u = v
	for term in reversed(args):
		u = prev(term,u)
	if v-u == len(args)-1:
		return [u,v]
	else: return nextPhrase(u,args)

def chkValid(d,q,multi_ip=False):
	if not multi_ip:
		try:
			if d[q] != []:
				return True
			return False
		except KeyError:
			return False
	else:
		try:
			for qt in q:
				if d[qt] == []:
					return False
			return True
		except KeyError:
			return False
def process_log_line(lineLog): # for apache access log only
	newLog = []
	char = ''
	type = ['[','"']
	typeend = [']','"']
	onProc = False
	temp = []
	for logPiece in lineLog.split(' '):
		if any(logPiece[0] == x for x in type) and not onProc:
			temp.append(logPiece)
			if any(logPiece[-1] == x for x in typeend):
				continue
			onProc  = True
			continue
		if onProc:
			temp.append(logPiece)
			if any(logPiece[-1] == x for x in typeend):
				onProc = False
				newLog.append(' '.join(temp)[1:-1])
				temp = []
		else:
			newLog.append(logPiece)
	return newLog

# indexing var
ipToken,typeToken,codeToken,timeToken= [],[],[],[]
ipDict,typeDict,contentDict,codeDict = ddict(list),ddict(list),ddict(list),ddict(list)
# ip related var
subIPDict = ddict(list)
ipLookupTable = []
ipCount = {}
ipCounted = False
# log related var
logLine = []
lineLookup = ddict(list)
# time relater var
# timeDict = ddict(list)
hoursDict = ddict(list)
timeLookup = []

def process_log():
	# fn = raw_input('input filename : ')
	# fn = 'access.log'
	if(len(sys.argv) < 2):
		print('No log file given, exit!')
		exit(0)
	fn = sys.argv[1]
	try:
		token = 1
		with open(fn,'r') as f:
			for liNum,li in enumerate(f):
				temp = process_log_line(li[:-1])
				logLine.append(li)
				lineLookup[token].append(liNum+1)
				ipToken.append((temp[0],token))
				timeToken.append((temp[3],token+3))
				typeToken.append((temp[4].split(' ')[0],temp[4],token+4))
				codeToken.append((temp[5],token+5))
				token+=7
	except IOError:
		print('File \'{}\' not found or unable to process'.format(fn))
		exit(0)
		return None

	# create ip dict
	subIPToken = []
	subToken = 1
	for ip,tk in ipToken:
		ipDict[ip].append(tk)
		if ip == '::1':
			subIPToken.append((ip,subToken))
			ipLookupTable.append(tk)
			subToken+=1
			continue
		for subIP in ip.split('.'):
			subIPToken.append((subIP,subToken))
			ipLookupTable.append(tk)
			subToken+=1
	for ip,tk in subIPToken:
		subIPDict[ip].append(tk)

	# create time dict
	split_time = lambda time : re.sub(r'[\:\/]+',' ',time.split(' ')[0]).split(' ')
	subToken = 1
	subTimeToken = []
	for time,tk in timeToken:
		# timeDict[time].append(tk)
		for titem in split_time(time):
			subTimeToken.append((titem,subToken))
			timeLookup.append(tk)
			subToken+=1
	for itime,tt in enumerate(subTimeToken):
		if itime > 0 and itime%3==0:
			hoursDict[tt[0]].append(tt[1])

	for type,content,t in typeToken:
		typeDict[type].append(t)
		contentDict[content].append(t)
	for code,t in codeToken:
		codeDict[code].append(t)

def s_rawLog():
	def print_log(pos,findIP = False,multi_ip = False): # magic, don't touch it
		if not multi_ip:
			if not findIP:
				t = 1
				while t<=pos:
					t+=7
				t-=7
				print "line {} :".format(lineLookup[t][0]),
				print logLine[lineLookup[t][0]-1]
			else:
				print "line {} :".format(lineLookup[ipLookupTable[pos-1]][0]),
				print logLine[lineLookup[ipLookupTable[pos-1]][0]-1]
		else:
			l = [lineLookup[ipLookupTable[p-1]][0] for p in pos]
			if all(map(lambda x: x==l[0],l)):
				print "line {} :".format(lineLookup[ipLookupTable[pos[0]-1]][0]),
				print logLine[lineLookup[ipLookupTable[pos[0]-1]][0]-1]
			else:
				print "No IP found"
				return -1
		return 0
	def print_time(pos):
		print "line {} :".format(lineLookup[timeLookup[pos-1]-3][0]), # -3 at timeLookup because token shifted for 3
		print logLine[lineLookup[timeLookup[pos-1]-3][0]-1]

	def timeSearch():
		global d
		d = hoursDict
		search = 'Y'
		while search == 'Y':
			query = raw_input('hours : ')
			if len(query) == 1:
				query = '0'+query
			cur = 0
			if query == '':
				continue
			elif chkValid(hoursDict,query):
				found = True
				while found:
					res = next(query,cur)
					if res == '-inf' or res == 'inf':
						break
					print_time(res)	
					r = raw_input('Find next? [n to exit] : ')
					if r == 'n':
						found = False
					else:
						cur = res
			else:
				print('No query found')
			search = raw_input('Find new time? [Y/n] : ')
	def ipSearch():
		global d
		d = subIPDict
		search = 'Y'
		while search == 'Y':
			query = raw_input('ip : ').split('.')
			cur = 0
			if query == '':
				continue
			elif len(query) == 1 and chkValid(subIPDict,query[0]):
				query = query[0]
				found = True
				while found:
					res = next(query,cur)
					if res == '-inf' or res == 'inf':
						print('Reach EOF')
						break
					print_log(res,True)
					r = raw_input('Find next? [n to exit] : ')
					if r == 'n':
						found = False
					elif r == '':
						cur = res
			elif len(query) > 1 and chkValid(subIPDict,query,True):
				found = True
				while found:
					try:
						tempv = 0
						res = nextPhrase(cur,query)
						err_flag = 0
						if res == ['-inf','inf']:
							print('no more result')
							break
						rc = print_log(res,True,True)
						if rc == -1: 
							found = False
							break
						r = raw_input('Find next? [n to exit] : ')
						if r == 'n':
							found = False
						elif r == '':
							cur = res[1]
					except RuntimeError:
						print('Cannot find result from query')
						break
			else:
				print('No query found')
			search = raw_input('Find new ip? [Y/n] : ')
	def codeSearch():
		global d
		d = codeDict
		search = 'Y'
		while search == 'Y':
			query = raw_input('code : ')
			cur = 0
			if query == '':
				continue
			elif chkValid(codeDict,query):
				found = True
				while found:
					res = next(query,cur)
					if res == '-inf' or res == 'inf':
						break
					print_log(res)	
					r = raw_input('Find next? [n to exit] : ')
					if r == 'n':
						found = False
					else:
						cur = res
			else:
				print('No query found')
			search = raw_input('Find new code? [Y/n] : ')
	def typeSearch():
		global d
		d = typeDict
		search = 'Y'
		while search == 'Y':
			query = raw_input('type : ').upper()
			cur = 0
			if query == '':
				continue
			elif chkValid(typeDict,query):
				found = True
				while found:
					res = next(query,cur)
					if res == '-inf' or res == 'inf':
						print('no more result')
						break
					print_log(res)	
					r = raw_input('Find next? [n to exit] : ')
					if r == 'n':
						found = False
					else:
						cur = res
			else:
				print('No query found')
			search = raw_input('Find new type? [Y/n] : ')
	inputType = {
		'lst':list_type,
		'ip':ipSearch,
		'code':codeSearch,
		'type':typeSearch,
		'time':timeSearch
	}
	while True:	
		raw = raw_input('~/search_log> ')
		try:
			if raw == '':
				continue
			elif raw == 'exit' or raw == 'q':
				return None
			inputType[raw]()
		except KeyError:
			print('Parameters not found!')

def categorize():
	def codeCat():
		for code in codeDict.items():
			print "HTTP status code {} : {} times".format(code[0],len(code[1]))
	def typeCat():
		for type in typeDict.items():
			print "HTTP request method {} : {} times".format(type[0],len(type[1]))
	inputType = {
		'lst':list_type,
		'code':codeCat,
		'type':typeCat
	}
	while True:	
		raw = raw_input('~/categorize> ')
		try:
			if raw == '':
				continue
			elif raw == 'exit' or raw == 'q':
				return None
			inputType[raw]()
		except KeyError:
			print('Parameters not found!')

def rank():
	global ipCounted
	if ipCounted == False:
		i = 0
		for ip in ipToken:
			key = ipToken[i][0]
			if key in ipCount:
				ipCount[key] = ipCount[key]+1
			else:
				ipCount[key] = 1
			i += 1
		ipCounted = True
	print('Top 5 access IP address :')
	sorted_ip = sorted(ipCount.items(), key=operator.itemgetter(1), reverse = True)
	for key, value in sorted_ip[:5]:
		print 'ip >>',key,'\tocccurs',value,'times'

def _help():
	print('''type 'lsfn' to see all of function
type 'lst' to see all type of parameters''')
def list_fn():
	print('- sl -> search in log, after that you can select type of parameters for searching')
	print('- cat -> categorize log access by parameters that selected')
	print('- rank -> ranking ip access and show most access result')
	print('- q, exit -> exit')
	print('- help -> show help')
def list_type():
	print('- ip -> find log from ip (can be ::1,one of octet or more than that')
	print('- code -> find log from http status code')
	print('- type -> find log from http request method')
	print('- time -> find log from time')

options = {'lsfn':list_fn,
'lst':list_type,
'help':_help,
'sl':s_rawLog,
'rank':rank,
'cat':categorize
}

operate = True
print('Search Engine and Internet Mining mini project : Search engine for log analytics')
_help()
process_log()
while operate:
	raw = raw_input('~> ')
	try:
		if raw == '':
			continue
		elif raw == 'exit' or raw == 'q':
			print('Exit!')
			exit(0)
		options[raw]()
	except KeyError:
		print('Function not found!')
