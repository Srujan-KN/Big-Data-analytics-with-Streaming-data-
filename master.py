from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext, SparkSession
import sys
import json
import csv
from operator import add
import os
from shutil import rmtree

conf=SparkConf()
conf.setAppName('FPL_Project')

sc=SparkContext(conf=conf)
spark=SparkSession(sc)
sqlContext=SQLContext(sc)

ssc=StreamingContext(sc,5)
ssc.checkpoint('FPL_Project_Checkpoint')

def metric(x):
	'''
		Finds the metrics peer match per player
	'''
	e=json.loads(x)
	pid=e['playerId']
	eid=e['eventId']
	tags=e['tags']
	mid=e['matchId']
	pl=[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
	
	#----------------------------------GOALS------------------------------------
	if {'id':101} in tags:
		pl[15]=1   #goals
		
	#----------------------------------OWN GOALS--------------------------------
	if {'id':102} in tags:
		pl[14]=1   #own goals
	
	#---------------------------------PASS ACCURACY---------------------------------
	if eid==8:
		pl[0]=1   #total passes
		if {'id':302} in tags:
			if{'id':1801} in tags:
				pl[1]=1   #accurate key pass
			pl[2]=1          #key pass
		if{'id':1801} in tags:
			pl[3]=1  	#accurate pass
		return ((pid,mid),pl)
		
	#------------------------------DUEL EFFECTIVENESS-------------------------------
	if eid==1:
		pl[4]=1    #total duels
		if {'id':702} in e['tags']:
			pl[5]=1    #neutral
		elif {'id':701} in e['tags']:
			pl[6]=1    #won
		return ((pid,mid),pl)
		
	#-----------------------------FREE KICK EFFECTIVENESS-----------------------------
	if eid==3:
		pl[7]=1   #total free kicks
		if {'id':1801} in tags:
			pl[8]=1   #effective free kick'
		if e['subEventId']==35:
			if {'id':101} in tags:
				pl[9]=1   #penalties
		return ((pid,mid),pl)
	
	#----------------------------------SHOTS EFFECTIVE----------------------------------
	if eid==10:
		pl[10]=1  #total shots
		if {'id':1801} in tags:
			if {'id':101} in tags:
				pl[11]=1   #target and goal shots
			pl[12]=1   #target shots
		return ((pid,mid),pl)
		
	#-----------------------------------FOULS----------------------------------
	if eid==2:
		pl[13]=1   #total fouls
		return ((pid,mid),pl)
		
	return ((pid,mid),[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0])
	

def PlayerMetric(new,old):
	'''
		Computes the metrics per player per match
	'''
	pl=[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
	for m in new:
		if m!=None:
			pl=[m[i]+pl[i] for i in range(16)]

	if (old == None):
		return pl
		
	pl=[old[i]+pl[i] for i in range(16)]
	return pl
	
	
def MatchMetric(x):
	'''
		Computes the final metric of each player in the match
	'''
	new=x[1]
	if len(new)>0:
		#PASS ACCURACY
		if new[0]:
			pass_acc=(((new[3]-new[1])+(new[1]*2))/((new[0]-new[2])+(new[2]*2)))
		else:
			pass_acc=0
		#DUEL EFFECTIVENESS
		if new[4]:
			duel_eff=(new[6]+float(new[5])*0.5)/new[4]
		else:
			duel_eff=0
		#FREE KICK EFFECTIVENESS
		if new[7]:
			free_kick_eff=(new[8]+new[9])/new[7]
		else:
			free_kick_eff=0
		#SHOTS EFFECTIVENESS
		if new[10]:
			shots_eff=(new[11]+(float(new[12]-new[11])*0.5))/new[10]
		else:
			shots_eff=0
		#FOULS
		fouls=new[13]
		
		#OWN GOALS
		own_goals=new[14]
		
		#PLAYER CONTRIBUTION
		contr=(pass_acc+duel_eff+free_kick_eff+shots_eff)/4
		
		#GOALS
		goals=new[15]
		
		return ((x[0][0]),[pass_acc, duel_eff, free_kick_eff, shots_eff, fouls, own_goals, goals, contr, x[0][1]])
		
def TotalPlayerMetric(new, old):
	if old==None:
		old=[0,0,0,0,0,0,0,0,-1]
	new[0]=(new[0]+old[0])/2
	new[1]=(new[1]+old[1])/2
	new[2]=(new[2]+old[2])/2
	new[3]=(new[3]+old[3])/2
	new[4]+=old[4]
	new[5]+=old[5]
	new[6]+=old[6]
	new[7]=(new[7]+old[7])/2
	return new
	
	
def PlayerRating(new,old):
	'''
		Finds the rating of a player per match
	'''
	if len(new)>0:
		new=new[0]
		metrics=new[0]
		time=new[1]
		if old==None:
			old=[]
			old.append([-1,0,0.5,0])
		rating=[]
		rating.append(metrics[-1])
		m=0
		if (time[1][1]-time[1][0])>0:				#checks if player played the match
			m=1
		rating.append(m)
		
		if time[1][1]-time[1][0]==90:
			contribution=(1.05*metrics[-2])		#player contribution
		else:
			t=time[1][1]-time[1][0]
			contribution=((t/2)*metrics[-2])
			
		performance=contribution-((0.005*metrics[5]+0.05*metrics[6])*contribution)    #player performance
		rate=(performance+old[-1][2])/2						#player rating
		change=rate-old[-1][2]								#change in rating
		rating.append(rate)
		rating.append(change)
		old.append(rating)
	return old	

def CrossPlayers(x,y):
	'''
		Performs and returns cartesian product of the teams
	'''
	cross=x.cartesian(y)
	return cross
	
def getPlayers(x):
	'''
		Gets players from the cartesian product
	'''
	players=[x[0][0],x[1][0]]
	players.sort()
	players=tuple(players)
	return (players,x)
	
def PlayersChemistry(new,old):
	'''
		Finds the chemistry between players
	'''
	try:
		if old==None:
			old=0.5
		tid1=new[0][0][1][0][1][0]
		tid2=new[0][1][1][0][1][0]
		#print('tid -> ',tid1)
		r1=new[0][0][-1][-1][-1][-1]
		r2=new[0][1][-1][-1][-1][-1]
		#print('r1 -> ',r1)
		if tid1==tid2:
			if (r1>0 and r2>0) or (r1<0 and r2<0):
				chem=old+abs((r1+r2)/2)
			else:
				chem=old-abs((r1+r2)/2)
		else:
			if (r1>0 and r2>0) or (r1<0 and r2<0):
				chem=old-abs((r1+r2)/2)
			else:
				chem=old+abs((r1+r2)/2)
		return chem
	except:
		return old

def playersub(x):
	'''
		Returns the details of all the substitution players
	'''
	l={}
	for i in x:
		l[i['playerIn']]=i['minute']
		l[i['playerOut']]=i['minute']
	return l
	
def PlayerTime(x):
	'''
		Calculates and returns the time for which each player was on field
	'''
	playertime=[]
	for i in x:
		tid=x[i]['teamId']
		subs=x[i]['formation']['substitutions']
		subplayers=playersub(subs)
		bench=[j['playerId'] for j in x[i]['formation']['bench']]
		lineup=[j['playerId'] for j in x[i]['formation']['lineup']]
		for player in lineup:
			if player in subplayers:
				playertime.append((player,[tid,[0,subplayers[player]]]))
			else:
				playertime.append((player,[tid,[0,90]]))
		for player in bench:
			if player in subplayers:
				playertime.append((player,[tid,[subplayers[player],90]]))
			else:
				playertime.append((player,[tid,[0,0]]))
	return playertime
		
def match(x):
	'''
		Returns the match data required for task 3
	'''
	m=json.loads(x)
	m1={}
	m1['matchId']=m['wyId']
	m1['date']=m['dateutc'].split()[0]
	m1['label']=m['label']
	m1['duration']=m['duration']
	m1['winner']=m['winner']
	m1['venue']=m['venue']
	m1['gameweek']=m['gameweek']
	m1['teamsData']=m['teamsData']
	m1=json.dumps(m1)
	with open('MatchData.txt','a') as f:
		f.write(m1+'\n')
	return m1

if os.path.exists("MatchData.txt"):
	os.remove("MatchData.txt")

input_stream=ssc.socketTextStream("localhost",6100)

matches=input_stream.filter(lambda x: 'status' in x)
matches.pprint()

matchdata=matches.map(lambda x: match(x))
matchdata.pprint()

teamsd=matches.map(lambda x: eval(x)['teamsData'])
teamsd.pprint()

playertime=teamsd.flatMap(lambda x: PlayerTime(x))
playertime.pprint()

events=input_stream.filter(lambda x: 'eventId' in x)
events.pprint()

metrics=events.map(lambda x: metric(x))
metrics.pprint()

playermetrics=metrics.updateStateByKey(PlayerMetric)
playermetrics.pprint()

matchmetrics=playermetrics.map(MatchMetric)
matchmetrics.pprint()

playerdata=matchmetrics.join(playertime)
playerdata.pprint()

playerrating=playerdata.updateStateByKey(PlayerRating)
playerrating.pprint()

finalplayerdata=matchmetrics.updateStateByKey(TotalPlayerMetric)
finalplayerdata.pprint()

playerprofile=finalplayerdata.join(playerrating)
playerprofile.pprint()

crossPlayers = playerprofile.transformWith(CrossPlayers, playerprofile)
crossPlayers.pprint()

pairPlayers=crossPlayers.map(getPlayers)
pairPlayers.pprint()

chemistry = pairPlayers.updateStateByKey(PlayersChemistry)
chemistry.pprint()

if os.path.exists("chemdata"):
	rmtree("chemdata")

chemistry.saveAsTextFiles("chemdata/Chemistry")

if os.path.exists("data"):
	rmtree("data")

playerprofile.saveAsTextFiles("data/PlayerProfile")

ssc.start()
ssc.awaitTermination(50)
ssc.stop()
