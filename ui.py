from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql import functions as F
import sys
import json
import csv
import os
from pyspark.sql.types import StringType, IntegerType, StructType, StructField, TimestampType
from pyspark.ml.clustering import KMeans
from shutil import rmtree
from pyspark.ml.evaluation import ClusteringEvaluator
from datetime import datetime
from pyspark.ml.regression import LinearRegression 
from pyspark.ml.linalg import Vectors 
from pyspark.ml.feature import VectorAssembler 

conf=SparkConf()
conf.setAppName('FPL_Project_UI')

sc=SparkContext(conf=conf)
spark=SparkSession(sc)
sqlContext=SQLContext(sc)

def findTeam(tid):
	'''
		Returns the team name associated with the given team id
	'''
	for i in teams:
		if i['Id']==tid:
			return i['name']
			
def getPlayerName(pid):
	'''
		Returns the name of the player with the given id
	'''
	for i in players:
		if i['Id']==pid:
			return i['name']

def MatchData(x):
	'''
		Returns the match data
	'''
	goals=[]
	owngoals=[]
	yc=[]
	rc=[]
	for i in x['teamsData']:
		for j in x['teamsData'][i]['formation']['bench']:
			if j['goals']!='0':
				g={}
				g['name']=getPlayerName(str(j['playerId']))
				g['team']=findTeam(i)
				g['number_of_goals']=int(j['goals'])
				goals.append(g)
			if j['ownGoals']!='0':
				g={}
				g['name']=getPlayerName(str(j['playerId']))
				g['team']=findTeam(i)
				g['number_of_goals']=int(j['ownGoals'])
				owngoals.append(g)
			if j['yellowCards']!='0':
				yc.append(getPlayerName(str(j['playerId'])))
			if j['redCards']!='0':
				rc.append(getPlayerName(str(j['playerId'])))
		for j in x['teamsData'][i]['formation']['lineup']:
			if j['goals']!='0':
				g={}
				g['name']=getPlayerName(str(j['playerId']))
				g['team']=findTeam(i)
				g['number_of_goals']=int(j['goals'])
				goals.append(g)
			if j['ownGoals']!='0':
				g={}
				g['name']=getPlayerName(str(j['playerId']))
				g['team']=findTeam(i)
				g['number_of_goals']=int(j['ownGoals'])
				owngoals.append(g)
			if j['yellowCards']!='0':
				yc.append(getPlayerName(str(j['playerId'])))
			if j['redCards']!='0':
				rc.append(getPlayerName(str(j['playerId'])))
				
	match_data={}
	match_data['date']=x['date']
	match_data['duration']=x['duration']
	if x['winner']=='draw':
		match_data['winner']='null'
	else:
		match_data['winner']=findTeam(x['winner'])
	match_data['venue']=x['venue']
	match_data['gameweek']=int(x['gameweek'])
	match_data['goals']=goals
	match_data['own_goals']=owngoals
	match_data['yellow_cards']=yc
	match_data['red_cards']=rc
	return match_data

def getMatchData(date,label):
	'''
		TASK 3
	'''
	datel=mrdd.filter(lambda x: x['date']==date and x['label']==label)
	try:
		pldata=datel.map(lambda x: MatchData(x)).collect()[0]
		mdata=json.dumps(pldata,indent=4)
		with open('Matches/match_output.json','w') as f:
			f.write(mdata)
	except:
		with open('Matches/match_output.json','w') as f:
			f.write('Invalid query')

def findPlayer(name):
	'''
		Returns the details of the player with the given name
	'''
	details={}
	for i in players:
		if i['name']==name:
			details=i
			break
	return details

def PlayerProfile(name):
	'''
		TASK 2
	'''
	details=findPlayer(name)
	playerId=int(details['Id'])
	try:
		details['fouls']=playerprofile[playerId]['fouls']
		details['goals']=playerprofile[playerId]['goals']
		details['own_goals']=playerprofile[playerId]['own_goals']
		details['percent_pass_accuracy']=playerprofile[playerId]['percent_pass_accuracy']
		details['percent_shots_on_target']=playerprofile[playerId]['percent_shots_on_target']
	except:
		playerprofile[playerId]={}
		playerprofile[playerId]['fouls']=0
		playerprofile[playerId]['goals']=0
		playerprofile[playerId]['own_goals']=0
		playerprofile[playerId]['percent_pass_accuracy']=0
		playerprofile[playerId]['percent_shots_on_target']=0
		details['fouls']=playerprofile[playerId]['fouls']
		details['goals']=playerprofile[playerId]['goals']
		details['own_goals']=playerprofile[playerId]['own_goals']
		details['percent_pass_accuracy']=playerprofile[playerId]['percent_pass_accuracy']
		details['percent_shots_on_target']=playerprofile[playerId]['percent_shots_on_target']
	pdata=json.dumps(details,indent=4)
	with open('Players/player_profile_output.json','w') as f:
		f.write(pdata)
	return details

def check(team):
	'''
		Checks if a team is valid
	'''
	gk=0
	df=0
	md=0
	fw=0
	for player in list(team.keys()):
		if player!='name':
			details=findPlayer(team[player])
			if details['role']=='GK':
				gk+=1
			elif details['role']=='DF':
				df+=1
			elif details['role']=='MD':
				md+=1
			elif details['role']=='FW':
				fw+=1
	if gk>=1 and df>=3 and md>=2 and fw>=1:
		return 1
	return 0
	
def checkPlayers(team):
	'''
		Finds the players who have played less than 5 matches
	'''
	l=[]
	for player in list(team.keys()):
		if player!='name':
			details=findPlayer(team[player])
			playerId=int(details['Id'])
			try:
				rating=playerrating[playerId]
			except:
				playerrating[playerId]=[[-1,0,0.5,0]]
				rating=playerrating[playerId]
			count=0
			for i in rating:
				if i[1]==1:
					count+=1
			if count<5:
				l.append(team[player])
	return l
				
def kmeansData(p):
	'''
		Returns the data required for performing KMeans
	'''
	t=[]
	for i in p:
		pp=PlayerProfile(i)
		pprating=playerrating[int(pp['Id'])][-1][2]
		if pprating<5:
			s=str(pp['Id'])+' 1:'+str(pp['fouls'])+' 2:'+str(pp['goals'])+' 3:'+str(pp['own_goals'])+' 4:'+str(pp['percent_pass_accuracy'])+' 5:'+str(pp['percent_shots_on_target'])+' 6:'+str(pprating)+'\n'
			with open ('kmeans_data.txt','a+') as f:
				f.write(s)
			t.append(int(pp['Id']))
	return t

def kmeans():
	'''
		Performs KMeans
	'''
	dataset = spark.read.format("libsvm").load("kmeans_data.txt")     # Loads data.
	kmeans = KMeans().setK(5).setSeed(1)                              # Trains a k-means model.
	model = kmeans.fit(dataset)
	predictions = model.transform(dataset)			    # Make predictions
	evaluator = ClusteringEvaluator()				    # Evaluate clustering by computing Silhouette score
	silhouette = evaluator.evaluate(predictions)
	print("Silhouette with squared euclidean distance = " + str(silhouette))  # Shows the result.
	centers = model.clusterCenters()
	print("Cluster Centers: ")
	for center in centers:
	    print(center)
	return predictions
	
def chemcoeff(cp,p1,p2):
	'''
		Calculates the chemistry coefficient
	'''
	cp.sort()
	ch1=[]
	ch2=[]
	ch_avg1=0
	ch_avg2=0
	for i in range(len(cp)):
		for j in range(i,len(cp)):
			if ((cp[i] in p1) and (cp[j] in p1)):
				try:
					ch1.append(chemistry[(cp[i],cp[j])])
				except:
					chemistry[(cp[i],cp[j])]=0.5
					ch1.append(chemistry[(cp[i],cp[j])])
			elif ((cp[i] in p2) and (cp[j] in p2)):
				try:
					ch2.append(chemistry[(cp[i],cp[j])])
				except:
					chemistry[(cp[i],cp[j])]=0.5
					ch2.append(chemistry[(cp[i],cp[j])])
	if len(ch1)>0:
		ch_avg1=sum(ch1)/len(ch1)
	if len(ch2)>0:
		ch_avg2=sum(ch2)/len(ch2)
	return [ch_avg1, ch_avg2]

def getplayerdata(team):	
	'''
		Returns the player data
	'''
	l=[]
	for player in list(team.keys()):
		if player!='name':
			details=findPlayer(team[player])
			l.append(details)
	return l		

def getDate(mid):
	'''
		Returns the date of the match
	'''
	match=mrdd.filter(lambda x: x['matchId']==mid).collect()
	if len(match)>0:
		date=match[0]['date']
		return date

def getAge(d,dob): 
	'''
		Returns the age of the player
	'''
	d=datetime.strptime(d,'%Y-%m-%d')
	dob=datetime.strptime(dob,'%Y-%m-%d')
	try:  
		birthday = dob.replace(year = d.year) 

	# raised when birth date is February 29 
	# and the current year is not a leap year 
	except ValueError:  
		birthday = dob.replace(year = d.year, 
		  month = dob.month + 1, day = 1) 

	if birthday > d: 
		return d.year - dob.year - 1
	else: 
		return d.year - dob.year 

def regmodel():
	'''
		Regression Model
	'''
	l=[]
	k=[]
	for player in players:
		regdata=Regression(player['Id'], player['birthDate'])
		if len(regdata)>0:
			for j in regdata:
				l.append({'Id':player['Id'], 'Rating':j[1], 'Age': j[0]})
			k.append(player['Id'])
	df=sqlContext.createDataFrame(l)
	for col in df.columns:
		if col=='Rating':
			df= df.withColumn(col,df[col].cast('float'))
		
	assembler=VectorAssembler(inputCols=['Age'],outputCol='features') 
	output=assembler.transform(df) 
	final_data=output.select('features','Rating') 
	train_data,test_data=final_data.randomSplit([0.7,0.3]) 
	rating_lr=LinearRegression(featuresCol='features',labelCol='Rating')
	train_rating_model=rating_lr.fit(train_data) 
	rating_results=train_rating_model.evaluate(train_data)
	unlabeled_data=test_data.select('features')
	predictions=train_rating_model.transform(unlabeled_data)
	return train_rating_model

def Regression(pid,dob):
	'''
		Returns the data required for Regression
	'''
	try:
		pr=playerrating[int(pid)]
	except:
		playerrating[int(pid)]=[[-1,0,0.5,0]]
		pr=playerrating[int(pid)]
	regdata=[]
	for i in pr:
		l=[]
		d=getDate(i[0])
		if d!=None:
			age=getAge(d,dob)
			l.append(age)
			l.append(i[2])
			regdata.append(l)
	return regdata

def predictWin(date,team1,team2):
	'''
		TASK 1
	'''
	t1=check(team1)
	t2=check(team2)
	if t1==0 or t2==0:
		print('Invalid team')
		return -1
	p1=checkPlayers(team1)
	p2=checkPlayers(team2)
	if os.path.exists("kmeans_data.txt"):
		os.remove("kmeans_data.txt")
	ti1=kmeansData(p1)
	ti2=kmeansData(p2)
	
	ch_avg1=0
	ch_avg2=0
	if len(ti1)>0 or len(ti2)>0:
		predictions=kmeans()
		predictions.show()
		c1=predictions.filter("prediction==0")
		c2=predictions.filter("prediction==1")
		c3=predictions.filter("prediction==2")
		c4=predictions.filter("prediction==3")
		c5=predictions.filter("prediction==4")
		cp1 = c1.select("label").rdd.map(lambda x: int(x[0])).collect()
		cp2 = c2.select("label").rdd.map(lambda x: int(x[0])).collect()
		cp3 = c3.select("label").rdd.map(lambda x: int(x[0])).collect()
		cp4 = c4.select("label").rdd.map(lambda x: int(x[0])).collect()
		cp5 = c5.select("label").rdd.map(lambda x: int(x[0])).collect()
		ch1=chemcoeff(cp1,ti1,ti2)
		ch2=chemcoeff(cp2,ti1,ti2)
		ch3=chemcoeff(cp3,ti1,ti2)
		ch4=chemcoeff(cp4,ti1,ti2)
		ch5=chemcoeff(cp5,ti1,ti2)
		ch_avg1=(ch1[0]+ch2[0]+ch3[0]+ch4[0]+ch5[0])/5
		ch_avg2=(ch1[0]+ch2[0]+ch3[0]+ch4[0]+ch5[0])/5
	playdata1=getplayerdata(team1)
	playdata2=getplayerdata(team2)
	pl1=[]
	pl2=[]
	for i in playdata1:
		if int(i['Id']) not in p1:
			pl1.append(int(i['Id']))
	for i in playdata2:
		if int(i['Id']) not in p2:
			pl2.append(int(i['Id']))
	chem1=chemcoeff(pl1,pl1,pl2)[0]
	chem2=chemcoeff(pl2,pl1,pl2)[1]
	chemistry_coeff1=(chem1+ch_avg1)/2
	chemistry_coeff2=(chem2+ch_avg2)/2
	
	train_rating_model=regmodel()
	
	playdata1=getplayerdata(team1)
	playdata2=getplayerdata(team2)
	agerating=[]
	k=[]
	for i in playdata1:
		dob=i['birthDate']
		age=getAge(date, dob)
		k.append({'Id':i['Id'],'Age':age})
	k2=[]
	for i in playdata2:
		dob=i['birthDate']
		age=getAge(date, dob)
		k2.append({'Id':i['Id'],'Age':age})
	
	team1age=sqlContext.createDataFrame(k)
	team2age=sqlContext.createDataFrame(k2)
	assembler=VectorAssembler(inputCols=['Age'],outputCol='features') 
	output1=assembler.transform(team1age) 
	output2=assembler.transform(team2age)
	predictions1=train_rating_model.transform(output1)
	predictions2=train_rating_model.transform(output2)
	predictions1=predictions1.select("Id","prediction").collect()
	predictions2=predictions2.select("Id","prediction").collect()
	team1_r=[predictions1[i][1] for i in range(11)] #team1 rating, we can check if r<0.2 condition here
	team2_r=[predictions2[i][1] for i in range(11)]  # team2 rating
	team1_id=[predictions1[i][0] for i in range(11)] 
	team2_id=[predictions2[i][0] for i in range(11)]
	print(team1_r)
	print(team2_r)
	print(team1_id)
	print(team2_id)
	plstrength1=[]
	plstrength2=[]
	tv=0
	for i in team1_r:
		if i<0.2:
			tv=1
			break
		plstrength1.append(chemistry_coeff1*i)
	else:
		for i in team2_r:
			if i<0.2:
				tv=1
				break
			plstrength2.append(chemistry_coeff1*i)
		else:
			avgstrength1=sum(plstrength1)/11
			avgstrength2=sum(plstrength2)/11
			chance_of_A=round((0.5+avgstrength1-((avgstrength1+avgstrength2)/2))*100)
			chance_of_B=100-chance_of_A
			
			preddata={'team1':{'name':team1['name'], 'winning chance': chance_of_A}, 'team2':{'name':team2['name'], 'winning chance': chance_of_B}}
			windata=json.dumps(preddata,indent=4)
			with open('Pred/prediction_output.json','w') as f:
				f.write(windata)	
	if tv==1:
		print('Player has retired')
		return -1


players = csv.DictReader(open("players.csv"))
players=list(players)

teams=csv.DictReader(open("teams.csv"))
teams=list(teams)

playerprofile={}
playerrating={}
chemistry={}

my_list = os.listdir('data')
for i in my_list:
	path='data/'+i
	for j in os.listdir(path):
		if j=='part-00000' or j=='part-00001':
			path1=path+'/'+j
			fo=open(path1,'r+')
			lines=fo.readlines()
			for k in lines:
				k=eval(k)
				#print(k)
				playerprofile[k[0]]={}
				playerprofile[k[0]]['fouls']=k[1][0][0][5]
				playerprofile[k[0]]['goals']=k[1][0][0][7]
				playerprofile[k[0]]['own_goals']=k[1][0][0][6]
				playerprofile[k[0]]['percent_pass_accuracy']=k[1][0][0][0]*100
				playerprofile[k[0]]['percent_shots_on_target']=k[1][0][0][4]*100
				playerrating[k[0]]=k[1][1]
			fo.close()

my_list1 = os.listdir('chemdata')
for i in my_list1:
	path='chemdata/'+i
	for j in os.listdir(path):
		if j=='part-00000' or j=='part-00001':
			path1=path+'/'+j
			fo=open(path1,'r+')
			lines=fo.readlines()
			for k in lines:
				k=eval(k)
				chemistry[k[0]]=k[1]
				
with open('MatchData.txt','r') as f:
	matchdata=[json.loads(line) for line in f]
mrdd=sc.parallelize(matchdata)	
			
	

with open(sys.argv[1],'r') as fi:
	query=fi.read()
	query=query.replace('\t','')
	query=query.replace('\n','')
	query=query.replace(',}','}')
	query=query.replace(',]',']')
	query=json.loads(query)
	
if 'req_type' in list(query.keys()):
	if query['req_type']==1:
		date=query['date']
		team1=query['team1']
		team2=query['team2']
		predictWin(date,team1,team2)
	if query['req_type']==2:
		name=query['name']
		PlayerProfile(name)
elif 'date' in list(query.keys()):
	date=query['date']
	label=query['label']
	getMatchData(date,label)
