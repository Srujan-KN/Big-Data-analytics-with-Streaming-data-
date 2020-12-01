import time
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
import sys
import json
import requests
import csv
import os
from pyspark.ml.regression import LinearRegression
from pyspark.ml.clustering import KMeans



#players=sys.argv[1]
#teams=sys.argv[1]



conf=SparkConf()
conf.setAppName('FPL_Project')

sc = SparkContext(conf=conf)
spark=SparkSession(sc)
sqlContext=SQLContext(sc)


#players=spark.read.csv(sys.argv[1], header=True)
#teams=spark.read.csv(sys.argv[2], header=True)

#def f2(x):
#	return {x.Id:{'contr':0,'perf':0,'rating':0.5}}

#pp=players.rdd.map(lambda x: f2(x))
#print(pp.collect())

ssc=StreamingContext(sc,2)
ssc.checkpoint('FPL_Project Checkpoint')
input_stream=ssc.socketTextStream("localhost",6100)
	
def acc(x,val):
	for i in x:
		try:
			if i['id']==val:
				return val,1	
		except:
			continue
			
def f3(x,val1,val2):
	for i in x:
		if i['id']==val1:
			for j in x:
				if j['id']==val2:
					return val1,1

def f1(rdd,p):
	if not rdd.isEmpty():
		pl={}
		lin=[]
		lout=[]
		date=''
		team1=[]
		team2=[]
		#a=rdd.map(lambda x: x)
		#print(a.collect())
		
		matches=rdd.filter(lambda x: 'status' in x)
		#print(matches.collect())
		
		teamsd=matches.map(lambda x: eval(x)['teamsData'])
		
		team1=list(teamsd.collect()[0].keys())[0]
		#print(team1)
		team2=list(teamsd.collect()[0].keys())[1]
		try:
			tb1=teamsd.map(lambda x: x[team1]['bench']['playerId'])
			tb2=teamsd.map(lambda x: x[team2]['bench']['playerId']) #.map(lambda x: x['playerId'])
			
			tl1=teamsd.map(lambda x: x[team1]['lineup']['playerId'])#.map(lambda x: x['playerId'])
			tl2=teamsd.map(lambda x: x[team2]['lineup']['playerId'])#.map(lambda x: x['playerId'])
			
			#team1=sc.parallelize(Seq(team1))
			#team2=sc.parallelize(Seq(team2))
			t1=tb1.union(tl1)
			t2=tb2.union(tl2)
			t11=t1.cartesian(t1)
			t22=t2.cartesian(t2)
			t12=t1.cartesian(t2)
			#print(t12.collect())
			
			for i in t11.collect():
				print(i)
		except:
			pass
		
		
		
		events=rdd.filter(lambda x: 'eventId' in x)
		#print(events.collect())
		
		#PASS ACCURACY
		b=events.filter(lambda x: eval(x)['eventId']==8)
		#print(b.collect())
		
		passes=b.map(lambda x: eval(x)['tags'])
		#print(passes.collect())
		
		tpass=len(b.collect())
		
		
		acc_pass=passes.map(lambda x: acc(x,1801)).filter(lambda x: x!=None)
		#print(acc_pass.collect())
		
		apass=acc_pass.reduceByKey(lambda a,b:a+b)
		#print(apass.collect())
		ap=0
		if len(apass.collect())>0:
			ap=apass.collect()[0][1]
			print(ap)
		
		kpass=passes.map(lambda x: acc(x,302)).filter(lambda x: x!=None)
		#print(kpass.collect())
		
		nkpass=kpass.reduceByKey(lambda a,b:a+b)
		#print(nkpass.collect())
		nkp=0
		if len(nkpass.collect())>0:
			nkp=nkpass.collect()[0][1]
		
		acc_kpass=passes.map(lambda x: f3(x,1801,302)).filter(lambda x: x!=None)
		#print(acc_kpass.collect())
		akp=0
		if len(acc_kpass.collect())>0:
			akp=acc_kpass.collect()[0][1]
		
		nakpass=acc_kpass.reduceByKey(lambda a,b:a+b)
		#print(nakpass.collect())
		nakp=0
		if len(nakpass.collect())>0:
			nakp=nakpass.collect()[0][1]
		
		pass_accuracy=((ap-akp)+(akp*2))/((tpass-nkp)+(nkp*2))
		print('Pass accuracy: ',pass_accuracy)
		
		#duel effectiveness
		c=events.filter(lambda x: eval(x)['eventId']==1)
		
		duel=c.map(lambda x: eval(x)['tags'])
		
		tduel=len(duel.collect())
		
		dlost=duel.map(lambda x: acc(x,701)).filter(lambda x: x!=None)
		
		dneut=duel.map(lambda x: acc(x,702)).filter(lambda x: x!=None).reduceByKey(lambda a,b: a+b)
		dn=0
		if len(dneut.collect())>0:
			dn=dneut.collect()[0][1]
		#print('dn: ',dn)
		
		dwon=duel.map(lambda x: acc(x,703)).filter(lambda x: x!=None).reduceByKey(lambda a,b: a+b)
		dw=0
		if len(dwon.collect())>0:
			dw=dwon.collect()[0][1]
		#print('dw: ',dw)
		
		duel_eff=(dw+(float(dn)*0.5))/tduel
		print('Duel effectiveness: ',duel_eff)
		
		#free kick effectiveness
		d=events.filter(lambda x: eval(x)['eventId']==3)
		
		tfree=len(d.collect())
		
		free=d.map(lambda x: eval(x)['tags'])
		
		fke=free.map(lambda x: acc(x,1801)).filter(lambda x: x!=None).reduceByKey(lambda a,b: a+b)
		fk_eff=0
		if len(fke.collect())>0:
			fk_eff=fke.collect()[0][1]
		#print('fke: ',fk_eff)
		
		pen=d.filter(lambda x: eval(x)['subEventId']==35)
		
		free1=pen.map(lambda x: eval(x)['tags'])
		
		fpen=free1.map(lambda x: acc(x,101)).filter(lambda x: x!=None).reduceByKey(lambda a,b: a+b)
		fp=0
		if len(fpen.collect())>0:
			fp=fpen.collect()[0][1]
			print('fp: ',fp)
		
		free_eff=(fk_eff+fp)/tfree
		print('free kick eff: ',free_eff)
		
		#shots eff
		e=events.filter(lambda x: eval(x)['eventId']==10)
		
		tshots=len(d.collect())
		
		shots=e.map(lambda x: eval(x)['tags'])
		
		sh=shots.map(lambda x: acc(x,1801)).filter(lambda x: x!=None).reduceByKey(lambda a,b: a+b)
		shh=0
		if len(sh.collect())>0:
			shh=sh.collect()[0][1]
		#print('shh: ',shh)
		
		
		tgs=shots.map(lambda x: f3(x,1801,101)).filter(lambda x: x!=None).reduceByKey(lambda a,b: a+b)
		tg=0
		if len(tgs.collect())>0:
			tg=tgs.collect()[0][1]
		#print('tg: ',tg)
		
		shots_eff=(tg+(float(shh-tg)*0.5))/tshots
		print('shots eff: ',shots_eff)
		
		#foul
		f=events.filter(lambda x: eval(x)['eventId']==2)
		fouls=len(f.collect())
		print('Fouls: ',fouls)
		
		#own goal
		og=events.map(lambda x: acc(x,102)).filter(lambda x: x!=None).reduceByKey(lambda a,b: a+b)
		own_goals=0
		if len(og.collect())>0:
			own_goals=og.collect()[0][1]
		print('Own goals: ',own_goals)
		
		player_contrib=(pass_accuracy+duel_eff+free_eff+shots_eff)/4
		print('Player contribution: ',player_contrib)
		
		player_perf=player_contrib-((0.005*fouls)+(0.05*own_goals))
		
		player_rating=0.5
		player_rating=(player_perf+player_rating)/2
		
		change_in_rating=player_rating-0.5
		
p={}		

input_stream.foreachRDD(lambda rdd: f1(rdd,p))

ssc.start()
ssc.awaitTermination()
ssc.stop()


df=pd.read_csv(sys.argv[1]) #doubt
s_df=sqlContext.createDataFrame(df)

kmeans=KMeans().setK(5).setSeed(1) #here we have to check for player 
model = kmeans.fit(dataset)
wssse= kmeans.fit(dataset)


predictions=model.transform(test_data)
evaluator = ClusteringEvaluator() #include module


