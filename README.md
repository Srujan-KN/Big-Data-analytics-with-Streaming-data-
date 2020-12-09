# BD_1306_1489_1829_1995-Project_FPL

This repository is for Big Data Final project at PES University to analyse various events that happen in a football league (Fantasy Premier League).
These events are associated to many metrics such as players’ free kick effectiveness, pass accuracy , duel effectiveness , etc. We use streaming data for the analysis.
The matches and the events data were streamed through port 6100, code for which was provided and we used Streaming Spark to read the streamed data.<br>
The players and teams data were stored in the local directory (but can be stored in hdfs also) and taken as command line arguments while running the code. <br>

Commands to run the master.py on Linux :<br>
$ python3 stream.py<br>
$ spark-submit master.py > output.txt<br>
These two commands should be run parallely on separate terminals. This creates data/PlayerProfile.txt and chemdata/Chemistry.txt and MatchData.txt files and the output.txt is the output file to which we redirected the standard output to.<br>

Commands to run the ui.py on Linux :
$ spark-submit ui.py <queryfile.json> > outputfile.txt
This program takes in the query file and stores the output in the respective directory as a json file. The queries can be one of the following:
  - to predict the winning chance of a team given two teams data and the date of the match
  - request for a player's profile
  - request for a match's data
