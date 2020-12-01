# BD_1306_1489_1829_1995-Project_FPL

This repository is for Big Data Final project at PES University to analyse various events that happen in a football league (Fantasy Premier League).
These events are associated to many metrics such as players’ free kick effectiveness, pass accuracy , duel effectiveness , etc. We use streaming data for the analysis.
The matches and the events data were streamed through port 6100, code for which was provided and we used Streaming Spark to read the streamed data.
The players and teams data were stored in hdfs and taken as command line arguments while running the code. 
Here we have two files namely master.py and stream.py
Commands to run the above files on linux :(on seperate Terminal)
$ python3 stream.py
$ spark-submit master.py hdfs://localhost:9000/"players.csv-file path in hdfs" hdfs://localhost:9000/"teams.csv-file path in hdfs" > output.txt
Here, output.txt is the output file to which we redirected the output
