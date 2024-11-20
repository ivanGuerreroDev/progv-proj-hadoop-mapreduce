hadoop-mapreduce-apps
=========================

Some simple and complex of mapreduce tasks for Hadoop. The main idea is to use a build tool (Gradle) and to show how standard map/reduce tasks can be executed on Hadoop2.

Build
=========================

Simply clone the repository to your local file system

gradle clean build

Afterwards you will find a jar in the directory build/libs

Run on Hadoop
=========================

You should have Hadoop installed locally, run it on a cluster or leverage a cloud service, such as Amazon EMR, Google Compute or Microsoft Azure.


RemoveFirstColumn
=========

hadoop fs -mkdir /tmp

hadoop fs -copyFromLocal ./input /tmp

Execute in the command line the following command:

hadoop jar build/libs/hadoop-apps-0.1.0.jar org.ivan.mapreduce.RemoveFirstColumn.Driver /tmp/input /tmp/output

After some time you will see that the job successfully finished. You can see the output by using the following command:

hadoop fs -cat /tmp/output/part-r-00000
