# Generate-Sort-Validate
1. Download hadoop. Here is a nice tutorial I found - 
https://amodernstory.com/2014/09/23/installing-hadoop-on-mac-osx-yosemite/

2. Once you have made sure hadoop in installed and running. Follow below steps to run the entire project of generate, sort and validate in one go.

#### NOTE : 

1. Only caveat to running the below script is to generate,sort and validate on 10GB data, you need to have atleast 80-100GB free space in your laptop. If you donot have that run the below script to generate less amount of data.

2. run.sh should be followed by a number indicating the amount of data in GBs you want to generate.

3. As shown below , I am running the run.sh script with parameter as 5 to generate 5GB file.

------------------------------------------------------------------------------------------------------------------------------

From the root of the project go "sort-map-reduce-master" folder, run below 2 commands.

```bash
cd mapreduce-sort-example
sudo -E ./run.sh 5
```

#### Explanation of run.sh script (Do not run below steps again)

This run.sh does the following steps

1) Build the project using maven

```bash
mvn clean install
mvn assembly : assembly
```

2) Generate data

```bash
java -classpath target/generate.jar org.hadoop.generation.DataGenerator $1
```

3) Clean up previos input,output folders in hadoop
```bash
hdfs dfs -rm -r /inputFolder
hdfs dfs -rm -r /outputFolder
hdfs dfs -rm -r /validatedFolder
```

4) Copy generated file in hadoop and remove it from local.
```bash
hdfs dfs -mkdir /inputFolder
rm ./output/outputFile.txt
```

5) Sort the input file.
```bash
hadoop jar target/sort.jar /inputFolder /outputFolder
```

6) Validate the output of sorted file.
```bash
hadoop jar target/validate.jar /outputFolder/part-r-00000 /validatedFolder
```

7) Check the status of validation output.

If everything goes good. the output of the script will be similar to below one.
```bash
Data Validated status : STATUS
Time taken for generation, sorting and validation : 2591 seconds
```
If it results in any error, the output will show it as "FAILED".

8) You can verify the output of validated fileas follows

```bash
hdfs dfs -cat /validatedFolder/part-00000 | less
```
