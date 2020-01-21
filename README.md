# Big data course - Spark

## Dataset
Step 1 : [Download data](https://osu.app.box.com/v/lstw-traffic-weather-v2)

Step 2 : Extract to data folder

Step 3 : Rename the extracted file to `traffic-data`

## Spark MacOS
Step 1 : Install Homebrew
    `/usr/bin/ruby -e “$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)”`

Step 2 : Install xcode-select
    `xcode-select –install`

Step 3 : Install Java 1.8 sdk, 
    Installation guide at Oracle

Step 4 : Install Spark
    `brew install apache-spark`

Spark version 2.4.4 with hadoop 2.7

## Microsoft spark worker 
Worker installed in `~/`, 
Worker version 0.8.0

## Run project command

```
export DOTNET_WORKER_DIR="/home/strahinja/Microsoft.Spark.Worker-0.8.0/"

copy SparkClient.dll to /home/strahinja/Microsoft.Spark.Worker-0.8.0/

spark-submit --class org.apache.spark.deploy.dotnet.DotnetRunner --master local bin/Debug/netcoreapp2.2/microsoft-spark-2.4.x-0.7.0.jar dotnet bin/Debug/netcoreapp2.2/SparkClient.dll
```
