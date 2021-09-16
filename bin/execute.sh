java -cp ./cli/target/cli-1.0-SNAPSHOT-jar-with-dependencies.jar com.logicalclocks.cli.HopsCreateJob -jobType FLINK  -simulation no -jarFilePath $(pwd)/flink/target/flink-1.0-SNAPSHOT.jar
java -cp ./cli/target/cli-1.0-SNAPSHOT-jar-with-dependencies.jar com.logicalclocks.cli.HopsRunJob -jobType FLINK  -simulation no -jarFilePath $(pwd)/flink/target/flink-1.0-SNAPSHOT.jar
