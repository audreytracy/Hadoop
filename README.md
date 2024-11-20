# Hadoop Map Reduce

## How to run  
*  Compile program with  `hadoop com.sun.tools.javac.Main BinCount.java`
*  Create jar file with `jar cf bc.jar BinCount*.class`  
*  Execute jar file with `hadoop jar bc.jar BinCount /user/username/bincount/input /user/username/bincount/output`

List output files with `hadoop fs -ls /user/username/bincount/output/`

To get output in one single file, either add `conf.setNumReduceTasks(1);` to [line 74](https://github.com/audreytracy/Hadoop/blob/main/BinCount.java#L74) to declare the number of reduce tasks as 1, or use `hadoop fs -getmerge /user/username/bincount/output merged` to merge the files. View with `cat merged`


## Demo  
https://github.com/user-attachments/assets/4d1ef797-3e5e-4e23-92c2-1012704bfbd8

