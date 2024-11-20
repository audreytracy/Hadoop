import java.io.IOException;
import java.util.StringTokenizer;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BinCount {
   

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private int[] chromosomes = new int[]{0, 248956422, 242193529, 198295559, 190214555, 181538259, 170805979, 159345973, 145138636, 138394717, 133797422, 135086622, 133275309, 114364328, 107043718, 101991189, 90338345, 83257441, 80373285, 58617616, 64444167, 46709983, 50818468, 156040895, 57227415}; // index chromosome 1 for index 1, chromosome 2 for index 2, X = 23, Y = 24

        private int[] bins = new int[]{0, 0, 2490, 4912, 6895, 8798, 10614, 12323, 13917, 15369, 16753, 18091, 19442, 20775, 21919, 22990, 24010, 24914, 25747, 26551, 27138, 27783, 28251, 28760, 30321, 30894}; // index chromosome 1 for index 1, chromosome 2 for index 2, X = 23, Y = 24
    

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            StringTokenizer line;
 	    System.out.println("hi");
 	    while (itr.hasMoreTokens()) {
                line = new StringTokenizer(itr.nextToken().toString(), " \t");
                System.out.println("hi");
		if (line.countTokens() == 4){ // line only valid if 4 values
                    int chromosome1 = Integer.parseInt(line.nextToken());
                    int position1 = Integer.parseInt(line.nextToken());
                    int chromosome2 = Integer.parseInt(line.nextToken());
                    int position2 = Integer.parseInt(line.nextToken());
                    if(position1 <= chromosomes[chromosome1] && position2 <= chromosomes[chromosome2]){
                        int bin1 = bins[chromosome1] + 1 + (int)Math.ceil(position1/100000);
                        int bin2 = bins[chromosome2] + 1 + (int)Math.ceil(position2/100000);
                        if(bin1 > bin2){
			    int temp = bin1;
			    bin1 = bin2;
			    bin2 = temp;
			}
			word.set("(" + bin1 + ", " + bin2 + ")");
                        context.write(word, one);
                    }
                }
            }
        }

    }
    
    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "chromosome bin count");
        job.setJarByClass(BinCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
	try{
	    FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
	}
	catch(Exception e){ } // if directory already exists
	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
