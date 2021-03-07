import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import java.io.IOException;
import java.util.Iterator;

public class preJoiner {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splitUpKeyValue = value.toString().split("\t");
            String myKey = splitUpKeyValue[0];
            String myValue = splitUpKeyValue[1];
            context.write(new Text(myKey), new Text(myValue));
        }
    }
    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        int dpCounter;
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            dpCounter = 0;
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            for(Text value: values)
            {
                String[] splitUpValues = value.toString().split(",");
                String noun = splitUpValues[0];
                String counter = splitUpValues[1];
                String ann = splitUpValues[2];
                context.write(new Text(noun), new Text(dpCounter +"," + counter + "," + ann));

            }
            dpCounter++;
            context.write(new Text(Character.MIN_VALUE+""), new Text(key.toString()));
        }
    }
    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return  0;
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "preJoiner");
        job.setJarByClass(preJoiner.class);

        //Setting Mapper / Reducer / Combiner / Partitioner
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setPartitionerClass(PartitionerClass.class);

        //Setting MAP output
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //input - reading both step1 output and annotated input
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        //num of reducers
        job.setNumReduceTasks(1);
        //output
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
