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

public class Joiner {
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
        int dpsCount;
        int DPMin;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            DPMin = Integer.parseInt(context.getConfiguration().get("DPMin"));
            dpsCount=0;
        }
        private void createRelationAndAtrributes(Context context, Iterable<Text> values) throws IOException,  InterruptedException
        {
            context.write(new Text("@RELATION hypernym"), null);
            for(Text dp: values)
            {
                context.write(new Text("@ATTRIBUTE \"" + dp + "\"  NUMERIC"), null);
                dpsCount++;
            }
            context.write(new Text("@ATTRIBUTE class {false,true}"), null);
            context.write(new Text(""), null);
            context.write(new Text("@DATA"), null);
        }
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            if (key.toString().equals(Character.MIN_VALUE + "")) {
                createRelationAndAtrributes(context, values);
            }
            else {
                int current_counter_valid = 0;
                String[] outputArr = new String[dpsCount];
                String output = "";
                String ann = "";
                for (int i = 0; i < outputArr.length; i++)
                    outputArr[i] = "0";
                for (Text value : values) {
                    String[] splitUpValue = value.toString().split(",");
                    int dpNum = Integer.parseInt(splitUpValue[0]);
                    String count = splitUpValue[1];
                    ann = splitUpValue[2];
                    outputArr[dpNum] = count;
                    current_counter_valid++;
                }
                if(current_counter_valid>=DPMin) {
                    for (int i = 0; i < outputArr.length; i++) {
                        output += outputArr[i] + ",";
                    }
                    context.write(new Text("%"), new Text(key.toString()));
                    context.write(new Text(output + ann.toLowerCase()), null);
                }
            }
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
        conf.set("DPMin", args[2]);
        Job job = Job.getInstance(conf, "Joiner");
        job.setJarByClass(Joiner.class);

        //Setting Mapper / Reducer / Combiner / Partitioner
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        //num of reducers
        job.setNumReduceTasks(1);
        //Setting MAP output
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //input - reading both step1 output and annotated input
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));

        //output
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
