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
import java.util.HashMap;
import java.util.Iterator;

public class Creator {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        private String stem(String s)
        {
            Stemmer st = new Stemmer();
            st.add(s.toCharArray(), s.length());
            st.stem();
            return st.toString();
        }
        public boolean handleAnnotated(String[] splitUpValue, Context context) throws IOException, InterruptedException
        {
            if(!splitUpValue[0].startsWith("-"))
            {
                context.write(new Text(stem(splitUpValue[0])+":"+stem(splitUpValue[1])), new Text(splitUpValue[2]));
                return true;
            }
            return false;
        }
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splitUpValue = value.toString().split("\t");
            if(handleAnnotated(splitUpValue, context))
                return;

            String rKey = splitUpValue[0].substring(1);
            String rValue = splitUpValue[1];
            context.write(new Text(rKey), new Text(rValue));

        }
    }
    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            boolean isAnnotated = false;
            String annotatedLabel = "NA";
            HashMap<String, String> map = new HashMap<>();
            //Check if should build vector
            for(Text value: values)
            {
                if(value.toString().equals("False") || value.toString().equals("True")) {
                    isAnnotated = true;
                    annotatedLabel = value.toString().toLowerCase();
                }
                else
                {
                    map.put(value.toString(),"");
                }
            }
            if(isAnnotated) {
                Iterator it = map.entrySet().iterator();
                while (it.hasNext()) {
                        HashMap.Entry pair = (HashMap.Entry)it.next();
                        String nouns = key.toString();
                        String dp = pair.getKey().toString().split(",")[0];
                        String counter = pair.getKey().toString().split(",")[1];
                        context.write(new Text(dp), new Text(nouns + "," + counter + "," + annotatedLabel));

                }
            }

        }
    }
    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return  Math.abs(key.hashCode()) % numPartitions;
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Creator");
        job.setJarByClass(Creator.class);

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
        FileInputFormat.addInputPath(job, new Path("s3n://nitay-omer-assignment3/annotatedset"));

        //output
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
