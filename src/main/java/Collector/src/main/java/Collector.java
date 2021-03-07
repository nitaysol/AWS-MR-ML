
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
import java.util.LinkedList;
import java.util.List;

public class Collector {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        private String stem(String s)
        {
            Stemmer st = new Stemmer();
            st.add(s.toCharArray(), s.length());
            st.stem();
            return st.toString();
        }
        private boolean isNoun(String s)
        {
            //NN NNS NNP NNPS
            return s.toLowerCase().startsWith("nn");
        }
        private void searchDP(String[] synTree,String counter, int curr, String sentNouns, String startWord, Context context)
                              throws IOException, InterruptedException
        {
            String[] recordsFirst = synTree[curr].split("/");
            String dp = recordsFirst[2].toLowerCase() + ":N";
            curr = Integer.parseInt(recordsFirst[3]) - 1;
            while(curr >= 0)
            {
                String[] records = synTree[curr].split("/");
                if(isNoun(records[1]))
                {
                    //if not already sent this noun
                    if(!sentNouns.contains("," + stem(records[0]) +","))
                    {
                        //mark this noun as already sent
                        sentNouns = sentNouns + "," + stem(records[0]) +",";
                        String nouns =  startWord+":"+stem(records[0]);
                        //send context
                        context.write(new Text("N:"+dp), new Text(nouns +"," + counter));
                    }
                }
                // add node + edge
                dp =   records[2].toLowerCase() + ":" + stem(records[0]).toUpperCase() + ":" + dp;
                curr = Integer.parseInt(records[3]) - 1;

            }
        }
        private boolean isValid(String[] arr)
        {
            for(int i=0; i<arr.length; i++)
            {
                if(!isValidRow(arr[i]))
                    return false;
            }
            return true;
        }
        private boolean isValidRow(String ngramsEncoded){
            String[] ngramArray = ngramsEncoded.split("/");
            if(ngramArray.length!=4)
                return false;
            return (ngramArray[0].matches("[a-zA-Z0-9]*") && ngramArray[1].matches("[a-zA-Z0-9]*"));
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] recordsArr = value.toString().split("\t");
            //get the syntactic tree of the sentences
            String[] synTree = recordsArr[1].split(" ");
            if(!isValid(synTree))
                return;
            //get nouns
            for(int i=0; i<synTree.length;i++)
            {
                if(isNoun(synTree[i].split("/")[1]))
                    searchDP(synTree, recordsArr[2],i,  "", stem(synTree[i].split("/")[0]), context);

            }

        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        int DPMin;
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            DPMin = Integer.parseInt(context.getConfiguration().get("DPMin"));
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            HashMap<String, Integer> map= new HashMap<>();
            for(Text value: values) {
                String[] split = value.toString().split(",");
                map.putIfAbsent(split[0], 0);
                map.compute(split[0], (k,v)-> v+Integer.parseInt(split[1]));
            }
            int size = map.size();
            if(DPMin>size)
                return;
            Iterator it = map.entrySet().iterator();
            while (it.hasNext()) {
                HashMap.Entry pair = (HashMap.Entry)it.next();
                String nouns = pair.getKey().toString();
                //sending (-nouns, (dp, number of shows for that specific nouns combination on that dp))
                context.write(new Text("-" + nouns), new Text(key + "," + pair.getValue()));
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
        //setting dpmin
        conf.set("DPMin", args[2]);
        Job job = Job.getInstance(conf, "Collector");
        job.setJarByClass(Collector.class);

        //Setting Mapper / Reducer / Combiner / Partitioner
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setPartitionerClass(PartitionerClass.class);

        //Setting MAP output
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //input - reading biarcs
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
