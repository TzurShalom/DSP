package com.step3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Main
{
    public static class mapper extends Mapper<LongWritable, Text, Text, Text>
    {
        private Text key = new Text();
        private Text value = new Text();
        private String firstWord;
        private String secondWord;
        private int decade;
        private long c_W1W2;
        private long c_N;
        private long c_W1;
        private String[] tokensA;
        private String[] tokensB;
        private String[] tokensC;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            tokensA = value.toString().split("\t");
            if (tokensA.length > 1)
            {
                tokensB = tokensA[0].split(" ");
                if (tokensB.length > 1)
                {
                    tokensC = tokensA[1].split(" ");
                    if (tokensC.length > 1)
                    {
                        decade = Integer.parseInt(tokensB[0]);
                        firstWord = tokensB[1];
                        secondWord = tokensB[2];

                        c_W1W2 = Long.parseLong(tokensC[0]);
                        c_N = Long.parseLong(tokensC[1]);
                        c_W1 = Long.parseLong(tokensC[2]);

                        this.key.set(decade + " " + firstWord + " " + secondWord);
                        this.value.set(c_W1W2 + " " + c_N + " " + c_W1);
                        context.write(this.key, this.value);

                        this.key.set(decade + " " + "*" + " " + secondWord);
                        this.value.set(String.valueOf(c_W1W2));
                        context.write(this.key, this.value);
                    }
                }
            }
        }
    }

    public static class reducer extends Reducer<Text, Text, Text, Text>
    {
        private Text value = new Text();
        private long sum;

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            if (key.toString().contains("*"))
            {
                sum = 0;
                for (Text val : values)
                {
                    sum += Long.parseLong(val.toString());
                }

                this.value.set(String.valueOf(sum));
                context.write(key,this.value);
            }
            else
            {
                for (Text val : values)
                {
                    context.write(key,val);
                }
            }
        }
    }

    public static class partition extends Partitioner<Text, Text>
    {
        private String decade;
        public int getPartition(Text key, Text value, int numPartitions)
        {
            decade = key.toString().split(" ")[0];
            return Math.abs(decade.hashCode() % numPartitions);
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Step 3");
        job.setJarByClass(Main.class);

        job.setMapperClass(mapper.class);
        job.setReducerClass(reducer.class);
        job.setCombinerClass(reducer.class);
        job.setPartitionerClass(partition.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}