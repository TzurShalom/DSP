package com.step7;

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
    public static class mapper extends Mapper<LongWritable, Text, Key, Text>
    {
        private Text value = new Text();
        private String firstWord;
        private String secondWord;
        private int decade;
        private double calculation;
        private String[] tokensA;
        private  String[] tokensB;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            tokensA = value.toString().split("\t");
            if (tokensA.length > 1)
            {
                tokensB = tokensA[0].split(" ");
                if (tokensB.length > 1)
                {
                    decade = Integer.parseInt(tokensB[0]);
                    firstWord = tokensB[1];
                    secondWord = tokensB[2];
                    calculation = Double.parseDouble(tokensA[1]);

                    this.value.set(decade + " " + firstWord + " " + secondWord);
                    context.write(new Key(calculation,decade),this.value);
                }
            }
        }
    }

    public static class reducer extends Reducer<Key, Text, Text, Text>
    {
        private Text key = new Text();

        public void reduce(Key key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            for (Text val : values)
            {
                this.key.set(String.valueOf(key.getCalculation()));
                context.write(this.key,val);
            }
        }
    }

    public static class partition extends Partitioner<Key, Text>
    {
        private String decade;
        public int getPartition(Key key, Text value, int numPartitions)
        {
            decade = String.valueOf(key.getDecade());
            return Math.abs(decade.hashCode() % numPartitions);
        }
    }


    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Step 7");
        job.setJarByClass(Main.class);

        job.setMapperClass(mapper.class);
        job.setReducerClass(reducer.class);
        job.setPartitionerClass(partition.class);

        job.setOutputKeyClass(Key.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}