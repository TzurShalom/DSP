package com.step6;

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
        private double calculation;
        private String[] tokensA;
        private  String[] tokensB;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            tokensA = value.toString().split("\t");
            if (tokensA.length > 1)
            {
                tokensB = tokensA[1].split(" ");
                if (tokensB.length > 1)
                {
                    decade = Integer.parseInt(tokensA[0]);
                    firstWord = tokensB[0];
                    secondWord = tokensB[1];
                    calculation = Double.parseDouble(tokensB[2]);

                    this.key.set(decade + " " + firstWord + " " + secondWord);
                    this.value.set(String.valueOf(calculation));
                    context.write(this.key,this.value);
                }
            }
        }
    }

    public static class reducer extends Reducer<Text, Text, Text, Text>
    {
        private Text key = new Text();
        private Text value = new Text();
        private int decade;
        private String firstWord;
        private String secondWord;
        private double calculation;
        private double sum;
        private String[] tokensA;
        private static double minPmi;
        private static double relMinPmi;

        public void setup(Context context) throws IOException, InterruptedException
        {
            super.setup(context);
            minPmi = Double.parseDouble(context.getConfiguration().get("minPmi","0.5"));
            relMinPmi = Double.parseDouble(context.getConfiguration().get("relMinPmi","0.2"));
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            for (Text val : values)
            {
                tokensA = key.toString().split(" ");
                decade = Integer.parseInt(tokensA[0]);
                firstWord = tokensA[1];
                secondWord = tokensA[2];

                calculation = Double.parseDouble(val.toString());

                if (firstWord.contains("*") & secondWord.contains("*"))
                {
                    sum = calculation;
                }
                else
                {
                    if (calculation >= minPmi || (calculation / sum) >= relMinPmi)
                    {
                        this.key.set(decade + " " + firstWord + " " + secondWord);
                        this.value.set(String.valueOf(calculation));
                        context.write(this.key, this.value);
                    }
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
        conf.set("minPmi",args[2]);
        conf.set("relMinPmi",args[3]);

        Job job = Job.getInstance(conf, "Step 6");
        job.setJarByClass(Main.class);

        job.setMapperClass(mapper.class);
        job.setReducerClass(reducer.class);
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