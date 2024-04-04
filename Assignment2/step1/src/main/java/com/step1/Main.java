package com.step1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;

public class Main
{
    public static class mapperENG extends Mapper<LongWritable, Text, Text, Text>
    {
        private Text key = new Text();
        private Text value = new Text();
        private String firstWord;
        private String secondWord;
        private int decade;
        private long occurrences;
        private String regex = "^[a-zA-Z]+$";
        private String[] tokensA;
        private  String[] tokensB;
        HashMap<String, Integer> stopWords = new HashMap<>();

        public void setup(Context context)  throws IOException, InterruptedException
        {
            String[] array = new String[]{"a", "about", "above", "across", "after", "afterwards", "again",
                    "against", "all", "almost", "alone", "along", "already", "also", "although", "always", "am",
                    "among", "amongst", "amoungst", "amount", "an", "and", "another", "any", "anyhow", "anyone",
                    "anything", "anyway", "anywhere", "are", "around", "as", "at", "back", "be", "became",
                    "because", "become", "becomes", "becoming", "been", "before", "beforehand", "behind", "being",
                    "below", "beside", "besides", "between", "beyond", "bill", "both", "bottom", "but", "by", "call",
                    "can", "cannot", "cant", "co", "computer", "con", "could", "couldnt", "cry", "de", "describe",
                    "detail", "do", "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven",
                    "else", "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything",
                    "everywhere", "except", "few", "fifteen", "fify", "fill", "find", "fire", "first", "five", "for",
                    "former", "formerly", "forty", "found", "four", "from", "front", "full", "further", "get",
                    "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter", "hereby",
                    "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how", "however", "hundred",
                    "i", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its", "itself", "keep",
                    "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me", "meanwhile",
                    "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must", "my",
                    "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody",
                    "none", "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once",
                    "one", "only", "onto", "or", "other", "others", "otherwise", "our", "ours", "ourselves", "out",
                    "over", "own", "part", "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem",
                    "seemed", "seeming", "seems", "serious", "several", "she", "should", "show", "side", "since",
                    "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something", "sometime",
                    "sometimes", "somewhere", "still", "such", "system", "take", "ten", "than", "that", "the",
                    "their", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "therefore",
                    "therein", "thereupon", "these", "they", "thick", "thin", "third", "this", "those", "though",
                    "three", "through", "throughout", "thru", "thus", "to", "together", "too", "top", "toward",
                    "towards", "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us", "very", "via",
                    "was", "we", "well", "were", "what", "whatever", "when", "whence", "whenever", "where",
                    "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which",
                    "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "with", "within",
                    "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves"};

            for (String StopWord : array) {stopWords.put(StopWord,0);}
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            tokensA = value.toString().split("\t");
            if (tokensA.length > 1)
            {
                tokensB = tokensA[0].split(" ");
                if (tokensB.length > 1)
                {
                    firstWord = tokensB[0];
                    secondWord = tokensB[1];

                    decade = (Integer.parseInt(tokensA[1]) / 10) * 10;
                    occurrences = Long.parseLong(tokensA[2]);

                    if ((!stopWords.containsKey(firstWord)) & (!stopWords.containsKey(secondWord)))
                    {
                        if (firstWord.matches(regex) & secondWord.matches(regex))
                        {
                            this.key.set(decade + " " + firstWord + " " + secondWord);
                            this.value.set(String.valueOf(occurrences));
                            context.write(this.key, this.value);

                            this.key.set(decade + " " + firstWord + " " + "*");
                            this.value.set(String.valueOf(occurrences));
                            context.write(this.key, this.value);

                            this.key.set(decade + " " + "*" + " " + "*");
                            this.value.set(String.valueOf(occurrences));
                            context.write(this.key, this.value);
                        }
                    }
                }
            }
        }
    }

    public static class mapperHEB extends Mapper<LongWritable, Text, Text, Text>
    {
        private Text key = new Text();
        private Text value = new Text();
        private String firstWord;
        private String secondWord;
        private int decade;
        private long occurrences;
        private String[] tokensA;
        private  String[] tokensB;
        HashMap<String, Integer> stopWords = new HashMap<>();

        public void setup(Context context)  throws IOException, InterruptedException
        {
            String[] array = new String[]{"״","׳", "של","רב","פי","עם","עליו","עליהם","על","עד","מן","מכל","מי","מהם",
                    "מה","מ","למה","לכל","לי","לו","להיות","לה","לא","כן","כמה","כלי","כל","כי","יש","ימים","יותר",
                    "יד","י","זה","ז","ועל","ומי","ולא","וכן","וכל","והיא","והוא","ואם","ו","הרבה","הנה","היו",
                    "היה","היא","הזה","הוא","דבר","ד","ג","בני","בכל","בו","בה","בא","את","אשר","אם","אלה","אל",
                    "אך","איש","אין","אחת","אחר","אחד","אז","אותו","־","^","?", ";",":","1",".","-","*","\"","!",
                "שלשה","בעל","פני",")","גדול","שם","עלי","עולם","מקום","לעולם","לנו","להם","ישראל","יודע","זאת",
                "השמים","הזאת","הדברים","הדבר","הבית","האמת","דברי","במקום","בהם","אמרו","אינם","אחרי","אותם","אדם",
                "(","חלק","שני","שכל","שאר","ש","ר","פעמים","נעשה","ן","ממנו","מלא","מזה","ם","לפי","ל","כמו","כבר",
                "כ","זו","ומה","ולכל","ובין","ואין","הן","היתה","הא","ה","בל","בין","בזה","ב","אף","אי","אותה",
                "או","אבל","א"};

            for (String StopWord : array) {stopWords.put(StopWord,0);}
        }

        public static boolean containsHebrew(String str)
        {
            for (char c : str.toCharArray())
            {
                if (Character.UnicodeBlock.of(c) != Character.UnicodeBlock.HEBREW)
                {
                    return false;
                }
            }
            return true;
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            tokensA = value.toString().split("\t");
            if (tokensA.length > 1)
            {
                tokensB = tokensA[0].split(" ");
                if (tokensB.length > 1)
                {
                    firstWord = tokensB[0];
                    secondWord = tokensB[1];

                    decade = (Integer.parseInt(tokensA[1]) / 10) * 10;
                    occurrences = Long.parseLong(tokensA[2]);

                    if ((!stopWords.containsKey(firstWord)) & (!stopWords.containsKey(secondWord)))
                    {
                        if (containsHebrew(firstWord) & containsHebrew(secondWord))
                        {
                            this.key.set(decade + " " + firstWord + " " + secondWord);
                            this.value.set(String.valueOf(occurrences));
                            context.write(this.key, this.value);

                            this.key.set(decade + " " + firstWord + " " + "*");
                            this.value.set(String.valueOf(occurrences));
                            context.write(this.key, this.value);

                            this.key.set(decade + " " + "*" + " " + "*");
                            this.value.set(String.valueOf(occurrences));
                            context.write(this.key, this.value);
                        }
                    }
                }
            }
        }
    }

    public static class reducer extends Reducer<Text, Text, Text, Text>
    {
        private Text value = new Text();
        private long sum = 0;

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            sum = 0;
            for (Text val : values)
            {
                sum += Long.parseLong(val.toString());
            }

            this.value.set(String.valueOf(sum));
            context.write(key,this.value);
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
        //conf.set("mapred.max.split.size", "33554432");

        Job job = Job.getInstance(conf, "Step 1");

        job.setJarByClass(Main.class);

        if (args[0].contains("eng")) {job.setMapperClass(mapperENG.class);}
        else {job.setMapperClass(mapperHEB.class);}

        job.setReducerClass(reducer.class);
        job.setCombinerClass(reducer.class);
        job.setPartitionerClass(partition.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}