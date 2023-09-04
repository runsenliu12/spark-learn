package com.other;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class MergeSmallFiles {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        SparkConf conf = new SparkConf().setAppName("MergeSmallFiles").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String inputPath = "path/to/small/files"; // 输入小文件所在的目录
        String outputPath = "path/to/output"; // 合并后的输出目录

        // 将小文件合并成一个大文件
        mergeSmallFiles(inputPath, outputPath, sc.hadoopConfiguration());

        sc.stop();
        sc.close();
    }

    // 使用Hadoop的TextInputFormat将小文件合并成大文件
    public static void mergeSmallFiles(String inputPath, String outputPath, org.apache.hadoop.conf.Configuration hadoopConfig)
            throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(hadoopConfig);
        job.setJobName("MergeSmallFiles");

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapperClass(MergeMapper.class);
        job.setReducerClass(MergeReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }

    // 自定义Mapper将小文件内容复制到大文件
    public static class MergeMapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, Text> {
        private Text filename = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String filenameStr = ((org.apache.hadoop.mapreduce.lib.input.FileSplit) context.getInputSplit()).getPath().getName();
            filename.set(filenameStr);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(filename, value);
        }
    }

    // 自定义Reducer将小文件内容合并成大文件
    public static class MergeReducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }
}
