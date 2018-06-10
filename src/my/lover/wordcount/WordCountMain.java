package my.lover.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountMain {

	public static void main(String[] args) throws Exception {
		// 创建一个job = map + reduce
		// Configuration conf = new Configuration();
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://192.168.89.128:9000");

		// 创建一个Job
		Job job = Job.getInstance(conf);
		// 指定任务的入口
		job.setJarByClass(WordCountMain.class);

		// 指定job的mapper
		job.setMapperClass(WordCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		// 指定job的reducer
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		// hadoop jar wc.jar /linjing/mytest /linjing/output/wc
		// 指定任务的输入和输出
		FileInputFormat.setInputPaths(job, new Path("/linjing/mytest2"));
		FileOutputFormat.setOutputPath(job, new Path("/linjing/wc2"));

		// 提交任务
		job.waitForCompletion(true);
	}

}
