package itemCF.step4;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MR4 {

	// 输入文件相对路径 matrix1.txt 左矩阵
	private static String inPath = "/itemCF/step2_output";
	// 输出文件的相对路径
	private static String outPath = "/itemCF/step4_output";
	// 将step1输出的转置矩阵作为全局缓存
	private static String cache = "/itemCF/step3_output/part-r-00000";
	// hdfs地址
	private static String hdfs = "hdfs://192.168.89.128:9000";

	public int run() {
		try {
			// 创建job配置类
			Configuration conf = new Configuration();
			// 设置hdfs的地址
			conf.set("fs.default.name", hdfs);
			// 创建一个job实例
			Job job = Job.getInstance(conf, "step4");
			// 缓存 #加别名，在mapper类中有别名
			job.addCacheArchive(new URI(cache + "#itemUserScore2"));
			// 设置job的主类
			job.setJarByClass(MR4.class);
			// 设置job的mapper类和reduce类
			job.setMapperClass(Mapper4.class);
			job.setReducerClass(Reducer4.class);
			// 设置mapper输出类型
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			// 设置reduce输出类型
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			FileSystem fs = FileSystem.get(conf);
			// 设置输入输出路径
			Path inputPath = new Path(inPath);

			if (fs.exists(inputPath)) {
				FileInputFormat.addInputPath(job, inputPath);
			}
			Path outputPath = new Path(outPath);
			fs.delete(outputPath, true);

			FileOutputFormat.setOutputPath(job, outputPath);
			return job.waitForCompletion(true) ? 1 : -1;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return -1;
	}

	public static void main(String[] args) {
		int result = -1;
		result = new MR4().run();
		if (result == 1) {
			System.out.println("itemCF step4 run successfully!!");
		} else if (result == -1) {
			System.out.println("itemCF step4 run fail!!");
		}
	}
}
