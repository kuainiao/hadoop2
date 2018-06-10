package my.lover;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Test {

	public static void main(String[] args) throws Exception {
  /*System.setProperty("hadoop.home.dir",
		  "F:\\bigdata\\大数据开发必备工具2\\hadoop安装包\\hadoop-2.7.3");*/
       Configuration configuration=new Configuration();
       configuration.set("fs.default.name", "hdfs://192.168.89.128:9000");
       
       FileSystem fs=FileSystem.get(configuration);
//       fs.mkdirs(new Path("/linjing"));
       
       FileStatus[] listStatus = fs.listStatus(new Path("/linjing"));  
       
       for(FileStatus fileStatus:listStatus){  
           System.out.println(fileStatus.getPath().getName());  
           System.out.println(fileStatus.isFile()?"file":"dir");  
       }  
       
	}

}
