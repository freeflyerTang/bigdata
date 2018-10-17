package com.carol.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @author Carol Tang
 * @date 2018年10月16日 下午1:43:13 
 * @version v1.0
 * @Description 使用hdfsApi从hdfs文件系统下载文件到本地
 * 
 * 注意事项：
 * 		① 配置hadoop环境变量
 * 		② hadoop的bin目录添加编译好的winutil包
 */
public class CopyToLocalFile {
	public static void main(String[] args) {

		//1.加载配置文件
		Configuration conf = new Configuration();
		
		//2.加载文件系统
		FileSystem fs = null;
		try {
			fs = FileSystem.get(new URI("hdfs://192.168.1.111:9000"),conf,"root");
			
			//3.从hdfs文件系统下载文件到windows本地
			fs.copyToLocalFile(new Path("/helloitstart.txt"), new Path("F:/bigdata"));
			
			//4.关闭资源
			fs.close();
		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
		
}
