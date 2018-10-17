package com.carol.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @author Carol Tang
 * @date 2018年10月16日 上午10:43:01 
 * @version v1.0
 * @Description 使用hdfsApi上传本地文件到hdfs文件系统
 */
public class CopyFromLocalFile {

	public static void main(String[] args) {
		//1.客户端加载配置文件
		Configuration conf = new Configuration();
		//2.指定配置(设置成2个副本数)
		conf.set("dfs.replication", "2");
		//3.指定块大小
		conf.set("dfs.blocksize", "64m");
		//4.加载文件系统
		FileSystem fs = null;
		try {
			 fs = FileSystem.get(new URI("hdfs://192.168.1.111:9000/"), conf,"root");
			
			 //5.上传文件
			 fs.copyFromLocalFile(new Path("F:\\bigdata\\权限的管理.png"),new Path("/"));
			 
			 //6.关闭资源
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
