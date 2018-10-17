package com.carol.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Carol Tang
 * @date 2018年10月16日 下午2:52:15 
 * @version v1.0
 * @Description hdfsAPI测试类(使用junit测试)
 */
public class HdfsClientDemo {

	FileSystem fs = null;
	
	/**
	 * 初始化初始化加载hdfs客户端信息
	 * @param
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 */
	@Before
	public void init() throws IOException, InterruptedException, URISyntaxException {
		//加载配置文件
		Configuration conf = new Configuration();
		//设置副本数
		conf.set("dfs.replication", "2");
		//设置块大小
		conf.set("dfs.blocksize", "64m");
		//获取文件系统
		fs = FileSystem.get(new URI("hdfs://192.168.1.111:9000"), conf, "root");
		
	}
	/**
	 * 在hdfs中创建文件夹
	 * 	  ---对应命令方式：hdfs dfs -mkdir /文件名
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void mkdir() throws IllegalArgumentException, IOException{
		//调用创建文件夹的方法
		fs.mkdirs(new Path("/hdfsmkdir"));
		
		//关闭资源
		fs.close();
	}
	
	/**
	 * 在hdfs中移动/修改文件名
	 * 	  ---对应命令方式：hdfs dfs -mv /hdfs路径 /hdfs路径
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void rename() throws IllegalArgumentException, IOException {
		//调用修改文件名的方法(移动并修改)
		fs.rename(new Path("/hdfs-site.xml"), new Path("/hdfsmkdir/hdfs-site-new.xml"));
		//关闭资源
		fs.close();
	}
	
	/**
	 * 删除hdfs中的文件
	 * 	 ---对应命令方式：hdfs dfs rm /文件名
	 * 				 hdfs dfs rm -r /文件夹名 
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
//	@Test
//	public void delete() throws IllegalArgumentException, IOException {
//		
//		//fs.delete(new Path("/helloitstart.txt")); 删除文件，该方法已过时，但可用
//		//fs.delete(new Path("/caroltang"));删除文件夹，该方法已过时，但可用
//		
//		//调用删除文件的方法(参数1：删除的路径  参数二：是否递归删除)
//		//该方法删除了hdfsmkdir文件夹下的内容，并未删除该文件夹
//		fs.delete(new Path("/hdfsmkdir"),true);
//		//关闭资源
//		fs.close();
//	}
	
	/**
	 * 查看hdfs指定目录下的信息
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void listFiles() throws IllegalArgumentException, IOException {
		
		//调用展示文件夹下信息的方法
		RemoteIterator<LocatedFileStatus> iter = fs.listFiles(new Path("/"), true);
		//循环遍历
		while(iter.hasNext()) {
			//获取数据信息
			LocatedFileStatus status = iter.next();
			
			System.out.println("文件的路径：" + status.getPath().toString());
			System.out.println("副本数：" + status.getReplication());
			System.out.println("块大小：" + status.getBlockSize());
			System.out.println("文件长度：" + status.getLen());
			System.out.println("块信息" + Arrays.toString(status.getBlockLocations()));
			
			System.out.println("--------------------------------------------");
		}
		
		//关闭资源
		fs.close();
	}
	
	/**
	 * 判断是文件还是文件夹
	 * @throws FileNotFoundException
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void isFile() throws FileNotFoundException, IllegalArgumentException, IOException {
		//获取状态信息
		FileStatus[] Status = fs.listStatus(new Path("/"));
		//遍历状态信息
		for (FileStatus ls : Status) {
			if (ls.isFile()) {
				System.out.println("文件---f---"+ls.getPath().getName());
			}else {
				System.out.println("文件夹---d---"+ls.getPath().getName());
			}
		}
		
	}
}
