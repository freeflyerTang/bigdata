package com.carol.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Carol Tang
 * @date 2018年10月17日 上午10:49:40 
 * @version v1.0
 * @Description 使用IOUtils方式上传下载HDFS文件
 */
public class IoUtilsPG {
	
	Configuration conf = null;
	FileSystem fs = null;
	
	@Before
	public void init() throws IOException, InterruptedException, URISyntaxException {
		//读取配置文件
		conf = new Configuration();
		//创建客户端
		fs = FileSystem.get(new URI("hdfs://192.168.1.111:9000"),conf,"root");
	}
	/**
	 * 将本地文件上传到HDFS
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void putFileToHdfs() throws IllegalArgumentException, IOException{
		//创建输入流
		FileInputStream fis = new FileInputStream(new File("E:\\test.xls"));
		//创建输出流
		FSDataOutputStream fos = fs.create(new Path("/test.xls"));
		//流的对拷
		IOUtils.copyBytes(fis, fos, conf);
		//关闭资源
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fs);
	}
	
	/**
	 * 从HDFS下载文件到本地
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void getFileFromHdfs() throws IllegalArgumentException, IOException{
		//创建输入流
		FSDataInputStream fis = fs.open(new Path("/hdfstest.txt"));
		//创建输出流
		FileOutputStream fos = new FileOutputStream(new File("e:/hdfstest.txt"));
		//流的对拷
		IOUtils.copyBytes(fis, fos, conf);
		//关闭资源
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fs);
	}

}
