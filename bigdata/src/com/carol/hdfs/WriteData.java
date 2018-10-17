package com.carol.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Carol Tang
 * @date 2018年10月17日 上午10:48:18 
 * @version v1.0
 * @Description 写入数据
 */
public class WriteData {

	FileSystem fs = null ;
	@Before
	public void init() throws IOException, InterruptedException, URISyntaxException {
		//获取配置文件
		Configuration conf = new Configuration();
		//获取客户端
		fs = FileSystem.get(new URI("hdfs://192.168.1.111:9000"), conf,"root");
	}
	/**
	 * 向HDFS写入数据
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void write() throws IllegalArgumentException, IOException {
		//创建输入流
		FileInputStream fis = new FileInputStream(new File("e:/write.txt"));
		//创建输出流
		FSDataOutputStream fos = fs.create(new Path("/write.txt"));
		//写入数据
		fos.write("hello hdfs".getBytes());
		
		//关闭资源
		fis.close();
		fos.close();
		fs.close();
	}
	/**
	 * 按字节读取后向HDFS写入数据
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void byteWrite() throws IllegalArgumentException, IOException {
		//创建输入流
		FileInputStream fis = new FileInputStream(new File("e:/test.xls"));
		//创建输出流
		FSDataOutputStream fos = fs.create(new Path("/testnew.xls"));
		
		byte[] by = new byte[1024];
		int read = 0;
		
		//循环写入
		while((read = fis.read(by))!=-1) {
			fos.write(by, 0, read);
		}
		
		
		//关闭资源
		fis.close();
		fos.close();
		fs.close();
	}
	
}
