package com.carol.hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Carol Tang
 * @date 2018年10月16日 下午5:21:25 
 * @version v1.0
 * @Description 读取数据
 */
public class ReadData {
	FileSystem fs = null ;
	@Before
	public void init() throws IOException, InterruptedException, URISyntaxException {
		//加载配置
		Configuration conf = new Configuration();
		//获取文件系统
		fs = FileSystem.get(new URI("hdfs://192.168.1.111:9000"), conf,"root");
	}
	
	/**
	 * 字节方式读取
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void byteRead() throws IllegalArgumentException, IOException {
		//获取文件流
		FSDataInputStream is = fs.open(new Path("/hdfsmkdir/merge.txt"));
		
		byte[] by = new byte[1024];
		is.read(by);
		System.out.println(new String(by));
		
		is.close();
		fs.close();
	}
	
	/**
	 * BufferedReader缓冲流方式读取（性能高）
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void bufferRead() throws IllegalArgumentException, IOException {
		//获取文件流
		FSDataInputStream is = fs.open(new Path("/hdfsmkdir/merge.txt"));
		
		//缓冲流
		BufferedReader br = new BufferedReader(new InputStreamReader(is,"UTF-8"));
		//按行读取
		String line = null;
		//读数据
		while ((line = br.readLine()) != null) {
			System.out.println(line);
		}
		br.close();
		is.close();
		fs.close();
	}
	/**
	 * 指定偏移量读取
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void RandomRead() throws IllegalArgumentException, IOException {
		//获取文件流
		FSDataInputStream is = fs.open(new Path("/hdfsmkdir/merge.txt"));
		//设置偏移量
		is.seek(4);
		//设置按字节读取
		byte[] by = new byte[5];
		is.read(by);
		System.out.println(new String(by));
		is.close();
		fs.close();
	}
}
