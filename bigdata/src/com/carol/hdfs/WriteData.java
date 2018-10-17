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
 * @date 2018��10��17�� ����10:48:18 
 * @version v1.0
 * @Description д������
 */
public class WriteData {

	FileSystem fs = null ;
	@Before
	public void init() throws IOException, InterruptedException, URISyntaxException {
		//��ȡ�����ļ�
		Configuration conf = new Configuration();
		//��ȡ�ͻ���
		fs = FileSystem.get(new URI("hdfs://192.168.1.111:9000"), conf,"root");
	}
	/**
	 * ��HDFSд������
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void write() throws IllegalArgumentException, IOException {
		//����������
		FileInputStream fis = new FileInputStream(new File("e:/write.txt"));
		//���������
		FSDataOutputStream fos = fs.create(new Path("/write.txt"));
		//д������
		fos.write("hello hdfs".getBytes());
		
		//�ر���Դ
		fis.close();
		fos.close();
		fs.close();
	}
	/**
	 * ���ֽڶ�ȡ����HDFSд������
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void byteWrite() throws IllegalArgumentException, IOException {
		//����������
		FileInputStream fis = new FileInputStream(new File("e:/test.xls"));
		//���������
		FSDataOutputStream fos = fs.create(new Path("/testnew.xls"));
		
		byte[] by = new byte[1024];
		int read = 0;
		
		//ѭ��д��
		while((read = fis.read(by))!=-1) {
			fos.write(by, 0, read);
		}
		
		
		//�ر���Դ
		fis.close();
		fos.close();
		fs.close();
	}
	
}
