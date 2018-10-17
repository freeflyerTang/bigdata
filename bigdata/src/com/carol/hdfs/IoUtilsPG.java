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
 * @date 2018��10��17�� ����10:49:40 
 * @version v1.0
 * @Description ʹ��IOUtils��ʽ�ϴ�����HDFS�ļ�
 */
public class IoUtilsPG {
	
	Configuration conf = null;
	FileSystem fs = null;
	
	@Before
	public void init() throws IOException, InterruptedException, URISyntaxException {
		//��ȡ�����ļ�
		conf = new Configuration();
		//�����ͻ���
		fs = FileSystem.get(new URI("hdfs://192.168.1.111:9000"),conf,"root");
	}
	/**
	 * �������ļ��ϴ���HDFS
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void putFileToHdfs() throws IllegalArgumentException, IOException{
		//����������
		FileInputStream fis = new FileInputStream(new File("E:\\test.xls"));
		//���������
		FSDataOutputStream fos = fs.create(new Path("/test.xls"));
		//���ĶԿ�
		IOUtils.copyBytes(fis, fos, conf);
		//�ر���Դ
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fs);
	}
	
	/**
	 * ��HDFS�����ļ�������
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void getFileFromHdfs() throws IllegalArgumentException, IOException{
		//����������
		FSDataInputStream fis = fs.open(new Path("/hdfstest.txt"));
		//���������
		FileOutputStream fos = new FileOutputStream(new File("e:/hdfstest.txt"));
		//���ĶԿ�
		IOUtils.copyBytes(fis, fos, conf);
		//�ر���Դ
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fs);
	}

}
