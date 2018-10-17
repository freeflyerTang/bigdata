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
 * @date 2018��10��16�� ����5:21:25 
 * @version v1.0
 * @Description ��ȡ����
 */
public class ReadData {
	FileSystem fs = null ;
	@Before
	public void init() throws IOException, InterruptedException, URISyntaxException {
		//��������
		Configuration conf = new Configuration();
		//��ȡ�ļ�ϵͳ
		fs = FileSystem.get(new URI("hdfs://192.168.1.111:9000"), conf,"root");
	}
	
	/**
	 * �ֽڷ�ʽ��ȡ
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void byteRead() throws IllegalArgumentException, IOException {
		//��ȡ�ļ���
		FSDataInputStream is = fs.open(new Path("/hdfsmkdir/merge.txt"));
		
		byte[] by = new byte[1024];
		is.read(by);
		System.out.println(new String(by));
		
		is.close();
		fs.close();
	}
	
	/**
	 * BufferedReader��������ʽ��ȡ�����ܸߣ�
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void bufferRead() throws IllegalArgumentException, IOException {
		//��ȡ�ļ���
		FSDataInputStream is = fs.open(new Path("/hdfsmkdir/merge.txt"));
		
		//������
		BufferedReader br = new BufferedReader(new InputStreamReader(is,"UTF-8"));
		//���ж�ȡ
		String line = null;
		//������
		while ((line = br.readLine()) != null) {
			System.out.println(line);
		}
		br.close();
		is.close();
		fs.close();
	}
	/**
	 * ָ��ƫ������ȡ
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void RandomRead() throws IllegalArgumentException, IOException {
		//��ȡ�ļ���
		FSDataInputStream is = fs.open(new Path("/hdfsmkdir/merge.txt"));
		//����ƫ����
		is.seek(4);
		//���ð��ֽڶ�ȡ
		byte[] by = new byte[5];
		is.read(by);
		System.out.println(new String(by));
		is.close();
		fs.close();
	}
}
