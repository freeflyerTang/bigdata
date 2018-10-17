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
 * @date 2018��10��16�� ����2:52:15 
 * @version v1.0
 * @Description hdfsAPI������(ʹ��junit����)
 */
public class HdfsClientDemo {

	FileSystem fs = null;
	
	/**
	 * ��ʼ����ʼ������hdfs�ͻ�����Ϣ
	 * @param
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 */
	@Before
	public void init() throws IOException, InterruptedException, URISyntaxException {
		//���������ļ�
		Configuration conf = new Configuration();
		//���ø�����
		conf.set("dfs.replication", "2");
		//���ÿ��С
		conf.set("dfs.blocksize", "64m");
		//��ȡ�ļ�ϵͳ
		fs = FileSystem.get(new URI("hdfs://192.168.1.111:9000"), conf, "root");
		
	}
	/**
	 * ��hdfs�д����ļ���
	 * 	  ---��Ӧ���ʽ��hdfs dfs -mkdir /�ļ���
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void mkdir() throws IllegalArgumentException, IOException{
		//���ô����ļ��еķ���
		fs.mkdirs(new Path("/hdfsmkdir"));
		
		//�ر���Դ
		fs.close();
	}
	
	/**
	 * ��hdfs���ƶ�/�޸��ļ���
	 * 	  ---��Ӧ���ʽ��hdfs dfs -mv /hdfs·�� /hdfs·��
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void rename() throws IllegalArgumentException, IOException {
		//�����޸��ļ����ķ���(�ƶ����޸�)
		fs.rename(new Path("/hdfs-site.xml"), new Path("/hdfsmkdir/hdfs-site-new.xml"));
		//�ر���Դ
		fs.close();
	}
	
	/**
	 * ɾ��hdfs�е��ļ�
	 * 	 ---��Ӧ���ʽ��hdfs dfs rm /�ļ���
	 * 				 hdfs dfs rm -r /�ļ����� 
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
//	@Test
//	public void delete() throws IllegalArgumentException, IOException {
//		
//		//fs.delete(new Path("/helloitstart.txt")); ɾ���ļ����÷����ѹ�ʱ��������
//		//fs.delete(new Path("/caroltang"));ɾ���ļ��У��÷����ѹ�ʱ��������
//		
//		//����ɾ���ļ��ķ���(����1��ɾ����·��  ���������Ƿ�ݹ�ɾ��)
//		//�÷���ɾ����hdfsmkdir�ļ����µ����ݣ���δɾ�����ļ���
//		fs.delete(new Path("/hdfsmkdir"),true);
//		//�ر���Դ
//		fs.close();
//	}
	
	/**
	 * �鿴hdfsָ��Ŀ¼�µ���Ϣ
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void listFiles() throws IllegalArgumentException, IOException {
		
		//����չʾ�ļ�������Ϣ�ķ���
		RemoteIterator<LocatedFileStatus> iter = fs.listFiles(new Path("/"), true);
		//ѭ������
		while(iter.hasNext()) {
			//��ȡ������Ϣ
			LocatedFileStatus status = iter.next();
			
			System.out.println("�ļ���·����" + status.getPath().toString());
			System.out.println("��������" + status.getReplication());
			System.out.println("���С��" + status.getBlockSize());
			System.out.println("�ļ����ȣ�" + status.getLen());
			System.out.println("����Ϣ" + Arrays.toString(status.getBlockLocations()));
			
			System.out.println("--------------------------------------------");
		}
		
		//�ر���Դ
		fs.close();
	}
	
	/**
	 * �ж����ļ������ļ���
	 * @throws FileNotFoundException
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void isFile() throws FileNotFoundException, IllegalArgumentException, IOException {
		//��ȡ״̬��Ϣ
		FileStatus[] Status = fs.listStatus(new Path("/"));
		//����״̬��Ϣ
		for (FileStatus ls : Status) {
			if (ls.isFile()) {
				System.out.println("�ļ�---f---"+ls.getPath().getName());
			}else {
				System.out.println("�ļ���---d---"+ls.getPath().getName());
			}
		}
		
	}
}
