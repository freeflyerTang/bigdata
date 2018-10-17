package com.carol.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @author Carol Tang
 * @date 2018��10��16�� ����10:43:01 
 * @version v1.0
 * @Description ʹ��hdfsApi�ϴ������ļ���hdfs�ļ�ϵͳ
 */
public class CopyFromLocalFile {

	public static void main(String[] args) {
		//1.�ͻ��˼��������ļ�
		Configuration conf = new Configuration();
		//2.ָ������(���ó�2��������)
		conf.set("dfs.replication", "2");
		//3.ָ�����С
		conf.set("dfs.blocksize", "64m");
		//4.�����ļ�ϵͳ
		FileSystem fs = null;
		try {
			 fs = FileSystem.get(new URI("hdfs://192.168.1.111:9000/"), conf,"root");
			
			 //5.�ϴ��ļ�
			 fs.copyFromLocalFile(new Path("F:\\bigdata\\Ȩ�޵Ĺ���.png"),new Path("/"));
			 
			 //6.�ر���Դ
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
