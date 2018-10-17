package com.carol.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @author Carol Tang
 * @date 2018��10��16�� ����1:43:13 
 * @version v1.0
 * @Description ʹ��hdfsApi��hdfs�ļ�ϵͳ�����ļ�������
 * 
 * ע�����
 * 		�� ����hadoop��������
 * 		�� hadoop��binĿ¼��ӱ���õ�winutil��
 */
public class CopyToLocalFile {
	public static void main(String[] args) {

		//1.���������ļ�
		Configuration conf = new Configuration();
		
		//2.�����ļ�ϵͳ
		FileSystem fs = null;
		try {
			fs = FileSystem.get(new URI("hdfs://192.168.1.111:9000"),conf,"root");
			
			//3.��hdfs�ļ�ϵͳ�����ļ���windows����
			fs.copyToLocalFile(new Path("/helloitstart.txt"), new Path("F:/bigdata"));
			
			//4.�ر���Դ
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
