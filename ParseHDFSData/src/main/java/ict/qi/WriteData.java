package ict.qi;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class WriteData {
	static String [] contents = new String[]{
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		"cccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		"ddddddddddddddddddddddddddddddddddd",
		"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	};
	public static void main(String args[]){
		String file="hdfs://data12:9000/parsedata/write-data";
		Path path = new Path(file);
		Configuration conf = new Configuration();
		FileSystem fs =null;
		FSDataOutputStream output=null;
		try{
			fs = path.getFileSystem(conf);
			output = fs.create(path);
			for(String line: contents){
				output.write(line.getBytes("UTF-8"));
				output.flush();
			}
		}catch (IOException e){
			e.printStackTrace();
		}finally{
			try {
				output.close();
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
		
		
	}
}
