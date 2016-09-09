package ict.qi;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class CopyOfParseIZPData {

	static String filePath = "/opt/qhl/downloaddata/part-00000";
	static String writeFileName = "/opt/qhl/downloaddata/praseFile/out";
	static FileOutputStream fos = null;
	static BufferedWriter bw = null;
	static String str = "\r\n";

	/**
	 * 从sequence file文件中读取数据
	 * 
	 * @param sequeceFilePath
	 * @param conf
	 * @return
	 */
	public static List<String> readSequenceFile(String sequeceFilePath,
			Configuration conf) {
		List<String> result = null;
		FileSystem fs = null;
		SequenceFile.Reader reader = null;
		Path path = null;
		Text key = new Text();
		Text value = new Text();

		try {
			fos = new FileOutputStream(writeFileName);
			bw = new BufferedWriter(new OutputStreamWriter(fos));
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			fs = FileSystem.get(conf);
			result = new ArrayList<String>();
			path = new Path(sequeceFilePath);
			reader = new SequenceFile.Reader(fs, path, conf);
			long posion = reader.getPosition();
			int count = 0;
			while (reader.next(key, value)) {
				result.add(key + "\t" + value);
				String syncMark = reader.syncSeen() ? "*" : "";
//				System.out.printf("[%s\t%s]\t%s\t%s\n", posion, syncMark, key,
//						value);
				key = new Text();
				value = new Text();
				posion = reader.getPosition();
				if (result.size() > 1000) {
					write2File(result);
					result.clear();
					count+=1000;
					System.out.println("total count----------->"+count);
				}
			}
			if(result.size()>0){
				write2File(result);
				result.clear();
				count+=result.size();
				System.out.println("total count----------->"+count);
			}

		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(reader);
		}
		return result;
	}

	private static Configuration getDefaultConf() {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "file:///");
		conf.set("mapred.job.tracker", "local");
		return conf;
	}

	public static void write2File(List<String> readDatas) {
		try {
			for (String writeStr : readDatas) {
				bw.write(writeStr + str);
			}
			bw.flush();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void closeStream() {
		try {
			if (fos != null) {
				fos.close();
			}
			if (bw != null) {
				bw.close();
			}
		} catch (Exception e2) {
			e2.printStackTrace();
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		readSequenceFile(filePath, getDefaultConf());

	}

}