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

public class ParseIZPData {

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
		Text key =  new Text();
		Text value = new Text();

		int count = 0;

		try {
			fs = FileSystem.get(conf);
			result = new ArrayList<String>();
			path = new Path(sequeceFilePath);
			reader = new SequenceFile.Reader(fs, path, conf);
			long posion  = reader.getPosition();
			while (reader.next(key, value)) {
				result.add(key+"\t"+value);
				String syncMark = reader.syncSeen()?"*":"";
				System.out.printf("[%s\t%s]\t%s\t%s\n",posion,syncMark,key,value);
				key = new Text();
				value = new Text();
				posion = reader.getPosition();
//				count++;
//				if (count > 100) {
//					break;
//				}
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

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String filePath = "/opt/qhl/downloaddata/part-00000";
		String writeFileName = "/opt/qhl/downloaddata/praseFile/out";
		// 从sequence file中读取
		List<String> readDatas = readSequenceFile(filePath, getDefaultConf());

		FileOutputStream fos = null;
		BufferedWriter bw = null;
		String str = "\r\n";
		try {
			fos = new FileOutputStream(writeFileName);
			bw = new BufferedWriter(new OutputStreamWriter(fos));
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			for (String writeStr : readDatas) {
//				System.out.printf("%s,%s\n",writeStr,"&&&&&&&&&&&&&&&&&&&&&&");
//				System.out
//						.println("writeStr---------------------->" + writeStr);
				bw.write(writeStr + str);
			}
			bw.flush();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
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

	}

}