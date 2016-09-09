package ict.qi;

import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class CopyOfSequenceFileStudy {

	public static class UserWritable implements Writable, Comparable {
		private long userId;
		private String userName;
		private int userAge;

		public long getUserId() {
			return userId;
		}

		public void setUserId(long userId) {
			this.userId = userId;
		}

		public String getUserName() {
			return userName;
		}

		public void setUserName(String userName) {
			this.userName = userName;
		}

		public int getUserAge() {
			return userAge;
		}

		public void setUserAge(int userAge) {
			this.userAge = userAge;
		}

		public UserWritable(long userId, String userName, int userAge) {
			super();
			this.userId = userId;
			this.userName = userName;
			this.userAge = userAge;
		}

		public UserWritable() {
			super();
		}

		public void write(DataOutput out) throws IOException {
			out.writeLong(this.userId);
			out.writeUTF(this.userName);
			out.writeInt(this.userAge);
		}

		public void readFields(DataInput in) throws IOException {
			this.userId = in.readLong();
			this.userName = in.readUTF();
			this.userAge = in.readInt();
		}

		@Override
		public String toString() {
			return this.userId + "\t" + this.userName + "\t" + this.userAge;
		}

		/**
		 * 只对比userId
		 */
		@Override
		public boolean equals(Object obj) {
			if (obj instanceof UserWritable) {
				UserWritable u1 = (UserWritable) obj;
				return u1.getUserId() == this.getUserId();
			}
			return false;
		}

		/**
		 * 只对比userId
		 */
		public int compareTo(Object obj) {
			int result = -1;
			if (obj instanceof UserWritable) {
				UserWritable u1 = (UserWritable) obj;
				if (this.userId > u1.userId) {
					result = 1;
				} else if (this.userId == u1.userId) {
					result = 1;
				}
			}
			return result;
		}

		@Override
		public int hashCode() {
			return (int) this.userId & Integer.MAX_VALUE;
		}

	}

	/**
	 * 写入到sequence file
	 * 
	 * @param filePath
	 * @param conf
	 * @param datas
	 */
	public static void write2SequenceFile(String hdfsURI, String filePath,
			Configuration conf, Collection<UserWritable> datas) {
		FileSystem fs = null;
		SequenceFile.Writer writer = null;
		Path path = null;
		LongWritable idKey = new LongWritable(0);

		try {
			fs = FileSystem.get(URI.create(hdfsURI), conf);
			path = new Path(filePath);
			writer = SequenceFile.createWriter(fs, conf, path,
					LongWritable.class, UserWritable.class);

			for (UserWritable user : datas) {
				idKey.set(user.getUserId()); // userID为Key
				writer.append(idKey, user);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(writer);
		}
	}

	/**
	 * 从sequence file文件中读取数据
	 * 
	 * @param sequeceFilePath
	 * @param conf
	 * @return
	 */
	public static List<Text> readSequenceFile(String hdfsURI ,String sequeceFilePath,
			Configuration conf) {
		List<Text> result = null;
		FileSystem fs = null;
		SequenceFile.Reader reader = null;
		Path path = null;
		Writable key = null;
//		UserWritable value = new UserWritable();
		Text value = new Text();

		try {
//			fs = FileSystem.get(URI.create(hdfsURI),conf);
			fs = FileSystem.get(conf);
			result = new ArrayList<Text>();
			path = new Path(sequeceFilePath);
			reader = new SequenceFile.Reader(fs, path, conf);
			key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(),
					conf); // 获得Key，也就是之前写入的userId
			System.out.println("key--------------------->"+key.toString());
			while (reader.next(key, value)) {
				result.add(value);
				System.out.println("value--------------------->"+value);
				value = new Text();
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
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
//		conf.addResource("conf/core-site.xml");
//		conf.addResource("conf/hdfs-site.xml");
//		conf.addResource("conf/mapred-site.xml");
//		conf.addResource("conf/yarn-site.xml");
		
		
		
		conf.set("fs.defaultFS", "file:///");  
		// conf.set("io.compression.codecs",
		// "com.hadoop.compression.lzo.LzoCodec");
		return conf;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String hdfsURI = "hdfs://data7:9000";
//		String filePath = "/parsedata/sequence";
		
		String filePath ="/opt/qhl/downloaddata/part-00001"; //000000_0		sequence
		
		String writeFileName = "/root/Downloads/praseFile";
		Set<UserWritable> users = new HashSet<UserWritable>();
		UserWritable user = null;
		// 生成数据
		for (int i = 1; i <= 10; i++) {
			user = new UserWritable(i + (int) (Math.random() * 100000), "name-"
					+ (i + 1), (int) (Math.random() * 50) + 10);
			users.add(user);
		}
		// 写入到sequence file
//		write2SequenceFile(hdfsURI,filePath, getDefaultConf(), users);
		
		
		// 从sequence file中读取
		List<Text> readDatas = readSequenceFile(hdfsURI,filePath,
				getDefaultConf());

		// 对比数据是否正确并输出
		FileOutputStream fos =null;
		BufferedWriter bw=null;
		String str = "\r\n";
		int  count = 0;
		
		try {
			fos = new FileOutputStream(writeFileName);
			bw = new BufferedWriter(new OutputStreamWriter(fos));
		}catch(Exception e){
			e.printStackTrace();
		}
		
		
		try {
			for (Text writeStr : readDatas) {
				System.out.println("writeStr---------------->"+writeStr);
				bw.write(writeStr+str);
				count++;
				if(count>10000){
					break;
				}
			}
			bw.flush();
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			try {
				if(fos!=null){
					fos.close();
				}
				if(bw!=null){
					bw.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
	}
	
	
	
//	public static void write2File(String fileName,String writeStr){
//		FileOutputStream fos =null;
//		BufferedWriter bw=null;
//		String str = "\r\n";
//		try {
//			fos = new FileOutputStream(fileName);
//			bw = new BufferedWriter(new OutputStreamWriter(fos));
//			
//			bw.write(writeStr+str);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}finally{
//			try {
//				if(fos!=null){
//					fos.close();
//				}
//				if(bw!=null){
//					bw.close();
//				}
//			} catch (Exception e2) {
//				e2.printStackTrace();
//			}
//		}
//	}

}