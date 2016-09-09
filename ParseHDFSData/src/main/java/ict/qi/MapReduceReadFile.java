package ict.qi;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient.Conf;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

public class MapReduceReadFile {

	private static SequenceFile.Reader reader = null;
	
	private static Configuration getDefaultConf() {
		Configuration conf = new Configuration();
		conf.addResource("conf/core-site.xml");
		conf.addResource("conf/hdfs-site.xml");
		conf.addResource("conf/mapred-site.xml");
		conf.addResource("conf/yarn-site.xml");
//		conf.set("fs.defaultFS", "file:///");
//		conf.set("mapred.job.tracker", "local");
		return conf;
	}

	public static class ReadFileMapper extends Mapper<Text, Text, Text, Text> {

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
		 * org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		public void map(Text key, Text value, Context context) {
			key = (Text) ReflectionUtils.newInstance(
					reader.getKeyClass(), getDefaultConf());
			value = (Text) ReflectionUtils.newInstance(reader.getValueClass(),
					getDefaultConf());
			try {
				System.out.println("reader.next(key)----------->"+reader.next(key));
			} catch (IOException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
			int count = 0;
			try {
				while (reader.next(key, value)) {
					System.out.printf("%s\t%s\n", key, value);
					context.write(key, value);
					count++;
					if(count>50){
						break;
					}
				}
			} catch (IOException e1) {
				e1.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		Job job = new Job(getDefaultConf(), "read seq file");
		job.setJarByClass(MapReduceReadFile.class);
		job.setMapperClass(ReadFileMapper.class);
		job.setMapOutputValueClass(Text.class);
		Path path = new Path("/opt/qhl/downloaddata/part-00001");
		FileSystem fs = FileSystem.get(getDefaultConf());
		reader = new SequenceFile.Reader(fs, path, getDefaultConf());
		FileInputFormat.addInputPath(job, path);
		FileOutputFormat.setOutputPath(job, new Path("/opt/qhl/downloaddata/praseFile"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}