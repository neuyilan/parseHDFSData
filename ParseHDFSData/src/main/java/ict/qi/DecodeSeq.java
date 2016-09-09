package ict.qi;

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.IOUtils;  
import org.apache.hadoop.io.compress.CompressionCodec;  
import org.apache.hadoop.io.compress.CompressionCodecFactory;  

import java.io.IOException;  
import java.io.InputStream;  
import java.io.OutputStream;  
import java.net.URI;  
/*
如果你想读取一个被压缩的文件的话，首先你得先通过扩展名判断该用哪种codec，当然有更简便得办法，CompressionCodecFactory已经帮你把这件事做了，
通过传入一个Path调用它得getCodec方法,即可获得相应得codec。
注意看下removeSuffix方法，这是一个静态方法，它可以将文件的后缀去掉，然后我们将这个路径做为解压的输出路径。
CompressionCodecFactory能找到的codec也是有限的，
默认只有三种org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.BZip2Codec,
org.apache.hadoop.io.compress.DefaultCodec,
如果想添加其他的codec你需要更改io.compression.codecs属性，并注册codec。
*/
public class DecodeSeq {  
public static void main(String[] args) throws Exception {  
	String uri = "/opt/qhl/downloaddata/part-00001";
	Configuration conf = new Configuration();  
	FileSystem fs = FileSystem.get(URI.create(uri), conf);  

	Path inputPath = new Path(uri);  
	CompressionCodecFactory factory = new CompressionCodecFactory(conf);  
	CompressionCodec codec = factory.getCodec(inputPath);  
	if (codec == null) {  
		System.out.println("No codec found:" + uri);  
		System.exit(1);  
	}  
//	String outputUri = CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());  
	String outputUri  ="/opt/qhl/downloaddata/out";
	InputStream in = null;  
	OutputStream out = null;  

	try {  
		in = codec.createInputStream(fs.open(inputPath));  
		out = fs.create(new Path(outputUri));  
		IOUtils.copyBytes(in,out,conf);  
	} finally {  
		IOUtils.closeStream(in);  
		IOUtils.closeStream(out);  
	}  
}  
}


