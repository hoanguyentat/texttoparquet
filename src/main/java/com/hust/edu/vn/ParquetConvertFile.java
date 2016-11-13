package com.hust.edu.vn;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.avro.AvroParquetOutputFormat;

@SuppressWarnings("deprecation")
public class ParquetConvertFile extends Configured implements Tool {
	
	private static final Schema SCHEMA = new Schema.Parser().parse(
			"{\n" +
					" 	\"type\": \"record\",\n" +
					" 	\"name\": \"Line\",\n" +
					" 	\"fields\": [\n" +
					"	{\"name\": \"timeCreate\", \"type\": \"string\"},\n" +
					"	{\"name\": \"cookieCreate\", \"type\": \"string\"},\n" +
					"	{\"name\": \"browserCode\", \"type\": \"int\"},\n" +
					"	{\"name\": \"browserVer\", \"type\": \"string\"},\n" +
					"	{\"name\": \"osCode\", \"type\": \"int\"},\n" +
					"	{\"name\": \"osVer\", \"type\": \"string\"},\n" +
					"	{\"name\": \"ip\", \"type\": \"long\"},\n" +
					"	{\"name\": \"locId\", \"type\": \"int\"},\n" +
					"	{\"name\": \"domain\", \"type\": \"string\"},\n" +
					"	{\"name\": \"siteId\", \"type\": \"int\"},\n" +
					"	{\"name\": \"cId\", \"type\": \"int\"},\n" +
					"	{\"name\": \"path\", \"type\": \"string\"},\n" +
					"	{\"name\": \"referer\", \"type\": \"string\"},\n" +
					"	{\"name\": \"guid\", \"type\": \"long\"},\n" +
					"	{\"name\": \"flashVersion\", \"type\": \"string\"},\n" +
					"	{\"name\": \"jre\", \"type\": \"string\"},\n" +
					"	{\"name\": \"sr\", \"type\": \"string\"},\n" +
					"	{\"name\": \"sc\", \"type\": \"string\"},\n" +
					"	{\"name\": \"geographic\", \"type\": \"int\"}\n" +
					" ]\n" +
					"}");
	
	public static class MapPQ extends Mapper<LongWritable, Text, Void, GenericRecord> {
		private String[] field = {"timeCreate",
		                          "cookieCreate",
		                          "browserCode",
		                          "browserVer",
		                          "osCode",
		                          "osVer",
		                          "ip",
		                          "locId",
		                          "domain",
		                          "siteId",
		                          "cId",
		                          "path" ,
		                          "referer",
		                          "guid",
		                          "flashVersion",
		                          "jre" ,
		                          "sr" ,
		                          "sc",
		                          "geographic"
		};
		private GenericRecord record = new GenericData.Record(SCHEMA);
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
//			StringTokenizer tokenizer = new StringTokenizer(line);
//			while(tokenizer.hasMoreTokens()){
//				String data = tokenizer.nextToken();
				System.out.println(line);
				String[] fields =  line.split("\t");
				System.out.println(fields.length);
				for(int i = 0; i < field.length; i++){
					try{
						if(i == 2 || i == 4 || i == 7 || i == 9 || i == 10 || i == 18){
							record.put(field[i], Integer.parseInt(fields[i]));
						} else if(i == 6 || i == 13){
							record.put(field[i], Long.parseLong(fields[i]));
						} else{
							record.put(field[i], fields[i]);
						}
					} catch(Exception e){
						System.out.println(e.toString());
					}
				}
//			}
			context.write(null, record);
		}
	}
//	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		
		conf.addResource(new Path("resource/config/core-site.xml"));
		conf.addResource(new Path("resource/config/hbase-site.xml"));
		conf.addResource(new Path("resource/config/hdfs-site.xml"));
		conf.addResource(new Path("resource/config/mapred-site.xml"));
		conf.addResource(new Path("resource/config/yarn-site.xml"));
		conf.set("fs.default.name", "hdfs://10.3.24.154:9000");
		
		Job job = new Job(conf, "parquet");
		job.setJarByClass(getClass());
		
		job.setMapperClass(MapPQ.class);
		job.setNumReduceTasks(0);
		
		job.setOutputFormatClass(AvroParquetOutputFormat.class);
		AvroParquetOutputFormat.setSchema(job, SCHEMA);
		
		job.setOutputKeyClass(Void.class);
		job.setOutputValueClass(Group.class);
		
//		String input = "/user/hoant/data/rawText";
//		String output = "/user/hoant/parquet/output";
		
		String input = "hdfs://10.3.24.154:9000/data/rawText";
		String output = "hdfs://10.3.24.154:9000/user/hoant/parquet/output";
		
		FileSystem fs = FileSystem.get(conf);
		
		if(fs.exists(new Path(output))){
			fs.delete(new Path(output), true);
		}
		
		FileInputFormat.addInputPath(job,new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job.waitForCompletion(true)? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new ParquetConvertFile(), args);
		System.exit(1);
	}
}
