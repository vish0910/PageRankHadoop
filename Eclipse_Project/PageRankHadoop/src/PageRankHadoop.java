import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * @author Vishal Doshi
 *
 */
public class PageRankHadoop extends Configured implements Tool {
	public static int iterationCount = 0;
	public static boolean shouldContinue = true;
	
	public int preProcessForDeadEndNodes(String args[], Configuration conf) throws Exception{
		int count=0;
		FileSystem fs = FileSystem.get(conf);
		Path graphFile = new Path(args[0]);
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(graphFile)));
		String line = br.readLine();
		while(line!=null){
			count++;
			line = br.readLine();
		}
		System.out.println("Number of Pages: "+count);
		br.close();
		//creating list of all pages in  a string
		String allPages = "";
		for(int i = 0; i<count ; i++){
			allPages+="Page"+i+",";
		}
	
		//Re-opening the file to process the dead end pages
		br = new BufferedReader(new InputStreamReader(fs.open(graphFile)));
		Path newGraphFile = new Path("pageRankInput/input_"+iterationCount+".txt");
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(newGraphFile,true)));
		
		line = br.readLine();
		while(line!=null){
			if(line.split("\t").length == 1){
				bw.write(line+allPages.substring(0, allPages.length()-1));
			}
			else if(line.split("\t")[0].split(",")[0].equals("<"+line.split("\t")[1])){
				bw.write(line.split("\t")[0]+"\t"+allPages.substring(0, allPages.length()-1));
			}
			else{
				bw.write(line);
			}
			line = br.readLine();
			bw.write("\n");
		}
		bw.close();
		br.close();
		return count;
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if(args.length!=2){
			System.out.println("Invalid Parameters\nEnter Input file path and beta value");
			return -1;
		}
		Configuration conf = getConf(); // THIS IS THE CORRECTWAY
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
		conf.set("beta", args[1]);
		int countN= preProcessForDeadEndNodes(args, conf);
		conf.set("numberOfNodes", countN+"");
		while(shouldContinue){
			String ip, op;
			Job job = new Job(conf,"Page Rank"+iterationCount);
			
			//Driver Class
			job.setJarByClass(PageRankHadoop.class);
			
			//Setting Mapper and Reducer Class
			job.setMapperClass(PageRankMapper.class);
			job.setReducerClass(PageRankReducer.class);
			
			//Setting Mapper Key and Value output type
	        job.setMapOutputKeyClass(Text.class);
	        job.setMapOutputValueClass(Text.class);
	        
	        //Setting final Key and Value Type
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
	        
	        job.setInputFormatClass(KeyValueTextInputFormat.class);
	        
	        //Set number of Reducers for job1
			job.setNumReduceTasks(4);
	        
	        //Creating file-paths

	        ip="pageRankInput/input_"+iterationCount+".txt";

			op = "pageRankOutput/out"+(iterationCount+1);
	        
	        //Setting the paths
	        FileInputFormat.setInputPaths(job, new Path(ip));
	        FileOutputFormat.setOutputPath(job, new Path(op));
	        
	        //Running the job.
	        job.waitForCompletion(true);
	        
	        //Getting the value of counter.

	        iterationCount++;
	        
	        int i =shouldNextIteration(conf);
	        System.out.println(""+i);
//	   
//			
		} //End of While
		return 0;
	}

	int shouldNextIteration(Configuration conf) throws IOException{
		double newVal=0.0;
		double oldVal=0.0;

		//Merging output from reducer
		FileSystem fs = FileSystem.get(conf);
		Path reducerOutput = new Path("pageRankOutput/out"+iterationCount); // FOR JAR
		Path pageRankInputOld = new Path("pageRankInput/input_"+(iterationCount-1)+".txt");
		Path pageRankInputNew = new Path("pageRankInput/input_"+iterationCount+".txt");
		FileUtil.copyMerge(fs,reducerOutput,fs,pageRankInputNew,false,conf,null);
		    
		//Reading new input file
		String line;
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pageRankInputNew)));
			
		line= br.readLine();
		while(line!=null){
			newVal+=Double.parseDouble(line.split(">")[0].split(",")[1]);	
			line=br.readLine();
		}
		br.close();
			
		//Reading old input file
		line="";    
		br = new BufferedReader(new InputStreamReader(fs.open(pageRankInputOld)));
		
		line= br.readLine();
		
		while(line!=null){
			oldVal+=Double.parseDouble(line.split(">")[0].split(",")[1]);	
			line=br.readLine();
		}
		br.close();
		
		System.out.println(oldVal+"==>"+newVal);
		if(Math.abs(oldVal-newVal) < 0.05){
			shouldContinue =false;
		}
		return 1;
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		 int res = ToolRunner.run(new Configuration(), new PageRankHadoop(), args);
		 System.exit(res);
	}

}
