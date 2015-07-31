import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Vishal Doshi
 *
 */
public class PageRankMapper extends Mapper<Text, Text, Text, Text> {

	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		String keyStr = key.toString();
		String valStr = value.toString();
		
		String[] pageNumRank = keyStr.split(",");
		String pageNumber = pageNumRank[0].substring(1);
		double rank = Double.parseDouble(pageNumRank[1].substring(0,pageNumRank[1].length()-1));
		
		String[] linksTo = valStr.split(",");
		double newRank = rank/linksTo.length;
		for(String s : linksTo){
			context.write(new Text(s), new Text(pageNumber+ ","+newRank));
		}
		//prefixing outlinks of Page K with character "o"
		context.write(new Text(pageNumber), new Text("o"+valStr));
		
	}
}
