import java.io.IOException;
import java.text.DecimalFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * @author Vishal Doshi
 *
 */
public class PageRankReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		String listOfPages = "";
		DecimalFormat dec = new DecimalFormat("##.###");
		double rankk = 0.0;
		Configuration conf = context.getConfiguration();
		double beta = Double.parseDouble(conf.get("beta"));
		int n = Integer.parseInt(conf.get("numberOfNodes"));
		
		for(Text t: values){
			String str = t.toString();
			if(str.charAt(0)=='o'){
				listOfPages= str.substring(1);
			}
			else{
				String[] s = str.split(",");
				rankk+= (Double.parseDouble(s[1]))*beta;
				
			}
			
		}
		
		rankk+=(1-beta)/n;// n is total number of nodes
		context.write(new Text("<"+key+","+dec.format(rankk)+">"), new Text(listOfPages));
	}
}
