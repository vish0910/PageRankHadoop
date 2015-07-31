import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

public class PageRankInputGenerator {

	static double precision = 0.1;
	public static void main(String[] args) {
		// TODO Auto-generated method 
		generateGraphFile();
	}
	
	//Random number generator
	static int random(){
		int r = (Math.random() < precision)?1:0; 
		return r;
	}
	
	public static void generateGraphFile(){
		String nodeInfo;
		String s= "";
		int n=100; // Number of nodes
		System.out.println("Graph File Generator: ");
		System.out.println("Enter number of nodes in the graph\nDefault Size is 100 (Press return to continue)");
		try{
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			s= br.readLine();
			if(!s.isEmpty())
				n=Integer.parseInt(s);
			String path = System.getProperty("user.dir");
			path = path.replace("jars", "");
			PrintWriter pr = new PrintWriter(path+"/pageRankInput.txt","UTF-8");
			int[][] adjacencyList = new int[n][n];
			for(int i = 0; i<n; i++){
				for(int j=0; j<n; j++ ){
					adjacencyList[i][j] =random();
				}
			}
			

			for(int i=0; i< n ; i++){
				nodeInfo="";
				String nodeInfo1="";
				nodeInfo1+="<Page"+(i)+",0.5>\t";
				//Attaching Neighbors
				for( int j = 0;j<n;j++){
					if(adjacencyList[i][j]== 1){
						nodeInfo+="Page"+(j)+",";
					}
				}
					if(!nodeInfo.isEmpty())
						nodeInfo1+= nodeInfo.substring(0, (nodeInfo.length()-1)); 
					else
						nodeInfo1+= nodeInfo;
				

				System.out.println(nodeInfo1);
				pr.println(nodeInfo1);
			}
		
			pr.close(); // Closing output file
		}//Try
		catch(FileNotFoundException fe){
			System.out.println("File not found.");
		}
		catch(IOException ie){
			System.out.println("IO Exception");
		}
	}//end of method
}


