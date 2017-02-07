package uk.ac.soton.ldanalytics.hubber.bench.etl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;

public class GraphETL {
	
	private static Map<Integer,Integer> index = new HashMap<Integer,Integer>(); 

	public static void main(String[] args) {
		try {
			int previousSrc = -1;
			BufferedReader br = new BufferedReader(new FileReader("graphs/ca-GrQc.txt"));
			BufferedWriter bw = new BufferedWriter(new FileWriter("graphs/ca-GrQc-p.txt"));
			String line = "";
			while((line = br.readLine())!=null) {
				if(!line.trim().startsWith("#")) {
					String[] split = line.trim().split("\t");
					int src = GetIndex(Integer.parseInt(split[0]));
					int dest = GetIndex(Integer.parseInt(split[1]));
					if(src==previousSrc)
						bw.append(","+dest);
					else 
						bw.append("\n"+src + "\t" + dest);
					previousSrc = src;
				}
			}
			
			br.close();
			bw.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static int GetIndex(int key) {
		Integer val = index.get(key);
		if(val==null) {
			val = index.size();
			index.put(key, val);
		}
		return val;
	}

}
