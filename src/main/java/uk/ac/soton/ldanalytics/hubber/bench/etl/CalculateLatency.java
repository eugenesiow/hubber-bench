package uk.ac.soton.ldanalytics.hubber.bench.etl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;

public class CalculateLatency {

	public static void main(String[] args) {
		String interval = "10s_wiki";
		String folderPath = "/Users/eugene/Desktop/results_swot_"+interval;
		String outputPath = "/Users/eugene/Desktop/results_swot";
		File folder = new File(folderPath);
		try {
			Map<Integer,Integer> relMap = new HashMap<Integer,Integer>();
			BufferedReader brRelations = new BufferedReader(new FileReader("graphs/wiki-Vote-ps.txt"));
			String rels="";
			while((rels=brRelations.readLine())!=null) {
				String[] parts = rels.split("\t");
				String[] relArr = parts[1].split(",");
				relMap.put(Integer.parseInt(parts[0]), relArr.length);
			}
			brRelations.close();
			
			BufferedWriter bw = new BufferedWriter(new FileWriter(outputPath + File.separator + interval + ".tsv"));
			int totalCount = 0;
			double sumOfAverages = 0;
			for(File file:folder.listFiles()) {
				String filename = file.getName();
				if(filename.startsWith(".") && !filename.endsWith(".log")) {
					continue;
				}
				String clientId = filename.replace(".log","");
				
				BufferedReader br = new BufferedReader(new FileReader(folderPath + File.separator + filename));
				long total = 0;
				int count = 0;
				String line="";
				while((line=br.readLine())!=null) {
					Integer val = Integer.parseInt(line);
					if(val==null)
						continue;
					else if(val<0) {
						val *= -1;
//						val = 0;
					}
					total += val;
					count++;
				}
				double average = total*1.0/count;
				if(total!=0) {
					bw.append(clientId + "\t" + relMap.get(Integer.parseInt(clientId)) + "\t" + average + "\n");
					sumOfAverages += average;
					totalCount++;
				}
				br.close();
			}
			double overallAvg = sumOfAverages/totalCount;
			bw.append("OverallAvg\t"+overallAvg);
			bw.close();
			System.out.println(overallAvg);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

}
