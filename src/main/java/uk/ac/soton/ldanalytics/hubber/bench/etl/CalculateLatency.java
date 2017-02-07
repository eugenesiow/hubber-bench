package uk.ac.soton.ldanalytics.hubber.bench.etl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

public class CalculateLatency {

	public static void main(String[] args) {
		String interval = "5s";
		String folderPath = "/Users/eugene/Desktop/results_swot_"+interval;
		String outputPath = "/Users/eugene/Desktop/results_swot";
		File folder = new File(folderPath);
		try {
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
					total += Integer.parseInt(line);
					count++;
				}
				double average = total*1.0/count;
				bw.append(clientId + "\t" + average + "\n");
				br.close();
				sumOfAverages += average;
				totalCount++;
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
