package uk.ac.soton.ldanalytics.hubber.bench.etl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

public class GraphToJson {
	public static void main(String[] args) {
		try {
			BufferedReader br = new BufferedReader(new FileReader("graphs/ca-GrQc-ps.txt"));
			BufferedWriter bw = new BufferedWriter(new FileWriter("graphs/ca-GrQc-ps.json"));
			bw.append("{\n");
			String line = "";
			while((line = br.readLine())!=null) {
				String[] split = line.trim().split("\t");
				bw.append("\t\""+split[0]+"\":["+split[1]+"],\n");
			}
			bw.append("}");
			br.close();
			bw.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
