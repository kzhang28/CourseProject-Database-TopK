
package main;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;

public class ReadCSV {

	private int delNonnumberFlag = 0;
	private String nextStr = "";
	private ArrayList<ArrayList<Float>> tableData = new ArrayList<ArrayList<Float>>();

	public ArrayList<ArrayList<Float>> getTable(String fname) throws FileNotFoundException {
		{
			Scanner scanCSV = new Scanner(new File(fname));
			scanCSV.useDelimiter(",");

			while (scanCSV.hasNextLine()) {
				ArrayList<Float> rowDataTmp = new ArrayList<Float>();

				if (delNonnumberFlag == 0) {
					delNonnumberFlag = 1;
					//System.out.println("Delete the non-number elements.");
					String delFirstRow = "";
					delFirstRow = scanCSV.nextLine();
					continue;
				}

				String strTmp = scanCSV.nextLine() + ",";
				nextStr = "";

				for (int i = 0; i < strTmp.length(); i++) {
					if (strTmp.charAt(i) == ',') {
						rowDataTmp.add(Float.valueOf(nextStr));
						nextStr = "";
					} else {
						nextStr += strTmp.charAt(i);
					}
				}

				tableData.add(rowDataTmp);
			}

			scanCSV.close();
			return tableData;
		}

	}

}
