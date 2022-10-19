package cn.edu.sustech.cs307;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import cn.edu.sustech.cs307.datamanager.DataManager;
import cn.edu.sustech.cs307.datamanager.DataRecord;
import cn.edu.sustech.cs307.datamanager.DataRecord.RecordAttribute;
import cn.edu.sustech.cs307.datamanager.SimpleDataManager;
import cn.edu.sustech.cs307.utils.PostgreSQLConnector;

public class Main {
	
	private static PostgreSQLConnector connector;
	private static PrintWriter writer;
	
	public static void debug(String raw, boolean isWarnning) {
		Calendar c = Calendar.getInstance();
		String msg = String.format("[" + (!isWarnning ? "INFO" : "WARN") + "][%d/%d/%d %02d:%02d:%02d] %s", c.get(Calendar.YEAR), c.get(Calendar.MONTH), c.get(Calendar.DATE), c.get(Calendar.HOUR), c.get(Calendar.MINUTE), c.get(Calendar.SECOND), raw);
		writer.println(msg);
		writer.flush();
	}
	
	public static void main(String args[]) throws SQLException, IOException {
		writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(System.out)));
		connector = new PostgreSQLConnector("localhost", 5432, "cslab1", "test", "123456");
		if (connector.connect()) {
			debug("Database Connected", false);
		}
		
		File file = new File("G:\\data.csv");
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
		String titles[] = reader.readLine().substring(1).split(","); //Read Title Line
		String line = null;
		
		List<DataRecord> records = new ArrayList<>();
		int total = 0;
		while ((line = reader.readLine()) != null) {
			String[] datas = line.split(",");
			DataRecord record = new DataRecord();
			int i = 0;
			for (String title : titles) {
				RecordAttribute attribute = RecordAttribute.valueOf(title.replace(" ", "_").toUpperCase());
				if (!datas[i++].isEmpty()) {					
					record.putValue(attribute, datas[i - 1]);
				}
			}
			records.add(record);
			++total;
			if (total % 100 == 0) {
				debug("Successfully load " + total + " items from csv file...", false);
			}
		}
		
		debug("Successfully load all items from csv file, " + total + " in total...", false);
		
		DataManager dbManager = new SimpleDataManager(connector);
		dbManager.init(records);
	}
	
	public static PostgreSQLConnector getSQLConnector() {
		return connector;
	}
	
}
