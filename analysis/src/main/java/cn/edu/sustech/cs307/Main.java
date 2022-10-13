package cn.edu.sustech.cs307;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import cn.edu.sustech.cs307.datamanager.DataManager;
import cn.edu.sustech.cs307.datamanager.DataRecord;
import cn.edu.sustech.cs307.datamanager.DataRecord.RecordAttribute;
import cn.edu.sustech.cs307.datamanager.SimpleDataManager;
import cn.edu.sustech.cs307.utils.PostgreSQLConnector;

public class Main {
	
	private static PostgreSQLConnector connector;
	
	public static void main(String args[]) throws SQLException, IOException {
		
		connector = new PostgreSQLConnector("localhost", 5432, "cslab1", "test", "123456");
		//connector.connect();
		
		File file = new File("G:\\data.csv");
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
		String titles[] = reader.readLine().substring(1).split(","); //Read Title Line
		String line = null;
		List<DataRecord> records = new ArrayList<>();
		while ((line = reader.readLine()) != null) {
			String[] datas = line.split(",");
			DataRecord record = new DataRecord();
			int i = 0;
			for (String title : titles) {
				RecordAttribute attribute = RecordAttribute.valueOf(title.replace(" ", "_").toUpperCase());
				record.putValue(attribute, datas[i++]);
			}
		}
		
		
		DataManager dbManager = new SimpleDataManager(connector);
		dbManager.init(records);
	}
	
	public static PostgreSQLConnector getSQLConnector() {
		return connector;
	}
	
}
