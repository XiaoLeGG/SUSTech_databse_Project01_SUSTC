package cn.edu.sustech.cs307;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import cn.edu.sustech.cs307.datamanager.DataManager;
import cn.edu.sustech.cs307.datamanager.DataRecord;
import cn.edu.sustech.cs307.datamanager.DataRecord.RecordAttribute;
import cn.edu.sustech.cs307.sqlconnector.PostgreSQLConnector;
import cn.edu.sustech.cs307.datamanager.FastDataManager;
import cn.edu.sustech.cs307.datamanager.SimpleDataManager;

public class Main {
	
	private static PostgreSQLConnector connector;
	private static PrintWriter writer;
	private static Properties ppty;
	
	
	public static void debug(String raw, boolean isWarnning) {
		Calendar c = Calendar.getInstance();
		String msg = String.format("[" + (!isWarnning ? "INFO" : "WARN") + "][%d/%d/%d %02d:%02d:%02d] %s", c.get(Calendar.YEAR), c.get(Calendar.MONTH), c.get(Calendar.DATE), c.get(Calendar.HOUR), c.get(Calendar.MINUTE), c.get(Calendar.SECOND), raw);
		writer.println(msg);
		writer.flush();
	}
	
	public static String getProperty(String key) {
		return ppty.getProperty(key);
	}
	
	private static void loadConfig() throws IOException {
		
		HashMap<String, String> pmap = new HashMap<>();
		pmap.put("postgresql-host", "localhost");
		pmap.put("postgresql-port", "5432");
		pmap.put("postgresql-user", "postgres");
		pmap.put("postgresql-password", "123456");
		pmap.put("postgresql-database", "database");
		pmap.put("data-file-path", "G:\\data.csv");
		pmap.put("table-maker-file-path", "G:\\table_maker.sql");
		pmap.put("file-storage-directory", "G:\\CS307Project1");
		
		
		ppty = new Properties();
		File configFile = new File("config.properties");
		if (!configFile.exists()) {
			configFile.createNewFile();
		}
		InputStream configInput = new FileInputStream(configFile);
		ppty.load(configInput);
		
		for (Map.Entry<String, String> entry : pmap.entrySet()) {
			if (ppty.getProperty(entry.getKey()) == null) {
				ppty.setProperty(entry.getKey(), entry.getValue());
			}
		}
		OutputStream configOutput = new FileOutputStream(configFile);
		ppty.store(configOutput, "Course CS307 DBMS Project by Lun Tianle and Luo Jiacheng");
		
	}
	
	public static void main(String args[]) throws SQLException, IOException {
		writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(System.out)));
		
		loadConfig();
		
		String host = getProperty("postgresql-host");
		int port = Integer.parseInt(getProperty("postgresql-port"));
		String user = getProperty("postgresql-user");
		String password = getProperty("postgresql-password");
		String database = getProperty("postgresql-database");
		
		connector = new PostgreSQLConnector(host, port, database, user, password);
		if (connector.connect()) {
			debug("Database Connected", false);
		}
		
		File file = new File(getProperty("data-file-path"));
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
					if (attribute.name().contains("PHONE_NUMBER")) {
						datas[i - 1] = datas[i - 1].split("-")[1];
					}
					record.putValue(attribute, datas[i - 1]);
				}
			}
			records.add(record);
			
			
			++total;
			if (total % 10000 == 0) {
				debug("Successfully load " + total + " items from csv file...", false);
			}
		}
		
		debug("Successfully load all items from csv file, " + total + " in total...", false);
		
		DataManager dbManager = new FastDataManager(connector);
		dbManager.init(records);
	}
	
	public static PostgreSQLConnector getSQLConnector() {
		return connector;
	}
	
}
