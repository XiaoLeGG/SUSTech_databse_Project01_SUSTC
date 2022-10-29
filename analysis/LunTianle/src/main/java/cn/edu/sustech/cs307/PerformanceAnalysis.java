package cn.edu.sustech.cs307;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import cn.edu.sustech.cs307.datamanager.DataRecord;
import cn.edu.sustech.cs307.datamanager.FastDataManager;
import cn.edu.sustech.cs307.datamanager.FileDataManager;
import cn.edu.sustech.cs307.datamanager.MultiThreadDataManager;
import cn.edu.sustech.cs307.datamanager.SimpleDataManager;
import cn.edu.sustech.cs307.sqlconnector.MySQLConnector;
import cn.edu.sustech.cs307.sqlconnector.PostgreSQLConnector;
import cn.edu.sustech.cs307.sqlconnector.SQLConnector;
import cn.edu.sustech.cs307.sqlconnector.SQLUtils;

public class PerformanceAnalysis {
	
	protected static void initTest() throws IOException, SQLException {
		
		List<DataRecord> raw = Main.loadRecords(new File(Main.getProperty("data-file-path")));
		List<DataRecord> records = new ArrayList<>();
		records.addAll(raw.subList(0, 10));
		records.addAll(raw.subList(100000, 100010));
		records.addAll(raw.subList(200000, 200010));
		records.addAll(raw.subList(300000, 300010));
		records.addAll(raw.subList(400000, 400010));
		//PostgreSQL part
		
		PostgreSQLConnector psql = SQLUtils.newPostgreSQLConnector();
		psql.connect();
		psql.setAutoCommit(true);
		
		for (int i = 0; i < 3; ++i) {
			resetTable(psql);
			long cost = testTime(() -> {
				new SimpleDataManager(psql).init(records);
			});
			Main.log("PostgreSQL SimpleDataManager " + records.size() + " Records Init Cost #" + i + ": " + cost + "ms");
		}
		for (int i = 0; i < 3; ++i) {
			resetTable(psql);
			long cost = testTime(() -> {
				new FastDataManager(psql).init(records);
			});
			Main.log("PostgreSQL FastDataManager " + records.size() + " Records Init Cost #" + i + ": " + cost + "ms");
		}
		for (int i = 0; i < 3; ++i) {
			resetTable(psql);
			long cost = testTime(() -> {
				new MultiThreadDataManager(PostgreSQLConnector.class).init(records);
			});
			Main.log("PostgreSQL MutilThreadDataManager " + records.size() + " Records Init Cost #" + i + ": " + cost + "ms");
		}
		
		//MySQL part
		MySQLConnector msql = SQLUtils.newMySQLConnector();
		msql.connect();
		msql.setAutoCommit(true);
		
		for (int i = 0; i < 3; ++i) {
			resetTable(msql);
			long cost = testTime(() -> {
				new FastDataManager(msql).init(records);
			});
			Main.log("MySQL FastDataManager " + records.size() + " Records Init Cost #" + i + ": " + cost + "ms");
		}
		
		for (int i = 0; i < 3; ++i) {
			resetTable(msql);
			long cost = testTime(() -> {
				new MultiThreadDataManager(MySQLConnector.class).init(records);
			});
			Main.log("MySQL MutilThreadDataManager " + records.size() + " Records Init Cost #" + i + ": " + cost + "ms");
		}
		
		for (int i = 0; i < 3; ++i) {
			resetFile();
			long cost = testTime(() -> {
				new FileDataManager(new File(Main.getProperty("file-storage-directory"))).init(records);
			});
			Main.log("FileIO " + records.size() + " Records Init Cost #" + i +": " + cost + "ms");
		}
		
//		for (int i = 0; i < 3; ++i) {
//			resetTable(msql);
//			long cost = testTime(() -> {
//				new SimpleDataManager(msql).init(records);
//			});
//			Main.log("MySQL SimpleDataManager " + records.size() + " Records Init Cost #" + i + ": " + cost + "ms");
//		}
		
	}
	
	private static void deleteDirectory(File dir) {
		if (!dir.exists()) {
			return;
		}
		for (File file : dir.listFiles()) {
			if (file.isDirectory()) {
				deleteDirectory(file);
			} else {
				file.delete();
			}
		}
		dir.delete();
	}
	
	private static void resetFile() {
		File dir = new File(Main.getProperty("file-storage-directory"));
		deleteDirectory(dir);
	}
	
	protected static long testTime(Runnable runnable) {
		long start = System.currentTimeMillis();
		runnable.run();
		return System.currentTimeMillis() - start;
	}
	
	protected static void generateDatas() {
		
	}
	
	private static void executeSQLFile(SQLConnector connector, File file) throws SQLException, IOException {
		BufferedReader reader = new BufferedReader(new FileReader(file));
		StringBuilder builder = new StringBuilder();
		for (String line = reader.readLine(); line != null; line = reader.readLine()) {
			builder.append(line);
		}
		String[] statements = builder.toString().split(";");
		for (String statement : statements) {
			connector.prepareStatement(statement).execute();
		}
	}
	
	//Test code
	static void resetTable(SQLConnector connector) throws IOException, SQLException {
		File maker = new File(Main.getProperty("table-maker-file-path"));
		File dropper = new File(Main.getProperty("table-dropper-file-path"));
		executeSQLFile(connector, dropper);
		executeSQLFile(connector, maker);
		if (connector instanceof MySQLConnector) {
			connector.prepareStatement("ALTER TABLE container ENGINE = InnoDB").execute();
			connector.prepareStatement("ALTER TABLE courier ENGINE = InnoDB").execute();
			connector.prepareStatement("ALTER TABLE ship ENGINE = InnoDB").execute();
			connector.prepareStatement("ALTER TABLE item ENGINE = InnoDB").execute();
			connector.prepareStatement("ALTER TABLE import_information ENGINE = InnoDB").execute();
			connector.prepareStatement("ALTER TABLE export_information ENGINE = InnoDB").execute();
			connector.prepareStatement("ALTER TABLE delivery_information ENGINE = InnoDB").execute();
			connector.prepareStatement("ALTER TABLE retrieval_information ENGINE = InnoDB").execute();
			
		}
	}
	
}
