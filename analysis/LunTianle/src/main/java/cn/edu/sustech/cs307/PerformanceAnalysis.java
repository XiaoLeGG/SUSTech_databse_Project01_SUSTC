package cn.edu.sustech.cs307;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import cn.edu.sustech.cs307.datamanager.DataRecord;
import cn.edu.sustech.cs307.datamanager.FastDataManager;
import cn.edu.sustech.cs307.datamanager.FileDataManager;
import cn.edu.sustech.cs307.datamanager.SimpleDataManager;
import cn.edu.sustech.cs307.sqlconnector.MySQLConnector;
import cn.edu.sustech.cs307.sqlconnector.PostgreSQLConnector;
import cn.edu.sustech.cs307.sqlconnector.SQLConnector;
import cn.edu.sustech.cs307.sqlconnector.SQLUtils;

public class PerformanceAnalysis {
	
	protected static void initTest() throws IOException, SQLException {
		
		List<DataRecord> records = Main.loadRecords(new File(Main.getProperty("data-file-path")));
		
		//PostgreSQL part
		
		PostgreSQLConnector psql = SQLUtils.newPostgreSQLConnector();
		psql.connect();
		
		for (int i = 0; i < 3; ++i) {
			resetTable(psql);
			long cost = testTime(() -> {
				new SimpleDataManager(psql).init(records);
			});
			Main.debug("PostgreSQL SimpleDataManager Init Cost #" + i + ": " + cost + "ms", false);
		}
		for (int i = 0; i < 3; ++i) {
			resetTable(psql);
			long cost = testTime(() -> {
				new FastDataManager(psql).init(records);
			});
			Main.debug("PostgreSQL FastDataManager Init Cost #" + i + ": " + cost + "ms", false);
		}
		for (int i = 0; i < 3; ++i) {
			resetTable(psql);
			long cost = testTime(() -> {
				new FastDataManager(psql).init(records);
			});
			Main.debug("PostgreSQL MutilThreadDataManager Init Cost #" + i + ": " + cost + "ms", false);
		}
		for (int i = 0; i < 10; ++i) {
			resetFile();
			long cost = testTime(() -> {
				new FileDataManager(new File(Main.getProperty("file-storage-directory"))).init(records);
			});
		}
		
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
	}
	
}
