package cn.edu.sustech.cs307.datamanager;

import java.io.File;
import java.util.List;

import cn.edu.sustech.cs307.utils.PostgreSQLConnector;

public class SimpleDataManager extends DataManager {
	
	private PostgreSQLConnector sqlConnector;
	
	public SimpleDataManager(PostgreSQLConnector sqlConnector) {
		this.sqlConnector = sqlConnector;
	}
	
	@Override
	public void init(List<DataRecord> records) {
		
	}

}
