package cn.edu.sustech.dbmsanalysis;

import java.sql.SQLException;

public class DBMSAnalysis {
	
	public static void main(String args[]) {
		//Test connect
		try {
			System.out.println(new PostgreSQLConnector("localhost", 5432, "cslab1", "test", "123456").connect());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
