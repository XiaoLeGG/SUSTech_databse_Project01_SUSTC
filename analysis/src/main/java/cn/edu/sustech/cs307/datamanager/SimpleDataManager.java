package cn.edu.sustech.cs307.datamanager;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

import cn.edu.sustech.cs307.Main;
import cn.edu.sustech.cs307.datamanager.DataRecord.RecordAttribute;
import cn.edu.sustech.cs307.utils.PostgreSQLConnector;

public class SimpleDataManager extends DataManager {
	
	private PostgreSQLConnector sqlConnector;
	private boolean debug = true;
	private HashMap<String, Boolean> shipMap;
	private HashMap<String, Boolean> containerMap;
	private HashMap<String, Boolean> retrievalCourierMap;
	private HashMap<String, Boolean> deliveryCourierMap;
	
	public SimpleDataManager(PostgreSQLConnector sqlConnector) {
		this.sqlConnector = sqlConnector;
		shipMap = new HashMap<>();
		containerMap = new HashMap<>();
		retrievalCourierMap = new HashMap<>();
		deliveryCourierMap = new HashMap<>();
	}
	
	@Override
	public void init(List<DataRecord> records) {
		int i = 0;
		for (DataRecord record : records) {
			try {
				String companyName = (String) record.getValue(RecordAttribute.COMPANY_NAME);
				
				//container table
				String containerCode = (String) record.getValue(RecordAttribute.CONTAINER_CODE);
				if (containerCode != null && containerMap.get(containerCode) == null) {
					String containerSql = String.format("INSERT INTO container (code, type) VALUES('%s', '%s')", 
							containerCode, 
							(String) record.getValue(RecordAttribute.CONTAINER_TYPE));
					this.sqlConnector.prepareStatement(containerSql).execute();
					containerMap.put(containerCode, true);
				}
				
				//ship table
				String shipName = (String) record.getValue(RecordAttribute.SHIP_NAME);
				if (shipName != null && shipMap.get(shipName) == null) {
					String ship = String.format("INSERT INTO ship (name, company) VALUES('%s', '%s')", 
							shipName,
							companyName);
					this.sqlConnector.prepareStatement(ship).execute();
					shipMap.put(shipName, true);
				}	
				
				//import_information table
				String importCity = (String) record.getValue(RecordAttribute.ITEM_IMPORT_CITY);
				String importTime = (String) record.getValue(RecordAttribute.ITEM_IMPORT_TIME);
				String importTax = (String) record.getValue(RecordAttribute.ITEM_IMPORT_TAX);
				String importSql = String.format("INSERT INTO import_information(city, tax) VALUES('%s', %s) RETURNING id", 
						importCity,
						importTax);
				if (importTime != null) {
					importSql = String.format("INSERT INTO import_information(city, time, tax) VALUES('%s', '%s', %s) RETURNING id", 
							importCity,
							importTime,
							importTax);
				}
				PreparedStatement statement = this.sqlConnector.prepareStatement(importSql);
				try {
					statement.execute();
				} catch (Exception e) {
					System.out.println(importTax);
					break;
				}
				ResultSet rs = statement.getResultSet();
				rs.next();
				long importId = rs.getLong(1);
				
				//export_information table
				String exportCity = (String) record.getValue(RecordAttribute.ITEM_EXPORT_CITY);
				String exportTime = (String) record.getValue(RecordAttribute.ITEM_EXPORT_TIME);
				String exportTax = (String) record.getValue(RecordAttribute.ITEM_EXPORT_TAX);
				String exportSql = String.format("INSERT INTO export_information(city, tax) VALUES('%s', %s) RETURNING id", 
						exportCity,
						exportTax);
				if (exportTime != null) {
					exportSql = String.format("INSERT INTO export_information(city, time, tax) VALUES('%s', '%s', %s) RETURNING id", 
							exportCity,
							exportTime,
							exportTax);
				}
				statement = this.sqlConnector.prepareStatement(exportSql);
				statement.execute();
				rs = statement.getResultSet();
				rs.next();
				long exportId = rs.getLong(1);
				
				//delivery_courier table
				String deliveryCPN = (String) record.getValue(RecordAttribute.DELIVERY_COURIER_PHONE_NUMBER);
				if (deliveryCPN != null && deliveryCourierMap.get(deliveryCPN) == null) {
					String dcourierSql = String.format("INSERT INTO delivery_courier(phone_number, name, gender, age, company) VALUES('%s', '%s', '%s', %s, '%s')", 
							deliveryCPN,
							(String) record.getValue(RecordAttribute.DELIVERY_COURIER),
							(String) record.getValue(RecordAttribute.DELIVERY_COURIER_GENDER),
							(String) record.getValue(RecordAttribute.DELIVERY_COURIER_AGE),
							companyName);
					this.sqlConnector.prepareStatement(dcourierSql).execute();
					deliveryCourierMap.put(deliveryCPN, true);
				}
				
				//delivery_information table
				//Note that the courier can be empty
				
				String deliverySql = String.format("INSERT INTO delivery_information(city) VALUES('%s') RETURNING id", 
						(String) record.getValue(RecordAttribute.DELIVERY_CITY));
				if (deliveryCPN != null) {
					deliverySql = String.format("INSERT INTO delivery_information(city, courier_phone_number) VALUES('%s', '%s') RETURNING id", 
							(String) record.getValue(RecordAttribute.DELIVERY_CITY),
							deliveryCPN);
					String finishTime = (String) record.getValue(RecordAttribute.DELIVERY_FINISHED_TIME);
					if (finishTime != null) {
						deliverySql = String.format("INSERT INTO delivery_information(city, finish_time, courier_phone_number) VALUES('%s', '%s', '%s') RETURNING id", 
								(String) record.getValue(RecordAttribute.DELIVERY_CITY),
								finishTime,
								deliveryCPN);
					}
				}
				statement = this.sqlConnector.prepareStatement(deliverySql);
				statement.execute();
				rs = statement.getResultSet();
				rs.next();
				long deliveryId = rs.getLong(1);
				
				//retrieval_courier table
				String retrievalCPN = (String) record.getValue(RecordAttribute.RETRIEVAL_COURIER_PHONE_NUMBER);
				if (retrievalCourierMap.get(retrievalCPN) == null) {
					String rcourierSql = String.format("INSERT INTO retrieval_courier(phone_number, name, gender, age, company) VALUES('%s', '%s', '%s', %s, '%s')", 
							retrievalCPN,
							(String) record.getValue(RecordAttribute.RETRIEVAL_COURIER),
							(String) record.getValue(RecordAttribute.RETRIEVAL_COURIER_GENDER),
							(String) record.getValue(RecordAttribute.RETRIEVAL_COURIER_AGE),
							companyName);
					this.sqlConnector.prepareStatement(rcourierSql).execute();
					retrievalCourierMap.put(retrievalCPN, true);
				}
				
				//delivery_information table
				String retrievalSql = String.format("INSERT INTO retrieval_information(city, start_time, courier_phone_number) VALUES('%s', '%s', '%s') RETURNING id", 
						(String) record.getValue(RecordAttribute.RETRIEVAL_CITY),
						(String) record.getValue(RecordAttribute.RETRIEVAL_START_TIME),
						(String) record.getValue(RecordAttribute.RETRIEVAL_COURIER_PHONE_NUMBER));
				statement = this.sqlConnector.prepareStatement(retrievalSql);
				statement.execute();
				rs = statement.getResultSet();
				rs.next();
				long retrievalId = rs.getLong(1);
				
				//item table
				String itemSql = String.format("INSERT INTO item(name, type, price, import_information_id, export_information_id, delivery_information_id, retrieval_information_id, log_time) VALUES('%s', '%s', %s, %d, %d, %d, %d, '%s')", 
						(String) record.getValue(RecordAttribute.ITEM_NAME),
						(String) record.getValue(RecordAttribute.ITEM_TYPE),
						(String) record.getValue(RecordAttribute.ITEM_PRICE),
						importId,
						exportId,
						deliveryId,
						retrievalId,
						(String) record.getValue(RecordAttribute.LOG_TIME));
				if (containerCode != null && shipName == null) {
					itemSql = String.format("INSERT INTO item(name, type, price, container_code, import_information_id, export_information_id, delivery_information_id, retrieval_information_id, log_time) VALUES('%s', '%s', %s, '%s', %d, %d, %d, %d, '%s')", 
							(String) record.getValue(RecordAttribute.ITEM_NAME),
							(String) record.getValue(RecordAttribute.ITEM_TYPE),
							(String) record.getValue(RecordAttribute.ITEM_PRICE),
							containerCode,
							importId,
							exportId,
							deliveryId,
							retrievalId,
							(String) record.getValue(RecordAttribute.LOG_TIME));
				}
				if (containerCode == null && shipName != null) {
					itemSql = String.format("INSERT INTO item(name, type, price, ship_name, import_information_id, export_information_id, delivery_information_id, retrieval_information_id, log_time) VALUES('%s', '%s', %s, '%s', %d, %d, %d, %d, '%s')", 
							(String) record.getValue(RecordAttribute.ITEM_NAME),
							(String) record.getValue(RecordAttribute.ITEM_TYPE),
							(String) record.getValue(RecordAttribute.ITEM_PRICE),
							shipName,
							importId,
							exportId,
							deliveryId,
							retrievalId,
							(String) record.getValue(RecordAttribute.LOG_TIME));
				}
				if (containerCode != null && shipName != null) {
					itemSql = String.format("INSERT INTO item(name, type, price, container_code, ship_name, import_information_id, export_information_id, delivery_information_id, retrieval_information_id, log_time) VALUES('%s', '%s', %s, '%s', '%s', %d, %d, %d, %d, '%s')", 
							(String) record.getValue(RecordAttribute.ITEM_NAME),
							(String) record.getValue(RecordAttribute.ITEM_TYPE),
							(String) record.getValue(RecordAttribute.ITEM_PRICE),
							containerCode,
							shipName,
							importId,
							exportId,
							deliveryId,
							retrievalId,
							(String) record.getValue(RecordAttribute.LOG_TIME));
				}
				this.sqlConnector.prepareStatement(itemSql).execute();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			++i;
			if (debug && i % 100 == 0) {
				Main.debug("Successfully import " + i + " items...", false);
			}
		}
	}

}
