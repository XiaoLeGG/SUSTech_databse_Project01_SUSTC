package cn.edu.sustech.cs307.datamanager;

import java.io.File;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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
	private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	private static final SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	private static final String containerSql = "INSERT INTO container (code, type) VALUES(?, ?)";
	private static final String shipSql = "INSERT INTO ship (name, company) VALUES(?, ?)";
	private static final String importSql = "INSERT INTO import_information(city, time, tax) VALUES(?, ?, ?) RETURNING id";
	private static final String exportSql = "INSERT INTO export_information(city, time, tax) VALUES(?, ?, ?) RETURNING id";
	private static final String deliverySql = "INSERT INTO delivery_information(city, finish_time, courier_phone_number) VALUES(?, ?, ?) RETURNING id";
	private static final String rcourierSql = "INSERT INTO retrieval_courier(phone_number, name, gender, age, company) VALUES(?, ?, ?, ?, ?)";
	private static final String retrievalSql = "INSERT INTO retrieval_information(city, start_time, courier_phone_number) VALUES(?, ?, ?) RETURNING id";
	private static final String itemSql = "INSERT INTO item(name, type, price, container_code, ship_name, import_information_id, export_information_id, delivery_information_id, retrieval_information_id, log_time) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	private static final String dcourierSql = "INSERT INTO delivery_courier(phone_number, name, gender, age, company) VALUES(?, ?, ?, ?, ?)";
	
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
					PreparedStatement statement = this.sqlConnector.prepareStatement(containerSql);
					statement.setString(1, containerCode);
					statement.setString(2, (String) record.getValue(RecordAttribute.CONTAINER_TYPE));
					statement.execute();
					containerMap.put(containerCode, true);
				}
				
				//ship table
				String shipName = (String) record.getValue(RecordAttribute.SHIP_NAME);
				if (shipName != null && shipMap.get(shipName) == null) {
					PreparedStatement statement = this.sqlConnector.prepareStatement(shipSql);
					statement.setString(1, shipName);
					statement.setString(2, companyName);
					statement.execute();
					shipMap.put(shipName, true);
				}	
				
				//import_information table
				String importCity = (String) record.getValue(RecordAttribute.ITEM_IMPORT_CITY);
				String importTime = (String) record.getValue(RecordAttribute.ITEM_IMPORT_TIME);
				String importTax = (String) record.getValue(RecordAttribute.ITEM_IMPORT_TAX);
				
				PreparedStatement statement = this.sqlConnector.prepareStatement(importSql);
				statement.setString(1, importCity);
				if (importTime != null) {
					statement.setDate(2, new Date(dateFormat.parse(importTime).getTime()));
				} else {
					statement.setNull(2, Types.DATE);
				}
				statement.setBigDecimal(3, new BigDecimal(importTax));;
				statement.execute();
				ResultSet rs = statement.getResultSet();
				rs.next();
				long importId = rs.getLong(1);
				
				//export_information table
				String exportCity = (String) record.getValue(RecordAttribute.ITEM_EXPORT_CITY);
				String exportTime = (String) record.getValue(RecordAttribute.ITEM_EXPORT_TIME);
				String exportTax = (String) record.getValue(RecordAttribute.ITEM_EXPORT_TAX);
				statement = this.sqlConnector.prepareStatement(exportSql);
				statement.setString(1, exportCity);
				if (exportTime != null) {
					statement.setDate(2, new Date(dateFormat.parse(exportTime).getTime()));
				} else {
					statement.setNull(2, Types.DATE);
				}
				statement.setBigDecimal(3, new BigDecimal(exportTax));;
				statement.execute();
				rs = statement.getResultSet();
				rs.next();
				long exportId = rs.getLong(1);
				
				//delivery_courier table
				String deliveryCPN = (String) record.getValue(RecordAttribute.DELIVERY_COURIER_PHONE_NUMBER);
				if (deliveryCPN != null && deliveryCourierMap.get(deliveryCPN) == null) {
					statement = this.sqlConnector.prepareStatement(dcourierSql);
					statement.setString(1, deliveryCPN);
					statement.setString(2, (String) record.getValue(RecordAttribute.DELIVERY_COURIER));
					statement.setString(3, (String) record.getValue(RecordAttribute.DELIVERY_COURIER_GENDER));
					statement.setInt(4, (int) Double.parseDouble((String) record.getValue(RecordAttribute.DELIVERY_COURIER_AGE)));
					statement.setString(5, companyName);
					statement.execute();
					deliveryCourierMap.put(deliveryCPN, true);
				}
				
				//delivery_information table
				String finishTime = (String) record.getValue(RecordAttribute.DELIVERY_FINISHED_TIME);
				statement = this.sqlConnector.prepareStatement(deliverySql);
				statement.setString(1, (String) record.getValue(RecordAttribute.DELIVERY_CITY));
				if (finishTime != null) {
					statement.setDate(2, new Date(dateFormat.parse(finishTime).getTime()));
				} else {
					statement.setNull(2, Types.DATE);
				}
				
				if (deliveryCPN != null) {
					statement.setString(3, deliveryCPN);
				} else {
					statement.setNull(3, Types.VARCHAR);
				}
				statement.execute();
				rs = statement.getResultSet();
				rs.next();
				long deliveryId = rs.getLong(1);
				
				//retrieval_courier table
				String retrievalCPN = (String) record.getValue(RecordAttribute.RETRIEVAL_COURIER_PHONE_NUMBER);
				if (retrievalCourierMap.get(retrievalCPN) == null) {
					statement = this.sqlConnector.prepareStatement(rcourierSql);
					statement.setString(1, retrievalCPN);
					statement.setString(2, (String) record.getValue(RecordAttribute.RETRIEVAL_COURIER));
					statement.setString(3, (String) record.getValue(RecordAttribute.RETRIEVAL_COURIER_GENDER));
					statement.setInt(4, Integer.parseInt((String) record.getValue(RecordAttribute.RETRIEVAL_COURIER_AGE)));
					statement.setString(5, companyName);
					statement.execute();
					retrievalCourierMap.put(retrievalCPN, true);
				}
				
				//retrieval_information table
				statement = this.sqlConnector.prepareStatement(retrievalSql);
				statement.setString(1, (String) record.getValue(RecordAttribute.RETRIEVAL_CITY));
				statement.setDate(2, new Date(dateFormat.parse((String) record.getValue(RecordAttribute.RETRIEVAL_START_TIME)).getTime()));
				statement.setString(3, (String) record.getValue(RecordAttribute.RETRIEVAL_COURIER_PHONE_NUMBER));
				statement.execute();
				rs = statement.getResultSet();
				rs.next();
				long retrievalId = rs.getLong(1);
				
				//item table
				statement = sqlConnector.prepareStatement(itemSql);
				statement.setString(1, (String) record.getValue(RecordAttribute.ITEM_NAME));
				statement.setString(2, (String) record.getValue(RecordAttribute.ITEM_TYPE));
				statement.setBigDecimal(3, new BigDecimal((String) record.getValue(RecordAttribute.ITEM_PRICE)));
				if (containerCode != null) {
					statement.setString(4, containerCode);
				} else {
					statement.setNull(4, Types.VARCHAR);
				}
				if (shipName != null) {
					statement.setString(5, shipName);
				} else {
					statement.setNull(5, Types.VARCHAR);
				}
				statement.setLong(6, importId);
				statement.setLong(7, exportId);
				statement.setLong(8, deliveryId);
				statement.setLong(9, retrievalId);
				statement.setTimestamp(10, new Timestamp(timestampFormat.parse((String) record.getValue(RecordAttribute.LOG_TIME)).getTime()));
			} catch (Exception e) {
				e.printStackTrace();
			}
			++i;
			if (debug && i % 10000 == 0) {
				Main.debug("Successfully import " + i + " items...", false);
			}
		}
		Main.debug("Successfully import " + i + " items...", false);
	}

}