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
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;

import cn.edu.sustech.cs307.Main;
import cn.edu.sustech.cs307.datamanager.DataRecord.RecordAttribute;
import cn.edu.sustech.cs307.sqlconnector.PostgreSQLConnector;
import cn.edu.sustech.cs307.utils.CalendarUtils;

public class FastDataManager extends DataManager {

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
	private static final String importSql = "INSERT INTO import_information(item, city, time, tax) VALUES(?, ?, ?, ?)";
	private static final String exportSql = "INSERT INTO export_information(item, city, time, tax) VALUES(?, ?, ?, ?)";
	private static final String deliverySql = "INSERT INTO delivery_information(item, city, finish_time, courier_phone_number) VALUES(?, ?, ?, ?)";
	private static final String retrievalSql = "INSERT INTO retrieval_information(item, city, start_time, courier_phone_number) VALUES(?, ?, ?, ?)";
	private static final String itemSql = "INSERT INTO item(name, type, price, container_code, ship_name, log_time) VALUES(?, ?, ?, ?, ?, ?)";
	private static final String rcourierSql = "INSERT INTO retrieval_courier(phone_number, name, gender, birth_year, company) VALUES(?, ?, ?, ?, ?)";
	private static final String dcourierSql = "INSERT INTO delivery_courier(phone_number, name, gender, birth_year, company) VALUES(?, ?, ?, ?, ?)";

	public FastDataManager(PostgreSQLConnector sqlConnector) {
		this.sqlConnector = sqlConnector;
		shipMap = new HashMap<>();
		containerMap = new HashMap<>();
		retrievalCourierMap = new HashMap<>();
		deliveryCourierMap = new HashMap<>();
	}

	@Override
	public void init(List<DataRecord> records) {
		try {
			sqlConnector.setAutoCommit(false);
			PreparedStatement statement = sqlConnector.prepareStatement(containerSql);

			for (DataRecord record : records) {
				String containerCode = (String) record.getValue(RecordAttribute.CONTAINER_CODE);
				if (containerCode != null && containerMap.get(containerCode) == null) {
					statement.setString(1, containerCode);
					statement.setString(2, (String) record.getValue(RecordAttribute.CONTAINER_TYPE));
					statement.addBatch();
					containerMap.put(containerCode, true);
				}
			}
			statement.executeBatch();
			
			Main.debug("Successfully import container table", false);

			statement = sqlConnector.prepareStatement(shipSql);
			for (DataRecord record : records) {
				String companyName = (String) record.getValue(RecordAttribute.COMPANY_NAME);
				String shipName = (String) record.getValue(RecordAttribute.SHIP_NAME);
				if (shipName != null && shipMap.get(shipName) == null) {
					statement.setString(1, shipName);
					statement.setString(2, companyName);
					statement.addBatch();
					shipMap.put(shipName, true);
				}
			}
			statement.executeBatch();
			
			Main.debug("Successfully import ship table", false);
			
			statement = this.sqlConnector.prepareStatement(dcourierSql);
			for (DataRecord record : records) {
				// delivery_courier table
				String companyName = (String) record.getValue(RecordAttribute.COMPANY_NAME);
				String deliveryCPN = (String) record.getValue(RecordAttribute.DELIVERY_COURIER_PHONE_NUMBER);
				if (deliveryCPN != null && deliveryCourierMap.get(deliveryCPN) == null) {
					statement.setString(1, deliveryCPN);
					statement.setString(2, (String) record.getValue(RecordAttribute.DELIVERY_COURIER));
					statement.setString(3, (String) record.getValue(RecordAttribute.DELIVERY_COURIER_GENDER));
					int birthYear = CalendarUtils.getCalendar((String) record.getValue(RecordAttribute.DELIVERY_FINISHED_TIME)).get(Calendar.YEAR) - (int) Double.parseDouble((String) record.getValue(RecordAttribute.DELIVERY_COURIER_AGE));
					statement.setInt(4, birthYear);
					statement.setString(5, companyName);
					statement.addBatch();
					deliveryCourierMap.put(deliveryCPN, true);
				}
			}
			statement.executeBatch();
			
			Main.debug("Successfully import delivery_courier table", false);
			
			statement = this.sqlConnector.prepareStatement(rcourierSql);
			for (DataRecord record : records) {
				// retrieval_courier table
				String companyName = (String) record.getValue(RecordAttribute.COMPANY_NAME);
				String retrievalCPN = (String) record.getValue(RecordAttribute.RETRIEVAL_COURIER_PHONE_NUMBER);
				if (retrievalCourierMap.get(retrievalCPN) == null) {
					statement.setString(1, retrievalCPN);
					statement.setString(2, (String) record.getValue(RecordAttribute.RETRIEVAL_COURIER));
					statement.setString(3, (String) record.getValue(RecordAttribute.RETRIEVAL_COURIER_GENDER));
					int birthYear = CalendarUtils.getCalendar((String) record.getValue(RecordAttribute.RETRIEVAL_START_TIME)).get(Calendar.YEAR) - (int) Double.parseDouble((String) record.getValue(RecordAttribute.RETRIEVAL_COURIER_AGE));
					statement.setInt(4, birthYear);
					statement.setString(5, companyName);
					statement.addBatch();
					;
					retrievalCourierMap.put(retrievalCPN, true);
				}
			}
			statement.executeBatch();
			
			Main.debug("Successfully import retrieval_courier table", false);
			
			statement = sqlConnector.prepareStatement(itemSql);
			for (DataRecord record : records) {
				// item table
				String containerCode = (String) record.getValue(RecordAttribute.CONTAINER_CODE);
				String shipName = (String) record.getValue(RecordAttribute.SHIP_NAME);
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
				statement.setTimestamp(6, new Timestamp(
						timestampFormat.parse((String) record.getValue(RecordAttribute.LOG_TIME)).getTime()));
				statement.addBatch();
			}
			Main.debug("Prepare finish", false);
			statement.executeBatch();
			
			Main.debug("Successfully import item table", false);
			
			statement = sqlConnector.prepareStatement(importSql);
			for (DataRecord record : records) {
				// import_information table
				String importCity = (String) record.getValue(RecordAttribute.ITEM_IMPORT_CITY);
				String importTime = (String) record.getValue(RecordAttribute.ITEM_IMPORT_TIME);
				String importTax = (String) record.getValue(RecordAttribute.ITEM_IMPORT_TAX);
				statement.setString(1, (String) record.getValue(RecordAttribute.ITEM_NAME));
				statement.setString(2, importCity);
				if (importTime != null) {
					statement.setDate(3, new Date(dateFormat.parse(importTime).getTime()));
				} else {
					statement.setNull(3, Types.DATE);
				}
				statement.setBigDecimal(4, new BigDecimal(importTax));
				statement.addBatch();
			}
			
			statement.executeBatch();

			Main.debug("Successfully import import_information table", false);
			
			statement = this.sqlConnector.prepareStatement(exportSql);
			for (DataRecord record : records) {
				// export_information table
				String exportCity = (String) record.getValue(RecordAttribute.ITEM_EXPORT_CITY);
				String exportTime = (String) record.getValue(RecordAttribute.ITEM_EXPORT_TIME);
				String exportTax = (String) record.getValue(RecordAttribute.ITEM_EXPORT_TAX);
				statement.setString(1, (String) record.getValue(RecordAttribute.ITEM_NAME));
				statement.setString(2, exportCity);
				if (exportTime != null) {
					statement.setDate(3, new Date(dateFormat.parse(exportTime).getTime()));
				} else {
					statement.setNull(3, Types.DATE);
				}
				statement.setBigDecimal(4, new BigDecimal(exportTax));
				statement.addBatch();
			}
			
			statement.executeBatch();
			
			Main.debug("Successfully import export_information table", false);

			statement = this.sqlConnector.prepareStatement(deliverySql);
			for (DataRecord record : records) {
				// delivery_information table
				String deliveryCPN = (String) record.getValue(RecordAttribute.DELIVERY_COURIER_PHONE_NUMBER);
				String finishTime = (String) record.getValue(RecordAttribute.DELIVERY_FINISHED_TIME);
				statement.setString(1, (String) record.getValue(RecordAttribute.ITEM_NAME));
				statement.setString(2, (String) record.getValue(RecordAttribute.DELIVERY_CITY));
				if (finishTime != null) {
					statement.setDate(3, new Date(dateFormat.parse(finishTime).getTime()));
				} else {
					statement.setNull(3, Types.DATE);
				}

				if (deliveryCPN != null) {
					statement.setString(4, deliveryCPN);
				} else {
					statement.setNull(4, Types.VARCHAR);
				}
				statement.addBatch();
			}
			
			statement.executeBatch();
			
			Main.debug("Successfully import delivery_information table", false);

			statement = this.sqlConnector.prepareStatement(retrievalSql);
			for (DataRecord record : records) {
				// retrieval_information table
				statement.setString(1, (String) record.getValue(RecordAttribute.ITEM_NAME));
				statement.setString(2, (String) record.getValue(RecordAttribute.RETRIEVAL_CITY));
				statement.setDate(3, new Date(dateFormat.parse((String) record.getValue(RecordAttribute.RETRIEVAL_START_TIME)).getTime()));
				statement.setString(4, (String) record.getValue(RecordAttribute.RETRIEVAL_COURIER_PHONE_NUMBER));
				statement.addBatch();
			}
			
			statement.executeBatch();
			
			Main.debug("Successfully import retrieval_information table", false);
			
			sqlConnector.commit();
			sqlConnector.setAutoCommit(true);
			Main.debug("Successfully import All items...", false);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
