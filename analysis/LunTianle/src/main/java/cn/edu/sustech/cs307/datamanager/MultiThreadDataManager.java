package cn.edu.sustech.cs307.datamanager;

import java.sql.SQLException;
import java.text.ParseException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import cn.edu.sustech.cs307.Main;
import cn.edu.sustech.cs307.sqlconnector.MySQLConnector;
import cn.edu.sustech.cs307.sqlconnector.PostgreSQLConnector;
import cn.edu.sustech.cs307.sqlconnector.SQLConnector;
import cn.edu.sustech.cs307.sqlconnector.SQLUtils;

public class MultiThreadDataManager extends FastDataManager {

	private Class<? extends SQLConnector> useConnectorClass;

	public MultiThreadDataManager(Class<? extends SQLConnector> useConnectorClass) {
		super();
		this.useConnectorClass = useConnectorClass;
	}

	@Override 
	public void init(List<DataRecord> records) {
		SQLConnector connector;
		if (this.useConnectorClass == PostgreSQLConnector.class) {
			connector = SQLUtils.newPostgreSQLConnector();
		} else {
			connector = SQLUtils.newMySQLConnector();
		}
		try {
			connector.connect();
			connector.setAutoCommit(false);
			connector.prepareStatement("ALTER TABLE item DROP CONSTRAINT fk_item_cc").execute();
			connector.prepareStatement("ALTER TABLE item DROP CONSTRAINT fk_item_sn").execute();
			connector.prepareStatement("ALTER TABLE export_information DROP CONSTRAINT fk_ei").execute();
			connector.prepareStatement("ALTER TABLE import_information DROP CONSTRAINT fk_ii").execute();
			connector.prepareStatement("ALTER TABLE retrieval_information DROP CONSTRAINT fk_ri_in").execute();
			connector.prepareStatement("ALTER TABLE retrieval_information DROP CONSTRAINT fk_ri_cpn").execute();
			connector.prepareStatement("ALTER TABLE delivery_information DROP CONSTRAINT fk_di_in").execute();
			connector.prepareStatement("ALTER TABLE delivery_information DROP CONSTRAINT fk_di_cpn").execute();
			super.initContainer(records, connector);
			Main.debug("Successfully import container table", false);
			super.initShip(records, connector);
			Main.debug("Successfully import ship table", false);
			super.initCourier(records, connector);
			Main.debug("Successfully import courier table", false);
			connector.commit();
			connector.setAutoCommit(true);
			final CountDownLatch latch = new CountDownLatch(5);
			Thread itemThread = new Thread(() -> {
				SQLConnector sqlConnector = (this.useConnectorClass == PostgreSQLConnector.class
						? SQLUtils.newPostgreSQLConnector()
						: SQLUtils.newMySQLConnector());
				try {
					sqlConnector.connect();
					sqlConnector.setAutoCommit(false);
					super.initItem(records, sqlConnector);
					sqlConnector.commit();
					sqlConnector.setAutoCommit(true);
					sqlConnector.close();
					Main.debug("Successfully import item table", false);
					latch.countDown();
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
			itemThread.start();
			Thread retrievalThread = new Thread(() -> {
				SQLConnector sqlConnector = (this.useConnectorClass == PostgreSQLConnector.class
						? SQLUtils.newPostgreSQLConnector()
						: SQLUtils.newMySQLConnector());
				try {
					sqlConnector.connect();
					sqlConnector.setAutoCommit(false);
					super.initRetrievalInformation(records, sqlConnector);
					sqlConnector.commit();
					sqlConnector.setAutoCommit(true);
					sqlConnector.close();
					Main.debug("Successfully import retrieval_information table", false);
					latch.countDown();
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
			retrievalThread.start();
			Thread deliveryThread = new Thread(() -> {
				SQLConnector sqlConnector = (this.useConnectorClass == PostgreSQLConnector.class
						? SQLUtils.newPostgreSQLConnector()
						: SQLUtils.newMySQLConnector());
				try {
					sqlConnector.connect();
					sqlConnector.setAutoCommit(false);
					super.initDeliveryInformation(records, sqlConnector);
					sqlConnector.commit();
					sqlConnector.setAutoCommit(true);
					sqlConnector.close();
					Main.debug("Successfully import delivery_information table", false);
					latch.countDown();
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
			deliveryThread.start();
			Thread importThread = new Thread(() -> {
				SQLConnector sqlConnector = (this.useConnectorClass == PostgreSQLConnector.class
						? SQLUtils.newPostgreSQLConnector()
						: SQLUtils.newMySQLConnector());
				try {
					sqlConnector.connect();
					sqlConnector.setAutoCommit(false);
					super.initImportInformation(records, sqlConnector);
					sqlConnector.commit();
					sqlConnector.setAutoCommit(true);
					sqlConnector.close();
					Main.debug("Successfully import import_information table", false);
					latch.countDown();
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
			importThread.start();
			Thread exportThread = new Thread(() -> {
				SQLConnector sqlConnector = (this.useConnectorClass == PostgreSQLConnector.class
						? SQLUtils.newPostgreSQLConnector()
						: SQLUtils.newMySQLConnector());
				try {
					sqlConnector.connect();
					sqlConnector.setAutoCommit(false);
					super.initExportInformation(records, sqlConnector);
					sqlConnector.commit();
					sqlConnector.setAutoCommit(true);
					sqlConnector.close();
					Main.debug("Successfully import export_information table", false);
					latch.countDown();
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
			exportThread.start();
			latch.await();
			connector.prepareStatement("ALTER TABLE item ADD CONSTRAINT fk_item_cc FOREIGN KEY (container_code) REFERENCES container(code)").execute();
			connector.prepareStatement("ALTER TABLE item ADD CONSTRAINT fk_item_sc FOREIGN KEY (ship_name) REFERENCES  ship(name)").execute();
			connector.prepareStatement("ALTER TABLE import_information ADD CONSTRAINT fk_ii FOREIGN KEY (item) REFERENCES item(name)").execute();
			connector.prepareStatement("ALTER TABLE export_information ADD CONSTRAINT fk_ei FOREIGN KEY (item) REFERENCES item(name)").execute();
			connector.prepareStatement("ALTER TABLE retrieval_information ADD CONSTRAINT fk_ri_in FOREIGN KEY (item) REFERENCES item(name)").execute();
			connector.prepareStatement("ALTER TABLE retrieval_information ADD CONSTRAINT fk_ri_cpn FOREIGN KEY (courier_phone_number) REFERENCES courier(phone_number)").execute();
			connector.prepareStatement("ALTER TABLE delivery_information ADD CONSTRAINT fk_di_in FOREIGN KEY (item) REFERENCES item(name)").execute();
			connector.prepareStatement("ALTER TABLE delivery_information ADD CONSTRAINT fk_di_cpn FOREIGN KEY (courier_phone_number) REFERENCES courier(phone_number)").execute();
			connector.close();
			Main.debug("Successfully import All items...", false);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
