package cn.edu.sustech.cs307.datamanager;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.alibaba.fastjson.JSONObject;

import cn.edu.sustech.cs307.Main;

public class FileDataManager extends DataManager {

	@Override
	public void init(List<DataRecord> records) {
		File dir = new File(Main.getProperty("file-storage-directory"));
		dir.mkdirs();
		File containerFile = new File(dir, "container.json");
		File shipFile = new File(dir, "ship.json");
		File itemFile = new File(dir, "item.json");
		File importInformationFile = new File(dir, "import_information.json");
		File exportInformationFile = new File(dir, "export_information.json");
		File deliveryCourierFile = new File(dir, "delivery_courier.json");
		File deliveryInformationFile = new File(dir, "delivery_information");
		File retrievalCourierFile = new File(dir, "retrieval_courier");
		File retrievalInformationFile = new File(dir, "retrieval_information");
		List<File> fileList = Arrays.asList(containerFile, shipFile, itemFile, 
				importInformationFile, exportInformationFile, deliveryCourierFile, 
				retrievalCourierFile, deliveryInformationFile, retrievalInformationFile);
		fileList.forEach(file -> {
			if (!file.exists()) {
				try {
					file.createNewFile();
					JSONObject object = new JSONObject();
					writeContentToFile(file, object.toJSONString());
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});
		
		JSONObject container = JSONObject.parseObject(loadContentFromFile(containerFile));
		JSONObject ship = JSONObject.parseObject(loadContentFromFile(shipFile));
		JSONObject item = JSONObject.parseObject(loadContentFromFile(itemFile));
		JSONObject importInformation = JSONObject.parseObject(loadContentFromFile(importInformationFile));
		JSONObject exportInformation = JSONObject.parseObject(loadContentFromFile(exportInformationFile));
		JSONObject deliveryCourier = JSONObject.parseObject(loadContentFromFile(deliveryCourierFile));
		JSONObject deliveryInformation = JSONObject.parseObject(loadContentFromFile(deliveryInformationFile));
		JSONObject retrievalCourier = JSONObject.parseObject(loadContentFromFile(retrievalCourierFile));
		JSONObject retrievalInformation = JSONObject.parseObject(loadContentFromFile(retrievalInformationFile));
		
		
	}
	
	private static void writeContentToFile(File file, String content) throws IOException {
		FileOutputStream stream = new FileOutputStream(file);
		stream.write(content.getBytes());
		stream.close();
	}
	
	private static String loadContentFromFile(File file) {
		byte[] fileContent = new byte[((Long) file.length()).intValue()];
		try {
			FileInputStream in = new FileInputStream(file);
			in.read(fileContent);
			in.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return new String(fileContent);
		
	}

}
