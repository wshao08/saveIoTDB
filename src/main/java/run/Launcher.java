package run;

import cn.hutool.core.convert.Convert;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import kafka.CreateKafka;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ty.pub.ParsedPacketDecoder;
import ty.pub.TransPacket;

import java.io.InputStream;
import java.util.*;

import static cn.hutool.core.util.StrUtil.DOT;

public class Launcher {
	private static Logger log = LoggerFactory.getLogger(Launcher.class);
	public static Properties p = new Properties();


	public static final String INT = "int";
	public static final String LONG = "long";
	public static final String STRING = "string";
	public static final String DOUBLE = "double";
	public static final String FLOAT = "float";

	public static final String VALUE_INT64 = "valueInt64";
	public static final String VALUE_DOUBLE = "valueDouble";
	public static final String VALUE_TEXT = "valueText";



	public static SessionPool sessionPool = new SessionPool("192.168.35.42", 6667,
			"root", "root", 50, false);

	private static Set<String> cache = new HashSet<>();

	public static void main(String[] args) {
		String countStr = "50";
		int storageGroupCount = Convert.toInt(countStr);
		try {
			for (int i = 0; i < storageGroupCount; i++) {
				sessionPool.setStorageGroup(
						"root.CTY" + DOT + padLeft(String.valueOf(i), countStr.length(), '0'));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		new CreateKafka().consume();
	}

	public static String padLeft(String src, int len, char ch) {
		int diff = len - src.length();
		if (diff <= 0) {
			return src;
		}

		char[] charr = new char[len];
		System.arraycopy(src.toCharArray(), 0, charr, diff, src.length());
		for (int i = 0; i < diff; i++) {
			charr[i] = ch;
		}
		return new String(charr);
	}
}
