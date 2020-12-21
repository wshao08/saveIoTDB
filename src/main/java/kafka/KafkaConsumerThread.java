package kafka;

import cn.hutool.core.convert.Convert;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import ty.pub.TransPacket;
import ty.pub.ParsedPacketDecoder;
import static cn.hutool.core.util.StrUtil.DOT;

public class KafkaConsumerThread implements Runnable {
  public static final String INT = "int";
  public static final String LONG = "long";
  public static final String STRING = "string";
  public static final String DOUBLE = "double";
  public static final String FLOAT = "float";

  public static final String VALUE_INT64 = "valueInt64";
  public static final String VALUE_DOUBLE = "valueDouble";
  public static final String VALUE_TEXT = "valueText";

  private String messGenerateTime = "MsgTime@long";
  private String customTime = "CustomTime@long";
  private String tmnlID = "TmnlID@string";
  private KafkaStream<byte[], byte[]> stream;
  private static Set<String> cache = new HashSet<>();

  private static BlockingQueue<Runnable> threadQueue = new LinkedBlockingQueue<Runnable>(100000);

  private static ExecutorService insertThreadPool = new ThreadPoolExecutor(150, 150,
      0L, TimeUnit.MILLISECONDS, threadQueue, new CustomPolicy());



  public KafkaConsumerThread(KafkaStream<byte[], byte[]> stream) {
    this.stream = stream;
    /**
     * Establish JDBC connection of IoTDB
     */
  }

  private static class CustomPolicy implements RejectedExecutionHandler {
    public CustomPolicy() {}

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      try {
        synchronized (r) {
          r.wait(10000);
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void run() {
    for (MessageAndMetadata<byte[], byte[]> consumerIterator : stream) {
      try {
        byte[] uploadMessage = consumerIterator.message();
        //System.out.println(uploadMessage);

        ParsedPacketDecoder packetDecoder = new ParsedPacketDecoder();
        TransPacket transPacket = packetDecoder.deserialize(null, uploadMessage);
        String vclId = transPacket.getDeviceId();

        List<String> deviceIdList = new ArrayList<>();
        List<Long> timeList = new ArrayList<>();
        List<List<String>> metricLists = new ArrayList<>();
        List<List<TSDataType>> typeLists = new ArrayList<>();
        List<List<Object>> valueLists = new ArrayList<>();
        long secondaryTime = transPacket.getTimestamp();

        Map<String, Map<Long, String>> workStatusMap = transPacket.getWorkStatusMap();
        Map<String, String> baseInfoMap = transPacket.getBaseInfoMap();

        // 信息生成时间
        String s = baseInfoMap.get(messGenerateTime);
        if (s == null) {
          s = baseInfoMap.get("TY_0001_00_6@long");
          if (s == null) {
            return;
          }
        }
        long primaryTime = Long.parseLong(s);

        // 自定义时间(第三时间戳)
        long customtime = Long.parseLong(baseInfoMap.getOrDefault(customTime, "-1"));
        long randomLong = Long.parseLong(baseInfoMap.getOrDefault("Random@long", "-1"));
        String tmnlId = baseInfoMap.getOrDefault(tmnlID, transPacket.getDeviceId());

        String countStr = "50";
        int storageGroupCount = Convert.toInt(countStr);
        String deviceId = "root.CTY."
            + (padLeft(String.valueOf(Long.parseLong(vclId) % storageGroupCount), countStr.length(), '0'))
            + DOT + vclId + DOT + tmnlId;

        //createTimeSeries(transPacket, deviceId);

        writeBaseMap(deviceIdList, timeList, metricLists, typeLists, valueLists, secondaryTime, baseInfoMap,
            primaryTime, deviceId);

        writeWorkMap(deviceIdList, timeList, metricLists, typeLists, valueLists, secondaryTime, workStatusMap,
            primaryTime, customtime, randomLong, deviceId);

        SessionInsertThread sessionThread = new SessionInsertThread(deviceIdList, timeList, metricLists, typeLists, valueLists);
        //sessionPool.asyncInsertRecords(deviceIdList, timeList, metricLists, typeLists, valueLists);
        insertThreadPool.submit(sessionThread);

        /*
        long start = System.currentTimeMillis();
        Date date = new Date();
        sessionPool.insertRecords(deviceIdList, timeList, metricLists, valueLists);
        long end = System.currentTimeMillis();
        if (end - start > 10000) {
          System.out.println("@<<" + date + " " + Thread.currentThread().getName() + " " + deviceIdList.get(0) + " " + timeList.get(0));
          System.out.println("@<<" + new Date() + " " + Thread.currentThread().getName() + " " + deviceIdList.get(0) + " " + timeList.get(0) + "  " + (end - start) + " OK");
        }*/
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private static void writeBaseMap(List<String> deviceIdList, List<Long> timeList, List<List<String>> metricLists,
      List<List<TSDataType>> typeLists, List<List<Object>> valueLists, long secondaryTime,
      Map<String, String> baseInfoMap, long primaryTime, String deviceId) {
    for (Map.Entry<String, String> entry : baseInfoMap.entrySet()) {
      String[] arr = entry.getKey().split("@");
      if (arr.length < 2)
        continue;

      String value = entry.getValue();

      deviceIdList.add(deviceId);
      timeList.add(primaryTime);

      List<String> metricList = new ArrayList<>();
      metricList.add(arr[0] + "_secondary_time");
      metricList.add(arr[0] + "_tertiary_time");
      metricList.add(arr[0] + "_" + getValueStr(arr[1]));
      metricLists.add(metricList);

      List<TSDataType> typeList = new ArrayList<>();
      typeList.add(TSDataType.INT64);
      typeList.add(TSDataType.INT64);

      List<Object> valueList = new ArrayList<>();
      try {
        Long.valueOf(secondaryTime);
      } catch (Exception e) {
        System.out.println(e.getMessage());
      }
      valueList.add(secondaryTime);
      valueList.add(Long.valueOf(-1));

      switch (arr[1]) {
        case INT:
        case LONG:
          typeList.add(TSDataType.INT64);
          valueList.add(Long.valueOf(value));
          break;
        case FLOAT:
        case DOUBLE:
          typeList.add(TSDataType.DOUBLE);
          valueList.add(Double.valueOf(value));
          break;
        default:
          typeList.add(TSDataType.TEXT);
          value = addStrValue(value);
          valueList.add(value);
      }

      typeLists.add(typeList);
      valueLists.add(valueList);
    }
  }

  /**
   * 批量workmap
   *
   * @param deviceIdList
   *            时间序列集合
   * @param timeList
   *            第一时间戳集合
   * @param metricLists
   *            工况字段名
   * @param valueLists
   *            值
   * @param secondaryTime
   *            第二时间戳
   * @param workStatusMap
   *            workMap
   * @param primaryTime
   *            第一时间戳
   * @param customtime
   *            第三时间戳
   * @param randomLong
   *            随机数
   * @param deviceId
   *            设备id (root.CTY.1001xxxx.8800xxx)
   */
  private static void writeWorkMap(List<String> deviceIdList, List<Long> timeList, List<List<String>> metricLists,
      List<List<TSDataType>> typeLists, List<List<Object>> valueLists, long secondaryTime,
      Map<String, Map<Long, String>> workStatusMap, long primaryTime, long customtime, long randomLong, String deviceId) {
    for (Map.Entry<String, Map<Long, String>> entry : workStatusMap.entrySet()) {
      String[] arr = entry.getKey().split("@");
      String key = arr[0];
      String type = arr[1];
      Map<Long, String> map = entry.getValue();
      String timeseries = deviceId;
      switch (type) {
        case "string":
          try {
            long thrid = getThird(randomLong, customtime, arr);
            for (Map.Entry<Long, String> mapEntry : map.entrySet()) {
              long firstTime = primaryTime + mapEntry.getKey();
              String value = mapEntry.getValue();

              List<String> metricList = new ArrayList<>();
              List<TSDataType> typeList = new ArrayList<>();
              List<Object> valueList = new ArrayList<>();
              deviceIdList.add(timeseries);
              timeList.add(firstTime);

              metricList.add(arr[0] + "_secondary_time");
              metricList.add(arr[0] + "_tertiary_time");
              metricList.add(arr[0] + "_" + VALUE_TEXT);

              typeList.add(TSDataType.INT64);
              typeList.add(TSDataType.INT64);
              typeList.add(TSDataType.TEXT);

              valueList.add(secondaryTime);
              valueList.add(thrid);
              valueList.add(addStrValue(value));

              metricLists.add(metricList);
              typeLists.add(typeList);
              valueLists.add(valueList);
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
          break;
        case "int":
        case "long":
          try {
            long thrid = getThird(randomLong, customtime, arr);
            for (Map.Entry<Long, String> mapEntry : map.entrySet()) {
              long firstTime = primaryTime + mapEntry.getKey();
              String value = mapEntry.getValue();

              List<String> metricList = new ArrayList<>();
              List<TSDataType> typeList = new ArrayList<>();
              List<Object> valueList = new ArrayList<>();
              deviceIdList.add(timeseries);
              timeList.add(firstTime);

              metricList.add(arr[0] + "_secondary_time");
              metricList.add(arr[0] + "_tertiary_time");
              metricList.add(arr[0] + "_" + VALUE_INT64);

              typeList.add(TSDataType.INT64);
              typeList.add(TSDataType.INT64);
              typeList.add(TSDataType.INT64);

              if (value != null && value.contains(".")) {
                value = value.substring(0, value.indexOf("."));
              }

              valueList.add(secondaryTime);
              valueList.add(thrid);
              // need to check not null
              valueList.add(Long.valueOf(value));

              metricLists.add(metricList);
              typeLists.add(typeList);
              valueLists.add(valueList);
            }

          } catch (Exception e) {
            e.printStackTrace();
          }
          break;
        case "float":
        case "double":
          try {
            long thrid = getThird(randomLong, customtime, arr);
            for (Map.Entry<Long, String> mapEntry : map.entrySet()) {
              long firstTime = primaryTime + mapEntry.getKey();
              String value = mapEntry.getValue();

              List<String> metricList = new ArrayList<>();
              List<TSDataType> typeList = new ArrayList<>();
              List<Object> valueList = new ArrayList<>();
              List<String> valueList2 = new ArrayList<>();
              deviceIdList.add(timeseries);
              timeList.add(firstTime);

              metricList.add(arr[0] + "_secondary_time");
              metricList.add(arr[0] + "_tertiary_time");
              metricList.add(arr[0] + "_" + VALUE_DOUBLE);
              typeList.add(TSDataType.INT64);
              typeList.add(TSDataType.INT64);
              typeList.add(TSDataType.DOUBLE);

              if (!key.equals("TY_0002_00_4_GeoAltitude")) {
                valueList.add(secondaryTime);
                valueList.add(thrid);
                if (!value.contains(".")) {
                  value += ".0";
                }
                valueList.add(Double.valueOf(value));
              } else {
                JSONArray jsonArray = JSONArray.parseArray(value);
                if (jsonArray.size() == 0) {
                  System.out.println("@^^ERROR");
                }
                for (int i = 0; i < jsonArray.size(); i++) {
                  JSONObject jsonObject = jsonArray.getJSONObject(i);
                  double altitude = jsonObject.getDoubleValue("Altitude");
                  long pstnTime = jsonObject.getLongValue("PstnTime");

                  metricList = new ArrayList<>();
                  typeList = new ArrayList<>();
                  valueList = new ArrayList<>();

                  deviceIdList.add(timeseries);
                  timeList.add(firstTime);

                  metricList.add("secondary_time");
                  metricList.add("tertiary_time");
                  metricList.add(VALUE_DOUBLE);
                  typeList.add(TSDataType.INT64);
                  typeList.add(TSDataType.INT64);
                  typeList.add(TSDataType.DOUBLE);

                  valueList.add(secondaryTime);
                  valueList.add(pstnTime);
                  valueList.add(altitude);

                  metricLists.add(metricList);
                  typeLists.add(typeList);
                  valueLists.add(valueList);
                }
              }

              metricLists.add(metricList);
              typeLists.add(typeList);
              valueLists.add(valueList);

            }

          } catch (Exception e) {
            e.printStackTrace();
          }
          break;
        case "geo":
          try {
            for (Map.Entry<Long, String> mapEntry : map.entrySet()) {
              long firstTime = mapEntry.getKey() + primaryTime;
              String value = mapEntry.getValue();
              JSONArray jsonArray = JSON.parseArray(value);
              for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject jsonObject = jsonArray.getJSONObject(i);
                long thrid = jsonObject.getLongValue("PstnTime");

                List<String> metricList = new ArrayList<>();
                List<TSDataType> typeList = new ArrayList<>();
                List<Object> valueList = new ArrayList<>();
                deviceIdList.add(timeseries);
                timeList.add(firstTime);

                metricList.add(arr[0] + "_secondary_time");
                metricList.add(arr[0] + "_tertiary_time");
                metricList.add(arr[0] + "_" + VALUE_TEXT);

                typeList.add(TSDataType.INT64);
                typeList.add(TSDataType.INT64);
                typeList.add(TSDataType.TEXT);

                valueList.add(secondaryTime);
                valueList.add(thrid);
                valueList.add(addStrValue(jsonObject.toJSONString()));

                metricLists.add(metricList);
                typeLists.add(typeList);
                valueLists.add(valueList);
              }
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
          break;
        case "gps":
          try {
            for (Map.Entry<Long, String> mapEntry : map.entrySet()) {
              long thrid = mapEntry.getKey() + primaryTime;
              String value = mapEntry.getValue();
              JSONArray jsonArray = JSON.parseArray(value);
              for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject jsonObject = jsonArray.getJSONObject(i);
                long firstTime = jsonObject.getLongValue("PstnTime");
                jsonObject.remove("PstnTime");

                List<String> metricList = new ArrayList<>();
                List<TSDataType> typeList = new ArrayList<>();
                List<Object> valueList = new ArrayList<>();
                deviceIdList.add(timeseries);
                timeList.add(firstTime);

                metricList.add(arr[0] + "_secondary_time");
                metricList.add(arr[0] + "_tertiary_time");
                metricList.add(arr[0] + "_" + VALUE_TEXT);

                typeList.add(TSDataType.INT64);
                typeList.add(TSDataType.INT64);
                typeList.add(TSDataType.TEXT);

                valueList.add(secondaryTime);
                valueList.add(thrid);
                valueList.add(addStrValue(jsonObject.toJSONString()));

                metricLists.add(metricList);
                typeLists.add(typeList);
                valueLists.add(valueList);
              }
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
          break;
        case "map":
          try {
            long thrid = getThird(randomLong, customtime, arr);
            for (Map.Entry<Long, String> mapEntry : map.entrySet()) {
              long firstTime = mapEntry.getKey() + primaryTime;
              String value = mapEntry.getValue();
              JSONArray jsonArray = JSON.parseArray(value);
              for (int i = 0, n = jsonArray.size(); i < n; i++) {
                JSONObject jsonObject = jsonArray.getJSONObject(i);

                Object erc_startTime = jsonObject.remove("ERC_StartTime");
                if (erc_startTime != null) {
                  firstTime = Long.parseLong(erc_startTime.toString());
                }
                Object erc_endTime = jsonObject.remove("ERC_EndTime");
                if (erc_endTime != null) {
                  thrid = Long.parseLong(erc_endTime.toString());
                }

                switch (key) {
                  // region 所属日期
                  case "TY_0002_00_732_gather": {
                    // 所属日期
                    Map<Long, String> orDefault = workStatusMap.getOrDefault("TY_0002_00_732@long",
                        workStatusMap.get("TY_0002_00_920@long"));
                    if (orDefault == null)
                      continue;
                    long dataTime = Long.parseLong(orDefault.get(0L));
                    // 交换第一时间戳和第三时间戳
                    firstTime = firstTime ^ dataTime;
                    dataTime = firstTime ^ dataTime;
                    firstTime = firstTime ^ dataTime;
                    thrid = dataTime;
                    break;
                  }
                  case "TY_0002_00_733_gather": {
                    // 所属日期
                    long dataTime = Long
                        .parseLong(workStatusMap.get("TY_0002_00_733@long").get(0L));
                    // 交换第一时间戳和第三时间戳
                    firstTime = firstTime ^ dataTime;
                    dataTime = firstTime ^ dataTime;
                    firstTime = firstTime ^ dataTime;
                    thrid = dataTime;
                    break;
                  }
                  // endregion
                  // region 故障报警
                  case "TY_0002_00_8_Fault": // 故障
                    thrid = jsonObject.getIntValue("MsgFA_SPN") * 100
                        + jsonObject.getIntValue("MsgFA_FMI");
                    break;
                  case "TY_0001_00_45_Alarm": // 报警
                    thrid = jsonObject.getIntValue("alarmTypeId");
                    break;
                  // endregion
                  case "TY_0002_00_4_GeoAdt_State":// GPR详细信息后处理加位置点状态信息
                  case "TY_0002_00_4_GeoAltitude_State":// 海拔后处理加位置点状态信息
                  case "TY_0002_00_4_Geo_State":// 经纬度后处理加位置点状态信息
                    thrid = jsonObject.getLongValue("PstnTime");

                    firstTime = primaryTime + mapEntry.getKey();

                    firstTime = firstTime ^ thrid;
                    thrid = firstTime ^ thrid;
                    firstTime = firstTime ^ thrid;

                    break;
                }

                List<String> metricList = new ArrayList<>();
                List<TSDataType> typeList = new ArrayList<>();
                List<Object> valueList = new ArrayList<>();
                deviceIdList.add(timeseries);
                timeList.add(firstTime);

                metricList.add(arr[0] + "_secondary_time");
                metricList.add(arr[0] + "_tertiary_time");
                metricList.add(arr[0] + "_" + VALUE_TEXT);
                typeList.add(TSDataType.INT64);
                typeList.add(TSDataType.INT64);
                typeList.add(TSDataType.TEXT);

                valueList.add(secondaryTime);
                valueList.add(thrid);
                valueList.add(addStrValue(jsonObject.toJSONString()));

                metricLists.add(metricList);
                typeLists.add(typeList);
                valueLists.add(valueList);

              }
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
          break;
      }
    }
  }
/*
  private void createTimeSeries(TransPacket transPacket, String deviceId)
      throws StatementExecutionException, IoTDBConnectionException {

    Map<String, String> baseInfoMap = transPacket.getBaseInfoMap();
    for (Map.Entry<String, String> entry : baseInfoMap.entrySet()) {
      String[] arr = entry.getKey().split("@");
      if (arr.length < 2)
        continue;
      String timeseries_Value = deviceId + DOT + arr[0] + "_" + getValueStr(arr[1]);
      if (cache.contains(timeseries_Value)) {
        continue;
      }

      String timeseries_secondaryTime = deviceId + DOT + arr[0] + "_secondary_time";
      String timeseries_tertiaryTime = deviceId + DOT + arr[0] + "_tertiary_time";

      switch (arr[1]) {
        case "float":
        case "double":
          sessionPool.createTimeseries(timeseries_Value, TSDataType.DOUBLE, TSEncoding.PLAIN,
              CompressionType.SNAPPY);
        default:
          sessionPool.createTimeseries(timeseries_Value, getTsDataType(arr[1]), TSEncoding.PLAIN,
              CompressionType.SNAPPY);
      }

      sessionPool.createTimeseries(timeseries_secondaryTime, TSDataType.INT64, TSEncoding.PLAIN,
          CompressionType.SNAPPY);
      sessionPool.createTimeseries(timeseries_tertiaryTime, TSDataType.INT64, TSEncoding.PLAIN,
          CompressionType.SNAPPY);

      cache.add(timeseries_Value);
    }

    Map<String, Map<Long, String>> workStatusMap = transPacket.getWorkStatusMap();
    for (Map.Entry<String, Map<Long, String>> entry : workStatusMap.entrySet()) {
      String[] arr = entry.getKey().split("@");

      String timeseries_Value = deviceId + DOT + arr[0] + "_" + getValueStr(arr[1]);

      if (cache.contains(timeseries_Value)) {
        continue;
      }
      String timeseries_secondaryTime = deviceId + DOT + arr[0] + "_secondary_time";
      String timeseries_tertiaryTime = deviceId + DOT + arr[0] + "_tertiary_time";

      switch (arr[1]) {
        case "float":
        case "double":
          sessionPool.createTimeseries(timeseries_Value, TSDataType.DOUBLE, TSEncoding.PLAIN,
              CompressionType.SNAPPY);
        default:
          sessionPool.createTimeseries(timeseries_Value, getTsDataType(arr[1]), TSEncoding.PLAIN,
              CompressionType.SNAPPY);
      }
      sessionPool.createTimeseries(timeseries_secondaryTime, TSDataType.INT64, TSEncoding.PLAIN,
          CompressionType.SNAPPY);
      sessionPool.createTimeseries(timeseries_tertiaryTime, TSDataType.INT64, TSEncoding.PLAIN,
          CompressionType.SNAPPY);

      cache.add(timeseries_Value);
    }
  }*/

  public static TSDataType getTsDataType(String s) {
    switch (s) {
      case INT:
      case LONG:
        return TSDataType.INT64;
      case FLOAT:
      case DOUBLE:
        return TSDataType.DOUBLE;
      default:
        return TSDataType.TEXT;
    }
  }

  public static String getValueStr(String s) {
    switch (s) {
      case INT:
      case LONG:
        return VALUE_INT64;
      case FLOAT:
      case DOUBLE:
        return VALUE_DOUBLE;
      default:
        return VALUE_TEXT;
    }
  }

  public static long getThird(long randomLong, long customTime, String[] arr) {
    if (customTime == -1 && randomLong == -1 && arr.length == 2) {
      return customTime;
    } else if (customTime == -1 && randomLong != -1 && //
        arr.length == 3 && arr[2].equalsIgnoreCase("random")) {
      return randomLong;
    } else if (customTime != -1 && randomLong == -1 && arr.length == 2) {
      return customTime;
    }
    return -1;
  }

  private static String addStrValue(String value) {
    return "\"" + value + "\"";
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
