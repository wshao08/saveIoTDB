package kafka;

import static run.Launcher.sessionPool;

import java.util.Date;
import java.util.List;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class SessionInsertThread implements Runnable {
  private List<String> deviceIds;
  private List<Long> times;
  private List<List<String>> measurementsList;
  private List<List<TSDataType>> typeList;
  private List<List<Object>> valuesList;

  public SessionInsertThread(List<String> deviceIds, List<Long> times, List<List<String>> measurementsList,
      List<List<TSDataType>> typeList, List<List<Object>> valuesList) {
    this.deviceIds = deviceIds;
    this.times = times;
    this.measurementsList = measurementsList;
    this.typeList = typeList;
    this.valuesList = valuesList;
  }

  @Override
  public void run() {
    try {
      long start = System.currentTimeMillis();
      Date date = new Date();
      sessionPool.insertRecords(deviceIds, times, measurementsList, typeList, valuesList);

      long end = System.currentTimeMillis();
      /*if (end - start > 10000) {
        System.out.println("@<<" + date + " " + Thread.currentThread().getName() + " " + deviceIds.get(0) + " " + times.get(0));
        System.out.println("@<<" + new Date() + " " + Thread.currentThread().getName() + " " + deviceIds.get(0) + " " + times.get(0) + "  " + (end - start) + " OK");
      }*/
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
