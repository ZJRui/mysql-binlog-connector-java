package com.github.shyiko.mysql.binlog.zjr;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * @Author Sachin
 * @Date 2021/6/27
 **/
public class BinaryLogClientTest {

    private static LinkedBlockingQueue queue = new LinkedBlockingQueue<>();

    public static void main(String[] args) {

        BinlogTask binlogTask = new BinlogTask(queue);
        Thread thread = new Thread(binlogTask);
        thread.setName("task-for-read-binlog");
        thread.start();


    }


    public void testConnectInMainThread() {


        BinaryLogClient binaryLogClient;
        binaryLogClient = new BinaryLogClient("localhost", 3306, "root", "root");

        binaryLogClient.registerEventListener(event -> {
            EventData data = event.getData();
            //Listener 的执行和connect都处于main线程中
            System.out.println("ThreadName:" + Thread.currentThread().getName());
            System.out.println(event.toString());

            if (data instanceof UpdateRowsEventData) {
                System.out.println(" UpdateEventData: " + data.toString());
            }
            if (data instanceof WriteRowsEventData) {
                System.out.println("WriteEventData:" + data.toString());
            }
            if (data instanceof DeleteRowsEventData) {
                System.out.println("DeleteRowEventData:" + data.toString());
            }
        });

        try {
            binaryLogClient.connect();
//            binaryLogClient.connect(1000000);
        } catch (Exception ioException) {
            ioException.printStackTrace();
        }

    }

    public void testConnectInAnotherTherad() {


        BinaryLogClient binaryLogClient;
        binaryLogClient = new BinaryLogClient("localhost", 3306, "root", "root");

        binaryLogClient.registerEventListener(event -> {
            EventData data = event.getData();
            //Listener的执行在另外的一个线程中 ThreadName:blc-localhost:3306
            System.out.println("ThreadName:" + Thread.currentThread().getName());
            System.out.println(event.toString());

            if (data instanceof UpdateRowsEventData) {
                System.out.println(" UpdateEventData: " + data.toString());
            }
            if (data instanceof WriteRowsEventData) {
                System.out.println("WriteEventData:" + data.toString());
            }
            if (data instanceof DeleteRowsEventData) {
                System.out.println("DeleteRowEventData:" + data.toString());
            }
        });

        try {
            //Listener的执行在另外的一个线程中 ThreadName:blc-localhost:3306
            binaryLogClient.connect(1000000);
        } catch (Exception ioException) {
            ioException.printStackTrace();
        }

    }
}
