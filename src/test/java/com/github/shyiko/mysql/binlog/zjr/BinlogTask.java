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
public class BinlogTask implements Runnable {

    private volatile boolean running = true;
    private final LinkedBlockingQueue queue;

    public BinlogTask(LinkedBlockingQueue linkedBlockingQueue) {
        this.queue = linkedBlockingQueue;
    }

    public void stop() {
        this.running = false;
    }

    @Override
    public void run() {

        BinaryLogClient binaryLogClient;
        binaryLogClient = new BinaryLogClient("localhost", 3306, "root", "root");
        binaryLogClient.registerEventListener(event -> {
            EventData data = event.getData();
            System.out.println("ThreadName:" + Thread.currentThread().getName());
            if (data == null) {
                return;
            }
            System.out.println("将数据放入到队列：" + data.toString());
            try {
                queue.put(data);
            } catch (InterruptedException interruptedException) {
                interruptedException.printStackTrace();
            }
        });

        try {
            //启动另外一个线程读取binlog数据，上面注册了一个listener ，将这个listener接收到的数据放置到队列中.然后在当前线程中 从队列中取出数据
            //问题就是： 当mysql重启的时候，导致connect启动的那个线程死掉了，然后当前线程就无法从队列中取出数据了。
            //此时mysql重启之后 当前线程还能收到消息吗？答案是可以的，因为BinaryLogClient内部在connect的时候会创建一个线程池任务，该任务就是
            //不断向mysql发送ping消息，如果发送消息的时候出现了异常，则会重新connect 启动另外一个线程读取binlog
            binaryLogClient.connect(10000*60);
        } catch (Exception ioException) {
            ioException.printStackTrace();
        }

        while (running) {
            try {
                Object data = queue.take();
                System.out.println("从队列中取出数据:" + data.toString());
                if (data instanceof UpdateRowsEventData) {
                    System.out.println("  UpdateEventData: " + data.toString());
                }
                if (data instanceof WriteRowsEventData) {
                    System.out.println("WriteEventData:" + data.toString());
                }
                if (data instanceof DeleteRowsEventData) {
                    System.out.println("DeleteRowEventData:" + data.toString());
                }
            } catch (InterruptedException interruptedException) {
                interruptedException.printStackTrace();
                System.out.println("退出线程");
                return;
            }

        }//end while

    }//end run

}

