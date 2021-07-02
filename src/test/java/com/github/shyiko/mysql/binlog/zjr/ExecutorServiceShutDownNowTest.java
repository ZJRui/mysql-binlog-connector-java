package com.github.shyiko.mysql.binlog.zjr;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @Author Sachin
 * @Date 2021/6/28
 **/
public class ExecutorServiceShutDownNowTest {


    static class Task implements Runnable {
        int flag = 0;

        public Task(int flag) {
            this.flag = flag;
        }

        public String state = "notStart";

        @Override
        public void run() {
            state = "running";
            System.out.println("第+" + flag + "任务开始执行");

            try {

                Thread.sleep(40 * 1000);
            } catch (Exception e) {
                System.out.println(e);
            }
            state = "finished";
            System.out.println("第+" + flag + "任务执行完成");
        }

        @Override
        public String toString() {
            return "Task{" +
                "flag=" + flag +
                ", state='" + state + '\'' +
                '}';
        }
    }


    public static void main(String[] args) throws Exception {

        ExecutorService executorService = Executors.newFixedThreadPool(8);

        for (int i = 0; i < 20; i++) {
            executorService.submit(new Task(i));
        }
        try {
            Thread.sleep(5000);
        } catch (InterruptedException interruptedException) {
            interruptedException.printStackTrace();
        }
        List<Runnable> runnables = executorService.shutdownNow();
        System.out.println("runnables.size:" + runnables.size());
        for (Runnable runnable : runnables) {
            System.out.println(runnable.toString());
        }
        //阻塞直到所有任务在关机请求后完成执行，或超时发生，或当前线程被中断(以先发生者为准)。
        while (!awaitTerminationInterruptibly(executorService, Long.MAX_VALUE, TimeUnit.NANOSECONDS)) { /* retry */ }
        System.out.println("main--------");
        System.in.read();


    }

    private static boolean awaitTerminationInterruptibly(final ExecutorService executorService,
                                                         final long timeout, final TimeUnit unit) {
        try {
            /**
             *  Blocks until all tasks have completed execution after a shutdown
             *      * request, or the timeout occurs, or the current thread is
             *      * interrupted, whichever happens first.
             *      阻塞直到所有任务在关机请求后完成执行，或超时发生，或当前线程被中断(以先发生者为准)。
             *      问题：awaitTermination是等待任务执行完成，这些任务是哪些任务： 已经开始的，还未开始的？
             *
             *      awaotTernation 会等待已经开始的任务执行完成，如果awaitTermination的返回时因为已经开始的任务执行完成而返回的则返回值为true
             */
            return executorService.awaitTermination(timeout, unit);
        } catch (InterruptedException e) {
            return false;
        }
    }
}
