package com.coen6731.chat.client;

import com.coen6731.chat.ClientEvent;
import com.coen6731.chat.HeartbeatPing;
import io.grpc.stub.StreamObserver;
import java.time.Instant;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class HeartbeatManager {
    private final ScheduledExecutorService scheduler;
    private final Runnable reconnectCallback;
    private final AtomicInteger missedPongs = new AtomicInteger(0);
    
    private ScheduledFuture<?> heartbeatTask;
    private ScheduledFuture<?> pongTimeoutTask;
    
    // We need a way to get the current request observer to send Pings
    private Supplier<StreamObserver<ClientEvent>> requestObserverSupplier;

    public HeartbeatManager(ScheduledExecutorService scheduler, Runnable reconnectCallback) {
        this.scheduler = scheduler;
        this.reconnectCallback = reconnectCallback;
    }

    public void setRequestObserverSupplier(Supplier<StreamObserver<ClientEvent>> supplier) {
        this.requestObserverSupplier = supplier;
    }

    public void start() {
        stop(); // Ensure no duplicates
        // missedPongs.set(0);
        
        // System.out.println("[client] Ping task started!");
        // Schedule Ping every 10 seconds
        heartbeatTask = scheduler.scheduleAtFixedRate(this::sendPing, 30, 10, TimeUnit.SECONDS);
    }

    public void stop() {
        // System.out.println("[client] Stopping PING/PONG task...");
        if (heartbeatTask != null) {
            heartbeatTask.cancel(true);
            heartbeatTask = null;
        }
        if (pongTimeoutTask != null) {
            pongTimeoutTask.cancel(true);
            pongTimeoutTask = null;
        }
    }

    public void handlePong() {
        // System.out.println("[client] received heartbeat Pong");
        missedPongs.set(0);
        if (pongTimeoutTask != null) {
            pongTimeoutTask.cancel(false);
            pongTimeoutTask = null;
        }
    }

    private void sendPing() {
        try {
            StreamObserver<ClientEvent> observer = (requestObserverSupplier != null) ? requestObserverSupplier.get() : null;
            
            if (observer != null) {
                // 1. Send Ping
                HeartbeatPing ping = HeartbeatPing.newBuilder().setTs(Instant.now().toEpochMilli()).build();
                observer.onNext(ClientEvent.newBuilder().setHeartbeatPing(ping).build());
                // System.out.println("[client] sent heartbeat Ping");

                // if no Pong received in next 5 seconds, missed +1. if missed >= 3, server died, trigger reconnect process "reconnectCallback".
                // if Pong received(handlePong), missed reset to 0.
                if (pongTimeoutTask != null) {
                    pongTimeoutTask.cancel(false);
                }
                pongTimeoutTask = scheduler.schedule(this::handlePongTimeout, 10, TimeUnit.SECONDS);
                // System.out.println("[client] Pong task started.");
            }
        } catch (Exception e) {
            System.out.println("[client] Heartbeat failed: " + e.getMessage());
            reconnectCallback.run();
        }
    }

    private void handlePongTimeout() {
        int missed = missedPongs.incrementAndGet();
        System.out.println("[client] handlePongTimeout(), Missed: " + missed + "/3");
        if (missed >= 3) {
            System.out.println("[client] handlePongTimeout(), 3 Strikes! reconnect");
            reconnectCallback.run();
        }
    }

    public Boolean TaskIsRunning() {
        return (heartbeatTask != null && !heartbeatTask.isCancelled()) || (pongTimeoutTask != null && !pongTimeoutTask.isCancelled());
    }

    public Boolean isThreeStrikes() {
        return missedPongs.get() >= 3;
    }

    public void resetMissedPongs() {
        missedPongs.set(0);
    }
}
