/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.remoting.rpc.common;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.remoting.AsyncContext;
import com.alipay.remoting.BizContext;
import com.alipay.remoting.NamedThreadFactory;
import com.alipay.remoting.rpc.protocol.AsyncUserProcessor;

/**
 * a demo aysnc user processor for rpc client
 * 
 * @author xiaomin.cxm
 * @version $Id: SimpleClientUserProcessor.java, v 0.1 Jan 7, 2016 3:01:49 PM xiaomin.cxm Exp $
 */
public class AsyncClientUserProcessor extends AsyncUserProcessor<RequestBody> {

    /** logger */
    private static final Logger logger      = LoggerFactory
        .getLogger(AsyncClientUserProcessor.class);

    /** delay milliseconds */
    private long                delayMs;

    /** whether delay or not */
    private boolean             delaySwitch;

    /** whether exception */
    private boolean             isException;

    /** whether null */
    private boolean             isNull;

    /** executor */
    private ThreadPoolExecutor  executor;

    private ThreadPoolExecutor  asyncExecutor;

    private AtomicInteger       invokeTimes = new AtomicInteger();

    public AsyncClientUserProcessor() {
        this.delaySwitch = false;
        this.isException = false;
        this.isNull = false;
        this.delayMs = 0;
        this.executor = new ThreadPoolExecutor(1, 3, 60, TimeUnit.SECONDS,
            new ArrayBlockingQueue<Runnable>(4), new NamedThreadFactory("Request-process-pool"));
        this.asyncExecutor = new ThreadPoolExecutor(1, 3, 60, TimeUnit.SECONDS,
            new ArrayBlockingQueue<Runnable>(4),
            new NamedThreadFactory("Another-aysnc-process-pool"));
    }

    public AsyncClientUserProcessor(boolean isException, boolean isNull) {
        this();
        this.isException = isException;
        this.isNull = isNull;
    }

    public AsyncClientUserProcessor(long delay) {
        this();
        if (delay < 0) {
            throw new IllegalArgumentException("delay time illegal!");
        }
        this.delaySwitch = true;
        this.delayMs = delay;
    }

    public AsyncClientUserProcessor(long delay, int core, int max, int keepaliveSeconds,
                                    int workQueue) {
        this(delay);
        this.executor = new ThreadPoolExecutor(core, max, keepaliveSeconds, TimeUnit.SECONDS,
            new ArrayBlockingQueue<Runnable>(workQueue),
            new NamedThreadFactory("Request-process-pool"));
    }

    @Override
    public void handleRequest(BizContext bizCtx, AsyncContext asyncCtx, RequestBody request) {
        this.asyncExecutor.execute(new InnerTask(asyncCtx, request));
    }

    @Override
    public String interest() {
        return RequestBody.class.getName();
    }

    @Override
    public Executor getExecutor() {
        return executor;
    }

    public int getInvokeTimes() {
        return this.invokeTimes.get();
    }

    class InnerTask implements Runnable {
        private AsyncContext asyncCtx;
        private RequestBody  request;

        public InnerTask(AsyncContext asyncCtx, RequestBody request) {
            this.asyncCtx = asyncCtx;
            this.request = request;
        }

        public void run() {
            logger.warn("Request received:" + request);
            Assert.assertEquals(RequestBody.class, request.getClass());
            invokeTimes.incrementAndGet();
            if (isException) {
                this.asyncCtx.sendResponse(new IllegalArgumentException("Exception test"));
            } else if (isNull) {
                this.asyncCtx.sendResponse(null);
            } else {
                if (!delaySwitch) {
                    this.asyncCtx.sendResponse(RequestBody.DEFAULT_CLIENT_RETURN_STR);
                    return;
                }
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                this.asyncCtx.sendResponse(RequestBody.DEFAULT_CLIENT_RETURN_STR);
            }
        }
    }
}
