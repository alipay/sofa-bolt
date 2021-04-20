/*
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
package com.alipay.remoting.rpc.exception;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.remoting.ConnectionEventType;
import com.alipay.remoting.InvokeCallback;
import com.alipay.remoting.exception.RemotingException;
import com.alipay.remoting.rpc.RpcClient;
import com.alipay.remoting.rpc.RpcResponseFuture;
import com.alipay.remoting.rpc.common.AsyncExceptionUserProcessor;
import com.alipay.remoting.rpc.common.BoltServer;
import com.alipay.remoting.rpc.common.CONNECTEventProcessor;
import com.alipay.remoting.rpc.common.ExceptionUserProcessor;
import com.alipay.remoting.rpc.common.NullUserProcessor;
import com.alipay.remoting.rpc.common.PortScan;
import com.alipay.remoting.rpc.common.RequestBody;
import com.alipay.remoting.rpc.common.SimpleServerUserProcessor;

/**
 * exception test
 *
 * @author xiaomin.cxm
 * @version $Id: ExceptionTest.java, v 0.1 Apr 6, 2016 9:41:53 PM xiaomin.cxm Exp $
 */
public class ExceptionTest {
    static Logger logger      = LoggerFactory.getLogger(ExceptionTest.class);

    BoltServer    server;
    RpcClient     client;

    int           port        = PortScan.select();
    String        addr        = "127.0.0.1:" + port;
    int           invokeTimes = 5;

    @Before
    public void init() {
        server = new BoltServer(port);
        server.start();
        server
            .addConnectionEventProcessor(ConnectionEventType.CONNECT, new CONNECTEventProcessor());
        client = new RpcClient();
        client.init();
    }

    @After
    public void stop() {
        try {
            server.stop();
            Thread.sleep(100);
        } catch (InterruptedException e) {
            logger.error("Stop server failed!", e);
        }
    }

    
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    
    @Test
    public void testSyncNoProcessor() {
        server.registerUserProcessor(new SimpleServerUserProcessor());
        thrown.expect(InvokeServerException.class);        
        client.invokeSync(addr, "No processor for String now!", 3000);
    }

    @Test
    public void testProcessorReturnNull() {
        server.registerUserProcessor(new NullUserProcessor());
        RequestBody req = new RequestBody(4, "hello world");
        try {
            Object obj = client.invokeSync(addr, req, 3000);
            Assert.assertNull(obj);
        } catch (RemotingException e) {
            String errMsg = "RemotingException caught in testProcessorReturnNull!";
            logger.error(errMsg, e);
            Assert.fail(errMsg);
        } catch (InterruptedException e) {
            String errMsg = "InterruptedException caught in testProcessorReturnNull!";
            logger.error(errMsg, e);
            Assert.fail(errMsg);
        }
    }

    @Test
    public void testFutureWait0() {
        server.registerUserProcessor(new SimpleServerUserProcessor());
        RequestBody req = new RequestBody(4, "hello world future");

        Object res = null;
        try {
            RpcResponseFuture future = client.invokeWithFuture(addr, req, 1000);
            res = future.get(0);
            Assert.fail("Should not reach here!");
        } catch (InvokeTimeoutException e) {
            Assert.assertNull(res);
        } catch (RemotingException e) {
            String errMsg = "RemotingException caught in testFutureWait0!";
            logger.error(errMsg, e);
            Assert.fail(errMsg);
        } catch (InterruptedException e) {
            String errMsg = "InterruptedException caught in testFutureWait0!";
            logger.error(errMsg, e);
            Assert.fail(errMsg);
        }
    }

    @Test
    public void testFutureWaitShort() {
        server.registerUserProcessor(new SimpleServerUserProcessor(100));
        RequestBody req = new RequestBody(4, "hello world future");

        Object res = null;
        try {
            RpcResponseFuture future = client.invokeWithFuture(addr, req, 1000);
            res = future.get(10);
            Assert.fail("Should not reach here!");
        } catch (InvokeTimeoutException e) {
            Assert.assertNull(res);
        } catch (RemotingException e) {
            String errMsg = "RemotingException caught in testFutureWaitShort!";
            logger.error(errMsg, e);
            Assert.fail(errMsg);
        } catch (InterruptedException e) {
            String errMsg = "InterruptedException caught in testFutureWaitShort!";
            logger.error(errMsg, e);
            Assert.fail(errMsg);
        }
    }

    @Test
    public void testRegisterDuplicateProcessor() {
        server.registerUserProcessor(new SimpleServerUserProcessor());
        try {
            server.registerUserProcessor(new SimpleServerUserProcessor());
            String errMsg = "Can not register duplicate processor, should throw exception here";
            logger.error(errMsg);
            Assert.fail(errMsg);
        } catch (Exception e) {
            Assert.assertEquals(RuntimeException.class, e.getClass());
        }
        client.registerUserProcessor(new SimpleServerUserProcessor());
        try {
            client.registerUserProcessor(new SimpleServerUserProcessor());
            String errMsg = "Can not register duplicate processor, should throw exception here";
            logger.error(errMsg);
            Assert.fail(errMsg);
        } catch (Exception e) {
            Assert.assertEquals(RuntimeException.class, e.getClass());
        }
    }

    @Test
    public void testSyncException() {
        server.registerUserProcessor(new ExceptionUserProcessor());
        RequestBody b1 = new RequestBody(1, "Hello world!");
        try {
            client.invokeSync(addr, b1, 3000);
            String errMsg = "Should throw InvokeServerException!";
            logger.error(errMsg);
            Assert.fail(errMsg);
        } catch (InvokeServerException e) {
            Assert.assertTrue(true);
        } catch (RemotingException e) {
            String errMsg = "RemotingException in testSyncException ";
            logger.error(errMsg);
            Assert.fail(errMsg);
        } catch (InterruptedException e) {
            String errMsg = "InterruptedException in testSyncException ";
            logger.error(errMsg);
            Assert.fail(errMsg);
        }
    }

    @Test
    public void testSyncException1() {
        server.registerUserProcessor(new AsyncExceptionUserProcessor());
        RequestBody b1 = new RequestBody(1, "Hello world!");
        try {
            client.invokeSync(addr, b1, 3000);
            String errMsg = "Should throw InvokeServerException!";
            logger.error(errMsg);
            Assert.fail(errMsg);
        } catch (InvokeServerException e) {
            Assert.assertTrue(true);
        } catch (RemotingException e) {
            String errMsg = "RemotingException in testSyncException ";
            logger.error(errMsg);
            Assert.fail(errMsg);
        } catch (InterruptedException e) {
            String errMsg = "InterruptedException in testSyncException ";
            logger.error(errMsg);
            Assert.fail(errMsg);
        }
    }

    @Test
    public void testFutureException() {
        server.registerUserProcessor(new ExceptionUserProcessor());
        RequestBody b1 = new RequestBody(1, "Hello world!");
        try {
            RpcResponseFuture future = client.invokeWithFuture(addr, b1, 1000);
            future.get(1500);
            String errMsg = "Should throw InvokeServerException!";
            logger.error(errMsg);
            Assert.fail(errMsg);
        } catch (InvokeServerException e) {
            Assert.assertTrue(true);
        } catch (RemotingException e) {
            String errMsg = "RemotingException in testFutureException ";
            logger.error(errMsg);
            Assert.fail(errMsg);
        } catch (InterruptedException e) {
            String errMsg = "InterruptedException in testFutureException ";
            logger.error(errMsg);
            Assert.fail(errMsg);
        }
    }

    @Test
    public void testFutureException1() {
        server.registerUserProcessor(new AsyncExceptionUserProcessor());
        RequestBody b1 = new RequestBody(1, "Hello world!");
        try {
            RpcResponseFuture future = client.invokeWithFuture(addr, b1, 1000);
            future.get(1500);
            String errMsg = "Should throw InvokeServerException!";
            logger.error(errMsg);
            Assert.fail(errMsg);
        } catch (InvokeServerException e) {
            Assert.assertTrue(true);
        } catch (RemotingException e) {
            String errMsg = "RemotingException in testFutureException ";
            logger.error(errMsg);
            Assert.fail(errMsg);
        } catch (InterruptedException e) {
            String errMsg = "InterruptedException in testFutureException ";
            logger.error(errMsg);
            Assert.fail(errMsg);
        }
    }

    @Test
    public void testCallBackException() throws InterruptedException {
        server.registerUserProcessor(new ExceptionUserProcessor());
        RequestBody b3 = new RequestBody(3, "Hello world!");
        final CountDownLatch latch = new CountDownLatch(1);
        final List<Throwable> ret = new ArrayList<Throwable>(1);
        try {
            client.invokeWithCallback(addr, b3, new InvokeCallback() {

                @Override
                public void onResponse(Object result) {
                    Assert.fail("Should not reach here!");
                }

                @Override
                public void onException(Throwable e) {
                    logger.error("Error in callback", e);
                    ret.add(e);
                    latch.countDown();
                }

                @Override
                public Executor getExecutor() {
                    return null;
                }

            }, 1000);

        } catch (InvokeServerException e) {
            Assert.assertTrue(true);
        } catch (RemotingException e) {
            String errMsg = "InterruptedException in testCallBackException";
            logger.error(errMsg);
            Assert.fail(errMsg);
        }
        latch.await();
        Assert.assertEquals(InvokeServerException.class, ret.get(0).getClass());
    }

    @Test
    public void testCallBackException1() throws InterruptedException {
        server.registerUserProcessor(new AsyncExceptionUserProcessor());
        RequestBody b3 = new RequestBody(3, "Hello world!");
        final CountDownLatch latch = new CountDownLatch(1);
        final List<Throwable> ret = new ArrayList<Throwable>(1);
        try {
            client.invokeWithCallback(addr, b3, new InvokeCallback() {

                @Override
                public void onResponse(Object result) {
                    Assert.fail("Should not reach here!");
                }

                @Override
                public void onException(Throwable e) {
                    logger.error("Error in callback", e);
                    ret.add(e);
                    latch.countDown();
                }

                @Override
                public Executor getExecutor() {
                    return null;
                }

            }, 1000);

        } catch (InvokeServerException e) {
            Assert.assertTrue(true);
        } catch (RemotingException e) {
            String errMsg = "InterruptedException in testCallBackException";
            logger.error(errMsg);
            Assert.fail(errMsg);
        }
        latch.await();
        Assert.assertEquals(InvokeServerException.class, ret.get(0).getClass());
    }
}