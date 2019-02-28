/*
 *  Copyright (c) 2019  WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package org.wso2.extension.siddhi.io.googlepubsub.source;

import org.apache.log4j.Logger;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.source.Source;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.SiddhiTestHelper;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TestCaseOfGooglePubSubSource1 {

    private static Logger log = Logger.getLogger(TestCaseOfGooglePubSubSource1.class);
    private AtomicInteger count = new AtomicInteger(0);
    private AtomicInteger count1 = new AtomicInteger(0);
    private int waitTime = 50;
    private int timeout = 30000;
    private volatile boolean eventArrived;
    private volatile boolean eventArrived1;

    @BeforeMethod
    public void initBeforeMethod() {

        count.set(0);
        eventArrived = false;
    }

    /**
     * Test the ability to subscribe to a GooglePubSub topic and and receive messages.
     */
    @Test
    public void testGooglePubSubSourceEvent1() throws Exception {

        log.info("----------------------------------------------------------------------------");
        log.info("Test to receive messages by subscribing to a topic in google pub sub server.");
        log.info("----------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan1') "
                        + "define stream BarStream2 (message string); "
                        + "@info(name = 'query1') "
                        + "@source(type ='googlepubsub', "
                        + "credential.path = 'src/test/resources/security/sp.json',"
                        + "project.id = 'sp-path-1547649404768', "
                        + "topic.id = 'topic2', "
                        + "subscription.id = 'sub2', "
                        + "@map(type = 'text'))"
                        + "Define stream FooStream2 (message string);"
                        + "from FooStream2 select message insert into BarStream2;");
        siddhiAppRuntime.addCallback("BarStream2", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    log.info(event);
                    eventArrived = true;
                    count.incrementAndGet();
                }
            }
        });
        siddhiAppRuntime.start();
        TestPublisher testClient1 = new TestPublisher("sp-path-1547649404768", "topic2");
        testClient1.publish("How are you?");
        testClient1.publish("Hii");
        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        siddhiAppRuntime.shutdown();
        siddhiManager.shutdown();
    }

    /**
     * Test to configure that multiple subscribers can subscribe for a single topic and consume messages.
     */
    @Test
    public void testGooglePubSubSourceEvent2() throws Exception {

        log.info("--------------------------------------------------------------------------------------------------");
        log.info("Test to configure that multiple subscribers can subscribe for a single topic and consume messages.");
        log.info("--------------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan1') "
                        + "define stream BarStream2 (message string); "
                        + "@info(name = 'query1') "
                        + "@source(type ='googlepubsub', "
                        + "credential.path = 'src/test/resources/security/sp.json',"
                        + "project.id = 'sp-path-1547649404768', "
                        + "topic.id = 'topic4', "
                        + "subscription.id = 'sub4', "
                        + "@map(type = 'text'))"
                        + "Define stream FooStream2 (message string);"
                        + "from FooStream2 select message insert into BarStream2;");

        SiddhiAppRuntime siddhiAppRuntime1 = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan2') "
                        + "define stream BarStream2 (message string); "
                        + "@info(name = 'query1') "
                        + "@source(type ='googlepubsub', "
                        + "credential.path = 'src/test/resources/security/sp.json',"
                        + "project.id = 'sp-path-1547649404768', "
                        + "topic.id = 'topic4', "
                        + "subscription.id = 'sub44', "
                        + "@map(type = 'text'))"
                        + "Define stream FooStream2 (message string);"
                        + "from FooStream2 select message insert into BarStream2;");

        siddhiAppRuntime.addCallback("BarStream2", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    log.info(event);
                    eventArrived = true;
                    count.incrementAndGet();
                }
            }
        });
        siddhiAppRuntime1.addCallback("BarStream2", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                for (Event event : events) {
                    log.info(event);
                    eventArrived1 = true;
                    count1.incrementAndGet();
                }
            }
        });
        siddhiAppRuntime.start();
        siddhiAppRuntime1.start();
        TestPublisher testClient1 = new TestPublisher("sp-path-1547649404768", "topic4");
        testClient1.publish("Anne");
        testClient1.publish("John");
        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        SiddhiTestHelper.waitForEvents(waitTime, 2, count1, timeout);
        siddhiAppRuntime.shutdown();
        siddhiAppRuntime1.shutdown();
        siddhiManager.shutdown();

    }

    /**
     * If the subscription does not exist,google pub sub source creates a subscription and ingest messages coming to the
     * topic.
     */
    @Test
    public void testGooglePubSubSourceEvent3() throws Exception {

        log.info("--------------------------------------------------------------------------------------------");
        log.info("Test to receive messages by creating a new subscription to a topic in google pub sub server.");
        log.info("--------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan1') "
                        + "define stream BarStream2 (message string); "
                        + "@info(name = 'query1') "
                        + "@source(type ='googlepubsub', "
                        + "credential.path = 'src/test/resources/security/sp.json',"
                        + "project.id = 'sp-path-1547649404768', "
                        + "topic.id = 'topicA', "
                        + "subscription.id = 'subA8888811', "
                        + "@map(type = 'text'))"
                        + "Define stream FooStream2 (message string);"
                        + "from FooStream2 select message insert into BarStream2;");
        siddhiAppRuntime.addCallback("BarStream2", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    log.info(event);
                    eventArrived = true;
                    count.incrementAndGet();
                }
            }
        });
        siddhiAppRuntime.start();
        TestPublisher testClient1 = new TestPublisher("sp-path-1547649404768", "topicA");
        testClient1.publish("WSO2 2019");
        testClient1.publish("IBM");
        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        siddhiAppRuntime.shutdown();
        siddhiManager.shutdown();
    }


    /**
     * Test for configure the GooglePubSub Source with pausing and resuming functionality.
     */
    @Test
    public void testGooglePubSubSourcePause() throws Exception {

        log.info("--------------------------------------------------------------------------------");
        log.info("Test to configure Google Pub Sub Source with pausing and resuming functionality.");
        log.info("--------------------------------------------------------------------------------");
        log = Logger.getLogger(GooglePubSubSource.class);
        // deploying the execution plan
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan1') "
                        + "define stream BarStream2 (message string); "
                        + "@info(name = 'query1') "
                        + "@source(type ='googlepubsub', "
                        + "credential.path = 'src/test/resources/security/sp.json',"
                        + "project.id = 'sp-path-1547649404768', "
                        + "topic.id = 'topicB', "
                        + "subscription.id = 'subB', "
                        + "@map(type = 'text'))"
                        + "Define stream FooStream2 (message string);"
                        + "from FooStream2 select message insert into BarStream2;");
        Collection<List<Source>> sources = siddhiAppRuntime.getSources();
        siddhiAppRuntime.addCallback("BarStream2", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    log.info(event);
                    eventArrived = true;
                    count.incrementAndGet();
                }
            }
        });
        siddhiAppRuntime.start();
        TestPublisher testClient1 = new TestPublisher("sp-path-1547649404768", "topicB");

        // pause
        sources.forEach(e -> e.forEach(Source::pause));
        // send few events
        testClient1.publish("John");
        testClient1.publish("David");
        //resume
        sources.forEach(e -> e.forEach(Source::resume));
        // send few more events
        testClient1.publish("Marry");
        testClient1.publish("San");

        SiddhiTestHelper.waitForEvents(waitTime, 4, count, timeout);
        siddhiAppRuntime.shutdown();
        siddhiManager.shutdown();
    }

}


