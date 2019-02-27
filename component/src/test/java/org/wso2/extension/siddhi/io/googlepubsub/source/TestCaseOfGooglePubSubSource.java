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
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.input.source.Source;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.SiddhiTestHelper;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TestCaseOfGooglePubSubSource {

    private static Logger log = Logger.getLogger(TestCaseOfGooglePubSubSource.class);
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
                        + "subscription.id = 'subA18538611', "
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
     * If a property missing which defined as mandatory in the extension definition, then
     * {@link SiddhiAppValidationException} will be thrown.
     */
    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void testMissingGooglePubSubMandatoryProperty() {

        log.info("----------------------------------------------------------------------------");
        log.info("Test to configure the google pub sub source when missing mandatory property.");
        log.info("----------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan1') "
                        + "define stream BarStream2 (message string); "
                        + "@info(name = 'query1') "
                        + "@source(type ='googlepubsub', "
                        + "credential.path = 'src/test/resources/security/sp.json',"
                        + "project.id = 'sp-path-1547649404768', "
                        + "topic.id = 'topicJ', "
                        + "@map(type = 'text'))"
                        + "Define stream FooStream2 (message string);"
                        + "from FooStream2 select message insert into BarStream2;");
        siddhiAppRuntime.shutdown();
        siddhiManager.shutdown();
    }

    /**
     * If a user try to subscribe for a topic in a non-existing project in the google pub sub server an error may
     * thrown.
     */
    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testGooglePubSubSourceEvent4() {

        log.info("---------------------------------------------------------------------------------------------------");
        log.info("Test to receive messages by subscribing to topic in an unavailable project of google pubsub server.");
        log.info("---------------------------------------------------------------------------------------------------");
        log = Logger.getLogger(GooglePubSubSource.class);
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan1') "
                        + "define stream BarStream2 (message string); "
                        + "@info(name = 'query1') "
                        + "@source(type ='googlepubsub', "
                        + "credential.path = 'src/test/resources/security/sp.json',"
                        + "project.id = 'sxxxxx04768', "
                        + "topic.id = 'topicA', "
                        + "subscription.id = 'subA1', "
                        + "@map(type = 'text'))"
                        + "Define stream FooStream2 (message string);"
                        + "from FooStream2 select message insert into BarStream2;");
        siddhiAppRuntime.shutdown();
        siddhiManager.shutdown();

    }

    /*
     * If a user try to subscribe for a topic by giving a empty project.id an error may thrown.
     */
    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testGooglePubSubSourceEvent5() {

        log.info("------------------------------------------------------------------------------------------");
        log.info("Test to receive messages by subscribing to topic in by specifying the project id as empty.");
        log.info("------------------------------------------------------------------------------------------");
        log = Logger.getLogger(GooglePubSubSource.class);
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan1') "
                        + "define stream BarStream2 (message string); "
                        + "@info(name = 'query1') "
                        + "@source(type ='googlepubsub', "
                        + "credential.path = 'src/test/resources/security/sp.json',"
                        + "project.id = '', "
                        + "topic.id = 'topicA', "
                        + "subscription.id = 'subA1', "
                        + "@map(type = 'text'))"
                        + "Define stream FooStream2 (message string);"
                        + "from FooStream2 select message insert into BarStream2;");
        siddhiAppRuntime.shutdown();
        siddhiManager.shutdown();

    }

    /**
     * If a user try to subscribe for a non existing topic in the google pub sub server an error may
     * thrown.
     */
    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testGooglePubSubSourceEvent6() {

        log.info("----------------------------------------------------------------------------------------");
        log.info("Test to receive messages by subscribing to a non-existing topic in google pubsub server.");
        log.info("----------------------------------------------------------------------------------------");
        log = Logger.getLogger(GooglePubSubSource.class);
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan1') "
                        + "define stream BarStream2 (message string); "
                        + "@info(name = 'query1') "
                        + "@source(type ='googlepubsub', "
                        + "credential.path = 'src/test/resources/security/sp.json',"
                        + "project.id = 'sp-path-1547649404768', "
                        + "topic.id = 'topicX', "
                        + "subscription.id = 'subX', "
                        + "@map(type = 'text'))"
                        + "Define stream FooStream2 (message string);"
                        + "from FooStream2 select message insert into BarStream2;");
        siddhiAppRuntime.shutdown();
        siddhiManager.shutdown();
    }

    /**
     * If a user tries to subscribe for a topic without providing the file name of the
     * service account credentials correctly and error may thrown.
     */
    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testGooglePubSubSourceEvent7() {

        log.info("---------------------------------------------------------------------------------------------------");
        log.info("Test to receive messages without giving the file name of the service account credentials correctly.");
        log.info("---------------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan1') "
                        + "define stream BarStream2 (message string); "
                        + "@info(name = 'query1') "
                        + "@source(type ='googlepubsub', "
                        + "credential.path = 'src/test/resources/security/sp',"
                        + "project.id = 'sp-path-1547649404768', "
                        + "topic.id = 'topicB', "
                        + "subscription.id = 'subB', "
                        + "@map(type = 'text'))"
                        + "Define stream FooStream2 (message string);"
                        + "from FooStream2 select message insert into BarStream2;");
        siddhiAppRuntime.shutdown();
        siddhiManager.shutdown();
    }

    /**
     * If a user tries to subscribe for a topic without enabling billing in the project(without having permission),
     * then {@link SiddhiAppCreationException} will be thrown.
     */
    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testGooglePubSubSourceEvent8() {

        log.info("------------------------------------------------------------");
        log.info("Test to receive messages without a permitted authentication.");
        log.info("------------------------------------------------------------");
        log = Logger.getLogger(GooglePubSubSource.class);
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan1') "
                        + "define stream BarStream2 (message string); "
                        + "@info(name = 'query1') "
                        + "@source(type ='googlepubsub', "
                        + "credential.path = 'src/test/resources/security/Blue Eye-3d5d8a888785.json',"
                        + "project.id = 'sp-path-1547649404768', "
                        + "topic.id = 'topicB', "
                        + "subscription.id = 'subB', "
                        + "@map(type = 'text'))"
                        + "Define stream FooStream2 (message string);"
                        + "from FooStream2 select message insert into BarStream2;");
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


