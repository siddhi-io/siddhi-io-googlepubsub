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
package org.wso2.extension.siddhi.io.googlepubsub.sink;

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.io.googlepubsub.util.ResultContainer;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;;

public class TestCaseOfGooglePubSubSink1 {

    private static Logger log = Logger.getLogger(TestCaseOfGooglePubSubSink1.class);
    private volatile int count;
    private volatile boolean eventArrived;

    @BeforeMethod
    public void initBeforeMethod() {

        count = 0;
        eventArrived = false;
    }

    /**
     * Test to configure the GooglePubSub Sink publishes messages to a topic in  GooglePubSub server.
     */
    @Test
    public void googlePubSubSimplePublishTest1() {

        log.info("------------------------------------------------------------------------------------------");
        log.info("Test to configure GooglePubSubSink publish messages to a topic in the GooglePubSub Server.");
        log.info("------------------------------------------------------------------------------------------");
        ResultContainer resultContainer = new ResultContainer(2, 3);
        TestSubscriber testClient = new TestSubscriber("sp-path-1547649404768", "topicD",
                "subD", resultContainer);
        // deploying the execution plan
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "define stream FooStream (message string); " + "@info(name = 'query1') "
                        + "@sink(type='googlepubsub', "
                        + "topic.id = 'topicD', "
                        + "credential.path = 'src/test/resources/security/sp.json',"
                        + "project.id = 'sp-path-1547649404768', "
                        + "@map(type='text'))"
                        + "Define stream BarStream (message string);"
                        + "from FooStream select message insert into BarStream;");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    log.info(event);
                    eventArrived = true;
                    count++;
                }
            }
        });
        testClient.consumer();
        siddhiAppRuntime.start();
        try {
            fooStream.send(new Object[]{"Maths"});
            fooStream.send(new Object[]{"Stats"});
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            AssertJUnit.fail("Thread sleep was interrupted");
        }
        count = testClient.getCount();
        eventArrived = testClient.getEventArrived();
        AssertJUnit.assertEquals(2, count);
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertTrue(resultContainer.assertMessageContent("Maths"));
        AssertJUnit.assertTrue(resultContainer.assertMessageContent("Stats"));
        siddhiAppRuntime.shutdown();
        testClient.shutdown();
    }

    /**
     * Test to configure ,if the topic does not exist in the server, GooglePubSub Sink creates a topic and publish
     * messages to that topic in the GooglePubSub server.
     */
    @Test
    public void googlePubSubSimplePublishTest2() throws InterruptedException {

        log.info("----------------------------------------------------------------------------------------------");
        log.info("If a topic does not exist in the server,GooglePubSubSink creates a topic and publish messages.");
        log.info("----------------------------------------------------------------------------------------------");
        // deploying the execution plan
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "define stream FooStream (message string); " + "@info(name = 'query1') "
                        + "@sink(type='googlepubsub', "
                        + "topic.id = 'topickd4444021', "
                        + "credential.path = 'src/test/resources/security/sp.json',"
                        + "project.id = 'sp-path-1547649404768', "
                        + "@map(type='text'))"
                        + "Define stream BarStream (message string);"
                        + "from FooStream select message insert into BarStream;");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    log.info(event);
                    eventArrived = true;
                    count++;
                }
            }
        });
        siddhiAppRuntime.start();
        fooStream.send(new Object[]{"University of Jaffna"});
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }
}
