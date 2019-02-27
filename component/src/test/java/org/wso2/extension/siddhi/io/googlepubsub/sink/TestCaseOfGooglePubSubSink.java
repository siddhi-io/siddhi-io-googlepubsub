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
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

public class TestCaseOfGooglePubSubSink {

    private static Logger log = Logger.getLogger(TestCaseOfGooglePubSubSink.class);
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
                        + "topic.id = 'topickd31221', "
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

    /**
     * If a property(project.id, topic.id, file.name) which defined as mandatory in the extension definition is missing,
     * then{@link SiddhiAppValidationException} will be thrown.
     */
    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void googlePubSubPublishWithoutMandatoryPropertyTest() {

        log.info("-----------------------------------------------------------");
        log.info("Test to publish messages when missing a mandatory property.");
        log.info("-----------------------------------------------------------");
        // deploying the execution plan
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "define stream FooStream (message string); " + "@info(name = 'query1') "
                        + "@sink(type='googlepubsub', "
                        + "topic.id = 'topicJ', "
                        + "credential.path = 'src/test/resources/security/sp.json',"
                        + "@map(type='text'))"
                        + "Define stream BarStream (message string);"
                        + "from FooStream select message insert into BarStream;");
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    /**
     * If the file that contains service account credentials is not specified correctly, then
     * {@link SiddhiAppCreationException} will be thrown.
     */
    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void googlePubSubPublishWithoutServiceAccountCredentials() {

        log.info("------------------------------------------------------------------------------------------------");
        log.info("Test to publish messages without specifying the service account credentials file name correctly.");
        log.info("------------------------------------------------------------------------------------------------");
        // deploying the execution plan
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "define stream FooStream (message string); " + "@info(name = 'query1') "
                        + "@sink(type='googlepubsub', "
                        + "topic.id = 'topicD', "
                        + "credential.path = 'src/test/resources/security/sp',"
                        + "project.id = 'sp-path-1547649404768', "
                        + "@map(type='text'))"
                        + "Define stream BarStream (message string);"
                        + "from FooStream select message insert into BarStream;");
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    /**
     * Test to configure if user is not permitted to be authenticated, then
     * an error will be thrown.
     */
    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void googlePubSubPublishWithoutPermission() {

        log.info("------------------------------------------------------------------------------");
        log.info("Test to publish messages to a project without enabling billing in the project.");
        log.info("------------------------------------------------------------------------------");
        // deploying the execution plan
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "define stream FooStream (message string); " + "@info(name = 'query1') "
                        + "@sink(type='googlepubsub', "
                        + "topic.id = 'topicD', "
                        + "credential.path = 'src/test/resources/security/Blue Eye-3d5d8a888785.json',"
                        + "project.id = 'sp-path-1547649404768', "
                        + "@map(type='text'))"
                        + "Define stream BarStream (message string);"
                        + "from FooStream select message insert into BarStream;");
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    /**
     * Test to configure the GooglePubSub Sink publishes messages to a topic in a non existing project in
     * Google Pub Sub server.
     */
    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void googlePubSubSimplePublishTest3() throws InterruptedException {

        log.info("-----------------------------------------------------------------");
        log.info("Test to publish messages to a non-existing project in the server.");
        log.info("-----------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        // deploying the execution plan
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "define stream FooStream (message string); " + "@info(name = 'query1') "
                        + "@sink(type='googlepubsub', "
                        + "topic.id = 'topicD', "
                        + "credential.path = 'src/test/resources/security/sp.json',"
                        + "project.id = 'bvjj04768', "
                        + "@map(type='text'))"
                        + "Define stream BarStream (message string);"
                        + "from FooStream select message insert into BarStream;");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        fooStream.send(new Object[]{"World"});
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    /**
     * Test to configure the GooglePubSub Sink publishes messages to a topic by specifying project.id as empty
     */
    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void googlePubSubSimplePublishTest4() throws InterruptedException {

        log.info("-----------------------------------------------------------");
        log.info("Test to publish messages by specifying project.id as empty.");
        log.info("-----------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        // deploying the execution plan
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "define stream FooStream (message string); " + "@info(name = 'query1') "
                        + "@sink(type='googlepubsub', "
                        + "topic.id = 'topicD', "
                        + "credential.path = 'src/test/resources/security/sp.json',"
                        + "project.id = '', "
                        + "@map(type='text'))"
                        + "Define stream BarStream (message string);"
                        + "from FooStream select message insert into BarStream;");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        fooStream.send(new Object[]{"World"});
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }
}
