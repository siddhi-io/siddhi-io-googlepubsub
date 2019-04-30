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

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.io.googlepubsub.util.UnitTestAppender;

public class TestCaseOfGooglePubSubSink2 {

    private static Logger log = Logger.getLogger(TestCaseOfGooglePubSubSink1.class);
    private volatile int count;
    private volatile boolean eventArrived;

    @BeforeMethod
    public void initBeforeMethod() {

        count = 0;
        eventArrived = false;
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
     * Test to configure the GooglePubSub Sink publishes messages to a topic in a non existing project in
     * Google Pub Sub server.
     */
    @Test
    public void googlePubSubSimplePublishTest3() throws InterruptedException {

        log.info("-----------------------------------------------------------------");
        log.info("Test to publish messages to a non-existing project in the server.");
        log.info("-----------------------------------------------------------------");
        log = Logger.getLogger(GooglePubSubSink.class);
        UnitTestAppender testAppender = new UnitTestAppender();
        log.addAppender(testAppender);
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
        AssertJUnit.assertTrue(testAppender
                .getMessages()
                .contains("An error is caused due to a resource NOT_FOUND in Google Pub Sub server"));
        siddhiAppRuntime.shutdown();
    }

    /**
     * Test to configure the GooglePubSub Sink publishes messages to a topic by specifying project.id as empty
     */
    @Test
    public void googlePubSubSimplePublishTest4() throws InterruptedException {

        log.info("-----------------------------------------------------------");
        log.info("Test to publish messages by specifying project.id as empty.");
        log.info("-----------------------------------------------------------");
        log = Logger.getLogger(GooglePubSubSink.class);
        UnitTestAppender testAppender = new UnitTestAppender();
        log.addAppender(testAppender);
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
        AssertJUnit.assertTrue(testAppender
                .getMessages()
                .contains("An error is caused due to a resource NOT_FOUND in Google Pub Sub server"));
        siddhiAppRuntime.shutdown();
    }
}
