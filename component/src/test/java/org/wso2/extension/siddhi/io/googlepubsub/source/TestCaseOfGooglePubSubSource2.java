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
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.io.googlepubsub.util.UnitTestAppender;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.concurrent.atomic.AtomicInteger;

public class TestCaseOfGooglePubSubSource2 {

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
    @Test
    public void testGooglePubSubSourceEvent4() {

     log.info("---------------------------------------------------------------------------------------------------");
     log.info("Test to receive messages by subscribing to topic in an unavailable project of google pubsub server.");
     log.info("---------------------------------------------------------------------------------------------------");
     log = Logger.getLogger(GooglePubSubSource.class);
     UnitTestAppender appender = new UnitTestAppender();
     log.addAppender(appender);
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
     siddhiAppRuntime.start();
     AssertJUnit.assertTrue(appender.getMessages().contains("Error in connecting to the resources at "));
     siddhiAppRuntime.shutdown();
    }

    /**
     * If a user try to subscribe for a non existing topic in the google pub sub server an error may
     * thrown.
     */
    @Test
    public void testGooglePubSubSourceEvent6() {

        log.info("----------------------------------------------------------------------------------------");
        log.info("Test to receive messages by subscribing to a non-existing topic in google pubsub server.");
        log.info("----------------------------------------------------------------------------------------");
        log = Logger.getLogger(GooglePubSubSource.class);
        UnitTestAppender appender = new UnitTestAppender();
        log.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan1') "
                        + "define stream BarStream2 (message string); "
                        + "@info(name = 'query1') "
                        + "@source(type ='googlepubsub',"
                        + "credential.path = 'src/test/resources/security/sp.json',"
                        + "project.id = 'sp-path-1547649404768', "
                        + "topic.id = 'topicX', "
                        + "subscription.id = 'subX', "
                        + "@map(type = 'text'))"
                        + "Define stream FooStream2 (message string);"
                        + "from FooStream2 select message insert into BarStream2;");
        siddhiAppRuntime.start();
        AssertJUnit.assertTrue(appender.getMessages().contains("Error in connecting to the resources at "));
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

}

