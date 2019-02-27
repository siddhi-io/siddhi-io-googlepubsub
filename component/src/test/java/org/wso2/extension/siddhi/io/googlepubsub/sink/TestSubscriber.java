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

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.io.googlepubsub.util.ResultContainer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * A test client to check the google pub sub publishing tasks.
 */
public class TestSubscriber {

    private static final Logger log = Logger.getLogger(TestSubscriber.class);
    private ResultContainer resultContainer;
    private boolean eventArrived;
    private int count;
    private Subscriber subscriber;
    private String projectId;
    private String topicId;
    private String subscriptionId;
    private SubscriptionAdminClient subscriptionAdminClient;

    public TestSubscriber(String projectId, String topicId, String subscriptionId, ResultContainer resultContainer) {

        this.projectId = projectId;
        this.topicId = topicId;
        this.subscriptionId = subscriptionId;
        this.resultContainer = resultContainer;
    }

    public void shutdown() {

        if (subscriber != null) {
            subscriber.stopAsync().awaitTerminated();
        }
    }

    public void consumer() {

        GoogleCredentials credentials = null;
        File credentialsPath = new File("src/test/resources/security/sp.json");
        try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
            credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
        } catch (FileNotFoundException e) {
            log.error("The file that points to your service account credentials is not found.");
        } catch (IOException e) {
            log.error("Credentials are missing.");
        }

        ProjectTopicName topicName = ProjectTopicName.of(projectId, topicId);

        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(
                projectId, subscriptionId);

        try {
            SubscriptionAdminSettings subscriptionAdminSettings =
                    SubscriptionAdminSettings.newBuilder()
                            .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                            .build();
            subscriptionAdminClient =
                    SubscriptionAdminClient.create(subscriptionAdminSettings);

            subscriptionAdminClient.createSubscription(
                    subscriptionName, topicName, PushConfig.getDefaultInstance(), 10);
        } catch (ApiException e) {
            if (e.getStatusCode().getCode() != StatusCode.Code.ALREADY_EXISTS) {
                log.error("An error is caused due to resource " + e.getStatusCode().getCode() + "." +
                        " Check whether you have provided a proper " +
                        "project.id for " + projectId + "and make sure you have all the access to use the " +
                        "resources in API.", e);
            }

        } catch (IOException e) {
            log.error("Could not create a subscription for your topic.");
        } finally {
            if (subscriptionAdminClient != null) {
                subscriptionAdminClient.shutdown();
            }
        }

        Subscriber subscriber = null;
        TestReceiver receiver = new TestReceiver();
        subscriber =
                Subscriber.newBuilder(subscriptionName, receiver)
                        .setCredentialsProvider(FixedCredentialsProvider.create(credentials)).build();
        subscriber.startAsync().awaitRunning();

        class Listener implements Runnable {

            Subscriber subscriber = null;
            GoogleCredentials credentials;

            private Listener(Subscriber subscriber, GoogleCredentials credentials1) {

                this.subscriber = subscriber;
                this.credentials = credentials1;
            }

            @Override
            public void run() {

                while (true) {

                    try {
                        PubsubMessage message = receiver.getMessages().take();
                        resultContainer.eventReceived(message.getData().toStringUtf8());
                        count++;
                        eventArrived = true;

                    } catch (InterruptedException e) {
                        log.error("Error receiving your message.");
                    }

                }
            }
        }

        Thread thread = new Thread(new Listener(subscriber, credentials));
        thread.start();

    }

    public int getCount() {

        return count;
    }

    public boolean getEventArrived() {

        return eventArrived;
    }

}


