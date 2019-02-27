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

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.log4j.Logger;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * A test client to check the google pub sub receiving tasks.
 */
public class TestPublisher {

    private static final Logger log = Logger.getLogger(TestPublisher.class);

    private TopicAdminClient topicAdminClient;
    private String projectId;
    private String topicId;
    private Publisher publisher = null;

    public TestPublisher(String projectId, String topicId) {

        this.projectId = projectId;
        this.topicId = topicId;
    }

    public void publish(Object payload) throws Exception {

        GoogleCredentials credentials = null;
        File credentialsPath = new File("src/test/resources/security/sp.json");
        try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
            credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
        } catch (FileNotFoundException e) {
            log.error("The file that points to your service account credentials is not found.");
        } catch (IOException e) {
            log.error("Credentials are missing.");
        }

        String message = (String) payload;
        ProjectTopicName topic = ProjectTopicName.of(projectId, topicId);
        try {
            TopicAdminSettings topicAdminSettings =
                    TopicAdminSettings.newBuilder()
                            .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                            .build();
            topicAdminClient =
                    TopicAdminClient.create(topicAdminSettings);
            topicAdminClient.createTopic(topic);
        } catch (ApiException e) {
            if (e.getStatusCode().getCode() != StatusCode.Code.ALREADY_EXISTS) {
                log.error("Error connecting to the topic in google pub sub server.");
                throw new ConnectionUnavailableException("Error in connecting to the topic in google pub subs server");
            }
        } catch (IOException e) {
            log.error("Could not create an authenticated user.Check you have provided  your credentials : " + topic);
        } finally {
            if (topicAdminClient != null) {
                topicAdminClient.shutdown();
            }
        }

        try {
            publisher = Publisher.newBuilder(topic).setCredentialsProvider
                    (FixedCredentialsProvider.create(credentials)).build();

            ByteString data = ByteString.copyFromUtf8(message);
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                    .setData(data)
                    .build();

            publisher.publish(pubsubMessage);

        } catch (IOException e) {
            log.error("Error creating a publisher bound to the topic : " + topic);
            throw new SiddhiAppCreationException("Error creating a publisher bound to the topic : " +
                    topic, e);
        } finally {

            try {

                if (publisher != null) {
                    publisher.shutdown();
                    publisher.awaitTermination(1, TimeUnit.MINUTES);

                }

            } catch (Exception e) {
                log.error("Error in disconnecting the publisher.");
            }
        }
    }

}

