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
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.io.googlepubsub.util.GooglePubSubConstants;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.output.sink.Sink;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.DynamicOptions;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * {@code GooglePubSubSink } Handle the GooglePubSub publishing tasks.
 */
@Extension(
        name = "googlepubsub",
        namespace = "sink",
        description = "GooglePubSub Sink publishes messages to a topic in  GooglePubSub server. If the required "
                + "topic doesn't exist, GooglePubSub Sink creates a topic and publish messages to that topic.",
        parameters = {
                @Parameter(
                        name = GooglePubSubConstants.GOOGLE_PUB_SUB_SERVER_PROJECT_ID,
                        description = "The unique ID of the GCP console project within which the topic is created.",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = GooglePubSubConstants.TOPIC_ID,
                        description = "The topic ID of the topic to which the messages that are processed by Siddhi " +
                                "are published. ",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = GooglePubSubConstants.CREDENTIAL_PATH,
                        description = "The file path of the service account credentials.",
                        type = DataType.STRING
                ),
        },
        examples = {
                @Example(description = "This example shows how to publish messages to a topic in the GooglePubSub with "
                        + "all supportive configurations.Accordingly, the messages are published to a topic having the "
                        + "topic.id named topicA in the project with a project.id 'sp-path-1547649404768'. If the "
                        + "required topic already exists in the particular project the messages are directly published "
                        + "to that topic.If the required topic does not exist, a new topic is created according to the "
                        + "provided topic.id and project.id. Then the messages are published to the particular topic.",

                        syntax = "@sink(type = 'googlepubsub', @map(type= 'text'),\n"
                                + "project.id = 'sp-path-1547649404768', \n"
                                + "credential.path = 'src/test/resources/security/sp.json',\n"
                                + "topic.id ='topicA',\n "
                                + ")\n" +
                                "define stream inputStream(message string);"),
        }
)
/*
 * GooglePubSub publishing tasks.
 */
public class GooglePubSubSink extends Sink {

    private static final Logger log = Logger.getLogger(GooglePubSubSink.class);
    private String streamID;
    private String siddhiAppName;
    private GoogleCredentials credentials;
    private TopicAdminClient topicAdminClient;
    private String projectId;
    private ProjectTopicName topic;
    private Publisher publisher;

    @Override
    public Class[] getSupportedInputEventClasses() {

        return new Class[]{String.class};
    }

    @Override
    public String[] getSupportedDynamicOptions() {

        return new String[]{GooglePubSubConstants.TOPIC_ID, GooglePubSubConstants.GOOGLE_PUB_SUB_SERVER_PROJECT_ID,
                GooglePubSubConstants.CREDENTIAL_PATH};
    }

    @Override
    protected void init(StreamDefinition streamDefinition, OptionHolder optionHolder, ConfigReader configReader,
                        SiddhiAppContext siddhiAppContext) {

        this.siddhiAppName = siddhiAppContext.getName();
        this.streamID = streamDefinition.getId();
        String topicId = optionHolder.validateAndGetStaticValue(GooglePubSubConstants.TOPIC_ID);
        this.projectId = optionHolder.validateAndGetStaticValue(GooglePubSubConstants.GOOGLE_PUB_SUB_SERVER_PROJECT_ID);
        String credentialPath = optionHolder.validateAndGetStaticValue(GooglePubSubConstants.CREDENTIAL_PATH);
        this.topic = ProjectTopicName.of(projectId, topicId);
        File credentialsPath = new File(credentialPath);
        try {
            FileInputStream serviceAccountStream = new FileInputStream(credentialsPath);
            credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
        } catch (IOException e) {
            throw new SiddhiAppCreationException("The file that contains your service account credentials is not "
                    + "found or you are not permitted to make authenticated calls. Check the credential.path '"
                    + credentialPath + "' defined in stream " + siddhiAppName + " : " + streamID + ".", e);
        }
        createTopic();
        try {
            publisher = Publisher.newBuilder(topic).setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                    .build();
        } catch (IOException e) {
            throw new SiddhiAppCreationException("Could not create a publisher bound to the topic : " + topic, e);
        }
    }

    @Override
    public void publish(Object payload, DynamicOptions dynamicOptions) {

        String message = (String) payload;
        ByteString data = ByteString.copyFromUtf8(message);
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
        publisher.publish(pubsubMessage);
    }

    @Override
    public void connect() {

    }

    @Override
    public void disconnect() {

        if (publisher != null) {
            try {
                publisher.shutdown();
                publisher.awaitTermination(1, TimeUnit.MINUTES);
            } catch (Exception e) {
                log.error("Error in shutting down the publisher : " + publisher);
            }
        }
    }

    @Override
    public void destroy() {

    }

    @Override
    public Map<String, Object> currentState() {

        return null;
    }

    @Override
    public void restoreState(Map<String, Object> map) {
        //no state to restore
    }

    /**
     * Creating a topic.
     */
    private void createTopic() {

        try {
            TopicAdminSettings topicAdminSettings = TopicAdminSettings.newBuilder().setCredentialsProvider
                    (FixedCredentialsProvider.create(credentials)).build();
            topicAdminClient = TopicAdminClient.create(topicAdminSettings);
            topicAdminClient.createTopic(topic);
        } catch (ApiException e) {
            if (e.getStatusCode().getCode() != StatusCode.Code.ALREADY_EXISTS) {
                throw new SiddhiAppCreationException("An error is caused due to a resource "
                        + e.getStatusCode().getCode() + " in Google Pub Sub server." + " Check whether you have "
                        + "provided a proper project.id for '" + projectId + "' defined in stream " + siddhiAppName
                        + ": " + streamID + " and make sure you have enough access to use all resources in API.", e);
            }
        } catch (IOException e) {
            throw new SiddhiAppCreationException("Could not create the topic " + topic + "in the google pub sub "
                    + "server, under the project.id '" + projectId + "' defined in stream " + siddhiAppName + " : "
                    + streamID + ".", e);
        } finally {
            if (topicAdminClient != null) {
                topicAdminClient.shutdown();
            }
        }
    }
}
