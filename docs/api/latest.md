# API Docs - v1.0.0-SNAPSHOT

## Sink

### googlepubsub *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#sink">(Sink)</a>*

<p style="word-wrap: break-word">GooglePubSub Sink publishes messages to a topic in  GooglePubSub server. If the required topic doesn't exist, GooglePubSub Sink creates a topic and publish messages to that topic.</p>

<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>
```
@sink(type="googlepubsub", project.id="<STRING>", topic.id="<STRING>", credential.path="<STRING>", @map(...)))
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">project.id</td>
        <td style="vertical-align: top; word-wrap: break-word">The unique ID of the GCP console project within which the topic is created.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">topic.id</td>
        <td style="vertical-align: top; word-wrap: break-word">The topic ID of the topic to which the messages that are processed by Siddhi are published. </td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">credential.path</td>
        <td style="vertical-align: top; word-wrap: break-word">The file path of the service account credentials.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@sink(type = 'googlepubsub', @map(type= 'text'),
project.id = 'sp-path-1547649404768', 
credential.path = 'src/test/resources/security/sp.json',
topic.id ='topicA',
 )
define stream inputStream(message string);
```
<p style="word-wrap: break-word">This example shows how to publish messages to a topic in the GooglePubSub with all supportive configurations.Accordingly, the messages are published to a topic having the topic.id named topicA in the project with a project.id 'sp-path-1547649404768'. If the required topic already exists in the particular project the messages are directly published to that topic.If the required topic does not exist, a new topic is created according to the provided topic.id and project.id. Then the messages are published to the particular topic.</p>

## Source

### googlepubsub *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#source">(Source)</a>*

<p style="word-wrap: break-word">A GooglePubSub Source receives events to be processed by Siddhi, from a topic in GooglePubSub Server.Here, a subscriber client creates a subscription to that topic and consumes messages from the subscription. Only messages published to the topic after the subscription is created are available to subscriber applications. The subscription connects the topic to a subscriber application that receives and processes messages published to the topic. A topic can have multiple subscriptions, but a given subscription belongs to a single topic.</p>

<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>
```
@source(type="googlepubsub", project.id="<STRING>", topic.id="<STRING>", subscription.id="<STRING>", credential.path="<STRING>", @map(...)))
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">project.id</td>
        <td style="vertical-align: top; word-wrap: break-word">The unique ID of the GCP console project within which the topic is created.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">topic.id</td>
        <td style="vertical-align: top; word-wrap: break-word">The unique ID of the topic from which the messages are received.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">subscription.id</td>
        <td style="vertical-align: top; word-wrap: break-word">The unique ID of the subscription from which messages should be retrieved.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">credential.path</td>
        <td style="vertical-align: top; word-wrap: break-word">The file path of the service account credentials.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@source(type='googlepubsub',@map(type='text'),
topic.id='topicA',
project.id='sp-path-1547649404768',
credential.path = 'src/test/resources/security/sp.json',
subscription.id='subA',
)
define stream outputStream(message String);
```
<p style="word-wrap: break-word">This example shows how to subscribe to a googlepubsub topic with all supporting configurations. With the following configurations the identified source, will subscribe to a topic having topic.id named as topicA which resides in a googlepubsub instance with the project.id of 'sp-path-1547649404768'. This GooglePubSub Source configuration listens to the events coming to a googlepubsub topic. The events are received in the text format and mapped to a Siddhi event, and sent to a the outputStream.</p>

