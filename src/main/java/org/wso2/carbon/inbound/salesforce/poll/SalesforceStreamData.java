/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *   WSO2 Inc. licenses this file to you under the Apache License,
 *   Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.wso2.carbon.inbound.salesforce.poll;

import org.apache.synapse.config.Entry;
import org.apache.synapse.registry.AbstractRegistry;
import org.cometd.bayeux.Channel;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.InputMismatchException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.SynapseException;
import org.apache.synapse.core.SynapseEnvironment;
import org.eclipse.jetty.util.ajax.JSON;
import org.wso2.carbon.inbound.endpoint.protocol.generic.GenericPollingConsumer;

/**
 * Salesforce streaming api Inbound.
 */
public class SalesforceStreamData extends GenericPollingConsumer implements ConnectionFailureListener {

    private static final Log LOG = LogFactory.getLog(SalesforceStreamData.class);
    // Mandatory parameters.
    private String loginEndpoint;
    private String userName;
    private String password;
    private String salesforceObject;
    private String streamingEndpointUri;
    private String injectingSeq;
    private long replayFromOption;
    // optional parameters.
    private int connectionTimeout;
    private int waitTime;
    private boolean isPolled = false;
    private SalesforceDataHolderObject SalesforceDataHolderObject=new SalesforceDataHolderObject();
    private EmpConnector connector;
    private AbstractRegistry registry;
    private boolean isReplayEnabled = false;
    private boolean isReplayWithMultipleInboundsEnabled = false;
    private boolean isInitialEventIdUsed = false;

    // Flag indicating whether the connection has failed
    private boolean connectionFailed;

    public SalesforceStreamData(Properties salesforceProperties, String name, SynapseEnvironment synapseEnvironment,
                                long scanInterval, String injectingSeq, String onErrorSeq, boolean coordination,
                                boolean sequential) {
        super(salesforceProperties, name, synapseEnvironment, scanInterval, injectingSeq, onErrorSeq, coordination,
                sequential);
        SalesforceDataHolderObject.setProperties(salesforceProperties);
        this.registry = (AbstractRegistry) synapseEnvironment.getSynapseConfiguration().getRegistry();
        loadMandatoryParameters(salesforceProperties);
        loadOptionalParameters(salesforceProperties);
        streamingEndpointUri = loginEndpoint;
        this.injectingSeq = injectingSeq;
        this.connectionFailed = false;
    }

    /**
     * Handles connection failure notifications from the EmpConnector.
     * Sets the connectionFailed flag to true when a failure is detected.
     */
    @Override
    public void onConnectionFailure() {
        // Update the connection failed flag and log the failure
        this.connectionFailed = true;
        LOG.info("Connection failed. connectionFailed flag set to true in SalesforceStreamData.");
    }


    /**
     * Read event id to replay from specific file.
     * @param filePath path to text file.
     * @return
     */
    private long readFromGivenFile(String filePath) {
        String str;
        BufferedReader bufferedReader = null;
        try {
            bufferedReader = new BufferedReader(new FileReader(filePath));
            str = bufferedReader.readLine();
            if (str != null && !str.isEmpty()) {
                try {
                    return Long.parseLong(str);
                } catch (NumberFormatException e) {
                    LOG.warn("Event id is not a number. Default id used");
                }
            } else
                LOG.warn("Event id not specified in the file. Default id used");
            return SalesforceConstant.REPLAY_FROM_TIP;
        } catch (IOException e) {
            if (LOG.isDebugEnabled()) {
                LOG.error("Unable to read file from given path", e);
            }
        } finally {
            try {
                if (bufferedReader != null) {
                    bufferedReader.close();
                }
            } catch (IOException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.error("Unable to close resources", e);
                }
            }
        }
        LOG.warn("Failed to read id. Default event id will use");
        return SalesforceConstant.REPLAY_FROM_TIP;
    }

    /**
     * Store event id to replay in the config registry DB.
     * @param id eventID
     */
    private void updateRegistryEventID(long id) {
        if (registry != null) {
            String registryPath;
            String resourcePath;
            if (isReplayWithMultipleInboundsEnabled) {
                registryPath = SalesforceConstant.REGISTRY_PATH + "/" + this.name;
                resourcePath = registryPath + "/" + SalesforceConstant.SALESFORCE_EVENT;
            } else {
                registryPath = SalesforceConstant.REGISTRY_PATH;
                resourcePath = SalesforceConstant.RESOURCE_PATH;
            }
            Object registryResource = registry.getResource(new Entry(resourcePath), null);

            if (registryResource == null) {
                LOG.info("Registry resource not found. Creating new resource: " + resourcePath);
                registry.newResource(registryPath, true);
            }
            registry.newNonEmptyResource(resourcePath, false, "text/plain", "" + id,
                        SalesforceDataHolderObject.salesforceObject);
        }
    }

    /**
     * Read event id to replay from registry DB.
     * @return event id.
     */
    private long getRegistryEventID() {
        long eventIDFromDB;
        String resourcePath;
        registry = (AbstractRegistry) synapseEnvironment.getSynapseConfiguration().getRegistry();
        if (isReplayWithMultipleInboundsEnabled) {
            resourcePath = SalesforceConstant.REGISTRY_PATH + "/" + this.name + "/" + SalesforceConstant.SALESFORCE_EVENT;
        } else {
            resourcePath = SalesforceConstant.RESOURCE_PATH;
        }
        Object registryResource = registry.getResource(new Entry(resourcePath), null);
        if (registryResource != null) {
            Properties resourceProperties = registry.getResourceProperties(resourcePath);
            if (resourceProperties == null) {
                return SalesforceConstant.REPLAY_FROM_TIP;
            }
            String eventIDStr = resourceProperties.getProperty(SalesforceDataHolderObject.salesforceObject);
            try {
                eventIDFromDB = Long.parseLong(eventIDStr);
            } catch (NumberFormatException e) {
                eventIDFromDB = SalesforceConstant.REPLAY_FROM_TIP;
                LOG.warn("Event id mentioned in the registry property is not a number. Default id used to retrieve events");
            }
        } else {
            LOG.warn("Event id not specified in the resource in registry db. Default id used");
            eventIDFromDB = SalesforceConstant.REPLAY_FROM_TIP;
        }
        return eventIDFromDB;
    }
    
    /**
     * Connecting to Salesforce and listening to events
     * @throws Throwable
     */
    private void makeConnect() throws Throwable {
        // Check if an active connection already exists
        if (this.connector != null && this.connector.isConnected()) {
            LOG.info("Salesforce connector is already connected and active. Skipping new connection setup.");
            return;
        }

        // If a connector exists but is not connected, stop it to clean up resources
        if (this.connector != null) {
            LOG.info("Stopping existing (but not connected) Salesforce connector before creating a new one.");
            this.connector.stop();
        }

        Consumer<Map<String, Object>> consumer = event -> injectSalesforceMessage(JSON.toString(event),
                (Long) ((HashMap) event.
                        get(SalesforceConstant.EVENT)).get(SalesforceConstant.REPLAY_ID));
        BearerTokenProvider tokenProvider;
        tokenProvider = new BearerTokenProvider(() -> {
            try {
                return LoginHelper.login(new URL(streamingEndpointUri), userName, password);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        BayeuxParameters params = tokenProvider.login();
        // connector object is created now after initial checks.
        connector = new EmpConnector(params, this);
        LoggingListener loggingListener = new LoggingListener(true, true);
        connector.addListener(Channel.META_HANDSHAKE, loggingListener).addListener(Channel.META_CONNECT, loggingListener)
                .addListener(Channel.META_DISCONNECT, loggingListener).addListener(Channel.META_SUBSCRIBE, loggingListener)
                .addListener(Channel.META_UNSUBSCRIBE, loggingListener);
        connector.setBearerTokenProvider(tokenProvider);
        // Removed redundant connector.isConnected() check and stop() call here as it's handled above
        // and a new connector is always created if we reach this point.
        connector.start().get(waitTime, TimeUnit.MILLISECONDS);
        TopicSubscription subscription;

        //Read replayId
        if (isReplayEnabled && !isInitialEventIdUsed) {
            replayFromOption = getRegistryEventID();
        }

        try {
            subscription = connector.subscribe(salesforceObject, replayFromOption, consumer)
                    .get(waitTime, TimeUnit.MILLISECONDS);
            LOG.info("Subscribed: " + subscription);
        } catch (ExecutionException e) {
            LOG.error("Unable to subscribed for the event/topic", e);
        } catch (TimeoutException e) {
            LOG.error("Timed out subscribing", e);
        } catch (InterruptedException e) {
            LOG.error("Unexpected error occurred while subscribing to event/topic", e);
        }
    }

    /**
     * load essential property for salesforce inbound endpoint.
     * @param properties The mandatory parameters of Salesforce.
     */
    private void loadMandatoryParameters(Properties properties) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Starting to load the salesforce credentials");
        }
        userName = properties.getProperty(SalesforceConstant.USER_NAME);
        salesforceObject = properties.getProperty(SalesforceConstant.SOBJECT);
        password = properties.getProperty(SalesforceConstant.PASSWORD);
        loginEndpoint = properties.getProperty(SalesforceConstant.LOGIN_ENDPOINT);
        String packageVersion = properties.getProperty(SalesforceConstant.PACKAGE_VERSION);
        if (StringUtils.isEmpty(userName) && StringUtils.isEmpty(salesforceObject) && StringUtils.isEmpty(password)
                && StringUtils.isEmpty(loginEndpoint) && StringUtils.isEmpty(packageVersion)) {
            handleException("Mandatory Parameters can't be Empty...");
        }
        SalesforceDataHolderObject.setPackageVersion(packageVersion);
        SalesforceDataHolderObject.setObjectName(salesforceObject);
    }

    /**
     * Load optional parameters for salesforce inbound endpoint file.
     *
     * @param properties The Optional parameters of Salesforce.
     */
    private void loadOptionalParameters(Properties properties) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Starting to load the salesforce credentials");
        }
        if (properties.getProperty(SalesforceConstant.CONNECTION_TIMEOUT) == null) {
            connectionTimeout = SalesforceConstant.CONNECTION_TIMEOUT_DEFAULT;
        } else {
            try {
                connectionTimeout = Integer.parseInt(properties.getProperty(SalesforceConstant.CONNECTION_TIMEOUT));
            } catch (NumberFormatException e) {
                LOG.error("The Value should be in Number", e);
            }
        }
        SalesforceDataHolderObject.setConnectionTimeout(connectionTimeout);
        if (properties.getProperty(SalesforceConstant.WAIT_TIME) == null) {
            waitTime = SalesforceConstant.WAIT_TIME_DEFAULT;
        } else {
            try {
                waitTime = Integer.parseInt(properties.getProperty(SalesforceConstant.WAIT_TIME));
            } catch (NumberFormatException e) {
                LOG.error("The Value should be in Number", e);
            }
        }
        SalesforceDataHolderObject.setWaitTime(waitTime);
        //replay enable
        if (Boolean.parseBoolean(properties.getProperty(SalesforceConstant.REPLAY_FROM))) {
            isReplayEnabled = true;

            // Support Replay With Multiple Inbound Endpoints
            isReplayWithMultipleInboundsEnabled = Boolean.parseBoolean(properties.getProperty(
                    SalesforceConstant.REPLAY_WITH_MULTIPLE_INBOUNDS));

            if (properties.getProperty(SalesforceConstant.REPLAY_FROM_ID_Stored_File_Path) != null && !StringUtils.
                    isEmpty(properties.getProperty(SalesforceConstant.REPLAY_FROM_ID_Stored_File_Path))) {
                try {
                    //read id from file
                    replayFromOption = readFromGivenFile(
                            properties.getProperty(SalesforceConstant.REPLAY_FROM_ID_Stored_File_Path));
                    // Write replayFromOption to registry
                    updateRegistryEventID(replayFromOption);
                } catch (NumberFormatException e) {
                    LOG.error("The Value should be number", e);
                }
            } else if (properties.getProperty(SalesforceConstant.INITIAL_EVENT_ID) != null && StringUtils.
                    isNotEmpty(properties.getProperty(SalesforceConstant.INITIAL_EVENT_ID))) {
                try {
                    // If the initial eventId given in configuration is higher than the replayId in registry,
                    // initialEventId is used as the starting id for replaying
                    long initialEventId = Long.parseLong(properties.getProperty(SalesforceConstant.INITIAL_EVENT_ID));
                    long registryValue = getRegistryEventID();
                    if (initialEventId > registryValue) {
                        replayFromOption = initialEventId;
                        isInitialEventIdUsed = true;
                    } else {
                        replayFromOption = registryValue;
                    }
                } catch (NumberFormatException e) {
                    LOG.error("Initial event ID should be number", e);
                }
            } else {
                //read id from  registry db
                replayFromOption = getRegistryEventID();
            }
        } else {
            replayFromOption = SalesforceConstant.REPLAY_FROM_TIP;
        }
        SalesforceDataHolderObject.setReplayFromOption(replayFromOption);
        if (properties.getProperty(SalesforceConstant.SOAP_API_VERSION) == null) {
            SalesforceDataHolderObject.setSoapApiVersion(SalesforceConstant.DEFAULT_SOAP_API_VERSION);
        } else {
            try {
                SalesforceDataHolderObject
                        .setSoapApiVersion(properties.getProperty(SalesforceConstant.SOAP_API_VERSION));
            } catch (InputMismatchException e) {
                LOG.error("The Value should be in Number", e);
            }
        }
    }

    /**
     * Each polling time this method calls
     *
     * @return
     */
    public Object poll() {
        LOG.info("Salesforce Inbound Endpoint '" + name + "' : Poll cycle initiated. isPolled: " + isPolled +
                ", connectionFailed: " + connectionFailed);
        try {
            LOG.debug("Salesforce Inbound Endpoint '" + name + "' : Checking connection status. isPolled: " + isPolled +
                    ", connectionFailed: " + connectionFailed);
            if (!isPolled || connectionFailed) {
                LOG.info("Salesforce Inbound Endpoint '" + name + "' : Conditions met to call makeConnect(). isPolled: "
                        + isPolled + ", connectionFailed: " + connectionFailed);
                makeConnect();
                // Only set connectionFailed to false if makeConnect() was successful.
                // If makeConnect() itself throws an error, the catch block will handle setting connectionFailed = true.
                connectionFailed = false;
                isPolled = true;
                LOG.info("Salesforce Inbound Endpoint '" + name + "' : makeConnect() attempt finished. isPolled: " + isPolled +
                        ", connectionFailed: " + connectionFailed);
            }
        } catch (Throwable e) {
            LOG.error("Salesforce Inbound Endpoint '" + name + "' : Error during poll cycle or makeConnect(). Error: " + e.getMessage(), e);
            connectionFailed = true;
            LOG.info("Salesforce Inbound Endpoint '" + name + "' : Set connectionFailed to true due to error.");
        }
        return null;
    }

    /**
     * Injecting the salesforce Stream messages to the ESB sequence.
     *
     * @param message the salesforce response status
     */
    private void injectSalesforceMessage(String message, long id) {
        if (injectingSeq != null) {
            if (LOG.isDebugEnabled()) {
                // Retaining this log for visibility of received event ID before processing
                LOG.info("Salesforce Inbound Endpoint '" + name + "' : Received event with ID: " + id + ". Preparing for injection.");
            }
            // Call injectMessage first. If it fails, updateRegistryEventID will not be called.
            injectMessage(message, SalesforceConstant.CONTENT_TYPE);

            // If injectMessage was successful, then update the registry.
            if (LOG.isDebugEnabled()) {
                LOG.debug("Salesforce Inbound Endpoint '" + name + "' : Message injected successfully into sequence: " + injectingSeq +
                          ". Attempting to update registry with event ID: " + id);
            }
            updateRegistryEventID(id);
            if (LOG.isDebugEnabled()) {
                // This log confirms that updateRegistryEventID was called.
                LOG.debug("Salesforce Inbound Endpoint '" + name + "' : Registry update call completed for event ID: " + id);
            }
        } else {
            // It's important that handleException throws a runtime exception to halt processing if the sequence is null.
            // Otherwise, if it just logs, we might proceed as if successful.
            LOG.error("Salesforce Inbound Endpoint '" + name + "' : Injecting sequence is null. Cannot process event ID: " + id);
            handleException("The injecting sequence (injectingSeq) is not configured for Salesforce Inbound Endpoint '" + name + "'.");
        }
    }

    /**
     * handle Exception.
     *
     * @param msg error message.
     */
    private void handleException(String msg) {
        LOG.error(msg);
        throw new SynapseException(msg);
    }

    /**
     * stop the connector when redeploy or shutdown the server.
     */
    @Override
    public void destroy() {
        if (this.connector != null) {
            LOG.info("Salesforce Inbound Endpoint '" + name + "' : Connector stop requested.");
            this.connector.stop();
            LOG.info("Salesforce Inbound Endpoint '" + name + "' : Connector stopped.");
            this.connector = null;
        }
    }

    @Override
    public void resume() {
        LOG.info("Salesforce Inbound Endpoint '" + name + "' : Resuming...");
        if (this.connector != null) {
            LOG.info("Salesforce Inbound Endpoint '" + name + "' : Attempting to stop existing connector during resume.");
            this.connector.stop();
            this.connector = null; // Ensure a fresh connector is created
        }
        isPolled = false;
        isInitialEventIdUsed = false;
    }
}

