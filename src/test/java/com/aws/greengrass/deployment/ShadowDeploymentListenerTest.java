/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.deployment;

import com.aws.greengrass.config.ChildChanged;
import com.aws.greengrass.config.Node;
import com.aws.greengrass.config.Topic;
import com.aws.greengrass.config.WhatHappened;
import com.aws.greengrass.dependency.Context;
import com.aws.greengrass.deployment.exceptions.DeploymentException;
import com.aws.greengrass.deployment.exceptions.DeviceConfigurationException;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.mqttclient.MqttClient;
import com.aws.greengrass.mqttclient.StandaloneMqttConnector;
import com.aws.greengrass.security.SecurityService;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.crt.mqtt.QualityOfService;
import software.amazon.awssdk.iot.iotshadow.IotShadowClient;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.aws.greengrass.deployment.DeviceConfiguration.DEVICE_PARAM_IOT_DATA_ENDPOINT;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({GGExtension.class, MockitoExtension.class})
public class ShadowDeploymentListenerTest {

    @Mock
    private DeploymentQueue mockDeploymentQueue;
    @Mock
    private DeploymentStatusKeeper mockDeploymentStatusKeeper;
    @Mock
    private MqttClient mockMqttClient;
    @Mock
    private DeviceConfiguration mockDeviceConfiguration;
    @Mock
    private IotShadowClient mockIotShadowClient;
    @Mock
    private Kernel mockKernel;
    @Mock
    private Context mockContext;

    private ExecutorService mockExecutorService;

    private ShadowDeploymentListener shadowDeploymentListener;

    @BeforeEach
    public void setup() {
        mockExecutorService = Executors.newSingleThreadExecutor();
        shadowDeploymentListener = new ShadowDeploymentListener(mockDeploymentQueue, mockDeploymentStatusKeeper,
                mockMqttClient, mockExecutorService, mockDeviceConfiguration, mockIotShadowClient, mockKernel);
    }

    @AfterEach
    public void close() {
        mockExecutorService.shutdownNow();
    }

    @Test
    public void testCommunicationWithIotCore_successful() {
        when(mockKernel.getContext()).thenReturn(mockContext);
        when(mockContext.runOnPublishQueueAndWait(any())).thenReturn(null);
        doReturn(CompletableFuture.completedFuture(null)).when(mockIotShadowClient)
                .SubscribeToUpdateNamedShadowAccepted(any(), any(), any(), any());
        doReturn(CompletableFuture.completedFuture(null)).when(mockIotShadowClient)
                .SubscribeToUpdateNamedShadowRejected(any(), any(), any(), any());
        doReturn(CompletableFuture.completedFuture(null)).when(mockIotShadowClient)
                .SubscribeToGetNamedShadowAccepted(any(), any(), any(), any());
        shadowDeploymentListener.postInject();
        verify(mockMqttClient, times(1)).addToCallbackEvents(any());
        verify(mockIotShadowClient, timeout(500).times(1))
                .SubscribeToUpdateNamedShadowAccepted(any(), any(), any(), any());
        verify(mockIotShadowClient, timeout(500).times(1))
                .SubscribeToUpdateNamedShadowRejected(any(), any(), any(), any());
        verify(mockIotShadowClient, timeout(500).times(1))
                .SubscribeToGetNamedShadowAccepted(any(), any(), any(), any());
    }

    @Test
    public void testCommunicationWithIotCore_unsuccessful() throws DeviceConfigurationException {
        doThrow(new DeviceConfigurationException("Error")).when(mockDeviceConfiguration).validate();
        shadowDeploymentListener.postInject();
        verify(mockMqttClient, times(1)).addToCallbackEvents(any());
        verify(mockIotShadowClient, timeout(500).times(0))
                .SubscribeToUpdateNamedShadowAccepted(any(), any(), any(), any());
        verify(mockIotShadowClient, timeout(500).times(0))
                .SubscribeToUpdateNamedShadowRejected(any(), any(), any(), any());
        verify(mockIotShadowClient, timeout(500).times(0))
                .SubscribeToGetNamedShadowAccepted(any(), any(), any(), any());
    }

    @Test
    public void testCommunicationWithIotCore_unsuccessful_THEN_retry_on_update() throws DeviceConfigurationException {
        when(mockKernel.getContext()).thenReturn(mockContext);
        when(mockContext.runOnPublishQueueAndWait(any())).thenReturn(null);
        doThrow(new DeviceConfigurationException("Error")).doNothing().when(mockDeviceConfiguration).validate();
        doReturn(CompletableFuture.completedFuture(null)).when(mockIotShadowClient)
                .SubscribeToUpdateNamedShadowAccepted(any(), any(), any(), any());
        doReturn(CompletableFuture.completedFuture(null)).when(mockIotShadowClient)
                .SubscribeToUpdateNamedShadowRejected(any(), any(), any(), any());
        doReturn(CompletableFuture.completedFuture(null)).when(mockIotShadowClient)
                .SubscribeToGetNamedShadowAccepted(any(), any(), any(), any());
        shadowDeploymentListener.postInject();
        ArgumentCaptor<ChildChanged> ccCaptor = ArgumentCaptor.forClass(ChildChanged.class);
        verify(mockDeviceConfiguration).onAnyChange(ccCaptor.capture());
        Node mockNode = mock(Node.class);
        lenient().when(mockNode.childOf(DEVICE_PARAM_IOT_DATA_ENDPOINT)).thenReturn(true);
        ccCaptor.getValue().childChanged(WhatHappened.childChanged, mockNode);
        verify(mockMqttClient, times(1)).addToCallbackEvents(any());
        verify(mockIotShadowClient, timeout(1000).times(1))
                .SubscribeToUpdateNamedShadowAccepted(any(), any(), any(), any());
        verify(mockIotShadowClient, timeout(1000).times(1))
                .SubscribeToUpdateNamedShadowRejected(any(), any(), any(), any());
        verify(mockIotShadowClient, timeout(1000).times(1))
                .SubscribeToGetNamedShadowAccepted(any(), any(), any(), any());
    }

    @Test
    void GIVEN_source_endpoint_WHEN_report_status_to_source_THEN_connects_publishes_and_closes() throws Exception {
        SecurityService mockSecurityService = mock(SecurityService.class);
        shadowDeploymentListener.setSecurityService(mockSecurityService);

        Topic thingNameTopic = mock(Topic.class);
        when(thingNameTopic.getOnce()).thenReturn("TestThing");
        when(mockDeviceConfiguration.getThingName()).thenReturn(thingNameTopic);

        try (StandaloneMqttConnector mockConnector = mock(StandaloneMqttConnector.class);
             MockedStatic<StandaloneMqttConnector> staticMock = mockStatic(StandaloneMqttConnector.class)) {
            staticMock.when(() -> StandaloneMqttConnector.of(any(), any(), any(), any()))
                    .thenReturn(mockConnector);

            Map<String, String> statusDetails = new HashMap<>();
            statusDetails.put("status", "SUCCEEDED");
            shadowDeploymentListener.reportStatusToSourceEndpoint(
                    "source.iot.us-east-1.amazonaws.com",
                    "arn:aws:greengrass:us-east-1:123:config", "SUCCEEDED", statusDetails);

            verify(mockConnector).connect(60_000);
            verify(mockConnector).publish(
                    contains("shadow/name/AWSManagedGreengrassV2Deployment/update"),
                    any(byte[].class), any(QualityOfService.class), eq(60_000L));
            verify(mockConnector).close();
        }
    }

    @Test
    void GIVEN_source_endpoint_unreachable_WHEN_report_shadow_status_THEN_logs_warning_no_throw(
            ExtensionContext extContext) throws Exception {
        ignoreExceptionOfType(extContext, DeploymentException.class);

        SecurityService mockSecurityService = mock(SecurityService.class);
        shadowDeploymentListener.setSecurityService(mockSecurityService);

        Topic thingNameTopic = mock(Topic.class);
        when(thingNameTopic.getOnce()).thenReturn("TestThing");
        when(mockDeviceConfiguration.getThingName()).thenReturn(thingNameTopic);

        try (StandaloneMqttConnector mockConnector = mock(StandaloneMqttConnector.class);
             MockedStatic<StandaloneMqttConnector> staticMock = mockStatic(StandaloneMqttConnector.class)) {
            doThrow(new DeploymentException("connect failed")).when(mockConnector).connect(60_000);
            staticMock.when(() -> StandaloneMqttConnector.of(any(), any(), any(), any()))
                    .thenReturn(mockConnector);

            Map<String, String> statusDetails = new HashMap<>();
            // Should not throw
            shadowDeploymentListener.reportStatusToSourceEndpoint(
                    "unreachable.iot.us-east-1.amazonaws.com",
                    "arn:aws:greengrass:us-east-1:123:config", "SUCCEEDED", statusDetails);
        }
    }
}
