/*
 * Copyright 2018-present HiveMQ GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hivemq.extensions.me;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.interceptor.publish.PublishInboundInterceptor;
import com.hivemq.extension.sdk.api.interceptor.publish.parameter.PublishInboundInput;
import com.hivemq.extension.sdk.api.interceptor.publish.parameter.PublishInboundOutput;
import com.hivemq.extension.sdk.api.packets.publish.ModifiablePublishPacket;
import com.hivemq.extension.sdk.api.packets.publish.PublishPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Log every received retained publish from client with clientID startsWith(“ME”) with payload
 *
 * @author Dasha Samkova
 * @since 4.33.0
 */
public class MeInterceptor implements PublishInboundInterceptor {
    private static final @NotNull Logger log = LoggerFactory.getLogger(MeInterceptor.class);

    private static String getStringFromByteBuffer(final ByteBuffer buffer) {
        if (buffer == null) {
            return null;
        }
        final byte[] bytes = new byte[buffer.remaining()];
        for (int i = 0; i < buffer.remaining(); i++) {
            bytes[i] = buffer.get(i);
        }
        return new String(bytes, UTF_8);
    }

    @Override
    public void onInboundPublish(
            final @NotNull PublishInboundInput publishInboundInput,
            final @NotNull PublishInboundOutput publishInboundOutput) {

        final PublishPacket publishPacket = publishInboundInput.getPublishPacket();
        final String clientId = publishInboundInput.getClientInformation().getClientId();

        if (clientId.startsWith("ME") && publishPacket.getRetain()) {
            final String payloadAsString;
            if (publishPacket.getPayload().isPresent()) {
                payloadAsString = getStringFromByteBuffer(publishPacket.getPayload().get());
            } else {
                payloadAsString = null;
            }
            log.info("Received retained PUBLISH packet with clientId: '{}', payload: '{}'", clientId, payloadAsString);
        }
    }
}