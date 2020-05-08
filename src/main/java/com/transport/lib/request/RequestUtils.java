package com.transport.lib.request;

import com.transport.lib.TransportService;
import com.transport.lib.entities.Protocol;
import com.transport.lib.exception.TransportNoRouteException;
import com.transport.lib.zookeeper.Utils;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class RequestUtils {
    public static String getTopicForService(String service, String moduleId, boolean sync) {
        String serviceInterface = Utils.getServiceInterfaceNameFromClient(service);
        String availableModuleId = moduleId;
        if (moduleId != null) {
            Utils.getHostForService(serviceInterface, moduleId, Protocol.KAFKA);
        } else {
            availableModuleId = Utils.getModuleForService(serviceInterface, Protocol.KAFKA);
        }
        String topicName = serviceInterface + "-" + availableModuleId + "-server" + (sync ? "-sync" : "-async");
        if (!TransportService.getZkClient().topicExists(topicName))
            throw new TransportNoRouteException(serviceInterface, availableModuleId);
        else
            return topicName;
    }
}
