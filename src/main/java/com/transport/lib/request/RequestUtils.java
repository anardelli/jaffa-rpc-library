package com.transport.lib.request;

import com.transport.lib.TransportService;
import com.transport.lib.entities.Protocol;
import com.transport.lib.exception.TransportNoRouteException;
import com.transport.lib.zookeeper.Utils;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class RequestUtils {
    /*
        Returns server-side topic name for synchronous and asynchronous requests
        Topic name looks like <class name with packages>-<module.id>-server-<sync>/<async>
        - Fully-qualified target class name defined in Command
        - module.id could be provided by user or discovered from currently available in cluster
        - If no API implementations currently available or module is registered
          but his server topic does not exists - RuntimeException will be thrown
    */
    public static String getTopicForService(String service, String moduleId, boolean sync) {
        String serviceInterface = service.replace("Transport", "");
        String availableModuleId = moduleId;
        if (moduleId != null) {
            // Checks for active server for a given service with specific module id
            // or throws "transport no route" exception if there are none
            Utils.getHostForService(serviceInterface, moduleId, Protocol.KAFKA);
        } else {
            // if moduleId  was not specified - get module id of any active server for a given service
            // or throws "transport no route" exception if there are none
            availableModuleId = Utils.getModuleForService(serviceInterface, Protocol.KAFKA);
        }
        String topicName = serviceInterface + "-" + availableModuleId + "-server" + (sync ? "-sync" : "-async");
        // if necessary topic does not exist for some reason - throw "transport no route" exception
        if (!TransportService.getZkClient().topicExists(topicName))
            throw new TransportNoRouteException(serviceInterface, availableModuleId);
        else
            return topicName;
    }

}
