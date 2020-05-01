package com.transport.lib.http;

import com.transport.lib.entities.Protocol;
import com.transport.lib.exception.TransportExecutionException;
import com.transport.lib.exception.TransportExecutionTimeoutException;
import com.transport.lib.http.receivers.HttpAsyncAndSyncRequestReceiver;
import com.transport.lib.request.Sender;
import com.transport.lib.zookeeper.Utils;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.entity.ByteArrayEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;

public class HttpRequestSender extends Sender {

    private static final Logger logger = LoggerFactory.getLogger(HttpRequestSender.class);

    @Override
    public byte[] executeSync(byte[] message) {
        try {
            RequestConfig config = RequestConfig.custom()
                    .setConnectTimeout((int) this.timeout)
                    .setConnectionRequestTimeout((int) this.timeout)
                    .setSocketTimeout((int) this.timeout).build();
            HttpPost httpPost = new HttpPost(Utils.getHostForService(command.getServiceClass(), moduleId, Protocol.HTTP) + "/request");
            httpPost.setConfig(config);
            HttpEntity postParams = new ByteArrayEntity(message);
            httpPost.setEntity(postParams);
            CloseableHttpResponse httpResponse;
            try {
                httpResponse = HttpAsyncAndSyncRequestReceiver.client.execute(httpPost);
            }catch (ConnectTimeoutException | SocketTimeoutException e){
                throw new TransportExecutionTimeoutException();
            }
            int response = httpResponse.getStatusLine().getStatusCode();
            if (response != 200) {
                httpResponse.close();
                throw new TransportExecutionException("Response for RPC request " + command.getRqUid() + " returned status " + response);
            }
            InputStream responseBody = httpResponse.getEntity().getContent();
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            int nRead;
            byte[] data = new byte[1024];
            while ((nRead = responseBody.read(data, 0, data.length)) != -1) {
                buffer.write(data, 0, nRead);
            }
            buffer.flush();
            byte[] byteArray = buffer.toByteArray();
            httpResponse.close();
            return byteArray;
        } catch (IOException e) {
            logger.error("Error while sending sync HTTP request", e);
            throw new TransportExecutionException(e);
        }
    }

    @Override
    public void executeAsync(byte[] message) {
        try {
            HttpPost httpPost = new HttpPost(Utils.getHostForService(command.getServiceClass(), moduleId, Protocol.HTTP) + "/request");
            HttpEntity postParams = new ByteArrayEntity(message);
            httpPost.setEntity(postParams);
            CloseableHttpResponse httpResponse = HttpAsyncAndSyncRequestReceiver.client.execute(httpPost);
            int response = httpResponse.getStatusLine().getStatusCode();
            httpResponse.close();
            if (response != 200) {
                throw new TransportExecutionException("Response for RPC request " + command.getRqUid() + " returned status " + response);
            }
        } catch (IOException e) {
            logger.error("Error while sending async HTTP request", e);
            throw new TransportExecutionException(e);
        }
    }
}