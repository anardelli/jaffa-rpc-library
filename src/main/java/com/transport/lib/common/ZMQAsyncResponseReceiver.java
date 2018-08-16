package com.transport.lib.common;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.transport.lib.zookeeper.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import java.io.ByteArrayInputStream;
import java.lang.reflect.Method;

@SuppressWarnings("unchecked")
public class ZMQAsyncResponseReceiver implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(ZMQAsyncResponseReceiver.class);

    @Override
    public void run() {
        try {
            ZMQ.Context context = ZMQ.context(1);
            ZMQ.Socket socket =  context.socket(ZMQ.REP);
            socket.bind("tcp://" + Utils.getZeroMQCallbackBindAddress());

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    byte[] bytes = socket.recv();
                    Kryo kryo = new Kryo();
                    Input input = new Input(new ByteArrayInputStream(bytes));
                    CallbackContainer callbackContainer = kryo.readObject(input, CallbackContainer.class);
                    Class callbackClass = Class.forName(callbackContainer.getListener());
                    if(callbackContainer.getResult() instanceof ExceptionHolder) {
                        Method method = callbackClass.getMethod("callBackError", String.class, Throwable.class );
                        method.invoke(callbackClass.newInstance(), callbackContainer.getKey(), new Throwable(((ExceptionHolder) callbackContainer.getResult()).getStackTrace()));
                    }else {
                        Method method = callbackClass.getMethod("callBack", String.class, Class.forName(callbackContainer.getResultClass()));
                        if(Class.forName(callbackContainer.getResultClass()).equals(Void.class)){
                            method.invoke(callbackClass.newInstance(), callbackContainer.getKey(), null);
                        }else
                            method.invoke(callbackClass.newInstance(), callbackContainer.getKey(), callbackContainer.getResult());
                    }
                } catch (Exception e) {
                    logger.error("Error during receiving callback:", e);
                }
            }
            Utils.closeSocketAndContext(socket, context);
        }catch (Exception e){
            logger.error("Error during callback receiver startup:", e);
        }
    }

}
