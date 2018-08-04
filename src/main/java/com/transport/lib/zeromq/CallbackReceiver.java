package com.transport.lib.zeromq;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.transport.lib.zookeeper.ZKUtils;
import org.zeromq.ZMQ;
import java.io.ByteArrayInputStream;
import java.lang.reflect.Method;

@SuppressWarnings("unchecked")
public class CallbackReceiver implements Runnable {

    public static volatile boolean active = false;

    @Override
    public void run() {
        try {
            ZMQ.Context context = ZMQ.context(1);
            ZMQ.Socket socket =  context.socket(ZMQ.REP);
            socket.bind("tcp://" + ZKUtils.getZeroMQCallbackBindAddress());
            active = true;
            while (active) {
                try {
                    byte[] bytes = socket.recv();
                    Kryo kryo = new Kryo();
                    Input input = new Input(new ByteArrayInputStream(bytes));
                    final CallbackContainer callbackContainer = kryo.readObject(input, CallbackContainer.class);
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
                    System.out.println("Error during receiving callback:");
                    e.printStackTrace();
                }
            }
            ZKUtils.closeSocketAndContext(socket, context);
        }catch (Exception e){
            System.out.println("Error during callback receiver startup:");
            e.printStackTrace(System.out);
        }
    }

}
