package com.jaffa.rpc.lib.common;

import com.jaffa.rpc.lib.entities.CallbackContainer;
import com.jaffa.rpc.lib.entities.Command;
import com.jaffa.rpc.lib.entities.ExceptionHolder;
import com.jaffa.rpc.lib.zookeeper.Utils;
import lombok.Getter;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class RequestInvoker {

    private static final Map<Class<?>, Class<?>> primitiveToWrappers = new HashMap<>();
    @Getter
    private static final Map<Class<?>, Object> wrappedServices = new HashMap<>();

    static {
        primitiveToWrappers.put(boolean.class, Boolean.class);
        primitiveToWrappers.put(byte.class, Byte.class);
        primitiveToWrappers.put(short.class, Short.class);
        primitiveToWrappers.put(char.class, Character.class);
        primitiveToWrappers.put(int.class, Integer.class);
        primitiveToWrappers.put(long.class, Long.class);
        primitiveToWrappers.put(float.class, Float.class);
        primitiveToWrappers.put(double.class, Double.class);
        primitiveToWrappers.put(void.class, Void.class);
    }

    private static Object getTargetService(Command command) throws ClassNotFoundException {
        return wrappedServices.get(Class.forName(Utils.getServiceInterfaceNameFromClient(command.getServiceClass())));
    }

    private static Method getTargetMethod(Command command) throws ClassNotFoundException, NoSuchMethodException {
        Object wrappedService = getTargetService(command);
        if (command.getMethodArgs() != null && command.getMethodArgs().length > 0) {
            Class<?>[] methodArgClasses = new Class[command.getMethodArgs().length];
            for (int i = 0; i < command.getMethodArgs().length; i++) {
                methodArgClasses[i] = Class.forName(command.getMethodArgs()[i]);
            }
            return wrappedService.getClass().getMethod(command.getMethodName(), methodArgClasses);
        } else {
            return wrappedService.getClass().getMethod(command.getMethodName());
        }
    }

    public static Object invoke(Command command) {
        try {
            Object targetService = getTargetService(command);
            Method targetMethod = getTargetMethod(command);
            Object result;
            if (command.getMethodArgs() != null && command.getMethodArgs().length > 0)
                result = targetMethod.invoke(targetService, command.getArgs());
            else
                result = targetMethod.invoke(targetService);
            if (targetMethod.getReturnType().equals(Void.TYPE)) return Void.TYPE;
            else return result;
        } catch (Exception e) {
            return e.getCause();
        }
    }

    public static Object getResult(Object result) {
        if (result instanceof Throwable) {
            StringWriter sw = new StringWriter();
            ((Throwable) result).printStackTrace(new PrintWriter(sw));
            return new ExceptionHolder(sw.toString());
        } else return result;
    }

    public static CallbackContainer constructCallbackContainer(Command command, Object result) throws ClassNotFoundException, NoSuchMethodException {
        CallbackContainer callbackContainer = new CallbackContainer();
        callbackContainer.setKey(command.getCallbackKey());
        callbackContainer.setListener(command.getCallbackClass());
        callbackContainer.setResult(getResult(result));
        Method targetMethod = getTargetMethod(command);
        if (primitiveToWrappers.containsKey(targetMethod.getReturnType())) {
            callbackContainer.setResultClass(primitiveToWrappers.get(targetMethod.getReturnType()).getName());
        } else {
            callbackContainer.setResultClass(targetMethod.getReturnType().getName());
        }
        return callbackContainer;
    }
}
