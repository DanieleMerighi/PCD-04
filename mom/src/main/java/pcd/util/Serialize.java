package pcd.util;

import java.io.*;

public class Serialize {

    public static byte[] serialize(Object obj) throws IOException {
        var byteOutputStream = new ByteArrayOutputStream();
        try (var objectOutputStream = new ObjectOutputStream(byteOutputStream)) {
            objectOutputStream.writeObject(obj);
        }
        return byteOutputStream.toByteArray();
    }

    public static Object deserialize(byte[] data) throws IOException, ClassNotFoundException {
        try (var byteInputStream = new ByteArrayInputStream(data);
             var objectInputStream = new ObjectInputStream(byteInputStream)) {
            return objectInputStream.readObject();
        }
    }
}
