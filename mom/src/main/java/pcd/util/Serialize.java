package pcd.util;

import java.io.*;

/**
 * Utility class providing methods for object serialization and deserialization.
 * <p>
 * Used to convert Java message objects into byte arrays for transmission over RabbitMQ,
 * and to reconstruct the objects upon receipt.
 */
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
