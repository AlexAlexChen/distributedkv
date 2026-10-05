package com.distributedkv.common.util;

import com.distributedkv.common.Status;
import com.distributedkv.common.Result;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Utility class for serialization and deserialization
 */
public class SerializationUtil {
    /**
     * Serialize a Java object using standard Java serialization
     *
     * @param object the object to serialize
     * @return the serialized object as a byte array
     * @throws IOException if serialization fails
     */
    public static byte[] serializeObject(Serializable object) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(object);
        }
        return baos.toByteArray();
    }

    /**
     * Deserialize a Java object using standard Java serialization
     *
     * @param <T> the type of the object
     * @param bytes the serialized object
     * @return the deserialized object
     * @throws IOException if deserialization fails
     * @throws ClassNotFoundException if the class of the serialized object cannot be found
     */
    @SuppressWarnings("unchecked")
    public static <T> T deserializeObject(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        try (ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (T) ois.readObject();
        }
    }

    /**
     * Serialize a Protocol Buffers message
     *
     * @param message the message to serialize
     * @return the serialized message as a byte array
     */
    public static byte[] serializeProto(Message message) {
        return message.toByteArray();
    }

    /**
     * Deserialize a Protocol Buffers message
     *
     * @param <T> the type of the message
     * @param bytes the serialized message
     * @param parser the parser for the message type
     * @return the deserialized message
     * @throws InvalidProtocolBufferException if deserialization fails
     */
    public static <T extends Message> T deserializeProto(byte[] bytes, Parser<T> parser)
            throws InvalidProtocolBufferException {
        return parser.parseFrom(bytes);
    }

    /**
     * Compress data using GZIP
     *
     * @param data the data to compress
     * @return the compressed data
     * @throws IOException if compression fails
     */
    public static byte[] compress(byte[] data) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gos = new GZIPOutputStream(baos) {
            {
                def.setLevel(Deflater.BEST_COMPRESSION);
            }
        }) {
            gos.write(data);
        }
        return baos.toByteArray();
    }

    /**
     * Decompress data using GZIP
     *
     * @param compressedData the compressed data
     * @return the decompressed data
     * @throws IOException if decompression fails
     */
    public static byte[] decompress(byte[] compressedData) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(compressedData);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try (GZIPInputStream gis = new GZIPInputStream(bais)) {
            byte[] buffer = new byte[1024];
            int len;
            while ((len = gis.read(buffer)) > 0) {
                baos.write(buffer, 0, len);
            }
        }

        return baos.toByteArray();
    }

    /**
     * Encode binary data as a Base64 string
     *
     * @param data the data to encode
     * @return the Base64 encoded string
     */
    public static String encodeBase64(byte[] data) {
        return Base64.getEncoder().encodeToString(data);
    }

    /**
     * Decode a Base64 string to binary data
     *
     * @param base64 the Base64 encoded string
     * @return the decoded data
     */
    public static byte[] decodeBase64(String base64) {
        return Base64.getDecoder().decode(base64);
    }

    /**
     * Encode binary data as a URL-safe Base64 string
     *
     * @param data the data to encode
     * @return the URL-safe Base64 encoded string
     */
    public static String encodeBase64UrlSafe(byte[] data) {
        return Base64.getUrlEncoder().encodeToString(data);
    }

    /**
     * Decode a URL-safe Base64 string to binary data
     *
     * @param base64 the URL-safe Base64 encoded string
     * @return the decoded data
     */
    public static byte[] decodeBase64UrlSafe(String base64) {
        return Base64.getUrlDecoder().decode(base64);
    }

    /**
     * Serialize a long value
     *
     * @param value the long value
     * @return the serialized value
     */
    public static byte[] serializeLong(long value) {
        return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
    }

    /**
     * Deserialize a long value
     *
     * @param bytes the serialized value
     * @return the long value
     */
    public static long deserializeLong(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getLong();
    }

    /**
     * Serialize an int value
     *
     * @param value the int value
     * @return the serialized value
     */
    public static byte[] serializeInt(int value) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
    }

    /**
     * Deserialize an int value
     *
     * @param bytes the serialized value
     * @return the int value
     */
    public static int deserializeInt(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getInt();
    }

    /**
     * Serialize a double value
     *
     * @param value the double value
     * @return the serialized value
     */
    public static byte[] serializeDouble(double value) {
        return ByteBuffer.allocate(Double.BYTES).putDouble(value).array();
    }

    /**
     * Deserialize a double value
     *
     * @param bytes the serialized value
     * @return the double value
     */
    public static double deserializeDouble(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getDouble();
    }

    /**
     * Serialize a string using UTF-8 encoding
     *
     * @param value the string value
     * @return the serialized value
     */
    public static byte[] serializeString(String value) {
        return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * Deserialize a string using UTF-8 encoding
     *
     * @param bytes the serialized value
     * @return the string value
     */
    public static String deserializeString(byte[] bytes) {
        return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * Create a deserializer for Protocol Buffers messages with error handling
     *
     * @param <T> the type of the message
     * @param parser the parser for the message type
     * @return a function that deserializes a message and returns a status and message
     */
    public static <T extends Message> java.util.function.Function<byte[], Result<T>>
            protoDeserializer(Parser<T> parser) {
        return bytes -> {
            try {
                return Result.success(parser.parseFrom(bytes));
            } catch (InvalidProtocolBufferException e) {
                return Result.fail(Status.SERIALIZATION_ERROR, "Failed to deserialize protobuf message: " + e.getMessage());
            }
        };
    }

    /**
     * Create a serializer for Protocol Buffers messages with error handling
     *
     * @param <T> the type of the message
     * @return a function that serializes a message and returns a status and byte array
     */
    public static <T extends Message> java.util.function.Function<T, Result<byte[]>>
            protoSerializer() {
        return message -> {
            try {
                return Result.success(message.toByteArray());
            } catch (Exception e) {
                return Result.fail(Status.SERIALIZATION_ERROR, "Failed to serialize protobuf message: " + e.getMessage());
            }
        };
    }

    /**
     * Create a deserializer for Java objects with error handling
     *
     * @param <T> the type of the object
     * @return a function that deserializes an object and returns a status and object
     */
    public static <T> java.util.function.Function<byte[], Result<T>>
            objectDeserializer() {
        return bytes -> {
            try {
                return Result.success(deserializeObject(bytes));
            } catch (IOException | ClassNotFoundException e) {
                return Result.fail(Status.SERIALIZATION_ERROR, "Failed to deserialize object: " + e.getMessage());
            }
        };
    }

    /**
     * Create a serializer for Java objects with error handling
     *
     * @param <T> the type of the object
     * @return a function that serializes an object and returns a status and byte array
     */
    public static <T extends Serializable> java.util.function.Function<T, Result<byte[]>>
            objectSerializer() {
        return object -> {
            try {
                return Result.success(serializeObject(object));
            } catch (IOException e) {
                return Result.fail(Status.SERIALIZATION_ERROR, "Failed to serialize object: " + e.getMessage());
            }
        };
    }

    /**
     * Serialize an object to JSON
     *
     * @param object the object to serialize
     * @return the JSON string
     * @throws IOException if serialization fails
     */
    public static String toJson(Object object) throws IOException {
        return com.google.gson.Gson.class.cast(new com.google.gson.Gson()).toJson(object);
    }

    /**
     * Deserialize an object from JSON
     *
     * @param <T> the type of the object
     * @param json the JSON string
     * @param clazz the class of the object
     * @return the deserialized object
     */
    public static <T> T fromJson(String json, Class<T> clazz) {
        return com.google.gson.Gson.class.cast(new com.google.gson.Gson()).fromJson(json, clazz);
    }

    /**
     * Create a pretty-printed JSON string
     *
     * @param object the object to serialize
     * @return the pretty-printed JSON string
     */
    public static String toPrettyJson(Object object) {
        return com.google.gson.Gson.class.cast(
                new com.google.gson.GsonBuilder().setPrettyPrinting().create()
        ).toJson(object);
    }

    /**
     * Check if a byte array is compressed with GZIP
     *
     * @param data the data to check
     * @return true if the data is compressed with GZIP
     */
    public static boolean isGzipped(byte[] data) {
        return data.length >= 2 &&
               (data[0] == (byte) GZIPInputStream.GZIP_MAGIC) &&
               (data[1] == (byte) (GZIPInputStream.GZIP_MAGIC >> 8));
    }

    /**
     * Calculate the size of an object in bytes
     *
     * @param obj the object to measure
     * @return the size in bytes
     * @throws IOException if serialization fails
     */
    public static int sizeOf(Serializable obj) throws IOException {
        return serializeObject(obj).length;
    }
}
