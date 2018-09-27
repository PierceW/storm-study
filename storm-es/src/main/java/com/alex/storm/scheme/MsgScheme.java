package com.alex.storm.scheme;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;

public class MsgScheme implements Scheme {
    @Override
    public List<Object> deserialize(ByteBuffer byteBuffer) {
        String val = byteBufferToString(byteBuffer);
        return new Values(val);
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("msg");
    }

    private String byteBufferToString(ByteBuffer byteBuffer) {
        CharBuffer charBuffer = null;

        Charset charset = Charset.forName("UTF-8");
        CharsetDecoder decoder = charset.newDecoder();
        try {
            charBuffer = decoder.decode(byteBuffer);
            byteBuffer.flip();
            return charBuffer.toString();
        } catch (CharacterCodingException e) {
            e.printStackTrace();
            return null;
        }
    }
}
