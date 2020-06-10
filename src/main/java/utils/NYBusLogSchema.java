package utils;

import entity.NYBusLog;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;

public class NYBusLogSchema implements DeserializationSchema<NYBusLog> {

    public NYBusLogSchema(){}

    @Override
    public NYBusLog deserialize(byte[] bytes) throws IOException {
        return NYBusLog.fromString(new String(bytes));
    }

    @Override
    public boolean isEndOfStream(NYBusLog nyBusLog) {
        return false;
    }

    @Override
    public TypeInformation<NYBusLog> getProducedType() {
        return TypeExtractor.getForClass(NYBusLog.class);
    }
}
