package techrom.kafka.connect.util;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.TimeZone;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;


public abstract class GoldenGateSMT<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    public static final int MILLIS_LENGTH = 13;

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define("field.name", ConfigDef.Type.STRING, "kafkaKey", ConfigDef.Importance.HIGH,
                    "Name of the field to insert the Kafka key to")
            .define("field.delimiter", ConfigDef.Type.STRING, "-", ConfigDef.Importance.LOW,
                    "Delimiter to use when concatenating the key fields");

    private static final String PURPOSE = "adding key to record";
    private static final Logger LOGGER = LoggerFactory.getLogger(GoldenGateSMT.class);
    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public R apply(R record) {
        if (record.valueSchema() == null) {
            return record;
        } else {
            return applyWithSchema(record);
        }
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);


        for (Field field : updatedSchema.fields()) {
            final Object fieldValue = value.get(field.name());
            updatedValue.put(field.name(), fieldValue);
        }

        updatedValue.put("csn", value.get("lsn"));
        updatedValue.put("op_type", value.get("op"));
        updatedValue.put("op_ts", formatDate(value.get("ts_ms")));
        updatedValue.put("op_ts_epoch",value.get("ts_ms") );
        updatedValue.put("curr_ts", formatDate(Instant.now().toEpochMilli()));
        updatedValue.put("current_ts_epoch", Instant.now().toEpochMilli());

        return newRecord(record, updatedSchema, updatedValue);
    }

    private String formatDate(Object ob) {
        Date date = new Date(Long.parseLong(ob.toString()) * 1000L);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC")); // Formato UTC

        return sdf.format(date);
    }

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    private Schema makeUpdatedSchema(Schema schema) {
        LOGGER.trace("build the updated schema");
        SchemaBuilder newSchemabuilder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        for (org.apache.kafka.connect.data.Field field : schema.fields()) {
            newSchemabuilder.field(field.name(), field.schema());
        }

        LOGGER.trace("adding the new fieldS: {}");
        newSchemabuilder.field("csn", Schema.INT64_SCHEMA);
        newSchemabuilder.field("op_type", Schema.STRING_SCHEMA);
        newSchemabuilder.field("op_ts", Schema.STRING_SCHEMA);
        newSchemabuilder.field("op_ts_epoch", Schema.INT64_SCHEMA);
        newSchemabuilder.field("curr_ts", Schema.STRING_SCHEMA);
        newSchemabuilder.field("current_ts_epoch", Schema.INT64_SCHEMA);
        return newSchemabuilder.build();
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }

}
