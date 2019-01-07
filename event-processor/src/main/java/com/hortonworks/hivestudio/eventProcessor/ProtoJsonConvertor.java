/*
 *
 * HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 *
 * (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 *
 * This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 * Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 * to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 * properly licensed third party, you do not have any rights to this code.
 *
 * If this code is provided to you under the terms of the AGPLv3:
 * (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 * (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *   LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 * (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *   FROM OR RELATED TO THE CODE; AND
 * (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *   DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *   DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *   OR LOSS OR CORRUPTION OF DATA.
 *
 */
package com.hortonworks.hivestudio.eventProcessor;

import java.io.Closeable;
import java.io.EOFException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.hooks.proto.HiveHookEvents.HiveHookEventProto;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.dag.history.logging.proto.DatePartitionedLogger;
import org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos.HistoryEventProto;
import org.apache.tez.dag.history.logging.proto.ProtoMessageReader;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.io.SerializedString;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;

import io.dropwizard.cli.Command;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

public class ProtoJsonConvertor extends Command {

  public ProtoJsonConvertor() {
    super("protodump", "Convert proto files to readable json files.");
  }

  @Override
  public void configure(Subparser subparser) {
    subparser.addArgument("--type", "-t").type(String.class).required(true)
        .help("The proto type: <tez|hive>").choices("tez", "hive");

    subparser.addArgument("--src", "-s").required(true).type(String.class)
        .help("The file to convert to json.");

    subparser.addArgument("--dest", "-d").required(true).type(String.class)
        .help("File to save the json output.");
}

  @Override
  public void run(Bootstrap<?> bootstrap, Namespace namespace) throws Exception {
    String type = namespace.getString("type");
    String srcFile = namespace.getString("src");
    String destFile = namespace.getString("dest");
    try (ProtoMessageReader<? extends MessageOrBuilder> reader = getLoggerForType(type, srcFile);
        JsonFormatter formatter = new JsonFormatter(new FileOutputStream(destFile))) {
      for (MessageOrBuilder evt = reader.readEvent(); evt != null; evt = reader.readEvent()) {
        formatter.printMessage(evt);
      }
    } catch (EOFException e) {
      // All good.
    }
  }

  public ProtoMessageReader<? extends MessageOrBuilder> getLoggerForType(String type,
      String filePath) throws IOException {
    Configuration conf = new Configuration();
    SystemClock clock = SystemClock.getInstance();
    DatePartitionedLogger<? extends MessageOrBuilder> logger;
    if ("tez".equalsIgnoreCase(type)) {
      logger = new DatePartitionedLogger<>(HistoryEventProto.PARSER, new Path("/"), conf, clock);
    } else if ("hive".equalsIgnoreCase(type)) {
      logger = new DatePartitionedLogger<>(HiveHookEventProto.PARSER, new Path("/"), conf, clock);
    } else {
      throw new RuntimeException("Unexpected type : " + type);
    }
    return logger.getReader(new Path(filePath));
  }

  private static final class JsonFormatter implements Closeable {
    private static final JsonFactory jsonFactory = new JsonFactory();
    private final JsonGenerator generator;

    private JsonFormatter(OutputStream out) throws IOException {
      generator = jsonFactory.createGenerator(out);
      generator.setRootValueSeparator(new SerializedString("\n"));
    }

    @Override
    public void close() throws IOException {
      generator.close();
    }

    public void printMessage(MessageOrBuilder message) throws IOException {
      generator.writeStartObject();
      for (Map.Entry<FieldDescriptor, Object> field : message.getAllFields().entrySet()) {
        printField(field.getKey(), field.getValue());
      }
      generator.writeEndObject();
      // message.getUnknownFields() ignored, since we cannot do much about it.
    }

    private void printField(FieldDescriptor field, Object value) throws IOException {
      generator.writeFieldName(field.getName());
      if (field.isRepeated()) {
        generator.writeStartArray();
        for (Object element : (List<?>) value) {
          printFieldValue(field, element);
        }
        generator.writeEndArray();
      } else {
        printFieldValue(field, value);
      }
    }

    private void printFieldValue(FieldDescriptor field, Object value) throws IOException {
      switch (field.getJavaType()) {
        case INT:
          generator.writeNumber((Integer) value);
          break;
        case LONG:
          generator.writeNumber((Long) value);
          break;
        case BOOLEAN:
          generator.writeBoolean((Boolean) value);
          break;
        case FLOAT:
          generator.writeNumber((Float) value);
          break;
        case DOUBLE:
          generator.writeNumber((Double) value);
          break;
        case STRING:
          generator.writeString((String) value);
          break;
        case BYTE_STRING:
          ByteString byteString = ((ByteString) value);
          byte[] bytes = new byte[byteString.size()];
          byteString.copyTo(bytes, 0);
          generator.writeBinary(bytes);
          break;
        case ENUM:
          generator.writeString(((EnumValueDescriptor) value).getName());
          break;
        case MESSAGE:
          printMessage((Message) value);
          break;
      }
    }
  }
}
