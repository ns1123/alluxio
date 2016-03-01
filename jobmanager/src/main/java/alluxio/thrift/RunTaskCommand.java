/**
 * Autogenerated by Thrift Compiler (0.9.2)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package alluxio.thrift;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.annotation.Generated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
@Generated(value = "Autogenerated by Thrift Compiler (0.9.2)", date = "2016-3-1")
public class RunTaskCommand implements org.apache.thrift.TBase<RunTaskCommand, RunTaskCommand._Fields>, java.io.Serializable, Cloneable, Comparable<RunTaskCommand> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("RunTaskCommand");

  private static final org.apache.thrift.protocol.TField JOB_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("jobId", org.apache.thrift.protocol.TType.I64, (short)1);
  private static final org.apache.thrift.protocol.TField TASK_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("TaskId", org.apache.thrift.protocol.TType.I32, (short)2);
  private static final org.apache.thrift.protocol.TField JOB_CONFIG_FIELD_DESC = new org.apache.thrift.protocol.TField("jobConfig", org.apache.thrift.protocol.TType.STRING, (short)3);
  private static final org.apache.thrift.protocol.TField TASK_ARGS_FIELD_DESC = new org.apache.thrift.protocol.TField("taskArgs", org.apache.thrift.protocol.TType.STRING, (short)4);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new RunTaskCommandStandardSchemeFactory());
    schemes.put(TupleScheme.class, new RunTaskCommandTupleSchemeFactory());
  }

  private long jobId; // required
  private int TaskId; // required
  private ByteBuffer jobConfig; // required
  private ByteBuffer taskArgs; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    JOB_ID((short)1, "jobId"),
    TASK_ID((short)2, "TaskId"),
    JOB_CONFIG((short)3, "jobConfig"),
    TASK_ARGS((short)4, "taskArgs");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // JOB_ID
          return JOB_ID;
        case 2: // TASK_ID
          return TASK_ID;
        case 3: // JOB_CONFIG
          return JOB_CONFIG;
        case 4: // TASK_ARGS
          return TASK_ARGS;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __JOBID_ISSET_ID = 0;
  private static final int __TASKID_ISSET_ID = 1;
  private byte __isset_bitfield = 0;
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.JOB_ID, new org.apache.thrift.meta_data.FieldMetaData("jobId", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.TASK_ID, new org.apache.thrift.meta_data.FieldMetaData("TaskId", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.JOB_CONFIG, new org.apache.thrift.meta_data.FieldMetaData("jobConfig", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING        , true)));
    tmpMap.put(_Fields.TASK_ARGS, new org.apache.thrift.meta_data.FieldMetaData("taskArgs", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING        , true)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(RunTaskCommand.class, metaDataMap);
  }

  public RunTaskCommand() {
  }

  public RunTaskCommand(
    long jobId,
    int TaskId,
    ByteBuffer jobConfig,
    ByteBuffer taskArgs)
  {
    this();
    this.jobId = jobId;
    setJobIdIsSet(true);
    this.TaskId = TaskId;
    setTaskIdIsSet(true);
    this.jobConfig = org.apache.thrift.TBaseHelper.copyBinary(jobConfig);
    this.taskArgs = org.apache.thrift.TBaseHelper.copyBinary(taskArgs);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public RunTaskCommand(RunTaskCommand other) {
    __isset_bitfield = other.__isset_bitfield;
    this.jobId = other.jobId;
    this.TaskId = other.TaskId;
    if (other.isSetJobConfig()) {
      this.jobConfig = org.apache.thrift.TBaseHelper.copyBinary(other.jobConfig);
    }
    if (other.isSetTaskArgs()) {
      this.taskArgs = org.apache.thrift.TBaseHelper.copyBinary(other.taskArgs);
    }
  }

  public RunTaskCommand deepCopy() {
    return new RunTaskCommand(this);
  }

  @Override
  public void clear() {
    setJobIdIsSet(false);
    this.jobId = 0;
    setTaskIdIsSet(false);
    this.TaskId = 0;
    this.jobConfig = null;
    this.taskArgs = null;
  }

  public long getJobId() {
    return this.jobId;
  }

  public RunTaskCommand setJobId(long jobId) {
    this.jobId = jobId;
    setJobIdIsSet(true);
    return this;
  }

  public void unsetJobId() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __JOBID_ISSET_ID);
  }

  /** Returns true if field jobId is set (has been assigned a value) and false otherwise */
  public boolean isSetJobId() {
    return EncodingUtils.testBit(__isset_bitfield, __JOBID_ISSET_ID);
  }

  public void setJobIdIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __JOBID_ISSET_ID, value);
  }

  public int getTaskId() {
    return this.TaskId;
  }

  public RunTaskCommand setTaskId(int TaskId) {
    this.TaskId = TaskId;
    setTaskIdIsSet(true);
    return this;
  }

  public void unsetTaskId() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __TASKID_ISSET_ID);
  }

  /** Returns true if field TaskId is set (has been assigned a value) and false otherwise */
  public boolean isSetTaskId() {
    return EncodingUtils.testBit(__isset_bitfield, __TASKID_ISSET_ID);
  }

  public void setTaskIdIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __TASKID_ISSET_ID, value);
  }

  public byte[] getJobConfig() {
    setJobConfig(org.apache.thrift.TBaseHelper.rightSize(jobConfig));
    return jobConfig == null ? null : jobConfig.array();
  }

  public ByteBuffer bufferForJobConfig() {
    return org.apache.thrift.TBaseHelper.copyBinary(jobConfig);
  }

  public RunTaskCommand setJobConfig(byte[] jobConfig) {
    this.jobConfig = jobConfig == null ? (ByteBuffer)null : ByteBuffer.wrap(Arrays.copyOf(jobConfig, jobConfig.length));
    return this;
  }

  public RunTaskCommand setJobConfig(ByteBuffer jobConfig) {
    this.jobConfig = org.apache.thrift.TBaseHelper.copyBinary(jobConfig);
    return this;
  }

  public void unsetJobConfig() {
    this.jobConfig = null;
  }

  /** Returns true if field jobConfig is set (has been assigned a value) and false otherwise */
  public boolean isSetJobConfig() {
    return this.jobConfig != null;
  }

  public void setJobConfigIsSet(boolean value) {
    if (!value) {
      this.jobConfig = null;
    }
  }

  public byte[] getTaskArgs() {
    setTaskArgs(org.apache.thrift.TBaseHelper.rightSize(taskArgs));
    return taskArgs == null ? null : taskArgs.array();
  }

  public ByteBuffer bufferForTaskArgs() {
    return org.apache.thrift.TBaseHelper.copyBinary(taskArgs);
  }

  public RunTaskCommand setTaskArgs(byte[] taskArgs) {
    this.taskArgs = taskArgs == null ? (ByteBuffer)null : ByteBuffer.wrap(Arrays.copyOf(taskArgs, taskArgs.length));
    return this;
  }

  public RunTaskCommand setTaskArgs(ByteBuffer taskArgs) {
    this.taskArgs = org.apache.thrift.TBaseHelper.copyBinary(taskArgs);
    return this;
  }

  public void unsetTaskArgs() {
    this.taskArgs = null;
  }

  /** Returns true if field taskArgs is set (has been assigned a value) and false otherwise */
  public boolean isSetTaskArgs() {
    return this.taskArgs != null;
  }

  public void setTaskArgsIsSet(boolean value) {
    if (!value) {
      this.taskArgs = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case JOB_ID:
      if (value == null) {
        unsetJobId();
      } else {
        setJobId((Long)value);
      }
      break;

    case TASK_ID:
      if (value == null) {
        unsetTaskId();
      } else {
        setTaskId((Integer)value);
      }
      break;

    case JOB_CONFIG:
      if (value == null) {
        unsetJobConfig();
      } else {
        setJobConfig((ByteBuffer)value);
      }
      break;

    case TASK_ARGS:
      if (value == null) {
        unsetTaskArgs();
      } else {
        setTaskArgs((ByteBuffer)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case JOB_ID:
      return Long.valueOf(getJobId());

    case TASK_ID:
      return Integer.valueOf(getTaskId());

    case JOB_CONFIG:
      return getJobConfig();

    case TASK_ARGS:
      return getTaskArgs();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case JOB_ID:
      return isSetJobId();
    case TASK_ID:
      return isSetTaskId();
    case JOB_CONFIG:
      return isSetJobConfig();
    case TASK_ARGS:
      return isSetTaskArgs();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof RunTaskCommand)
      return this.equals((RunTaskCommand)that);
    return false;
  }

  public boolean equals(RunTaskCommand that) {
    if (that == null)
      return false;

    boolean this_present_jobId = true;
    boolean that_present_jobId = true;
    if (this_present_jobId || that_present_jobId) {
      if (!(this_present_jobId && that_present_jobId))
        return false;
      if (this.jobId != that.jobId)
        return false;
    }

    boolean this_present_TaskId = true;
    boolean that_present_TaskId = true;
    if (this_present_TaskId || that_present_TaskId) {
      if (!(this_present_TaskId && that_present_TaskId))
        return false;
      if (this.TaskId != that.TaskId)
        return false;
    }

    boolean this_present_jobConfig = true && this.isSetJobConfig();
    boolean that_present_jobConfig = true && that.isSetJobConfig();
    if (this_present_jobConfig || that_present_jobConfig) {
      if (!(this_present_jobConfig && that_present_jobConfig))
        return false;
      if (!this.jobConfig.equals(that.jobConfig))
        return false;
    }

    boolean this_present_taskArgs = true && this.isSetTaskArgs();
    boolean that_present_taskArgs = true && that.isSetTaskArgs();
    if (this_present_taskArgs || that_present_taskArgs) {
      if (!(this_present_taskArgs && that_present_taskArgs))
        return false;
      if (!this.taskArgs.equals(that.taskArgs))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_jobId = true;
    list.add(present_jobId);
    if (present_jobId)
      list.add(jobId);

    boolean present_TaskId = true;
    list.add(present_TaskId);
    if (present_TaskId)
      list.add(TaskId);

    boolean present_jobConfig = true && (isSetJobConfig());
    list.add(present_jobConfig);
    if (present_jobConfig)
      list.add(jobConfig);

    boolean present_taskArgs = true && (isSetTaskArgs());
    list.add(present_taskArgs);
    if (present_taskArgs)
      list.add(taskArgs);

    return list.hashCode();
  }

  @Override
  public int compareTo(RunTaskCommand other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetJobId()).compareTo(other.isSetJobId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetJobId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.jobId, other.jobId);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetTaskId()).compareTo(other.isSetTaskId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTaskId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.TaskId, other.TaskId);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetJobConfig()).compareTo(other.isSetJobConfig());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetJobConfig()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.jobConfig, other.jobConfig);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetTaskArgs()).compareTo(other.isSetTaskArgs());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTaskArgs()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.taskArgs, other.taskArgs);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("RunTaskCommand(");
    boolean first = true;

    sb.append("jobId:");
    sb.append(this.jobId);
    first = false;
    if (!first) sb.append(", ");
    sb.append("TaskId:");
    sb.append(this.TaskId);
    first = false;
    if (!first) sb.append(", ");
    sb.append("jobConfig:");
    if (this.jobConfig == null) {
      sb.append("null");
    } else {
      org.apache.thrift.TBaseHelper.toString(this.jobConfig, sb);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("taskArgs:");
    if (this.taskArgs == null) {
      sb.append("null");
    } else {
      org.apache.thrift.TBaseHelper.toString(this.taskArgs, sb);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class RunTaskCommandStandardSchemeFactory implements SchemeFactory {
    public RunTaskCommandStandardScheme getScheme() {
      return new RunTaskCommandStandardScheme();
    }
  }

  private static class RunTaskCommandStandardScheme extends StandardScheme<RunTaskCommand> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, RunTaskCommand struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // JOB_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.jobId = iprot.readI64();
              struct.setJobIdIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // TASK_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.TaskId = iprot.readI32();
              struct.setTaskIdIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // JOB_CONFIG
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.jobConfig = iprot.readBinary();
              struct.setJobConfigIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // TASK_ARGS
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.taskArgs = iprot.readBinary();
              struct.setTaskArgsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, RunTaskCommand struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(JOB_ID_FIELD_DESC);
      oprot.writeI64(struct.jobId);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(TASK_ID_FIELD_DESC);
      oprot.writeI32(struct.TaskId);
      oprot.writeFieldEnd();
      if (struct.jobConfig != null) {
        oprot.writeFieldBegin(JOB_CONFIG_FIELD_DESC);
        oprot.writeBinary(struct.jobConfig);
        oprot.writeFieldEnd();
      }
      if (struct.taskArgs != null) {
        oprot.writeFieldBegin(TASK_ARGS_FIELD_DESC);
        oprot.writeBinary(struct.taskArgs);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class RunTaskCommandTupleSchemeFactory implements SchemeFactory {
    public RunTaskCommandTupleScheme getScheme() {
      return new RunTaskCommandTupleScheme();
    }
  }

  private static class RunTaskCommandTupleScheme extends TupleScheme<RunTaskCommand> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, RunTaskCommand struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetJobId()) {
        optionals.set(0);
      }
      if (struct.isSetTaskId()) {
        optionals.set(1);
      }
      if (struct.isSetJobConfig()) {
        optionals.set(2);
      }
      if (struct.isSetTaskArgs()) {
        optionals.set(3);
      }
      oprot.writeBitSet(optionals, 4);
      if (struct.isSetJobId()) {
        oprot.writeI64(struct.jobId);
      }
      if (struct.isSetTaskId()) {
        oprot.writeI32(struct.TaskId);
      }
      if (struct.isSetJobConfig()) {
        oprot.writeBinary(struct.jobConfig);
      }
      if (struct.isSetTaskArgs()) {
        oprot.writeBinary(struct.taskArgs);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, RunTaskCommand struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(4);
      if (incoming.get(0)) {
        struct.jobId = iprot.readI64();
        struct.setJobIdIsSet(true);
      }
      if (incoming.get(1)) {
        struct.TaskId = iprot.readI32();
        struct.setTaskIdIsSet(true);
      }
      if (incoming.get(2)) {
        struct.jobConfig = iprot.readBinary();
        struct.setJobConfigIsSet(true);
      }
      if (incoming.get(3)) {
        struct.taskArgs = iprot.readBinary();
        struct.setTaskArgsIsSet(true);
      }
    }
  }

}

