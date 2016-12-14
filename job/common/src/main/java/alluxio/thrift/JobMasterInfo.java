/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package alluxio.thrift;

import org.apache.thrift.EncodingUtils;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;
import org.apache.thrift.scheme.TupleScheme;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Generated;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
@Generated(value = "Autogenerated by Thrift Compiler (0.9.3)")
public class JobMasterInfo implements org.apache.thrift.TBase<JobMasterInfo, JobMasterInfo._Fields>, java.io.Serializable, Cloneable, Comparable<JobMasterInfo> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("JobMasterInfo");

  private static final org.apache.thrift.protocol.TField WEB_PORT_FIELD_DESC = new org.apache.thrift.protocol.TField("webPort", org.apache.thrift.protocol.TType.I32, (short)1);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new JobMasterInfoStandardSchemeFactory());
    schemes.put(TupleScheme.class, new JobMasterInfoTupleSchemeFactory());
  }

  private int webPort; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    WEB_PORT((short)1, "webPort");

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
        case 1: // WEB_PORT
          return WEB_PORT;
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
  private static final int __WEBPORT_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.WEB_PORT, new org.apache.thrift.meta_data.FieldMetaData("webPort", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(JobMasterInfo.class, metaDataMap);
  }

  public JobMasterInfo() {
  }

  public JobMasterInfo(
    int webPort)
  {
    this();
    this.webPort = webPort;
    setWebPortIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public JobMasterInfo(JobMasterInfo other) {
    __isset_bitfield = other.__isset_bitfield;
    this.webPort = other.webPort;
  }

  public JobMasterInfo deepCopy() {
    return new JobMasterInfo(this);
  }

  @Override
  public void clear() {
    setWebPortIsSet(false);
    this.webPort = 0;
  }

  public int getWebPort() {
    return this.webPort;
  }

  public JobMasterInfo setWebPort(int webPort) {
    this.webPort = webPort;
    setWebPortIsSet(true);
    return this;
  }

  public void unsetWebPort() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __WEBPORT_ISSET_ID);
  }

  /** Returns true if field webPort is set (has been assigned a value) and false otherwise */
  public boolean isSetWebPort() {
    return EncodingUtils.testBit(__isset_bitfield, __WEBPORT_ISSET_ID);
  }

  public void setWebPortIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __WEBPORT_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case WEB_PORT:
      if (value == null) {
        unsetWebPort();
      } else {
        setWebPort((Integer)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case WEB_PORT:
      return getWebPort();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case WEB_PORT:
      return isSetWebPort();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof JobMasterInfo)
      return this.equals((JobMasterInfo)that);
    return false;
  }

  public boolean equals(JobMasterInfo that) {
    if (that == null)
      return false;

    boolean this_present_webPort = true;
    boolean that_present_webPort = true;
    if (this_present_webPort || that_present_webPort) {
      if (!(this_present_webPort && that_present_webPort))
        return false;
      if (this.webPort != that.webPort)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_webPort = true;
    list.add(present_webPort);
    if (present_webPort)
      list.add(webPort);

    return list.hashCode();
  }

  @Override
  public int compareTo(JobMasterInfo other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetWebPort()).compareTo(other.isSetWebPort());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetWebPort()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.webPort, other.webPort);
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
    StringBuilder sb = new StringBuilder("JobMasterInfo(");
    boolean first = true;

    sb.append("webPort:");
    sb.append(this.webPort);
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

  private static class JobMasterInfoStandardSchemeFactory implements SchemeFactory {
    public JobMasterInfoStandardScheme getScheme() {
      return new JobMasterInfoStandardScheme();
    }
  }

  private static class JobMasterInfoStandardScheme extends StandardScheme<JobMasterInfo> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, JobMasterInfo struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) {
          break;
        }
        switch (schemeField.id) {
          case 1: // WEB_PORT
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.webPort = iprot.readI32();
              struct.setWebPortIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, JobMasterInfo struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(WEB_PORT_FIELD_DESC);
      oprot.writeI32(struct.webPort);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class JobMasterInfoTupleSchemeFactory implements SchemeFactory {
    public JobMasterInfoTupleScheme getScheme() {
      return new JobMasterInfoTupleScheme();
    }
  }

  private static class JobMasterInfoTupleScheme extends TupleScheme<JobMasterInfo> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, JobMasterInfo struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetWebPort()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetWebPort()) {
        oprot.writeI32(struct.webPort);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, JobMasterInfo struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        struct.webPort = iprot.readI32();
        struct.setWebPortIsSet(true);
      }
    }
  }

}

