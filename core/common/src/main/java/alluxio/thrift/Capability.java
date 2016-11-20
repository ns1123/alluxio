/**
 * Autogenerated by Thrift Compiler (0.9.3)
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
@Generated(value = "Autogenerated by Thrift Compiler (0.9.3)")
public class Capability implements org.apache.thrift.TBase<Capability, Capability._Fields>, java.io.Serializable, Cloneable, Comparable<Capability> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("Capability");

  private static final org.apache.thrift.protocol.TField CONTENT_FIELD_DESC = new org.apache.thrift.protocol.TField("content", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField AUTHENTICATOR_FIELD_DESC = new org.apache.thrift.protocol.TField("authenticator", org.apache.thrift.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift.protocol.TField KEY_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("keyId", org.apache.thrift.protocol.TType.I64, (short)3);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new CapabilityStandardSchemeFactory());
    schemes.put(TupleScheme.class, new CapabilityTupleSchemeFactory());
  }

  private ByteBuffer content; // optional
  private ByteBuffer authenticator; // optional
  private long keyId; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    CONTENT((short)1, "content"),
    AUTHENTICATOR((short)2, "authenticator"),
    KEY_ID((short)3, "keyId");

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
        case 1: // CONTENT
          return CONTENT;
        case 2: // AUTHENTICATOR
          return AUTHENTICATOR;
        case 3: // KEY_ID
          return KEY_ID;
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
  private static final int __KEYID_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  private static final _Fields optionals[] = {_Fields.CONTENT,_Fields.AUTHENTICATOR,_Fields.KEY_ID};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.CONTENT, new org.apache.thrift.meta_data.FieldMetaData("content", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING        , true)));
    tmpMap.put(_Fields.AUTHENTICATOR, new org.apache.thrift.meta_data.FieldMetaData("authenticator", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING        , true)));
    tmpMap.put(_Fields.KEY_ID, new org.apache.thrift.meta_data.FieldMetaData("keyId", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(Capability.class, metaDataMap);
  }

  public Capability() {
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public Capability(Capability other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetContent()) {
      this.content = org.apache.thrift.TBaseHelper.copyBinary(other.content);
    }
    if (other.isSetAuthenticator()) {
      this.authenticator = org.apache.thrift.TBaseHelper.copyBinary(other.authenticator);
    }
    this.keyId = other.keyId;
  }

  public Capability deepCopy() {
    return new Capability(this);
  }

  @Override
  public void clear() {
    this.content = null;
    this.authenticator = null;
    setKeyIdIsSet(false);
    this.keyId = 0;
  }

  public byte[] getContent() {
    setContent(org.apache.thrift.TBaseHelper.rightSize(content));
    return content == null ? null : content.array();
  }

  public ByteBuffer bufferForContent() {
    return org.apache.thrift.TBaseHelper.copyBinary(content);
  }

  public Capability setContent(byte[] content) {
    this.content = content == null ? (ByteBuffer)null : ByteBuffer.wrap(Arrays.copyOf(content, content.length));
    return this;
  }

  public Capability setContent(ByteBuffer content) {
    this.content = org.apache.thrift.TBaseHelper.copyBinary(content);
    return this;
  }

  public void unsetContent() {
    this.content = null;
  }

  /** Returns true if field content is set (has been assigned a value) and false otherwise */
  public boolean isSetContent() {
    return this.content != null;
  }

  public void setContentIsSet(boolean value) {
    if (!value) {
      this.content = null;
    }
  }

  public byte[] getAuthenticator() {
    setAuthenticator(org.apache.thrift.TBaseHelper.rightSize(authenticator));
    return authenticator == null ? null : authenticator.array();
  }

  public ByteBuffer bufferForAuthenticator() {
    return org.apache.thrift.TBaseHelper.copyBinary(authenticator);
  }

  public Capability setAuthenticator(byte[] authenticator) {
    this.authenticator = authenticator == null ? (ByteBuffer)null : ByteBuffer.wrap(Arrays.copyOf(authenticator, authenticator.length));
    return this;
  }

  public Capability setAuthenticator(ByteBuffer authenticator) {
    this.authenticator = org.apache.thrift.TBaseHelper.copyBinary(authenticator);
    return this;
  }

  public void unsetAuthenticator() {
    this.authenticator = null;
  }

  /** Returns true if field authenticator is set (has been assigned a value) and false otherwise */
  public boolean isSetAuthenticator() {
    return this.authenticator != null;
  }

  public void setAuthenticatorIsSet(boolean value) {
    if (!value) {
      this.authenticator = null;
    }
  }

  public long getKeyId() {
    return this.keyId;
  }

  public Capability setKeyId(long keyId) {
    this.keyId = keyId;
    setKeyIdIsSet(true);
    return this;
  }

  public void unsetKeyId() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __KEYID_ISSET_ID);
  }

  /** Returns true if field keyId is set (has been assigned a value) and false otherwise */
  public boolean isSetKeyId() {
    return EncodingUtils.testBit(__isset_bitfield, __KEYID_ISSET_ID);
  }

  public void setKeyIdIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __KEYID_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case CONTENT:
      if (value == null) {
        unsetContent();
      } else {
        setContent((ByteBuffer)value);
      }
      break;

    case AUTHENTICATOR:
      if (value == null) {
        unsetAuthenticator();
      } else {
        setAuthenticator((ByteBuffer)value);
      }
      break;

    case KEY_ID:
      if (value == null) {
        unsetKeyId();
      } else {
        setKeyId((Long)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case CONTENT:
      return getContent();

    case AUTHENTICATOR:
      return getAuthenticator();

    case KEY_ID:
      return getKeyId();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case CONTENT:
      return isSetContent();
    case AUTHENTICATOR:
      return isSetAuthenticator();
    case KEY_ID:
      return isSetKeyId();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof Capability)
      return this.equals((Capability)that);
    return false;
  }

  public boolean equals(Capability that) {
    if (that == null)
      return false;

    boolean this_present_content = true && this.isSetContent();
    boolean that_present_content = true && that.isSetContent();
    if (this_present_content || that_present_content) {
      if (!(this_present_content && that_present_content))
        return false;
      if (!this.content.equals(that.content))
        return false;
    }

    boolean this_present_authenticator = true && this.isSetAuthenticator();
    boolean that_present_authenticator = true && that.isSetAuthenticator();
    if (this_present_authenticator || that_present_authenticator) {
      if (!(this_present_authenticator && that_present_authenticator))
        return false;
      if (!this.authenticator.equals(that.authenticator))
        return false;
    }

    boolean this_present_keyId = true && this.isSetKeyId();
    boolean that_present_keyId = true && that.isSetKeyId();
    if (this_present_keyId || that_present_keyId) {
      if (!(this_present_keyId && that_present_keyId))
        return false;
      if (this.keyId != that.keyId)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_content = true && (isSetContent());
    list.add(present_content);
    if (present_content)
      list.add(content);

    boolean present_authenticator = true && (isSetAuthenticator());
    list.add(present_authenticator);
    if (present_authenticator)
      list.add(authenticator);

    boolean present_keyId = true && (isSetKeyId());
    list.add(present_keyId);
    if (present_keyId)
      list.add(keyId);

    return list.hashCode();
  }

  @Override
  public int compareTo(Capability other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetContent()).compareTo(other.isSetContent());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetContent()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.content, other.content);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetAuthenticator()).compareTo(other.isSetAuthenticator());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetAuthenticator()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.authenticator, other.authenticator);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetKeyId()).compareTo(other.isSetKeyId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetKeyId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.keyId, other.keyId);
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
    StringBuilder sb = new StringBuilder("Capability(");
    boolean first = true;

    if (isSetContent()) {
      sb.append("content:");
      if (this.content == null) {
        sb.append("null");
      } else {
        org.apache.thrift.TBaseHelper.toString(this.content, sb);
      }
      first = false;
    }
    if (isSetAuthenticator()) {
      if (!first) sb.append(", ");
      sb.append("authenticator:");
      if (this.authenticator == null) {
        sb.append("null");
      } else {
        org.apache.thrift.TBaseHelper.toString(this.authenticator, sb);
      }
      first = false;
    }
    if (isSetKeyId()) {
      if (!first) sb.append(", ");
      sb.append("keyId:");
      sb.append(this.keyId);
      first = false;
    }
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

  private static class CapabilityStandardSchemeFactory implements SchemeFactory {
    public CapabilityStandardScheme getScheme() {
      return new CapabilityStandardScheme();
    }
  }

  private static class CapabilityStandardScheme extends StandardScheme<Capability> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, Capability struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // CONTENT
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.content = iprot.readBinary();
              struct.setContentIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // AUTHENTICATOR
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.authenticator = iprot.readBinary();
              struct.setAuthenticatorIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // KEY_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.keyId = iprot.readI64();
              struct.setKeyIdIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, Capability struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.content != null) {
        if (struct.isSetContent()) {
          oprot.writeFieldBegin(CONTENT_FIELD_DESC);
          oprot.writeBinary(struct.content);
          oprot.writeFieldEnd();
        }
      }
      if (struct.authenticator != null) {
        if (struct.isSetAuthenticator()) {
          oprot.writeFieldBegin(AUTHENTICATOR_FIELD_DESC);
          oprot.writeBinary(struct.authenticator);
          oprot.writeFieldEnd();
        }
      }
      if (struct.isSetKeyId()) {
        oprot.writeFieldBegin(KEY_ID_FIELD_DESC);
        oprot.writeI64(struct.keyId);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class CapabilityTupleSchemeFactory implements SchemeFactory {
    public CapabilityTupleScheme getScheme() {
      return new CapabilityTupleScheme();
    }
  }

  private static class CapabilityTupleScheme extends TupleScheme<Capability> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, Capability struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetContent()) {
        optionals.set(0);
      }
      if (struct.isSetAuthenticator()) {
        optionals.set(1);
      }
      if (struct.isSetKeyId()) {
        optionals.set(2);
      }
      oprot.writeBitSet(optionals, 3);
      if (struct.isSetContent()) {
        oprot.writeBinary(struct.content);
      }
      if (struct.isSetAuthenticator()) {
        oprot.writeBinary(struct.authenticator);
      }
      if (struct.isSetKeyId()) {
        oprot.writeI64(struct.keyId);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, Capability struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(3);
      if (incoming.get(0)) {
        struct.content = iprot.readBinary();
        struct.setContentIsSet(true);
      }
      if (incoming.get(1)) {
        struct.authenticator = iprot.readBinary();
        struct.setAuthenticatorIsSet(true);
      }
      if (incoming.get(2)) {
        struct.keyId = iprot.readI64();
        struct.setKeyIdIsSet(true);
      }
    }
  }

}

