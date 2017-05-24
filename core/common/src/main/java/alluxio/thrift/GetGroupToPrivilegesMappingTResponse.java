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
public class GetGroupToPrivilegesMappingTResponse implements org.apache.thrift.TBase<GetGroupToPrivilegesMappingTResponse, GetGroupToPrivilegesMappingTResponse._Fields>, java.io.Serializable, Cloneable, Comparable<GetGroupToPrivilegesMappingTResponse> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("GetGroupToPrivilegesMappingTResponse");

  private static final org.apache.thrift.protocol.TField GROUP_PRIVILEGES_MAP_FIELD_DESC = new org.apache.thrift.protocol.TField("groupPrivilegesMap", org.apache.thrift.protocol.TType.MAP, (short)1);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new GetGroupToPrivilegesMappingTResponseStandardSchemeFactory());
    schemes.put(TupleScheme.class, new GetGroupToPrivilegesMappingTResponseTupleSchemeFactory());
  }

  private Map<String,List<TPrivilege>> groupPrivilegesMap; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    GROUP_PRIVILEGES_MAP((short)1, "groupPrivilegesMap");

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
        case 1: // GROUP_PRIVILEGES_MAP
          return GROUP_PRIVILEGES_MAP;
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
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.GROUP_PRIVILEGES_MAP, new org.apache.thrift.meta_data.FieldMetaData("groupPrivilegesMap", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
                new org.apache.thrift.meta_data.EnumMetaData(org.apache.thrift.protocol.TType.ENUM, TPrivilege.class)))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(GetGroupToPrivilegesMappingTResponse.class, metaDataMap);
  }

  public GetGroupToPrivilegesMappingTResponse() {
  }

  public GetGroupToPrivilegesMappingTResponse(
    Map<String,List<TPrivilege>> groupPrivilegesMap)
  {
    this();
    this.groupPrivilegesMap = groupPrivilegesMap;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public GetGroupToPrivilegesMappingTResponse(GetGroupToPrivilegesMappingTResponse other) {
    if (other.isSetGroupPrivilegesMap()) {
      Map<String,List<TPrivilege>> __this__groupPrivilegesMap = new HashMap<String,List<TPrivilege>>(other.groupPrivilegesMap.size());
      for (Map.Entry<String, List<TPrivilege>> other_element : other.groupPrivilegesMap.entrySet()) {

        String other_element_key = other_element.getKey();
        List<TPrivilege> other_element_value = other_element.getValue();

        String __this__groupPrivilegesMap_copy_key = other_element_key;

        List<TPrivilege> __this__groupPrivilegesMap_copy_value = new ArrayList<TPrivilege>(other_element_value.size());
        for (TPrivilege other_element_value_element : other_element_value) {
          __this__groupPrivilegesMap_copy_value.add(other_element_value_element);
        }

        __this__groupPrivilegesMap.put(__this__groupPrivilegesMap_copy_key, __this__groupPrivilegesMap_copy_value);
      }
      this.groupPrivilegesMap = __this__groupPrivilegesMap;
    }
  }

  public GetGroupToPrivilegesMappingTResponse deepCopy() {
    return new GetGroupToPrivilegesMappingTResponse(this);
  }

  @Override
  public void clear() {
    this.groupPrivilegesMap = null;
  }

  public int getGroupPrivilegesMapSize() {
    return (this.groupPrivilegesMap == null) ? 0 : this.groupPrivilegesMap.size();
  }

  public void putToGroupPrivilegesMap(String key, List<TPrivilege> val) {
    if (this.groupPrivilegesMap == null) {
      this.groupPrivilegesMap = new HashMap<String,List<TPrivilege>>();
    }
    this.groupPrivilegesMap.put(key, val);
  }

  public Map<String,List<TPrivilege>> getGroupPrivilegesMap() {
    return this.groupPrivilegesMap;
  }

  public GetGroupToPrivilegesMappingTResponse setGroupPrivilegesMap(Map<String,List<TPrivilege>> groupPrivilegesMap) {
    this.groupPrivilegesMap = groupPrivilegesMap;
    return this;
  }

  public void unsetGroupPrivilegesMap() {
    this.groupPrivilegesMap = null;
  }

  /** Returns true if field groupPrivilegesMap is set (has been assigned a value) and false otherwise */
  public boolean isSetGroupPrivilegesMap() {
    return this.groupPrivilegesMap != null;
  }

  public void setGroupPrivilegesMapIsSet(boolean value) {
    if (!value) {
      this.groupPrivilegesMap = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case GROUP_PRIVILEGES_MAP:
      if (value == null) {
        unsetGroupPrivilegesMap();
      } else {
        setGroupPrivilegesMap((Map<String,List<TPrivilege>>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case GROUP_PRIVILEGES_MAP:
      return getGroupPrivilegesMap();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case GROUP_PRIVILEGES_MAP:
      return isSetGroupPrivilegesMap();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof GetGroupToPrivilegesMappingTResponse)
      return this.equals((GetGroupToPrivilegesMappingTResponse)that);
    return false;
  }

  public boolean equals(GetGroupToPrivilegesMappingTResponse that) {
    if (that == null)
      return false;

    boolean this_present_groupPrivilegesMap = true && this.isSetGroupPrivilegesMap();
    boolean that_present_groupPrivilegesMap = true && that.isSetGroupPrivilegesMap();
    if (this_present_groupPrivilegesMap || that_present_groupPrivilegesMap) {
      if (!(this_present_groupPrivilegesMap && that_present_groupPrivilegesMap))
        return false;
      if (!this.groupPrivilegesMap.equals(that.groupPrivilegesMap))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_groupPrivilegesMap = true && (isSetGroupPrivilegesMap());
    list.add(present_groupPrivilegesMap);
    if (present_groupPrivilegesMap)
      list.add(groupPrivilegesMap);

    return list.hashCode();
  }

  @Override
  public int compareTo(GetGroupToPrivilegesMappingTResponse other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetGroupPrivilegesMap()).compareTo(other.isSetGroupPrivilegesMap());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetGroupPrivilegesMap()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.groupPrivilegesMap, other.groupPrivilegesMap);
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
    StringBuilder sb = new StringBuilder("GetGroupToPrivilegesMappingTResponse(");
    boolean first = true;

    sb.append("groupPrivilegesMap:");
    if (this.groupPrivilegesMap == null) {
      sb.append("null");
    } else {
      sb.append(this.groupPrivilegesMap);
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
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class GetGroupToPrivilegesMappingTResponseStandardSchemeFactory implements SchemeFactory {
    public GetGroupToPrivilegesMappingTResponseStandardScheme getScheme() {
      return new GetGroupToPrivilegesMappingTResponseStandardScheme();
    }
  }

  private static class GetGroupToPrivilegesMappingTResponseStandardScheme extends StandardScheme<GetGroupToPrivilegesMappingTResponse> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, GetGroupToPrivilegesMappingTResponse struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // GROUP_PRIVILEGES_MAP
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map16 = iprot.readMapBegin();
                struct.groupPrivilegesMap = new HashMap<String,List<TPrivilege>>(2*_map16.size);
                String _key17;
                List<TPrivilege> _val18;
                for (int _i19 = 0; _i19 < _map16.size; ++_i19)
                {
                  _key17 = iprot.readString();
                  {
                    org.apache.thrift.protocol.TList _list20 = iprot.readListBegin();
                    _val18 = new ArrayList<TPrivilege>(_list20.size);
                    TPrivilege _elem21;
                    for (int _i22 = 0; _i22 < _list20.size; ++_i22)
                    {
                      _elem21 = alluxio.thrift.TPrivilege.findByValue(iprot.readI32());
                      _val18.add(_elem21);
                    }
                    iprot.readListEnd();
                  }
                  struct.groupPrivilegesMap.put(_key17, _val18);
                }
                iprot.readMapEnd();
              }
              struct.setGroupPrivilegesMapIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, GetGroupToPrivilegesMappingTResponse struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.groupPrivilegesMap != null) {
        oprot.writeFieldBegin(GROUP_PRIVILEGES_MAP_FIELD_DESC);
        {
          oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.LIST, struct.groupPrivilegesMap.size()));
          for (Map.Entry<String, List<TPrivilege>> _iter23 : struct.groupPrivilegesMap.entrySet())
          {
            oprot.writeString(_iter23.getKey());
            {
              oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.I32, _iter23.getValue().size()));
              for (TPrivilege _iter24 : _iter23.getValue())
              {
                oprot.writeI32(_iter24.getValue());
              }
              oprot.writeListEnd();
            }
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class GetGroupToPrivilegesMappingTResponseTupleSchemeFactory implements SchemeFactory {
    public GetGroupToPrivilegesMappingTResponseTupleScheme getScheme() {
      return new GetGroupToPrivilegesMappingTResponseTupleScheme();
    }
  }

  private static class GetGroupToPrivilegesMappingTResponseTupleScheme extends TupleScheme<GetGroupToPrivilegesMappingTResponse> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, GetGroupToPrivilegesMappingTResponse struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetGroupPrivilegesMap()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetGroupPrivilegesMap()) {
        {
          oprot.writeI32(struct.groupPrivilegesMap.size());
          for (Map.Entry<String, List<TPrivilege>> _iter25 : struct.groupPrivilegesMap.entrySet())
          {
            oprot.writeString(_iter25.getKey());
            {
              oprot.writeI32(_iter25.getValue().size());
              for (TPrivilege _iter26 : _iter25.getValue())
              {
                oprot.writeI32(_iter26.getValue());
              }
            }
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, GetGroupToPrivilegesMappingTResponse struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        {
          org.apache.thrift.protocol.TMap _map27 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.LIST, iprot.readI32());
          struct.groupPrivilegesMap = new HashMap<String,List<TPrivilege>>(2*_map27.size);
          String _key28;
          List<TPrivilege> _val29;
          for (int _i30 = 0; _i30 < _map27.size; ++_i30)
          {
            _key28 = iprot.readString();
            {
              org.apache.thrift.protocol.TList _list31 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.I32, iprot.readI32());
              _val29 = new ArrayList<TPrivilege>(_list31.size);
              TPrivilege _elem32;
              for (int _i33 = 0; _i33 < _list31.size; ++_i33)
              {
                _elem32 = alluxio.thrift.TPrivilege.findByValue(iprot.readI32());
                _val29.add(_elem32);
              }
            }
            struct.groupPrivilegesMap.put(_key28, _val29);
          }
        }
        struct.setGroupPrivilegesMapIsSet(true);
      }
    }
  }

}

