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
@Generated(value = "Autogenerated by Thrift Compiler (0.9.3)", date = "2016-11-23")
public class MetaJobMasterClientService {

  /**
   * This interface contains meta job master service endpoints.
   */
  public interface Iface extends alluxio.thrift.AlluxioService.Iface {

    /**
     * Returns information about the job master.
     * 
     * @param fields /** optional filter for what fields to return, defaults to all
     */
    public JobMasterInfo getInfo(Set<JobMasterInfoField> fields) throws org.apache.thrift.TException;

  }

  public interface AsyncIface extends alluxio.thrift.AlluxioService .AsyncIface {

    public void getInfo(Set<JobMasterInfoField> fields, org.apache.thrift.async.AsyncMethodCallback resultHandler) throws org.apache.thrift.TException;

  }

  public static class Client extends alluxio.thrift.AlluxioService.Client implements Iface {
    public static class Factory implements org.apache.thrift.TServiceClientFactory<Client> {
      public Factory() {}
      public Client getClient(org.apache.thrift.protocol.TProtocol prot) {
        return new Client(prot);
      }
      public Client getClient(org.apache.thrift.protocol.TProtocol iprot, org.apache.thrift.protocol.TProtocol oprot) {
        return new Client(iprot, oprot);
      }
    }

    public Client(org.apache.thrift.protocol.TProtocol prot)
    {
      super(prot, prot);
    }

    public Client(org.apache.thrift.protocol.TProtocol iprot, org.apache.thrift.protocol.TProtocol oprot) {
      super(iprot, oprot);
    }

    public JobMasterInfo getInfo(Set<JobMasterInfoField> fields) throws org.apache.thrift.TException
    {
      send_getInfo(fields);
      return recv_getInfo();
    }

    public void send_getInfo(Set<JobMasterInfoField> fields) throws org.apache.thrift.TException
    {
      getInfo_args args = new getInfo_args();
      args.setFields(fields);
      sendBase("getInfo", args);
    }

    public JobMasterInfo recv_getInfo() throws org.apache.thrift.TException
    {
      getInfo_result result = new getInfo_result();
      receiveBase(result, "getInfo");
      if (result.isSetSuccess()) {
        return result.success;
      }
      throw new org.apache.thrift.TApplicationException(org.apache.thrift.TApplicationException.MISSING_RESULT, "getInfo failed: unknown result");
    }

  }
  public static class AsyncClient extends alluxio.thrift.AlluxioService.AsyncClient implements AsyncIface {
    public static class Factory implements org.apache.thrift.async.TAsyncClientFactory<AsyncClient> {
      private org.apache.thrift.async.TAsyncClientManager clientManager;
      private org.apache.thrift.protocol.TProtocolFactory protocolFactory;
      public Factory(org.apache.thrift.async.TAsyncClientManager clientManager, org.apache.thrift.protocol.TProtocolFactory protocolFactory) {
        this.clientManager = clientManager;
        this.protocolFactory = protocolFactory;
      }
      public AsyncClient getAsyncClient(org.apache.thrift.transport.TNonblockingTransport transport) {
        return new AsyncClient(protocolFactory, clientManager, transport);
      }
    }

    public AsyncClient(org.apache.thrift.protocol.TProtocolFactory protocolFactory, org.apache.thrift.async.TAsyncClientManager clientManager, org.apache.thrift.transport.TNonblockingTransport transport) {
      super(protocolFactory, clientManager, transport);
    }

    public void getInfo(Set<JobMasterInfoField> fields, org.apache.thrift.async.AsyncMethodCallback resultHandler) throws org.apache.thrift.TException {
      checkReady();
      getInfo_call method_call = new getInfo_call(fields, resultHandler, this, ___protocolFactory, ___transport);
      this.___currentMethod = method_call;
      ___manager.call(method_call);
    }

    public static class getInfo_call extends org.apache.thrift.async.TAsyncMethodCall {
      private Set<JobMasterInfoField> fields;
      public getInfo_call(Set<JobMasterInfoField> fields, org.apache.thrift.async.AsyncMethodCallback resultHandler, org.apache.thrift.async.TAsyncClient client, org.apache.thrift.protocol.TProtocolFactory protocolFactory, org.apache.thrift.transport.TNonblockingTransport transport) throws org.apache.thrift.TException {
        super(client, protocolFactory, transport, resultHandler, false);
        this.fields = fields;
      }

      public void write_args(org.apache.thrift.protocol.TProtocol prot) throws org.apache.thrift.TException {
        prot.writeMessageBegin(new org.apache.thrift.protocol.TMessage("getInfo", org.apache.thrift.protocol.TMessageType.CALL, 0));
        getInfo_args args = new getInfo_args();
        args.setFields(fields);
        args.write(prot);
        prot.writeMessageEnd();
      }

      public JobMasterInfo getResult() throws org.apache.thrift.TException {
        if (getState() != org.apache.thrift.async.TAsyncMethodCall.State.RESPONSE_READ) {
          throw new IllegalStateException("Method call not finished!");
        }
        org.apache.thrift.transport.TMemoryInputTransport memoryTransport = new org.apache.thrift.transport.TMemoryInputTransport(getFrameBuffer().array());
        org.apache.thrift.protocol.TProtocol prot = client.getProtocolFactory().getProtocol(memoryTransport);
        return (new Client(prot)).recv_getInfo();
      }
    }

  }

  public static class Processor<I extends Iface> extends alluxio.thrift.AlluxioService.Processor<I> implements org.apache.thrift.TProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(Processor.class.getName());
    public Processor(I iface) {
      super(iface, getProcessMap(new HashMap<String, org.apache.thrift.ProcessFunction<I, ? extends org.apache.thrift.TBase>>()));
    }

    protected Processor(I iface, Map<String,  org.apache.thrift.ProcessFunction<I, ? extends  org.apache.thrift.TBase>> processMap) {
      super(iface, getProcessMap(processMap));
    }

    private static <I extends Iface> Map<String,  org.apache.thrift.ProcessFunction<I, ? extends  org.apache.thrift.TBase>> getProcessMap(Map<String,  org.apache.thrift.ProcessFunction<I, ? extends  org.apache.thrift.TBase>> processMap) {
      processMap.put("getInfo", new getInfo());
      return processMap;
    }

    public static class getInfo<I extends Iface> extends org.apache.thrift.ProcessFunction<I, getInfo_args> {
      public getInfo() {
        super("getInfo");
      }

      public getInfo_args getEmptyArgsInstance() {
        return new getInfo_args();
      }

      protected boolean isOneway() {
        return false;
      }

      public getInfo_result getResult(I iface, getInfo_args args) throws org.apache.thrift.TException {
        getInfo_result result = new getInfo_result();
        result.success = iface.getInfo(args.fields);
        return result;
      }
    }

  }

  public static class AsyncProcessor<I extends AsyncIface> extends alluxio.thrift.AlluxioService.AsyncProcessor<I> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncProcessor.class.getName());
    public AsyncProcessor(I iface) {
      super(iface, getProcessMap(new HashMap<String, org.apache.thrift.AsyncProcessFunction<I, ? extends org.apache.thrift.TBase, ?>>()));
    }

    protected AsyncProcessor(I iface, Map<String,  org.apache.thrift.AsyncProcessFunction<I, ? extends  org.apache.thrift.TBase, ?>> processMap) {
      super(iface, getProcessMap(processMap));
    }

    private static <I extends AsyncIface> Map<String,  org.apache.thrift.AsyncProcessFunction<I, ? extends  org.apache.thrift.TBase,?>> getProcessMap(Map<String,  org.apache.thrift.AsyncProcessFunction<I, ? extends  org.apache.thrift.TBase, ?>> processMap) {
      processMap.put("getInfo", new getInfo());
      return processMap;
    }

    public static class getInfo<I extends AsyncIface> extends org.apache.thrift.AsyncProcessFunction<I, getInfo_args, JobMasterInfo> {
      public getInfo() {
        super("getInfo");
      }

      public getInfo_args getEmptyArgsInstance() {
        return new getInfo_args();
      }

      public AsyncMethodCallback<JobMasterInfo> getResultHandler(final AsyncFrameBuffer fb, final int seqid) {
        final org.apache.thrift.AsyncProcessFunction fcall = this;
        return new AsyncMethodCallback<JobMasterInfo>() { 
          public void onComplete(JobMasterInfo o) {
            getInfo_result result = new getInfo_result();
            result.success = o;
            try {
              fcall.sendResponse(fb,result, org.apache.thrift.protocol.TMessageType.REPLY,seqid);
              return;
            } catch (Exception e) {
              LOGGER.error("Exception writing to internal frame buffer", e);
            }
            fb.close();
          }
          public void onError(Exception e) {
            byte msgType = org.apache.thrift.protocol.TMessageType.REPLY;
            org.apache.thrift.TBase msg;
            getInfo_result result = new getInfo_result();
            {
              msgType = org.apache.thrift.protocol.TMessageType.EXCEPTION;
              msg = (org.apache.thrift.TBase)new org.apache.thrift.TApplicationException(org.apache.thrift.TApplicationException.INTERNAL_ERROR, e.getMessage());
            }
            try {
              fcall.sendResponse(fb,msg,msgType,seqid);
              return;
            } catch (Exception ex) {
              LOGGER.error("Exception writing to internal frame buffer", ex);
            }
            fb.close();
          }
        };
      }

      protected boolean isOneway() {
        return false;
      }

      public void start(I iface, getInfo_args args, org.apache.thrift.async.AsyncMethodCallback<JobMasterInfo> resultHandler) throws TException {
        iface.getInfo(args.fields,resultHandler);
      }
    }

  }

  public static class getInfo_args implements org.apache.thrift.TBase<getInfo_args, getInfo_args._Fields>, java.io.Serializable, Cloneable, Comparable<getInfo_args>   {
    private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("getInfo_args");

    private static final org.apache.thrift.protocol.TField FIELDS_FIELD_DESC = new org.apache.thrift.protocol.TField("fields", org.apache.thrift.protocol.TType.SET, (short)1);

    private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
    static {
      schemes.put(StandardScheme.class, new getInfo_argsStandardSchemeFactory());
      schemes.put(TupleScheme.class, new getInfo_argsTupleSchemeFactory());
    }

    private Set<JobMasterInfoField> fields; // required

    /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
    public enum _Fields implements org.apache.thrift.TFieldIdEnum {
      /**
       * /** optional filter for what fields to return, defaults to all
       */
      FIELDS((short)1, "fields");

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
          case 1: // FIELDS
            return FIELDS;
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
      tmpMap.put(_Fields.FIELDS, new org.apache.thrift.meta_data.FieldMetaData("fields", org.apache.thrift.TFieldRequirementType.DEFAULT, 
          new org.apache.thrift.meta_data.SetMetaData(org.apache.thrift.protocol.TType.SET, 
              new org.apache.thrift.meta_data.EnumMetaData(org.apache.thrift.protocol.TType.ENUM, JobMasterInfoField.class))));
      metaDataMap = Collections.unmodifiableMap(tmpMap);
      org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(getInfo_args.class, metaDataMap);
    }

    public getInfo_args() {
    }

    public getInfo_args(
      Set<JobMasterInfoField> fields)
    {
      this();
      this.fields = fields;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public getInfo_args(getInfo_args other) {
      if (other.isSetFields()) {
        Set<JobMasterInfoField> __this__fields = new HashSet<JobMasterInfoField>(other.fields.size());
        for (JobMasterInfoField other_element : other.fields) {
          __this__fields.add(other_element);
        }
        this.fields = __this__fields;
      }
    }

    public getInfo_args deepCopy() {
      return new getInfo_args(this);
    }

    @Override
    public void clear() {
      this.fields = null;
    }

    public int getFieldsSize() {
      return (this.fields == null) ? 0 : this.fields.size();
    }

    public java.util.Iterator<JobMasterInfoField> getFieldsIterator() {
      return (this.fields == null) ? null : this.fields.iterator();
    }

    public void addToFields(JobMasterInfoField elem) {
      if (this.fields == null) {
        this.fields = new HashSet<JobMasterInfoField>();
      }
      this.fields.add(elem);
    }

    /**
     * /** optional filter for what fields to return, defaults to all
     */
    public Set<JobMasterInfoField> getFields() {
      return this.fields;
    }

    /**
     * /** optional filter for what fields to return, defaults to all
     */
    public getInfo_args setFields(Set<JobMasterInfoField> fields) {
      this.fields = fields;
      return this;
    }

    public void unsetFields() {
      this.fields = null;
    }

    /** Returns true if field fields is set (has been assigned a value) and false otherwise */
    public boolean isSetFields() {
      return this.fields != null;
    }

    public void setFieldsIsSet(boolean value) {
      if (!value) {
        this.fields = null;
      }
    }

    public void setFieldValue(_Fields field, Object value) {
      switch (field) {
      case FIELDS:
        if (value == null) {
          unsetFields();
        } else {
          setFields((Set<JobMasterInfoField>)value);
        }
        break;

      }
    }

    public Object getFieldValue(_Fields field) {
      switch (field) {
      case FIELDS:
        return getFields();

      }
      throw new IllegalStateException();
    }

    /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
    public boolean isSet(_Fields field) {
      if (field == null) {
        throw new IllegalArgumentException();
      }

      switch (field) {
      case FIELDS:
        return isSetFields();
      }
      throw new IllegalStateException();
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof getInfo_args)
        return this.equals((getInfo_args)that);
      return false;
    }

    public boolean equals(getInfo_args that) {
      if (that == null)
        return false;

      boolean this_present_fields = true && this.isSetFields();
      boolean that_present_fields = true && that.isSetFields();
      if (this_present_fields || that_present_fields) {
        if (!(this_present_fields && that_present_fields))
          return false;
        if (!this.fields.equals(that.fields))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      List<Object> list = new ArrayList<Object>();

      boolean present_fields = true && (isSetFields());
      list.add(present_fields);
      if (present_fields)
        list.add(fields);

      return list.hashCode();
    }

    @Override
    public int compareTo(getInfo_args other) {
      if (!getClass().equals(other.getClass())) {
        return getClass().getName().compareTo(other.getClass().getName());
      }

      int lastComparison = 0;

      lastComparison = Boolean.valueOf(isSetFields()).compareTo(other.isSetFields());
      if (lastComparison != 0) {
        return lastComparison;
      }
      if (isSetFields()) {
        lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.fields, other.fields);
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
      StringBuilder sb = new StringBuilder("getInfo_args(");
      boolean first = true;

      sb.append("fields:");
      if (this.fields == null) {
        sb.append("null");
      } else {
        sb.append(this.fields);
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

    private static class getInfo_argsStandardSchemeFactory implements SchemeFactory {
      public getInfo_argsStandardScheme getScheme() {
        return new getInfo_argsStandardScheme();
      }
    }

    private static class getInfo_argsStandardScheme extends StandardScheme<getInfo_args> {

      public void read(org.apache.thrift.protocol.TProtocol iprot, getInfo_args struct) throws org.apache.thrift.TException {
        org.apache.thrift.protocol.TField schemeField;
        iprot.readStructBegin();
        while (true)
        {
          schemeField = iprot.readFieldBegin();
          if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
            break;
          }
          switch (schemeField.id) {
            case 1: // FIELDS
              if (schemeField.type == org.apache.thrift.protocol.TType.SET) {
                {
                  org.apache.thrift.protocol.TSet _set0 = iprot.readSetBegin();
                  struct.fields = new HashSet<JobMasterInfoField>(2*_set0.size);
                  JobMasterInfoField _elem1;
                  for (int _i2 = 0; _i2 < _set0.size; ++_i2)
                  {
                    _elem1 = alluxio.thrift.JobMasterInfoField.findByValue(iprot.readI32());
                    struct.fields.add(_elem1);
                  }
                  iprot.readSetEnd();
                }
                struct.setFieldsIsSet(true);
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

      public void write(org.apache.thrift.protocol.TProtocol oprot, getInfo_args struct) throws org.apache.thrift.TException {
        struct.validate();

        oprot.writeStructBegin(STRUCT_DESC);
        if (struct.fields != null) {
          oprot.writeFieldBegin(FIELDS_FIELD_DESC);
          {
            oprot.writeSetBegin(new org.apache.thrift.protocol.TSet(org.apache.thrift.protocol.TType.I32, struct.fields.size()));
            for (JobMasterInfoField _iter3 : struct.fields)
            {
              oprot.writeI32(_iter3.getValue());
            }
            oprot.writeSetEnd();
          }
          oprot.writeFieldEnd();
        }
        oprot.writeFieldStop();
        oprot.writeStructEnd();
      }

    }

    private static class getInfo_argsTupleSchemeFactory implements SchemeFactory {
      public getInfo_argsTupleScheme getScheme() {
        return new getInfo_argsTupleScheme();
      }
    }

    private static class getInfo_argsTupleScheme extends TupleScheme<getInfo_args> {

      @Override
      public void write(org.apache.thrift.protocol.TProtocol prot, getInfo_args struct) throws org.apache.thrift.TException {
        TTupleProtocol oprot = (TTupleProtocol) prot;
        BitSet optionals = new BitSet();
        if (struct.isSetFields()) {
          optionals.set(0);
        }
        oprot.writeBitSet(optionals, 1);
        if (struct.isSetFields()) {
          {
            oprot.writeI32(struct.fields.size());
            for (JobMasterInfoField _iter4 : struct.fields)
            {
              oprot.writeI32(_iter4.getValue());
            }
          }
        }
      }

      @Override
      public void read(org.apache.thrift.protocol.TProtocol prot, getInfo_args struct) throws org.apache.thrift.TException {
        TTupleProtocol iprot = (TTupleProtocol) prot;
        BitSet incoming = iprot.readBitSet(1);
        if (incoming.get(0)) {
          {
            org.apache.thrift.protocol.TSet _set5 = new org.apache.thrift.protocol.TSet(org.apache.thrift.protocol.TType.I32, iprot.readI32());
            struct.fields = new HashSet<JobMasterInfoField>(2*_set5.size);
            JobMasterInfoField _elem6;
            for (int _i7 = 0; _i7 < _set5.size; ++_i7)
            {
              _elem6 = alluxio.thrift.JobMasterInfoField.findByValue(iprot.readI32());
              struct.fields.add(_elem6);
            }
          }
          struct.setFieldsIsSet(true);
        }
      }
    }

  }

  public static class getInfo_result implements org.apache.thrift.TBase<getInfo_result, getInfo_result._Fields>, java.io.Serializable, Cloneable, Comparable<getInfo_result>   {
    private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("getInfo_result");

    private static final org.apache.thrift.protocol.TField SUCCESS_FIELD_DESC = new org.apache.thrift.protocol.TField("success", org.apache.thrift.protocol.TType.STRUCT, (short)0);

    private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
    static {
      schemes.put(StandardScheme.class, new getInfo_resultStandardSchemeFactory());
      schemes.put(TupleScheme.class, new getInfo_resultTupleSchemeFactory());
    }

    private JobMasterInfo success; // required

    /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
    public enum _Fields implements org.apache.thrift.TFieldIdEnum {
      SUCCESS((short)0, "success");

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
          case 0: // SUCCESS
            return SUCCESS;
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
      tmpMap.put(_Fields.SUCCESS, new org.apache.thrift.meta_data.FieldMetaData("success", org.apache.thrift.TFieldRequirementType.DEFAULT, 
          new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, JobMasterInfo.class)));
      metaDataMap = Collections.unmodifiableMap(tmpMap);
      org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(getInfo_result.class, metaDataMap);
    }

    public getInfo_result() {
    }

    public getInfo_result(
      JobMasterInfo success)
    {
      this();
      this.success = success;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public getInfo_result(getInfo_result other) {
      if (other.isSetSuccess()) {
        this.success = new JobMasterInfo(other.success);
      }
    }

    public getInfo_result deepCopy() {
      return new getInfo_result(this);
    }

    @Override
    public void clear() {
      this.success = null;
    }

    public JobMasterInfo getSuccess() {
      return this.success;
    }

    public getInfo_result setSuccess(JobMasterInfo success) {
      this.success = success;
      return this;
    }

    public void unsetSuccess() {
      this.success = null;
    }

    /** Returns true if field success is set (has been assigned a value) and false otherwise */
    public boolean isSetSuccess() {
      return this.success != null;
    }

    public void setSuccessIsSet(boolean value) {
      if (!value) {
        this.success = null;
      }
    }

    public void setFieldValue(_Fields field, Object value) {
      switch (field) {
      case SUCCESS:
        if (value == null) {
          unsetSuccess();
        } else {
          setSuccess((JobMasterInfo)value);
        }
        break;

      }
    }

    public Object getFieldValue(_Fields field) {
      switch (field) {
      case SUCCESS:
        return getSuccess();

      }
      throw new IllegalStateException();
    }

    /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
    public boolean isSet(_Fields field) {
      if (field == null) {
        throw new IllegalArgumentException();
      }

      switch (field) {
      case SUCCESS:
        return isSetSuccess();
      }
      throw new IllegalStateException();
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof getInfo_result)
        return this.equals((getInfo_result)that);
      return false;
    }

    public boolean equals(getInfo_result that) {
      if (that == null)
        return false;

      boolean this_present_success = true && this.isSetSuccess();
      boolean that_present_success = true && that.isSetSuccess();
      if (this_present_success || that_present_success) {
        if (!(this_present_success && that_present_success))
          return false;
        if (!this.success.equals(that.success))
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      List<Object> list = new ArrayList<Object>();

      boolean present_success = true && (isSetSuccess());
      list.add(present_success);
      if (present_success)
        list.add(success);

      return list.hashCode();
    }

    @Override
    public int compareTo(getInfo_result other) {
      if (!getClass().equals(other.getClass())) {
        return getClass().getName().compareTo(other.getClass().getName());
      }

      int lastComparison = 0;

      lastComparison = Boolean.valueOf(isSetSuccess()).compareTo(other.isSetSuccess());
      if (lastComparison != 0) {
        return lastComparison;
      }
      if (isSetSuccess()) {
        lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.success, other.success);
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
      StringBuilder sb = new StringBuilder("getInfo_result(");
      boolean first = true;

      sb.append("success:");
      if (this.success == null) {
        sb.append("null");
      } else {
        sb.append(this.success);
      }
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws org.apache.thrift.TException {
      // check for required fields
      // check for sub-struct validity
      if (success != null) {
        success.validate();
      }
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

    private static class getInfo_resultStandardSchemeFactory implements SchemeFactory {
      public getInfo_resultStandardScheme getScheme() {
        return new getInfo_resultStandardScheme();
      }
    }

    private static class getInfo_resultStandardScheme extends StandardScheme<getInfo_result> {

      public void read(org.apache.thrift.protocol.TProtocol iprot, getInfo_result struct) throws org.apache.thrift.TException {
        org.apache.thrift.protocol.TField schemeField;
        iprot.readStructBegin();
        while (true)
        {
          schemeField = iprot.readFieldBegin();
          if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
            break;
          }
          switch (schemeField.id) {
            case 0: // SUCCESS
              if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
                struct.success = new JobMasterInfo();
                struct.success.read(iprot);
                struct.setSuccessIsSet(true);
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

      public void write(org.apache.thrift.protocol.TProtocol oprot, getInfo_result struct) throws org.apache.thrift.TException {
        struct.validate();

        oprot.writeStructBegin(STRUCT_DESC);
        if (struct.success != null) {
          oprot.writeFieldBegin(SUCCESS_FIELD_DESC);
          struct.success.write(oprot);
          oprot.writeFieldEnd();
        }
        oprot.writeFieldStop();
        oprot.writeStructEnd();
      }

    }

    private static class getInfo_resultTupleSchemeFactory implements SchemeFactory {
      public getInfo_resultTupleScheme getScheme() {
        return new getInfo_resultTupleScheme();
      }
    }

    private static class getInfo_resultTupleScheme extends TupleScheme<getInfo_result> {

      @Override
      public void write(org.apache.thrift.protocol.TProtocol prot, getInfo_result struct) throws org.apache.thrift.TException {
        TTupleProtocol oprot = (TTupleProtocol) prot;
        BitSet optionals = new BitSet();
        if (struct.isSetSuccess()) {
          optionals.set(0);
        }
        oprot.writeBitSet(optionals, 1);
        if (struct.isSetSuccess()) {
          struct.success.write(oprot);
        }
      }

      @Override
      public void read(org.apache.thrift.protocol.TProtocol prot, getInfo_result struct) throws org.apache.thrift.TException {
        TTupleProtocol iprot = (TTupleProtocol) prot;
        BitSet incoming = iprot.readBitSet(1);
        if (incoming.get(0)) {
          struct.success = new JobMasterInfo();
          struct.success.read(iprot);
          struct.setSuccessIsSet(true);
        }
      }
    }

  }

}
