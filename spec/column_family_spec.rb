require File.expand_path('spec_helper.rb', File.dirname(__FILE__))
include CassandraCQL

describe "ColumnFamily class" do
  let(:standard_column_family) { ColumnFamily.new(yaml_fixture(:standard_column_family)) }
  let(:super_column_family) { ColumnFamily.new(yaml_fixture(:super_column_family)) }

  context "initialize" do
    it "should set a cf_def" do
      super_column_family.cf_def.should_not be_nil
    end

    it "should have some common attributes" do
      [standard_column_family, super_column_family].each do |column|
        column.name.should_not be_nil
        column.id.should_not be_nil
        column.column_type.should_not be_nil
      end
    end

    it "should super method_missing" do
      expect {
        standard_column_family.this_method_does_not_exist
      }.to raise_error NoMethodError
      expect {
        super_column_family.this_method_does_not_exist
      }.to raise_error NoMethodError
    end
  end
  
  context "with a standard column family" do
    it "should be standard" do
      standard_column_family.type.should eq("Standard")
      standard_column_family.standard?.should be_true
      standard_column_family.super?.should_not be_true
    end
  end

  context "with a super column family" do
    it "should be super" do
      super_column_family.type.should eq("Super")
      super_column_family.standard?.should_not be_true
      super_column_family.super?.should be_true
    end
  end

  context "when calling self.cast" do
    it "should turn UUID bytes into a Time object" do
      ts = Time.new
      ColumnFamily.cast(UUID.new(ts).bytes, "org.apache.cassandra.db.marshal.TimeUUIDType").should eq(ts)
    end

    it "should turn a UUID bytes into a UUID object" do
      uuid = UUID.new
      ColumnFamily.cast(uuid.bytes, "org.apache.cassandra.db.marshal.UUIDType").should eq(uuid)
    end

    it "should turn a packed integer into a Fixnum" do
      ColumnFamily.cast([0x7FFFFFFF].pack("N"), "org.apache.cassandra.db.marshal.IntegerType").should eq(0x7FFFFFFF)
    end

    it "should turn a packed negative integer into a negative Fixnum" do
      ColumnFamily.cast([-68047].pack("N"), "org.apache.cassandra.db.marshal.IntegerType").should eq(-68047)
    end

    it "should turn a packed long into a number" do
      number = 2**33
      packed = [number >> 32, number].pack("N*")

      ColumnFamily.cast(packed, "org.apache.cassandra.db.marshal.LongType").should eq(number)
      ColumnFamily.cast(packed, "org.apache.cassandra.db.marshal.CounterColumnType").should eq(number)
    end

    it "should turn a packed negative long into a negative number" do
      number = -2**33
      packed = [number >> 32, number].pack("N*")

      ColumnFamily.cast(packed, "org.apache.cassandra.db.marshal.LongType").should eq(number)
      ColumnFamily.cast(packed, "org.apache.cassandra.db.marshal.CounterColumnType").should eq(number)
    end

    it "should call to_s with AsciiType" do
      obj = double("String")
      obj.should_receive(:to_s)
      ColumnFamily.cast(obj, "org.apache.cassandra.db.marshal.AsciiType")
    end

    it "should call to_s with UTF8Type" do
      obj = double("String")
      obj.should_receive(:to_s)
      ColumnFamily.cast(obj, "org.apache.cassandra.db.marshal.UTF8Type")
    end
    
    it "should return self with BytesType" do
      obj = Object.new
      ColumnFamily.cast(obj, "org.apache.cassandra.db.marshal.BytesType").object_id.should eq(obj.object_id)
    end
  end
  
  context "validations classes" do
    let(:column_family) { ColumnFamily.new(yaml_fixture(:standard_with_validations)) }
    it "should have a hash of column_names and validations" do
      column_family.columns.should be_kind_of(Hash)
    end
    
    it "should have a default validation class" do
      column_family.columns.default.should eq(column_family.cf_def.default_validation_class)
    end

    it "should have a validation class for the key" do
      column_family.columns.has_key?(column_family.key_alias).should be_true
      column_family.columns[column_family.key_alias].should eq(column_family.key_validation_class)
    end
  end
end