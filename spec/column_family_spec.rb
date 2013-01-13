require File.expand_path('spec_helper.rb', File.dirname(__FILE__))
include CassandraCQL

describe "ColumnFamily class" do
  before(:each) do
    @connection = setup_cassandra_connection
    @connection.execute("USE system")
    @super_column_family = @connection.schema.column_families["HintsColumnFamily"]
    @standard_column_family = @connection.schema.column_families["NodeIdInfo"]
  end

  context "initialize" do
    it "should set a cf_def" do
      @super_column_family.cf_def.should_not be_nil
      @standard_column_family.cf_def.should_not be_nil
    end

    it "should have some common attributes" do
      [@standard_column_family, @super_column_family].each do |column|
        column.name.should_not be_nil
        column.column_type.should_not be_nil

        # Only true for cassandra < 1.2
        if CassandraCQL.CASSANDRA_VERSION < "1.2"
          column.id.should_not be_nil
        end
      end
    end

    it "should super method_missing" do
      expect {
        @standard_column_family.this_method_does_not_exist
      }.to raise_error NoMethodError
      expect {
        @super_column_family.this_method_does_not_exist
      }.to raise_error NoMethodError
    end
  end

  context "with a standard column family" do
    it "should be standard" do
      @standard_column_family.super?.should be_false
      @standard_column_family.standard?.should be_true
      @standard_column_family.type.should eq("Standard")
    end
  end

  context "with a super column family" do
    it "should be super" do
      @super_column_family.super?.should be_true
      @super_column_family.standard?.should be_false
      @super_column_family.type.should eq("Super")
    end
  end

  context "when calling self.cast" do
    it "should turn UUID bytes into a UUID object" do
      uuid = UUID.new
      ColumnFamily.cast(uuid.bytes, "TimeUUIDType").should eq(uuid)
    end

    it "should turn a UUID bytes into a UUID object" do
      uuid = UUID.new
      ColumnFamily.cast(uuid.bytes, "UUIDType").should eq(uuid)
    end

    it "should turn a packed long into a number" do
      number = 2**33
      packed = [number >> 32, number % 2**32].pack("N*")

      ColumnFamily.cast(packed, "LongType").should eq(number)
      ColumnFamily.cast(packed, "CounterColumnType").should eq(number)
    end

    it "should turn a packed negative long into a negative number" do
      number = -2**33
      packed = [number >> 32, number % 2**32].pack("N*")

      ColumnFamily.cast(packed, "LongType").should eq(number)
      ColumnFamily.cast(packed, "CounterColumnType").should eq(number)
    end

    it "should call to_s with AsciiType" do
      obj = double("String")
      obj.stub(:to_s) { "string" }
      obj.should_receive(:to_s)
      ColumnFamily.cast(obj, "AsciiType")
    end

    it "should call to_s with UTF8Type" do
      obj = double("String")
      obj.stub(:to_s) { "string" }
      obj.should_receive(:to_s)
      ColumnFamily.cast(obj, "UTF8Type")
    end

    it "should return self with BytesType" do
      obj = Object.new
      ColumnFamily.cast(obj, "BytesType").object_id.should eq(obj.object_id)
    end

    it "should return nil for all types of nil" do
      %w(TimeUUIDType UUIDType LongType IntegerType
        UTF8Type AsciiType CounterColumnType).each do |type|
        ColumnFamily.cast(nil, type).should eq(nil)
      end
    end
  end
end
