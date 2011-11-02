require File.expand_path('spec_helper.rb', File.dirname(__FILE__))
include CassandraCQL

describe "Schema class" do
  before(:each) do
    @connection = setup_cassandra_connection
    @connection.execute("USE system")
  end

  context "initialize" do
    it "should set a thrift schema object" do
      @connection.schema.schema.should be_kind_of(CassandraCQL::Thrift::KsDef)
    end

    it "should set column family hash" do
      @connection.schema.column_families.should be_kind_of(Hash)
    end

    it "should set column family hash" do
      @connection.schema.column_families.should be_kind_of(Hash)
    end
  end

  it "should method_missing" do
    expect {
      @connection.schema.this_method_does_not_exist
    }.to raise_error NoMethodError
  end

  context "name" do
    it "should return keyspace name" do
      @connection.schema.name.should eq('system')
    end
  end

  context "to_s" do
    it "should return keyspace name" do
      @connection.schema.to_s.should eq(@connection.schema.name)
    end
  end

  context "column_family_names" do
    it "should return cf_def names" do
      @connection.schema.column_family_names.sort.should eq(@connection.schema.schema.cf_defs.map(&:name).sort)
    end
    
    it "should be the same as tables" do
      @connection.schema.column_family_names.should eq(@connection.schema.tables)
    end
  end
end