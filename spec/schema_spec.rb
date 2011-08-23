require File.expand_path('spec_helper.rb', File.dirname(__FILE__))
include CassandraCQL

describe "Schema class" do
  let(:schema_fixture) { yaml_fixture(:system_schema) }
  let(:schema) { Schema.new(schema_fixture) }

  context "initialize" do
    it "should set a thrift schema object" do
      schema.schema.should be_kind_of(CassandraCQL::Thrift::KsDef)
    end

    it "should set column family hash" do
      schema.column_families.should be_kind_of(Hash)
    end

    it "should set column family hash" do
      schema.column_families.should be_kind_of(Hash)
    end
    
    it "should have a column family hash with all cf_defs" do
      schema.column_families.size.should eq(schema_fixture.cf_defs.size)
    end
  end

  it "should method_missing" do
    expect {
      schema.this_method_does_not_exist
    }.to raise_error NoMethodError
  end

  context "name" do
    it "should return keyspace name" do
      schema.name.should eq(schema_fixture.name)
    end
  end

  context "to_s" do
    it "should return keyspace name" do
      schema.to_s.should eq(schema_fixture.name)
    end
  end

  context "column_family_names" do
    it "should return cf_def names" do
      schema.column_family_names.sort.should eq(schema_fixture.cf_defs.map(&:name).sort)
    end
    
    it "should be the same as tables" do
      schema.column_family_names.should eq(schema.tables)
    end
  end
end