require File.expand_path('spec_helper.rb', File.dirname(__FILE__))
include CassandraCQL

describe "void results" do
  let(:column_family) { ColumnFamily.new(yaml_fixture(:standard_with_validations)) }
  let(:cql_result) { yaml_fixture(:result_for_void_operations) }
  let(:result) { Result.new(cql_result, column_family) }
  it "should return true only for void?" do
    result.void?.should be_true
    result.rows?.should be_false
    result.int?.should be_false
  end
end

describe "long validation" do
  let(:column_family) { ColumnFamily.new(yaml_fixture(:standard_with_long_validation)) }
  let(:cql_result) { yaml_fixture(:result_for_standard_with_long_validation) }
  let(:result) { Result.new(cql_result, column_family) }
  it "should return UTF8 column_names and Fixnum values" do
    result.fetch do |row|
      row.column_names.should eq(['KEY', 'col1', 'col2', 'col3'])
      row.column_values.should eq(['row_key', 1, 2, 3])
    end
  end
end

describe "counter validation" do
  let(:column_family) { ColumnFamily.new(yaml_fixture(:standard_counter)) }
  let(:cql_result) { yaml_fixture(:result_for_standard_counter) }
  let(:result) { Result.new(cql_result, column_family) }
  it "should return UTF8 column_names and Fixnum values" do
    result.fetch do |row|
      row.column_names.should eq(['KEY', 'col_counter'])
      row.column_values.should eq(['row_key', 1])
    end
  end
end

describe "sparse row results" do
  let(:column_family) { ColumnFamily.new(yaml_fixture(:standard_with_validations)) }
  let(:cql_result) { yaml_fixture(:result_for_sparse_columns) }
  let(:result) { Result.new(cql_result, column_family) }
  it "should should be handled properly" do
    result.rows.should eq(2)
    # First column should have 3 columns set, one nil
    row = result.fetch
    row.columns.should eq(4)
    row.column_names.should eq(['col1', 'col2', 'col3', 'col4'])
    row.column_values.should eq(['val1', 'val2', 'val3', nil])

    # Second column should have the last column set
    row = result.fetch
    row.columns.should eq(4)
    row.column_names.should eq(['col1', 'col2', 'col3', 'col4'])
    row.column_values.should eq([nil, nil, nil, 'val4'])
  end
end

describe "row results" do
  let(:column_family) { ColumnFamily.new(yaml_fixture(:standard_with_validations)) }
  let(:cql_result) { yaml_fixture(:result_for_standard_with_validations) }
  let(:result) { Result.new(cql_result, column_family) }

  it "should return true only for rows?" do
    result.void?.should be_false
    result.rows?.should be_true
    result.int?.should be_false
  end

  it "should have two rows" do
    result.rows.should eq(2)
  end
  
  context "initialize" do
    it "should have a cursor set to 0" do
      result.instance_variable_get(:@cursor).should eq(0)
    end
  
    it "should have a duplicate of the column_family" do
      result.instance_variable_get(:@column_family).cf_def.should eq(column_family.cf_def)
    end

    it "should have a duplicate of the column_family" do
      result.instance_variable_get(:@column_family).should_not eq(column_family)
      result.instance_variable_get(:@column_family).cf_def.should eq(column_family.cf_def)
    end
    
    it "should have a result" do
      result.instance_variable_get(:@result).should be_kind_of(CassandraCQL::Thrift::CqlResult)
    end
  end
  
  context "setting the cursor" do
    it "should set the cursor" do
      expect {
        result.cursor = 15
      }.to_not raise_error
      result.instance_variable_get(:@cursor).should eq(15)
    end 

    it "should not set the cursor" do
      expect {
        result.cursor = Object
      }.to raise_error(CassandraCQL::Error::InvalidCursor)
    end 
  end

  context "fetching a single row" do
    it "should return a row object twice then nil" do
      result.fetch_row.should be_kind_of(Row)
      result.instance_variable_get(:@cursor).should eq(1)

      result.fetch_row.should be_kind_of(Row)
      result.instance_variable_get(:@cursor).should eq(2)
      
      result.fetch_row.should be_nil
      result.instance_variable_get(:@cursor).should eq(2)
    end
  end
  
  context "resetting cursor should fetch the same row" do
    it "should return the same row" do
      result.instance_variable_get(:@cursor).should eq(0)
      arr = result.fetch_array
      result.cursor = 0
      arr.should eq(result.fetch_array)
    end
  end
  
  context "fetch without a block" do
    it "should return a row twice then nil" do
      result.fetch.should be_kind_of(Row)
      result.instance_variable_get(:@cursor).should eq(1)

      result.fetch.should be_kind_of(Row)
      result.instance_variable_get(:@cursor).should eq(2)
      
      result.fetch.should be_nil
      result.instance_variable_get(:@cursor).should eq(2)
    end
  end
  
  context "fetch with a block" do
    it "fetched count should equal the number of rows" do
      counter = 0
      result.fetch do |row|
        counter += 1
        row.should be_kind_of(Row)
      end
      counter.should eq(result.rows)
    end
  end

  context "fetch_array without a block" do
    it "should return a row as an array" do
      row = result.fetch
      result.cursor = 0
      arr = result.fetch_array
      arr.should be_kind_of(Array)
      arr.should eq(row.column_values)
    end
  end
  
  context "fetch_array_with a block" do
    it "fetched count should equal the number of rows" do
      counter = 0
      result.fetch_array do |arr|
        counter += 1
        arr.should be_kind_of(Array)
      end
      counter.should eq(result.rows)
    end
  end

  context "fetch_hash without a block" do
    it "should return a hash" do
      row = result.fetch
      result.cursor = 0
      hash = result.fetch_hash
      hash.should be_kind_of(Hash)
      hash.should eq(row.to_hash)
    end
  end
  
  context "fetch_hash_with a block" do
    it "should iterate rows() times and return hashes" do
      counter = 0
      result.fetch_hash do |hash|
        counter += 1
        hash.should be_kind_of(Hash)
      end
      counter.should eq(result.rows)
    end
  end
end