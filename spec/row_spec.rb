require File.expand_path('spec_helper.rb', File.dirname(__FILE__))
include CassandraCQL


describe "basic methods" do
  let(:column_family) { ColumnFamily.new(yaml_fixture(:standard_with_validations)) }
  let(:cql_result_row) { yaml_fixture(:result_for_standard_with_validations).rows[0] }
  let(:row) { Row.new(cql_result_row, column_family) }

  let(:cf_time_uuid_comp) { ColumnFamily.new(yaml_fixture(:standard_column_family)) }
  let(:cql_result_time_uuid_row) { yaml_fixture(:result_for_timeuuid).rows[0] }
  let(:row_time_uuid) { Row.new(cql_result_time_uuid_row, cf_time_uuid_comp) }

  context "initialize" do
    it "should set row and column_family" do
      row.row.should eq(cql_result_row)
      row.instance_variable_get(:@column_family).should eq(column_family)
    end
  end
  
  context "column_names" do
    it "should return a list of column names" do
      row.column_names.sort.should eq(["created_at", "default_column", "id", "name", "serial"].sort)
    end
  end

  context "column_values" do
    it "should return a list of column values as Ruby objects" do
      row.column_values.should be_kind_of(Array)
      row.column_values.size.should eq(row.column_names.size)
    end
  end
  
  context "checking types" do
    it "should return a UUID for created_at" do
      row['created_at'].should be_kind_of(UUID)
    end

    it "should return a Fixnum for serial" do
      row['serial'].should be_kind_of(Fixnum)
    end

    it "should return a String for name" do
      row['name'].should be_kind_of(String)
    end

    it "should return a String for id" do
      row['id'].should be_kind_of(String)
    end

    it "should return a String for default_column" do
      row['default_column'].should be_kind_of(String)
    end

    it "should not crash when getting the row key name from column names" do
      lambda { row_time_uuid.column_names }.should_not raise_error
    end
  end
  
  context "columns" do
    it "should equal the number of columns" do
      row.columns.should eq(cql_result_row.columns.size)
    end
  end
  
  context "key" do
    it "should return the cql_result row key" do
      row.key.should eq(cql_result_row.key)
    end
  end

  context "checking coersion" do
    it "should return column_values for to_a" do
      row.to_a.should eq(row.column_values)
    end

    it "should return a hash for to_hash" do
      h = row.to_hash
      h.should be_kind_of(Hash)
      h.keys.sort.should eq(row.column_names.sort)
    end
  end
  
end
