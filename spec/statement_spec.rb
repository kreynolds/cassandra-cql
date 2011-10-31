require File.expand_path('spec_helper.rb', File.dirname(__FILE__))
include CassandraCQL

describe "initialize" do
  it "should set a handle and prepare statement" do
    statement = "use keyspace1"
    handle = double("Database")
    sth = Statement.new(handle, statement)
    sth.statement.should eq(statement)
    sth.instance_variable_get("@handle").should eq(handle)
  end
end

describe "execute" do
  context "when performing keyspace operations" do
    let(:cql_result) { yaml_fixture(:result_for_void_operations) }
    let(:handle) {
      handle = double("Database")
      handle.should_receive(:update_schema!)
      handle.stub(:execute_cql_query) { cql_result }
      handle
    }

    it "should set keyspace without compression" do
      handle.should_receive(:execute_cql_query).with("use keyspace1", CassandraCQL::Thrift::Compression::NONE)
      handle.should_receive(:keyspace=).with("keyspace1")
      Statement.new(handle, "use keyspace1").execute.should be_nil
    end

    it "should set keyspace with compression" do
      handle.should_receive(:execute_cql_query).with("x^+-NU\310N\255,.HLN5\004\000#\275\004\364", CassandraCQL::Thrift::Compression::GZIP)
      handle.should_receive(:keyspace=).with("keyspace1")
      Statement.new(handle, "use keyspace1").execute([], {:compression => true}).should be_nil
    end
    
    it "should set keyspace to nil when deleting keyspace" do
      handle.should_receive(:execute_cql_query).with("drop keyspace keyspace1", CassandraCQL::Thrift::Compression::NONE)
      handle.should_receive(:keyspace=).with(nil)
      Statement.new(handle, "drop keyspace keyspace1").execute.should be_nil
    end
  end
  
  context "when performing schema operations" do
    let(:cql_result) { yaml_fixture(:result_for_void_operations) }
    let(:handle) {
      handle = double("Database")
      handle.should_receive(:update_schema!)
      handle.stub(:execute_cql_query) { cql_result }
      handle
    }

    it "should update_schema when creating a column family" do
      handle.should_receive(:execute_cql_query).with("create columnfamily test_cf", CassandraCQL::Thrift::Compression::NONE)
      Statement.new(handle, "create columnfamily test_cf").execute.should be_nil
    end

    it "should update_schema when altering a column family" do
      handle.should_receive(:execute_cql_query).with("alter columnfamily test_cf", CassandraCQL::Thrift::Compression::NONE)
      Statement.new(handle, "alter columnfamily test_cf").execute.should be_nil
    end

    it "should update_schema when dropping a column family" do
      handle.should_receive(:execute_cql_query).with("drop columnfamily test_cf", CassandraCQL::Thrift::Compression::NONE)
      Statement.new(handle, "drop columnfamily test_cf").execute.should be_nil
    end
  end
  
  context "when performing result-returning column_family operations" do
    let(:cql_result) { yaml_fixture(:result_for_standard_with_validations) }
    let(:handle) {
      handle = double("Database")
      handle.stub(:execute_cql_query) { cql_result }
      handle
    }
    let(:schema) { double("Schema") }
    it "should set the column family when selecting" do
      schema = double("Schema")
      schema.stub(:column_families) { {'NodeIdInfo' => ColumnFamily.new(yaml_fixture(:standard_column_family))} }
      handle.stub(:schema) { schema }
      handle.should_receive(:schema)
      schema.should_receive(:column_families)
      handle.should_receive(:execute_cql_query).with("select column1 from NodeIdInfo", CassandraCQL::Thrift::Compression::NONE)
      result = Statement.new(handle, "select column1 from NodeIdInfo").execute
      result.column_family.name.should eq('NodeIdInfo')
    end
  end
  
  context "when performing void-returning column_family operations" do
    let(:cql_result) { yaml_fixture(:result_for_void_operations) }
    let(:handle) {
      handle = double("Database")
      handle.stub(:execute_cql_query) { cql_result }
      handle
    }
    it "should have a nil column family when inserting" do
      handle.should_receive(:execute_cql_query).with("insert into NodeIdInfo (column1) values ('foo')", CassandraCQL::Thrift::Compression::NONE)
      Statement.new(handle, "insert into NodeIdInfo (column1) values ('foo')").execute.should be_nil
    end

    it "should have a nil column family when updating" do
      handle.should_receive(:execute_cql_query).with("update NodeIdInfo set column1='foo'", CassandraCQL::Thrift::Compression::NONE)
      Statement.new(handle, "update NodeIdInfo set column1='foo'").execute.should be_nil
    end

    it "should have a nil column family when deleting" do
      handle.should_receive(:execute_cql_query).with("delete from column_family where key='whatever'", CassandraCQL::Thrift::Compression::NONE)
      Statement.new(handle, "delete from column_family where key='whatever'").execute.should be_nil
    end
  end
end

describe "escape" do
  it "should escape quotes" do
    Statement.escape(%q{'}).should eq(%q{''})
    Statement.escape(%q{\'}).should eq(%q{\''})
    Statement.escape(%q{''}).should eq(%q{''''})
  end
end

describe "quote" do
  context "with a string" do
    it "should add quotes" do
      Statement.quote("test").should eq("'test'")
    end
  end

  context "with an integer" do
    it "should not add quotes" do
      Statement.quote(15).should eq(15)
    end
  end

  context "with an array" do
    it "should return a comma-separated list" do
      Statement.quote([1, 2, 3]).should eq("1,2,3")
      Statement.quote(["a", "b''", "c"]).should eq("'a','b''','c'")
    end
  end

  context "with an unsupported object" do
    it "should raise an exception" do
      expect {
        Statement.quote(Time.new)
      }.to raise_error(CassandraCQL::Error::UnescapableObject)
    end
  end
end

describe "cast_to_cql" do
  context "with a Time object" do
    it "should return a guid of a UUID" do
      ts = Time.new - 86400 # set it to yesterday just to be sure no defaulting to today misses an error
      guid = Statement.cast_to_cql(ts)
      guid.should be_kind_of(String)
      expect {
        ret = UUID.new(guid)
        uuid_ts = Time.at(ret.seconds)
        [:year, :month, :day, :hour, :min, :sec].each do |sym|
          uuid_ts.send(sym).should eq(ts.send(sym))
        end
      }.to_not raise_error
    end
  end
  
  context "with a Fixnum object" do
    it "should return the same object" do
      Statement.cast_to_cql(15).should eq(15)
    end
  end
  
  context "with a UUID object" do
    it "should return the a guid" do
      uuid = UUID.new
      guid = Statement.cast_to_cql(uuid)
      guid.should eq(uuid.to_guid)
    end
  end

  context "with a String without quotes" do
    it "should return a copy of itself" do
      str = "This is a string"
      new_str = Statement.cast_to_cql(str)
      str.should eq(str)
      new_str.object_id.should_not eq(str.object_id)
    end
  end
  
  context "with a String with quotes" do
    it "should return a quoted version" do
      str = "This is a ' string"
      new_str = Statement.cast_to_cql(str)
      new_str.should_not eq(str)
      new_str.should eq(Statement.escape(str))
    end
  end
  
  context "with binary data" do
    it "should return an unpacked version" do
      bytes = "binary\x00"
      bytes = bytes.force_encoding('ASCII-8BIT') if RUBY_VERSION >= "1.9"
      new_data = Statement.cast_to_cql(bytes)
      new_data.should_not eq(bytes)
      [new_data].pack('H*').should eq(bytes)
    end
  end
  
  context "with an array of Fixnums" do
    it "should equal itself" do
      arr = [1, 2, 3]
      Statement.cast_to_cql(arr).should eq(arr)
    end
  end
  
  context "with an array of Strings" do
    it "should return quoted versions of itself" do
      arr = ["test", "'"]
      res = Statement.cast_to_cql(arr)
      arr.map { |o| Statement.cast_to_cql(o) }.should eq(res)
    end
  end
end

describe "sanitize" do
  context "with no bind vars" do
    it "should return itself" do
      Statement.sanitize("use keyspace").should eq("use keyspace")
    end
  end
  
  context "when expecting bind vars" do
    it "should raise an exception with bind variable mismatch" do
      expect {
        Statement.sanitize("use keyspace ?")
      }.to raise_error(Error::InvalidBindVariable)

      expect {
        Statement.sanitize("use keyspace ?", ['too', 'many'])
      }.to raise_error(Error::InvalidBindVariable)
    end

    it "should not raise an exception with matching bind vars" do
      expect {
        Statement.sanitize("use keyspace ?", ["test"]).should eq("use keyspace 'test'")
      }.to_not raise_error(Error::InvalidBindVariable)
    end

    it "should have bind vars in the right order" do
      expect {
        Statement.sanitize("use keyspace ? with randomness (?)", ["test", "stuff"]).should eq("use keyspace 'test' with randomness ('stuff')")
      }.to_not raise_error(Error::InvalidBindVariable)
    end

    it "should not double-escape the single quotes in your string" do
      Statement.sanitize(
        "insert into keyspace (key, ?) values (?)", ["vanilla", %Q{I\'m a string with \'cool\' quotes}]
      ).should eq("insert into keyspace (key, 'vanilla') values ('I''m a string with ''cool'' quotes')")
    end

    it "should handle numbers and stuff appropriately" do
      Statement.sanitize(
        "insert into keyspace (key, ?) values (?)", [488, 60.368]
      ).should eq("insert into keyspace (key, 488) values (60.368)")
    end

  end
end

describe "finish" do
  it "should just return true .. nothing to clean up yet" do
    Statement.new(nil, 'whatever').finish.should be_true
  end
end