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
    before(:each) do
      @connection = setup_cassandra_connection
    end

    it "should set keyspace without compression" do
      @connection.keyspace.should_not eq('system')
      stmt = @connection.prepare("use system")
      stmt.execute([], :compression => false).should be_nil
      @connection.keyspace.should eq('system')
    end

    it "should set keyspace with compression" do
      @connection.keyspace.should_not eq('system')
      stmt = @connection.prepare("use system")
      stmt.execute([], :compression => true).should be_nil
      @connection.keyspace.should eq('system')
    end
    
    it "should set keyspace to nil when deleting keyspace" do
      @connection.execute("DROP KEYSPACE #{@connection.keyspace}").should be_nil
      @connection.keyspace.should be_nil
    end
  end
  
  context "when performing void-returning column_family operations" do
    before(:each) do
      @connection = setup_cassandra_connection
      if !@connection.schema.column_family_names.include?('colfam_ops')
        @connection.execute("CREATE COLUMNFAMILY colfam_ops (id varchar PRIMARY KEY)")
      else
        @connection.execute("TRUNCATE colfam_ops")
      end
    end

    it "should return nil when inserting" do
      @connection.execute("INSERT INTO colfam_ops (id, column) VALUES (?, ?)", "key", "value").should be_nil
    end

    it "should return nil when updating" do
      @connection.execute("UPDATE colfam_ops SET column=? WHERE id=?", "value", "key").should be_nil
    end

    it "should return nil when deleting" do
      @connection.execute("DELETE FROM colfam_ops WHERE id=?", "key").should be_nil
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
    it "should return a Time object with the number of microseconds since epoch" do
      ts = Time.new - 86400 # set it to yesterday just to be sure no defaulting to today misses an error
      long = Statement.cast_to_cql(ts)
      long.should be_kind_of(Integer)
      Time.at(long / 1000.0).to_f.should be_within(0.001).of(ts.to_f)
    end
  end
  
  context "with a Date object" do
    it "should return a corresponding Time object" do
      date = Date.today << 1
      str = Statement.cast_to_cql(date)
      str.should eq(date.strftime('%Y-%m-%d'))
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

  context "with a SimpleUUID::UUID object" do
    it "should return the guid" do
      uuid = SimpleUUID::UUID.new
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