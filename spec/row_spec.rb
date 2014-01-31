require File.expand_path('spec_helper.rb', File.dirname(__FILE__))
include CassandraCQL


describe "basic methods" do
  context 'with basic column family' do
    before(:each) do
      @connection = setup_cassandra_connection
      drop_column_family_if_exists(@connection, 'basic_methods')
      @connection.execute("CREATE COLUMNFAMILY basic_methods (id varchar PRIMARY KEY, created_at uuid, default_column varchar, name varchar, serial int)")

      @connection.execute("INSERT INTO basic_methods (id, created_at, name, serial, default_column) VALUES (?, ?, ?, ?, ?)", 'test', CassandraCQL::UUID.new, 'name', 12345, 'snork')
      @row = @connection.execute("SELECT * FROM basic_methods WHERE id=?", "test").fetch
    end

    context "column_names" do
      it "should return a list of column names" do
        @row.column_names.sort.should eq(["created_at", "default_column", "id", "name", "serial"].sort)
      end
    end

    context "column_values" do
      it "should return a list of column values as Ruby objects" do
        @row.column_values.should be_kind_of(Array)
        @row.column_values.size.should eq(@row.column_names.size)
      end
    end

    context "columns" do
      it "should equal the number of columns" do
        @row.column_names.size.should eq(@row.column_values.size)
        @row.columns.should eq(@row.column_names.size)
      end
    end

    context "checking casting" do
      it "should return column_values for to_a" do
        @row.to_a.should eq(@row.column_values)
      end

      it "should return a hash for to_hash" do
        h = @row.to_hash
        h.should be_kind_of(Hash)
        h.keys.sort.should eq(@row.column_names.sort)
      end
    end
  end

  context 'with a column family with int comparators', :cql_version => '2.0.0' do
    before(:each) do
      @connection = setup_cassandra_connection
      if @connection.schema.column_family_names.include?('int_comparator')
        @connection.execute("DROP COLUMNFAMILY int_comparator")
      end
      @connection.execute("CREATE COLUMNFAMILY int_comparator (key text PRIMARY KEY) WITH comparator=int AND default_validation=text")

      @connection.execute("INSERT INTO int_comparator (key, 1, 2) VALUES (?, ?, ?)", 'test', 'value1', 'value2')
      @row = @connection.execute("SELECT * FROM int_comparator WHERE key=?", "test").fetch
    end

    context "column_names" do
      it "should return a list of column names" do
        @row.column_names.should =~ ['KEY', 1, 2]
      end
    end

    context "column_values" do
      it "should return a list of column values as Ruby objects" do
        @row.column_values.should be_kind_of(Array)
        @row.column_values.should =~ ['test', 'value1', 'value2']
      end
    end
  end

  context 'collections', :cql_version => '3.0.0' do
    before(:each) do
      @connection = setup_cassandra_connection
      if column_family_exists?(@connection, 'collections')
        @connection.execute("DROP COLUMNFAMILY collections")
      end
      @connection.execute(<<-CQL)
        CREATE COLUMNFAMILY collections (
          key text PRIMARY KEY,
          mylist LIST <int>,
          myset SET <varchar>,
          mymap MAP <uuid,timestamp>,
          myflattenmap MAP <uuid,timestamp>
        )
      CQL

      @map = {
        CassandraCQL::UUID.new => Time.at(Time.now.to_i),
        CassandraCQL::UUID.new => Time.at(Time.now.to_i) - 60
      }

      @connection.execute(
        "INSERT INTO collections (key, mylist, myset, mymap, myflattenmap) VALUES (?, [?], {?}, ?, {?:?,?:?})",
        'test', [1, 2, 3], ['some', 'set'], @map, *@map.to_a.flatten)

      @row = @connection.execute("SELECT * FROM collections WHERE key=?", "test").fetch
    end

    it 'should return list' do
      @row.to_hash['mylist'].should == [1, 2, 3]
    end

    it 'should return set' do
      @row.to_hash['myset'].should == Set['some', 'set']
    end

    it 'should return map' do
      @row.to_hash['mymap'].should == @map
    end
  end
end
