require File.expand_path('spec_helper.rb', File.dirname(__FILE__))
include CassandraCQL

describe "void results" do
  before(:each) do
    @connection = setup_cassandra_connection
  end

  it "should return nil" do
    @connection.execute("USE system").should be_nil
  end
end

describe "sparse row results" do
  before(:each) do
    @connection = setup_cassandra_connection
    if !@connection.schema.column_family_names.include?('sparse_results')
      @connection.execute("CREATE COLUMNFAMILY sparse_results (id varchar PRIMARY KEY)")
    else
      @connection.execute("TRUNCATE sparse_results")
    end
  end

  it "should should be handled properly" do
    @connection.execute("INSERT INTO sparse_results (id, col1, col2, col3) VALUES (?, ?, ?, ?)", 'key1', 'val1', 'val2', 'val3').should be_nil
    @connection.execute("INSERT INTO sparse_results (id, col4, col5, col6) VALUES (?, ?, ?, ?)", 'key2', 'val4', 'val5', 'val6').should be_nil
    result = @connection.execute("SELECT col1, col2, col3, col4 FROM sparse_results")
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
  before(:each) do
    @connection = setup_cassandra_connection
    @connection.execute("INSERT INTO sparse_results (id, col1, col2, col3) VALUES (?, ?, ?, ?)", 'key1', 'val1', 'val2', 'val3').should be_nil
    @connection.execute("INSERT INTO sparse_results (id, col4, col5, col6) VALUES (?, ?, ?, ?)", 'key2', 'val4', 'val5', 'val6').should be_nil
    @result = @connection.execute("SELECT col1, col2, col3, col4 FROM sparse_results")
  end

  it "should return true only for rows?" do
    @result.void?.should be_false
    @result.rows?.should be_true
    @result.int?.should be_false
  end

  it "should have two rows" do
    @result.rows.should eq(2)
  end

  it "should know size of rows" do
    @result.size.should eq(2)
  end

  it "should know count of rows" do
    @result.count.should eq(2)
  end

  it "should know length of rows" do
    @result.length.should eq(2)
  end

  context "initialize" do
    it "should have a cursor set to 0" do
      @result.instance_variable_get(:@cursor).should eq(0)
    end

    it "should have a result" do
      @result.instance_variable_get(:@result).should be_kind_of(CassandraCQL::Thrift::CqlResult)
    end
  end

  context "setting the cursor" do
    it "should set the cursor" do
      expect {
        @result.cursor = 15
      }.to_not raise_error
      @result.instance_variable_get(:@cursor).should eq(15)
    end

    it "should not set the cursor" do
      expect {
        @result.cursor = Object
      }.to raise_error(CassandraCQL::Error::InvalidCursor)
    end
  end

  context "fetching a single row" do
    it "should return a row object twice then nil" do
      @result.fetch_row.should be_kind_of(Row)
      @result.instance_variable_get(:@cursor).should eq(1)

      @result.fetch_row.should be_kind_of(Row)
      @result.instance_variable_get(:@cursor).should eq(2)

      @result.fetch_row.should be_nil
      @result.instance_variable_get(:@cursor).should eq(2)
    end
  end

  context "resetting cursor should fetch the same row" do
    it "should return the same row" do
      @result.instance_variable_get(:@cursor).should eq(0)
      arr = @result.fetch_array
      @result.cursor = 0
      arr.should eq(@result.fetch_array)
    end
  end

  context "fetch without a block" do
    it "should return a row twice then nil" do
      @result.fetch.should be_kind_of(Row)
      @result.instance_variable_get(:@cursor).should eq(1)

      @result.fetch.should be_kind_of(Row)
      @result.instance_variable_get(:@cursor).should eq(2)

      @result.fetch.should be_nil
      @result.instance_variable_get(:@cursor).should eq(2)
    end
  end

  context "fetch with a block" do
    it "fetched count should equal the number of rows" do
      counter = 0
      @result.fetch do |row|
        counter += 1
        row.should be_kind_of(Row)
      end
      counter.should eq(@result.rows)
    end
  end

  context "fetch_array without a block" do
    it "should return a row as an array" do
      row = @result.fetch
      @result.cursor = 0
      arr = @result.fetch_array
      arr.should be_kind_of(Array)
      arr.should eq(row.column_values)
    end
  end

  context "fetch_array_with a block" do
    it "fetched count should equal the number of rows" do
      counter = 0
      @result.fetch_array do |arr|
        counter += 1
        arr.should be_kind_of(Array)
      end
      counter.should eq(@result.rows)
    end
  end

  context "fetch_hash without a block" do
    it "should return a hash" do
      row = @result.fetch
      @result.cursor = 0
      hash = @result.fetch_hash
      hash.should be_kind_of(Hash)
      hash.should eq(row.to_hash)
    end

    it "should return nil where there is no row to fetch" do
      2.times { @result.fetch }
      @result.fetch_hash.should be_nil
    end
  end

  context "fetch_hash_with a block" do
    it "should iterate rows() times and return hashes" do
      counter = 0
      @result.fetch_hash do |hash|
        counter += 1
        hash.should be_kind_of(Hash)
      end
      counter.should eq(@result.rows)
    end
  end

  describe "#each" do
    it "yields each row as a hash" do
      rows = []
      @result.each { |row| rows << row }
      rows.each do |row|
        row.should be_instance_of(CassandraCQL::Row)
        row.column_names.should eq(['col1', 'col2', 'col3', 'col4'])
      end
    end
  end

  it "is enumerable" do
    @result.class.ancestors.should include(Enumerable)
    @result.map { |row| row['col1'] }.should eq(['val1', nil])
  end
end
