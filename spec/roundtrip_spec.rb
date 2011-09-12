require File.expand_path('spec_helper.rb', File.dirname(__FILE__))
include CassandraCQL

describe "RoundTrip tests" do
  before(:all) do
    def clear_keyspace!(connection)
      connection.schema.column_family_names.each do |cf|
        connection.execute("truncate #{cf}")
      end
    end

    conn = ["127.0.0.1:9160"]
    thrift_options = {:retries => 2, :timeout => 1}
    @type_conversions = CassandraCQL::Database.new(conn, {:keyspace => 'TypeConversions'}, thrift_options)
    clear_keyspace!(@type_conversions)
    @multiblog_long = CassandraCQL::Database.new(conn, {:keyspace => 'MultiblogLong'}, thrift_options)
    clear_keyspace!(@multiblog_long)
    @twitter = CassandraCQL::Database.new(conn, {:keyspace => 'Twitter'}, thrift_options)
    clear_keyspace!(@twitter)
  end

  context "with comparator IntegerType" do
    def create_and_fetch_integer_column(int_column_name)
      cql = 'insert into IntegerConversion (KEY, ?) values (?, ?)'
      row_key = 'opening-Melbourne'
      column_value = 'silted-misunderstanding'
      @type_conversions.execute(cql, int_column_name, row_key, column_value)
      return @type_conversions.execute('select ? from IntegerConversion where KEY = ?', int_column_name, row_key).fetch
    end

    it "should properly convert integer values that fit into 1 byte" do
      row = create_and_fetch_integer_column(1)
      row.column_names.should eq([1])
    end

    it "should properly convert integer values that fit into 2 bytes" do
      i = 2**8 + 80
      row = create_and_fetch_integer_column(i)
      row.column_names.should eq([i])
    end

    it "should properly convert integer values that fit into 3 bytes" do
      i = 2**16 + 622
      row = create_and_fetch_integer_column(i)
      row.column_names.should eq([i])
    end

    it "should properly convert integer values that fit into 4 bytes" do
      i = 2**24 + 45820
      row = create_and_fetch_integer_column(i)
      row.column_names.should eq([i])
    end

    it "should properly convert integer values that are negative" do
      i = -20681
      row = create_and_fetch_integer_column(i)
      row.column_names.should eq([i])
    end
  end

  context "with comparator LongType" do
    def create_and_fetch_long_column(long_column_name)
      cql = 'insert into Blogs (KEY, ?) values (?, ?)'
      row_key = 'rationalistic-hammock'
      column_value = 'thyroid-hallucinogen'
      @multiblog_long.execute(cql, long_column_name, row_key, column_value)
      return @multiblog_long.execute('select ? from Blogs where KEY = ?', long_column_name, row_key).fetch
    end

    it "should properly convert long values shorter than 4 bytes" do
      i = 190
      row = create_and_fetch_long_column(i)
      row.column_names.should eq([i])
      i = -i
      row = create_and_fetch_long_column(i)
      row.column_names.should eq([i])
    end

    it "should properly convert long values greater than 4 bytes" do
      i = 2**32 + 618387
      row = create_and_fetch_long_column(i)
      row.column_names.should eq([i])
      i = -i
      row = create_and_fetch_long_column(i)
      row.column_names.should eq([i])
    end
  end

  context "with comparator CounterColumnType" do
    it "should convert counters to long values" do
      i = 2**32 + 280647
      @twitter.execute("update UserCounters set Montezuma = Montezuma + #{i} where KEY = 'Houston'")
      row = @twitter.execute("select Montezuma from UserCounters where KEY = 'Houston'").fetch
      row.column_values.should eq([i])

      @twitter.execute("update UserCounters set Montezuma = Montezuma - #{i} where KEY = 'Houston'")
      row = @twitter.execute("select Montezuma from UserCounters where KEY = 'Houston'").fetch
      row.column_values.should eq([0])

      @twitter.execute("update UserCounters set Montezuma = Montezuma - #{i} where KEY = 'Houston'")
      row = @twitter.execute("select Montezuma from UserCounters where KEY = 'Houston'").fetch
      row.column_values.should eq([-i])
    end
  end
end
