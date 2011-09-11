require File.expand_path('spec_helper.rb', File.dirname(__FILE__))
include CassandraCQL

describe "RoundTrip tests" do
  before(:all) do
    @type_conversions = CassandraCQL::Database.new(["127.0.0.1:9160"], {:keyspace => 'TypeConversions'}, {:retries => 2, :timeout => 0.1})
  end

  def create_and_fetch_integer_column(int_column_name)
    cql = 'insert into IntegerConversion (KEY, ?) values (?, ?)'
    row_key = 'opening-Melbourne'
    column_value = 'silted-misunderstanding'
    @type_conversions.execute(cql, int_column_name, row_key, column_value)
    return @type_conversions.execute('select ? from IntegerConversion where KEY = ?', int_column_name, row_key).fetch
  end

  context "with comparator IntegerType" do
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
end
