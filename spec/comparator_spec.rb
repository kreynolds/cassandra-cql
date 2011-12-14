# encoding: utf-8
require File.expand_path('spec_helper.rb', File.dirname(__FILE__))
include CassandraCQL

describe "Comparator Roundtrip tests" do
  before(:each) do
    @connection = setup_cassandra_connection
  end

  def create_and_fetch_column(column_family, name)
    @connection.execute("insert into #{column_family} (id, ?) values (?, ?)", name, 'test', 'test')
    row =  @connection.execute("select ? from #{column_family} where id = ?", name, 'test').fetch
    row.column_names[0]
  end

  def create_column_family(name, comparator_type)
    if !@connection.schema.column_family_names.include?(name)
      @connection.execute("CREATE COLUMNFAMILY #{name} (id text PRIMARY KEY) WITH comparator=?", comparator_type)
    end
  end

  context "with ascii comparator" do
    let(:cf_name) { "comparator_cf_ascii" }
    before(:each) { create_column_family(cf_name, 'AsciiType') }

    it "should return an ascii string" do
      create_and_fetch_column(cf_name, "test string").should eq("test string")
    end
  end

  context "with bigint comparator" do
    let(:cf_name) { "comparator_cf_bigint" }
    before(:each) { create_column_family(cf_name, 'LongType') }

    def test_for_value(value)
      create_and_fetch_column(cf_name, value).should eq(value)
      create_and_fetch_column(cf_name, value*-1).should eq(value*-1)
    end
  
    it "should properly convert integer values that fit into 1 byte" do
      test_for_value(1)
    end
    it "should properly convert integer values that fit into 2 bytes" do
      test_for_value(2**8 + 80)
    end
    it "should properly convert integer values that fit into 3 bytes" do
      test_for_value(2**16 + 622)
    end
    it "should properly convert integer values that fit into 4 bytes" do
      test_for_value(2**24 + 45820)
    end
    it "should properly convert integer values that fit into 5 bytes" do
      test_for_value(2**32 + 618387)
    end
  end

  context "with blob comparator" do
    let(:cf_name) { "comparator_cf_blob" }
    before(:each) { create_column_family(cf_name, 'BytesType') }

    it "should return a blob" do
      bytes = "binary\x00"
      bytes = bytes.force_encoding('ASCII-8BIT') if RUBY_VERSION >= "1.9"
      create_and_fetch_column(cf_name, bytes).should eq(bytes)
    end
  end

  context "with boolean comparator" do
    let(:cf_name) { "comparator_cf_boolean" }
    before(:each) { create_column_family(cf_name, 'BooleanType') }

    it "should return true" do
      create_and_fetch_column(cf_name, true).should be_true
    end

    it "should return false" do
      create_and_fetch_column(cf_name, false).should be_false
    end
  end

  context "with date comparator" do
    let(:cf_name) { "comparator_cf_date" }
    before(:each) { create_column_family(cf_name, 'DateType') }

    it "should return a Time object" do
      ts = Time.new
      res = create_and_fetch_column(cf_name, ts)
      res.to_f.should be_within(0.001).of(ts.to_f)
      res.should be_kind_of(Time)
    end
    
    it "should return a timestamp given a date" do
      date = Date.today
      res = create_and_fetch_column(cf_name, date)
      [:year, :month, :day].each do |sym|
        res.send(sym).should eq(date.send(sym))
      end
      res.class.should eq(Time)
    end
  end

  if CASSANDRA_VERSION.to_f >= 1.0
    context "with decimal comparator" do
      let(:cf_name) { "comparator_cf_decimal" }
      before(:each) { create_column_family(cf_name, 'DecimalType') }

      def test_for_value(value)
        create_and_fetch_column(cf_name, value).should eq(value)
        create_and_fetch_column(cf_name, value*-1).should eq(value*-1)
      end

      it "should return a small decimal" do
        test_for_value(15.333)
      end
      it "should return a huge decimal" do
        test_for_value(BigDecimal.new('129182739481237481341234123411.1029348102934810293481039'))
      end
    end
  end

  context "with double comparator" do
    let(:cf_name) { "comparator_cf_double" }
    before(:each) { create_column_family(cf_name, 'DoubleType') }

    def test_for_value(value)
      create_and_fetch_column(cf_name, value).should be_within(0.1).of(value)
      create_and_fetch_column(cf_name, value*-1).should be_within(0.1).of(value*-1)
    end
  
    it "should properly convert some float values" do
      test_for_value(1.125)
      test_for_value(384.125)
      test_for_value(65540.125)
      test_for_value(16777217.125)
      test_for_value(109911627776.125)
    end
  end

  context "with float comparator" do
    let(:cf_name) { "comparator_cf_float" }
    before(:each) { create_column_family(cf_name, 'FloatType') }

    def test_for_value(value)
      create_and_fetch_column(cf_name, value*-1).should eq(value*-1)
      create_and_fetch_column(cf_name, value).should eq(value)
    end
  
    it "should properly convert some float values" do
      test_for_value(1.125)
      test_for_value(384.125)
      test_for_value(65540.125)
    end
  end

  if CASSANDRA_VERSION.to_f >= 1.0
    #Int32Type was added in 1.0 (CASSANDRA-3031)
    context "with int comparator" do
      let(:cf_name) { "comparator_cf_int" }
      before(:each) { create_column_family(cf_name, 'Int32Type') }

      def test_for_value(value)
        create_and_fetch_column(cf_name, value).should eq(value)
        create_and_fetch_column(cf_name, value*-1).should eq(value*-1)
      end

      it "should properly convert integer values that fit into 1 byte" do
        test_for_value(1)
      end
      it "should properly convert integer values that fit into 2 bytes" do
        test_for_value(2**8 + 80)
      end
      it "should properly convert integer values that fit into 3 bytes" do
        test_for_value(2**16 + 622)
      end
      it "should properly convert integer values that fit into 4 bytes" do
        test_for_value(2**24 + 45820)
      end
    end
  end

  context "with text comparator" do
    let(:cf_name) { "comparator_cf_text" }
    before(:each) { create_column_family(cf_name, 'UTF8Type') }

    it "should return a non-multibyte string" do
      create_and_fetch_column(cf_name, "snark").should eq("snark")
    end

    it "should return a multibyte string" do
      create_and_fetch_column(cf_name, "snårk").should eq("snårk")
    end
  end

  context "with timestamp comparator" do
    let(:cf_name) { "comparator_cf_timestamp" }
    before(:each) { create_column_family(cf_name, 'TimeUUIDType') }

    it "should return a timestamp" do
      uuid = UUID.new
      create_and_fetch_column(cf_name, uuid).should eq(uuid)
    end
  end

  context "with uuid comparator" do
    let(:cf_name) { "comparator_cf_uuid" }
    before(:each) { create_column_family(cf_name, 'UUIDType') }

    it "should return a uuid" do
      uuid = UUID.new
      create_and_fetch_column(cf_name, uuid).should eq(uuid)
    end
  end

  context "with varchar comparator" do
    let(:cf_name) { "comparator_cf_varchar" }
    before(:each) { create_column_family(cf_name, 'UTF8Type') }

    it "should return a non-multibyte string" do
      create_and_fetch_column(cf_name, "snark").should eq("snark")
    end
    
    it "should return a multibyte string" do
      create_and_fetch_column(cf_name, "snårk").should eq("snårk")
    end
  end

  context "with varint comparator" do
    let(:cf_name) { "comparator_cf_varint" }
    before(:each) { create_column_family(cf_name, 'IntegerType') }

    def test_for_value(value)
      create_and_fetch_column(cf_name, value).should eq(value)
      create_and_fetch_column(cf_name, value*-1).should eq(value*-1)
    end
  
    it "should properly convert integer values that fit into 1 byte" do
      test_for_value(1)
    end
    it "should properly convert integer values that fit into 2 bytes" do
      test_for_value(2**8 + 80)
    end
    it "should properly convert integer values that fit into 3 bytes" do
      test_for_value(2**16 + 622)
    end
    it "should properly convert integer values that fit into more than 8 bytes" do
      test_for_value(2**256)
    end
  end
end