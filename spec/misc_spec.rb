require File.expand_path('spec_helper.rb', File.dirname(__FILE__))
include CassandraCQL

describe "Miscellaneous tests that handle specific failures/regressions" do
  before(:each) do
    @connection = setup_cassandra_connection
    @connection.execute("DROP COLUMNFAMILY misc_tests") if @connection.schema.column_family_names.include?('misc_tests')
    @connection.execute("CREATE COLUMNFAMILY misc_tests (id text PRIMARY KEY)")
  end

  context "with ascii validation" do
    before(:each) do
      @connection.execute("ALTER COLUMNFAMILY misc_tests ADD test_column ascii")
    end
    
    it "should be consistent with ascii-encoded text" do
      @connection.execute("INSERT INTO misc_tests (id, test_column) VALUES (?, ?)", 'test', 'test_column').should be_nil
      row = @connection.execute("SELECT test_column FROM misc_tests WHERE id=?", 'test').fetch
      row['test_column'].should eq 'test_column'
      @connection.execute("INSERT INTO misc_tests (id, test_column) VALUES (?, ?)", 'test', row['test_column']).should be_nil
      row = @connection.execute("SELECT test_column FROM misc_tests WHERE id=?", 'test').fetch
      row['test_column'].should eq 'test_column'
    end
  end

  context "with unvalidatable data" do
    before(:each) do
      @connection.execute("INSERT INTO misc_tests (id, good_column, bad_column) VALUES (?, ?, ?)", 'test', 'blah', '')
      @connection.execute("ALTER COLUMNFAMILY misc_tests ADD bad_column int")
      @row = @connection.execute("SELECT good_column, bad_column FROM misc_tests WHERE id=?", 'test').fetch
    end
    
    it "should have valid column_names" do
      @row.column_names.should eq ['good_column', 'bad_column']
    end
    
    it "should have raise an exception with column_values" do
      expect { @row.column_values }.to raise_error(Error::CastException)
    end
    
    it "should be able to fetch good columns even with bad columns in the row" do
      @row['good_column'].should eq 'blah'
      @row[0].should eq 'blah'
    end

    it "should only cast a column once regardless of it's access method" do
      expect {
        @row['good_column'].should eq 'blah'
        @row[0].should eq 'blah'
      }.to change {
        @row.instance_variable_get(:@value_cache).size        
      }.by 1
    end

    it "should throw an error trying to fetch the bad column by name" do
      expect { @row['bad_column'] }.to raise_error(Error::CastException)
    end

    it "should throw an error trying to fetch the bad column by index" do
      expect { @row[1] }.to raise_error(Error::CastException)
    end

    it "the raw bytes should be accessible for repair" do
      @row.row.columns[1].name.should eq 'bad_column'
      @row.row.columns[1].value.should eq ''
    end
  end
end
