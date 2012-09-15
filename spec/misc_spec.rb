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
end
