require File.expand_path('spec_helper.rb', File.dirname(__FILE__))
include CassandraCQL

describe "Database" do
  before do
    @connection = setup_cassandra_connection
  end
  
  describe "reset!" do
    it "should create a new connection" do
      @connection.should_receive(:connect!)
      @connection.reset!
    end
  end
  
  describe "login!" do
    it "should call login! on connection" do
      creds = { 'username' => 'myuser', 'password' => 'mypass' }
      @connection.connection.should_receive(:login) do |auth|
        auth.credentials.should eq(creds)
      end
      @connection.login!(creds['username'], creds['password'])
    end
  end

  describe "execute" do
    before do
      @connection = setup_cassandra_connection
      if !column_family_exists?(@connection, 'conn_exec')
        @connection.execute("CREATE COLUMNFAMILY conn_exec (id varchar PRIMARY KEY, column varchar)")
      else
        @connection.execute("TRUNCATE conn_exec")
      end
    end

    it "should execute using a default consistency" do
      @connection.execute("INSERT INTO conn_exec (id, column) VALUES (?, ?)", "key", "value")
      @connection.execute("SELECT column FROM conn_exec") { |result| result.fetch["column"].should == "value" }
    end

    it "should execute using a given consistency" do
      @connection.execute_with_consistency("INSERT INTO conn_exec (id, column) VALUES (?, ?)", CassandraCQL::Thrift::ConsistencyLevel::ONE, "key", "value")
      @connection.execute_with_consistency("SELECT column FROM conn_exec", CassandraCQL::Thrift::ConsistencyLevel::ONE) { |result| result.fetch["column"].should == "value" }
    end
  end
end
