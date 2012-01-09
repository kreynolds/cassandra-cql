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
end