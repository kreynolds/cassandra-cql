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
  
end