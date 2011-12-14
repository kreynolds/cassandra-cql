require 'yaml'
require 'rspec'

CASSANDRA_VERSION = ENV['CASSANDRA_VERSION'] || '1.0' unless defined?(CASSANDRA_VERSION)

$LOAD_PATH << "#{File.expand_path(File.dirname(__FILE__))}/../lib"
require "cassandra-cql/#{CASSANDRA_VERSION}"

def yaml_fixture(file)
  if file.kind_of?(Symbol)
    file = "#{file}.yaml"
  elsif file !~ /\.yaml$/
    file = "#{file}.yaml"
  end
  YAML::load_file(File.dirname(__FILE__) + "/fixtures/#{file}")
end

def setup_cassandra_connection
  connection = CassandraCQL::Database.new(["127.0.0.1:9160"], {}, :retries => 2, :timeout => 1)
  if !connection.keyspaces.map(&:name).include?("CassandraCQLTestKeyspace")
    connection.execute("CREATE KEYSPACE CassandraCQLTestKeyspace WITH strategy_class='org.apache.cassandra.locator.SimpleStrategy' AND strategy_options:replication_factor=1")
  end
  connection.execute("USE CassandraCQLTestKeyspace")

  connection
end
