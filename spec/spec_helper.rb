if RUBY_VERSION >= "1.9"
  require 'simplecov'
  SimpleCov.start
end

require 'rubygems'
require 'bundler'
Bundler.setup(:default, :test)

require 'yaml'
require 'rspec'

CASSANDRA_VERSION = ENV['CASSANDRA_VERSION'] || '1.1' unless defined?(CASSANDRA_VERSION)

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
  host = ENV['CASSANDRA_CQL_HOST'] || '127.0.0.1'
  port = ENV['CASSANDRA_CQL_PORT'] || 9160

  cassandra_cql_options = {}
  cassandra_cql_options.merge!(:cql_version => '2.0.0') if CASSANDRA_VERSION >= '1.2'

  connection = CassandraCQL::Database.new(["#{host}:#{port}"], cassandra_cql_options, :retries => 5, :timeout => 5)
  if !connection.keyspaces.map(&:name).include?("CassandraCQLTestKeyspace")
    connection.execute("CREATE KEYSPACE CassandraCQLTestKeyspace WITH strategy_class='org.apache.cassandra.locator.SimpleStrategy' AND strategy_options:replication_factor=1")
  end
  connection.execute("USE CassandraCQLTestKeyspace")

  connection
end
