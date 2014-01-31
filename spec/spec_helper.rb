if RUBY_VERSION >= "1.9"
  require 'simplecov'
  SimpleCov.start
end

require 'rubygems'
require 'bundler'
Bundler.setup(:default, :test)

require 'yaml'
require 'rspec'

CASSANDRA_VERSION = ENV['CASSANDRA_VERSION'] || '1.2' unless defined?(CASSANDRA_VERSION)
CQL_VERSION = ENV['CQL_VERSION'] || '3.0.0'
USE_CQL3 = CQL_VERSION.split('.').first.to_i == 3 && CASSANDRA_VERSION >= '1.2'

require "cassandra-cql/#{CASSANDRA_VERSION}"

module Helpers
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
    if CASSANDRA_VERSION >= '1.2'
      cassandra_cql_options.merge!(:cql_version => CQL_VERSION)
    end

    keyspace_name = "cassandra_cql_test_keyspace_#{CASSANDRA_VERSION.gsub(/\D/, '')}_#{CQL_VERSION.gsub(/\D/, '')}"
    connection = CassandraCQL::Database.new(["#{host}:#{port}"], cassandra_cql_options, :retries => 5, :timeout => 5)
    if !connection.keyspaces.map(&:name).include?(keyspace_name)
      create_keyspace(connection, keyspace_name)
    end
    connection.execute("USE #{keyspace_name}")

    connection
  end

  def drop_column_family_if_exists(connection, cf)
    if column_family_exists?(connection, cf)
      connection.execute("DROP COLUMNFAMILY #{cf}")
    end
  end
end

module Cql2Helpers
  def column_family_exists?(connection, cf)
    @connection.schema.column_family_names.include?(cf.to_s)
  end

  def create_keyspace(connection, ks)
    connection.execute("CREATE KEYSPACE #{ks} WITH strategy_class='org.apache.cassandra.locator.SimpleStrategy' AND strategy_options:replication_factor=1")
  end
end

module Cql3Helpers
  def column_family_exists?(connection, cf)
    connection.execute(<<-CQL, connection.keyspace, cf).fetch_row
      SELECT * FROM system.schema_columnfamilies
      WHERE keyspace_name = ? AND columnfamily_name = ?
    CQL
  end

  def create_keyspace(connection, ks)
    connection.execute("CREATE KEYSPACE #{ks} WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}")
  end
end

RSpec.configure do |config|
  config.filter_run_excluding :cql_version =>
    lambda { |version| version != CQL_VERSION }

  config.include Helpers
  if CQL_VERSION == '3.0.0'
    config.include Cql3Helpers
  else
    config.include Cql2Helpers
  end
end
