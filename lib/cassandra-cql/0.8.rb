module CassandraCQL
  def self.CASSANDRA_VERSION
    "0.8"
  end
end

require "#{File.expand_path(File.dirname(__FILE__))}/../cassandra-cql"
