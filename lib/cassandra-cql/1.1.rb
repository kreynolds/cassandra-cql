module CassandraCQL
  def self.CASSANDRA_VERSION
    "1.1"
  end
end

require "#{File.expand_path(File.dirname(__FILE__))}/../cassandra-cql"
