here = File.dirname(__FILE__)
require "#{here}/../vendor/gen-rb/cassandra"

require 'thrift_client'
require 'cassandra-cql/utility'
require 'cassandra-cql/uuid'
require 'cassandra-cql/database'
require 'cassandra-cql/schema'
require 'cassandra-cql/statement'
require 'cassandra-cql/result'
require 'cassandra-cql/row'
