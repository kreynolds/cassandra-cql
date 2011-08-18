here = File.dirname(__FILE__)
$LOAD_PATH << "#{here}/../vendor/gen-rb"
require "#{here}/../vendor/gen-rb/cassandra"

require 'simple_uuid'
require 'thrift_client'
require 'cassandra-cql/utility'
require 'cassandra-cql/database'
require 'cassandra-cql/schema'
require 'cassandra-cql/statement'
require 'cassandra-cql/result'
require 'cassandra-cql/row'
