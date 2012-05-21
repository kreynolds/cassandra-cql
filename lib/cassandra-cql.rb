=begin
Copyright 2011 Inside Systems, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
=end

module CassandraCQL; end;
unless CassandraCQL.respond_to?(:CASSANDRA_VERSION)
  require "cassandra-cql/1.1"
end

here = File.expand_path(File.dirname(__FILE__))
require "#{here}/../vendor/#{CassandraCQL.CASSANDRA_VERSION}/gen-rb/cassandra"

require 'bigdecimal'
require 'date'
require 'thrift_client'
require 'cassandra-cql/types/abstract_type'
require 'cassandra-cql/types/ascii_type'
require 'cassandra-cql/types/boolean_type'
require 'cassandra-cql/types/bytes_type'
require 'cassandra-cql/types/date_type'
require 'cassandra-cql/types/decimal_type'
require 'cassandra-cql/types/double_type'
require 'cassandra-cql/types/float_type'
require 'cassandra-cql/types/integer_type'
require 'cassandra-cql/types/long_type'
require 'cassandra-cql/types/utf8_type'
require 'cassandra-cql/types/uuid_type'
require 'cassandra-cql/utility'
require 'cassandra-cql/uuid'
require 'cassandra-cql/database'
require 'cassandra-cql/schema'
require 'cassandra-cql/statement'
require 'cassandra-cql/result'
require 'cassandra-cql/row'

require "cassandra-cql/#{CassandraCQL.CASSANDRA_VERSION}/result"
require "cassandra-cql/#{CassandraCQL.CASSANDRA_VERSION}/statement"
