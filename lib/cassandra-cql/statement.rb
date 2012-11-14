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

module CassandraCQL
  module Error
    class InvalidBindVariable < Exception; end
    class UnescapableObject < Exception; end
  end

  class Statement

    KS_CHANGE_RE = /^use (\w+)/i
    KS_DROP_RE = /^drop keyspace (\w+)/i

    attr_reader :statement

    def initialize(handle, statement)
      @handle = handle
      prepare(statement)
    end

    def prepare(statement)
      @statement = statement
    end

    def execute(bind_vars=[], options={})
      sanitized_query = self.class.sanitize(@statement, bind_vars)
      compression_type = CassandraCQL::Thrift::Compression::NONE
      if options[:compression]
        compression_type = CassandraCQL::Thrift::Compression::GZIP
        sanitized_query = Utility.compress(sanitized_query)
      end

      res = Result.new(@handle.execute_cql_query(sanitized_query, compression_type))

      # Change our keyspace if required
      if @statement =~ KS_CHANGE_RE
        @handle.keyspace = $1
      elsif @statement =~ KS_DROP_RE
        @handle.keyspace = nil
      end

      # We let ints be fetched for now because they'll probably be deprecated later
      if res.void?
        nil
      else
        res
      end
    end

    def finish
      true
    end

    def self.escape(obj)
      obj.gsub("'", "''")
    end

    def self.quote(obj)
      if obj.kind_of?(Array)
        obj.map { |member| quote(member) }.join(",")
      elsif obj.kind_of?(String)
        "'" + obj + "'"
      elsif obj.kind_of?(Numeric)
        obj
      else
        raise Error::UnescapableObject, "Unable to escape object of class #{obj.class}"
      end
    end

    def self.cast_to_cql(obj)
      if obj.kind_of?(Array)
        obj.map { |member| cast_to_cql(member) }
      elsif obj.kind_of?(Fixnum) or obj.kind_of?(Float)
        obj
      elsif obj.kind_of?(Date)
        obj.strftime('%Y-%m-%d')
      elsif obj.kind_of?(Time)
        (obj.to_f * 1000).to_i
      elsif obj.kind_of?(SimpleUUID::UUID)
        obj.to_guid
      # There are corner cases where this is an invalid assumption but they are extremely rare.
      # The alternative is to make the user pack the data on their own .. let's not do that until we have to
      elsif obj.kind_of?(String) and Utility.binary_data?(obj)
        escape(obj.unpack('H*')[0])
      else
        RUBY_VERSION >= "1.9" ? escape(obj.to_s.dup.force_encoding('ASCII-8BIT')) : escape(obj.to_s.dup)
      end
    end
  
    def self.sanitize(statement, bind_vars=[])
      # If there are no bind variables, return the statement unaltered
      return statement if bind_vars.empty?

      bind_vars = bind_vars.dup
      expected_bind_vars = statement.count("?")

      raise Error::InvalidBindVariable, "Wrong number of bound variables (statement expected #{expected_bind_vars}, was #{bind_vars.size})" if expected_bind_vars != bind_vars.size
    
      statement.gsub(/\?/) {
        quote(cast_to_cql(bind_vars.shift))
      }
    end
  end
end