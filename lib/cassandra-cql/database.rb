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
    class InvalidRequestException < Exception; end
  end

  class Database
    attr_reader :connection, :schema, :keyspace
  
    def initialize(servers, options={}, thrift_client_options={})
      @options = {
        :keyspace => 'system'
      }.merge(options)

      @thrift_client_options = {
        :exception_class_overrides => CassandraCQL::Thrift::InvalidRequestException,
        :connect_timeout => 5
      }.merge(thrift_client_options)

      @keyspace = @options[:keyspace]
      @cql_version = @options[:cql_version]
      @servers = servers
      connect!
      execute("USE #{@keyspace}")
    end

    def connect!
      @connection = ThriftClient.new(CassandraCQL::Thrift::Client, @servers, @thrift_client_options)
      obj = self
      @connection.add_callback(:post_connect) do
        @connection.set_cql_version(@cql_version) if @cql_version
        execute("USE #{@keyspace}")
        @connection.login(@auth_request) if @auth_request
      end
    end
  
    def disconnect!
      @connection.disconnect! if active?
    end

    def active?
      # TODO: This should be replaced with a CQL call that doesn't exist yet
      @connection.describe_version 
      true
    rescue Exception
      false
    end
    alias_method :ping, :active?

    def reset!
      disconnect!
      connect!
    end
    alias_method :reconnect!, :reset!

    def statement_class
      return @statement_class if @statement_class

      version_module = 'V' + CassandraCQL.CASSANDRA_VERSION.gsub('.', '')
      return @statement_class = CassandraCQL.const_get(version_module).const_get(:Statement)
    end

    def prepare(statement, options={}, &block)
      stmt = statement_class.new(self, statement)
      if block_given?
        yield stmt
      else
        stmt
      end
    end

    def execute(statement, *bind_vars)
      result = statement_class.new(self, statement).execute(bind_vars)
      if block_given?
        yield result
      else
        result
      end
    rescue CassandraCQL::Thrift::InvalidRequestException
      raise Error::InvalidRequestException.new($!.why)
    end

    def execute_cql_query(cql, compression=CassandraCQL::Thrift::Compression::NONE)
      @connection.execute_cql_query(cql, compression)
    rescue CassandraCQL::Thrift::InvalidRequestException
      raise Error::InvalidRequestException.new($!.why)
    end
    
    def keyspace=(ks)
      @keyspace = (ks.nil? ? nil : ks.to_s)
    end
  
    def keyspaces
      # TODO: This should be replaced with a CQL call that doesn't exist yet
      @connection.describe_keyspaces.map { |keyspace| Schema.new(keyspace) }
    end
    
    def schema
      # TODO: This should be replaced with a CQL call that doesn't exist yet
      Schema.new(@connection.describe_keyspace(@keyspace))
    end

    def login!(username, password)
      request = CassandraCQL::Thrift::AuthenticationRequest.new
      request.credentials = {'username' => username, 'password' => password}
      ret = @connection.login(request)
      # To avoid a double login on the initial connect, we set
      # @auth_request after the first successful login.
      @auth_request = request
      ret
    end
  end
end
