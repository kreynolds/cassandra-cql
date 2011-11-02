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
        :exception_class_overrides => CassandraCQL::Thrift::InvalidRequestException
      }.merge(thrift_client_options)

      @keyspace = @options[:keyspace]
      @servers = servers
      connect!
      execute("USE #{@keyspace}")
    end

    def connect!
      @connection = ThriftClient.new(CassandraCQL::Thrift::Client, @servers, @thrift_client_options)
      obj = self
      @connection.add_callback(:post_connect) do
        execute("USE #{@keyspace}")
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
      reconnect!
    end
    alias_method :reconnect!, :reset!

    def prepare(statement, options={}, &block)
      stmt = Statement.new(self, statement)
      if block_given?
        yield stmt
      else
        stmt
      end
    end

    def execute(statement, *bind_vars)
      result = Statement.new(self, statement).execute(bind_vars)
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
  end
end
