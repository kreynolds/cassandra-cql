module CassandraCQL
  module Error
    class InvalidBindVariable < Exception; end
    class UnescapableObject < Exception; end
  end

  class Statement

    KS_CHANGE_RE = /^use (\w+)/i
    SCHEMA_CHANGE_RE = /\s*(create|drop|alter)\s+(\w+)/i
    KS_DROP_RE = /^drop keyspace (\w+)/i
    COLFAM_RE = /\s*select.*from\s+'?(\w+)/i

    attr_reader :statement

    def initialize(handle, statement)
      @handle = handle
      prepare(statement)
    end
  
    def prepare(statement)
      @statement = statement
    end
  
    def execute(bind_vars=[], options={})
      column_family = nil
      if @statement =~ COLFAM_RE
        column_family = @handle.schema.column_families[$1].dup
      end

      if options[:compression]
        res = Result.new(@handle.execute_cql_query(CassandraCQL::Utility.compress(self.class.sanitize(@statement, bind_vars)), CassandraThrift::Compression::GZIP), column_family)
      else
        res = Result.new(@handle.execute_cql_query(self.class.sanitize(@statement, bind_vars), CassandraThrift::Compression::NONE), column_family)
      end
    
      # Change our keyspace if required
      if @statement =~ KS_CHANGE_RE
        @handle.keyspace = $1
      end

      # If we are dropping a keyspace, we should set it to nil
      @handle.keyspace = nil if @statement =~ KS_DROP_RE
    
      # Update the schema if it has changed
      if @statement =~ KS_CHANGE_RE or @statement =~ SCHEMA_CHANGE_RE or @statement =~ KS_DROP_RE
        @handle.update_schema!
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
      # TODO: performance test using String.index as an optimized for skipping this regex
      obj.gsub("'", "\\\\'")
    end

    def self.quote(obj)
      if obj.kind_of?(Array)
        obj.map { |member| quote(member) }.join(",")
      elsif obj.kind_of?(String)
        "'" + escape(obj) + "'"
      elsif obj.kind_of?(Fixnum)
        obj
      else
        raise Error::UnescapableObject, "Unable to escape object of class #{obj.class}"
      end
    end
  
    def self.cast_to_cql(obj)
      if obj.kind_of?(Array)
        obj.map { |member| cast_to_cql(member) }
      elsif obj.kind_of?(Fixnum)
        obj
      elsif obj.kind_of?(Time)
        SimpleUUID::UUID.new(obj).to_guid
      elsif obj.kind_of?(SimpleUUID::UUID)
        obj.to_guid
      # There are corner cases where this is an invalid assumption but they are extremely rare.
      # The alternative is to make the user pack the data on their own .. let's not do that until we have to
      elsif obj.kind_of?(String) and Utility.binary_data?(obj)
        escape(obj.unpack('H*')[0])
      else
        escape(obj.to_s)
      end
    end
  
    def self.sanitize(statement, bind_vars=[])
      bind_vars = bind_vars.dup
      expected_bind_vars = statement.count("?")

      return statement if expected_bind_vars == 0 and bind_vars.empty?
      raise CassandraCQL::Error::InvalidBindVariable, "Wrong number of bound variables (statement expected #{expected_bind_vars}, was #{bind_vars.size})" if expected_bind_vars != bind_vars.size
    
      # TODO: This can be done better
      statement.chars.map { |c|
        c == '?' ? quote(cast_to_cql(bind_vars.shift)) : c    
      }.join
    end
  end
end