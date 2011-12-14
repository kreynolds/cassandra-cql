module CassandraCQL
  module V08
    class Statement < CassandraCQL::Statement
      SCHEMA_CHANGE_RE = /\s*(create|drop|alter)\s+(\w+)/i
      COLFAM_RE = /\s*select.*from\s+'?(\w+)/i

      def execute(bind_vars=[], options={})
        column_family = nil
        if @statement =~ COLFAM_RE
          column_family = @handle.schema.column_families[$1].dup
        end

        sanitized_query = self.class.sanitize(@statement, bind_vars)
        compression_type = CassandraCQL::Thrift::Compression::NONE
        if options[:compression]
          compression_type = CassandraCQL::Thrift::Compression::GZIP
          sanitized_query = Utility.compress(sanitized_query)
        end

        res = V08::Result.new(@handle.execute_cql_query(sanitized_query, compression_type), column_family)

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
    end
  end
end
