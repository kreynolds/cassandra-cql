module CassandraCQL
  module Error
    class InvalidResultType < Exception; end
    class InvalidCursor < Exception; end
  end

  class Result
    attr_reader :result, :column_family, :cursor

    def initialize(result, column_family=nil)
      @result, @column_family = result, column_family
      @column_family = @column_family.dup unless @column_family.nil?
      @cursor = 0
    end
  
    def void?
      @result.type == CassandraCQL::Thrift::CqlResultType::VOID
    end
    
    def int?
      @result.type == CassandraCQL::Thrift::CqlResultType::INT
    end

    def rows?
      @result.type == CassandraCQL::Thrift::CqlResultType::ROWS
    end

    def rows
      @result.rows.size
    end

    def cursor=(cursor)
      @cursor = cursor.to_i
    rescue Exception => e
      raise Error::InvalidCursor, e.to_s
    end
    
    def fetch_row
      case @result.type
      when CassandraCQL::Thrift::CqlResultType::ROWS
        return nil if @cursor >= rows

        row = Row.new(@result.rows[@cursor], @column_family)
        @cursor += 1
        return row
      when CassandraCQL::Thrift::CqlResultType::VOID
        return nil
      when CassandraCQL::Thrift::CqlResultType::INT
        return @result.num
      else
        raise Error::InvalidResultType, "Expects one of 0, 1, 2; was #{@result.type} "
      end
    end

    def fetch
      if block_given?
        while row = fetch_row
          yield row
        end
      else
        fetch_row
      end
    end

    def fetch_hash
      if block_given?
        while row = fetch_row
          if row.kind_of?(Fixnum)
            yield({row => row})
          else
            yield row.to_hash
          end
        end
      else
        if (row = fetch_row).kind_of?(Fixnum)
          {row => row}
        else
          row.to_hash
        end
      end
    end
  
    def fetch_array
      if block_given?
        while row = fetch_row
          if row.kind_of?(Fixnum)
            yield [row]
          else
            yield row.to_a
          end
        end
      else
        if (row = fetch_row).kind_of?(Fixnum)
          [row]
        else
          row.to_a
        end
      end
    end
  end
end