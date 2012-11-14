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
    class InvalidResultType < Exception; end
    class InvalidCursor < Exception; end
  end

  class ResultSchema
    attr_reader :names, :values

    def initialize(schema)
      # When https://issues.apache.org/jira/browse/CASSANDRA-3436 is resolve, no more need to split/last
      @names = Hash.new(schema.default_name_type.split(".").last)
      schema.name_types.each_pair { |key, type|
        @names[key] = type.split(".").last
      }
      @values = Hash.new(schema.default_value_type.split(".").last)
      schema.value_types.each_pair { |key, type|
        @values[key] = type.split(".").last
      }
    end
  end

  class Result
    include Enumerable

    attr_reader :result, :schema, :cursor

    def initialize(result)
      @result = result
      @schema = ResultSchema.new(result.schema) if rows?
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

    alias_method :size, :rows
    alias_method :count, :rows
    alias_method :length, :rows

    def cursor=(cursor)
      @cursor = cursor.to_i
    rescue Exception => e
      raise Error::InvalidCursor, e.to_s
    end

    def each
      fetch { |row| yield row }
    end

    def fetch_row
      case @result.type
      when CassandraCQL::Thrift::CqlResultType::ROWS
        return nil if @cursor >= rows

        row = Row.new(@result.rows[@cursor], @schema)
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
        if row = fetch_row
          if row.kind_of?(Fixnum)
            {row => row}
          else
            row.to_hash
          end
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
