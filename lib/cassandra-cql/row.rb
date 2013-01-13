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
  class Row
    attr_reader :row
    
    def initialize(row, schema)
      @row, @schema = row, schema
      @value_cache = Hash.new { |h, key|
        # If it's a number and not one of our columns, assume it's an index
        if key.kind_of?(Fixnum) and !column_indices.key?(key)
          column_name = column_names[key]
          column_index = key
        else
          column_name = key
          column_index = column_indices[key]
        end
        
        if column_index.nil?
          # Cache negative hits
          h[column_name] = nil
        else
          h[column_name] = ColumnFamily.cast(@row.columns[column_index].value, @schema.values[@row.columns[column_index].name])
        end
      }
    end
  
    def [](obj)
      @value_cache[obj]
    end

    def column_names
      @names ||= @row.columns.map do |column|
        ColumnFamily.cast(column.name, @schema.names[column.name])
      end
    end

    def column_indices
      @column_indices ||= Hash[column_names.each_with_index.to_a]
    end

    def column_values
      column_names.map do |name|
        @value_cache[name]
      end
    end
  
    def columns
      @row.columns.size
    end
  
    def to_a
      column_values
    end
  
    # TODO: This should be an ordered hash
    def to_hash
      Hash[([column_names, column_values]).transpose]
    end
  end
end
