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
    end
  
    def [](obj)
      column_index = obj.kind_of?(Fixnum) ? obj : column_names.index(obj)
      return nil if column_index.nil?
      column_values[column_index]
    end

    def column_names
      @names ||= @row.columns.map do |column|
        ColumnFamily.cast(column.name, @schema.names[column.name])
      end
    end
  
    def column_values
      @values ||= @row.columns.map { |column| ColumnFamily.cast(column.value, @schema.values[column.name]) }
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