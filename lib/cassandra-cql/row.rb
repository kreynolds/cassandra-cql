module CassandraCQL
  class Row
    attr_reader :row
    
    def initialize(row, schema)
      @row, @schema = row, schema
    end
  
    def [](obj)
      # Rows include the row key so we skip the first one
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
  
    def key
      ColumnFamily.cast(@row.key, @schema.values[@row.key])
    end
  end
end