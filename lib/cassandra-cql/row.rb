module CassandraCQL
  class Row
    attr_reader :row
    
    def initialize(row, column_family)
      @row, @column_family = row, column_family
    end
  
    def [](obj)
      # Rows include the row key so we skip the first one
      column_index = obj.kind_of?(Fixnum) ? obj : column_names.index(obj)
      return nil if column_index.nil?
      column_values[column_index]
    end

    def column_names
      @names ||= @row.columns.map do |column|
        if column.name == @column_family.key_alias
          column.name
        else
          ColumnFamily.cast(column.name, @column_family.comparator_type)
        end
      end
    end
  
    def column_values
      @values ||= @row.columns.map { |column| ColumnFamily.cast(column.value, @column_family.columns[column.name]) }
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
      ColumnFamily.cast(@row.key, @column_family.key_validation_class)
    end
  end
end