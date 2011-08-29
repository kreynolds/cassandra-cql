module CassandraCQL
  class Schema
    attr_reader :schema, :column_families

    def initialize(schema)
      @schema = schema
      @column_families = {}
      @schema.cf_defs.each { |cf|
        @column_families[cf.name] = ColumnFamily.new(cf)
      }
    end

    def method_missing(method, *args, &block)
      if @schema.respond_to?(method)
        @schema.send(method)
      else
        super(method, *args, &block)
      end
    end

    def to_s
      keyspace
    end
  
    def keyspace
      name
    end
  
    def column_family_names
      @column_families.keys
    end
    alias_method :tables, :column_family_names
  end

  class ColumnFamily
    attr_reader :cf_def
  
    def initialize(cf_def)
      @cf_def = cf_def
    end
  
    def method_missing(method, *args, &block)
      if @cf_def.respond_to?(method)
        @cf_def.send(method)
      else
        super(method, *args, &block)
      end
    end

    def columns
      return @columns if @columns
      
      @columns = Hash.new(default_validation_class)
      @cf_def.column_metadata.each do |col|
        @columns[col.name] = col.validation_class
      end
      @columns[key_alias] = key_validation_class

      @columns
    end
    
    def self.cast(value, type)
      case type
      when "org.apache.cassandra.db.marshal.TimeUUIDType"
        UUID.to_time(value)
      when "org.apache.cassandra.db.marshal.UUIDType"
        UUID.new(value)
      when "org.apache.cassandra.db.marshal.IntegerType"
        int = value.unpack('N')[0]
        if int & 2**31 == 2**31
          int - 2**32
        else
          int
        end
      when "org.apache.cassandra.db.marshal.LongType", "org.apache.cassandra.db.marshal.CounterColumnType"
        ints = value.unpack("NN")
        (ints[0] << 32) + ints[1]
      when "org.apache.cassandra.db.marshal.AsciiType", "org.apache.cassandra.db.marshal.UTF8Type"
        value.to_s
      else
        value
      end
    end
    
    def name
      @cf_def.name
    end
  
    def type
      @cf_def.column_type
    end

    def id
      @cf_def.id
    end

    def standard?
      type == 'Standard'
    end
  
    def super?
      type == 'Super'
    end
  end
end