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
      return nil if value.nil?

      if CassandraCQL::Types.const_defined?(type)
        CassandraCQL::Types.const_get(type).cast(value)
      else
        CassandraCQL::Types::AbstractType.cast(value)
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
