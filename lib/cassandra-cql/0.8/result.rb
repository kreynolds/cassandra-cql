module CassandraCQL
  module V08
    class ResultSchema < CassandraCQL::ResultSchema
      def initialize(column_family)
        type_slice = lambda {|type| type[type.rindex('.')+1..-1] }

        @names = Hash.new(type_slice.call(column_family.comparator_type))
        @values = Hash.new(type_slice.call(column_family.default_validation_class))
        column_family.columns.each_pair do |name, type|
          @values[name] = type_slice.call(type)
        end
      end
    end

    class Result < CassandraCQL::Result
      def initialize(result, column_family)
        @result = result
        @schema = ResultSchema.new(column_family) if rows?
        @cursor = 0
      end
    end
  end
end
