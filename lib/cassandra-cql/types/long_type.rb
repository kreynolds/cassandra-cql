module CassandraCQL
  module Types
    class LongType < AbstractType
      def self.cast(value)
        bytes_to_long(value)
      end
    end
    
    class CounterColumnType < LongType; end
  end
end
