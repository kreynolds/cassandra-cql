module CassandraCQL
  module Types
    class IntegerType < AbstractType
      def self.cast(value)
        bytes_to_int(value)
      end
    end
    
    class Int32Type < IntegerType; end
  end
end