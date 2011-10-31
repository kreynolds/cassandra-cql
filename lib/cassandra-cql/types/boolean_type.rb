module CassandraCQL
  module Types
    class BooleanType < AbstractType
      def self.cast(value)
        value.unpack('C') == [1]
      end
    end
  end
end
