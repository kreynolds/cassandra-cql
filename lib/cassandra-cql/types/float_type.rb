module CassandraCQL
  module Types
    class FloatType < AbstractType
      def self.cast(value)
        value.unpack('g')[0]
      end
    end
  end
end