module CassandraCQL
  module Types
    class DoubleType < AbstractType
      def self.cast(value)
        value.unpack('G')[0]
      end
    end
  end
end