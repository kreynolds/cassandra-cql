module CassandraCQL
  module Types
    class DecimalType < AbstractType
      def self.cast(value)
        BigDecimal.new(bytes_to_int(value[4..-1]).to_s) * BigDecimal.new('10')**(bytes_to_int(value[0..3])*-1)
      end
    end
  end
end
