module CassandraCQL
  module Types
    class UUIDType < AbstractType
      def self.cast(value)
        UUID.new(value)
      end
    end
    
    class TimeUUIDType < UUIDType; end
  end
end