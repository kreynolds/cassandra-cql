module CassandraCQL
  module Types
    class DateType < AbstractType
      def self.cast(value)
        Time.at(bytes_to_long(value) / 1000.0)
      end
    end
  end
end
