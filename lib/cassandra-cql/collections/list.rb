module CassandraCQL
  module Collections
    class List
      def self.cast(value)
        length = value.unpack('S>').first
        pos = 2
        Array.new(length) do
          value_length = value.byteslice(pos, 2).unpack('S>').first
          pos += 2
          element = value.byteslice(pos, value_length)
          pos += value_length
          yield element
        end
      end
    end
  end
end
