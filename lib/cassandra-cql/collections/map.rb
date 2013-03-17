module CassandraCQL
  module Collections
    class Map
      def self.cast(value)
        length = value.unpack('S>').first
        pos = 2
        result = {}
        length.times do
          key_length = value.byteslice(pos, 2).unpack('S>').first
          pos += 2
          key = value.byteslice(pos, key_length)
          pos += key_length

          val_length = value.byteslice(pos, 2).unpack('S>').first
          pos += 2
          val = value.byteslice(pos, val_length)
          pos += val_length
          cast_key, cast_val = yield key, val
          result[cast_key] = cast_val
        end
        result
      end
    end
  end
end

