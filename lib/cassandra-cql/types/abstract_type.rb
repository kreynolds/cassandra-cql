module CassandraCQL
  module Types
    class AbstractType
      def self.cast(value)
        value
      end

      private
      
      def self.bytes_to_int(bytes)
        int = 0
        values = bytes.unpack('C*')
        values.each {|v| int = int << 8; int += v; }
        if bytes[0].ord & 128 != 0
          int = int - (1 << bytes.length * 8)
        end
        int
      end

      def self.bytes_to_long(bytes)
        ints = bytes.unpack("NN")
        val = (ints[0] << 32) + ints[1]
        if val & 2**63 == 2**63
          val - 2**64
        else
          val
        end
      end
    end
  end
end