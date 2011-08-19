require 'zlib'

module CassandraCQL
  class Utility
    def self.compress(source, level=2)
      Zlib::Deflate.deflate(source, level)
    end
    
    def self.decompress(source)
      Zlib::Inflate.inflate(source)
    end

    def self.binary_data?(string)
      ( string.count( "^ -~", "^\r\n" ).fdiv(string.size) > 0.3 || string.index( "\x00" ) ) unless string.empty?
    end
  end
end