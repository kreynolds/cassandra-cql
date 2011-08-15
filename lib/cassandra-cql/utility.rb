require 'zlib'

module CassandraCQL
  class Utility
    def self.compress(source, level=2)
      Zlib::Deflate.deflate(source, level)
    end
    
    def self.decompress(source)
      Zlib::Inflate.inflate(source)
    end
  end
end