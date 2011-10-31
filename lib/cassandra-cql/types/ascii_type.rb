module CassandraCQL
  module Types
    class AsciiType < AbstractType
      def self.cast(value)
        RUBY_VERSION >= "1.9" ? value.to_s.dup.force_encoding('ASCII-8BIT') : value.to_s.dup
      end
    end
  end
end
