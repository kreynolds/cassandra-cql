module CassandraCQL
  module Types
    class UTF8Type < AbstractType
      def self.cast(value)
        RUBY_VERSION >= "1.9" ? value.to_s.dup.force_encoding('UTF-8') : value.to_s.dup
      end
    end
  end
end
