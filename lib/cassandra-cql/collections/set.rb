module CassandraCQL
  module Collections
    class Set < List
      def self.cast(value)
        super.to_set
      end
    end
  end
end

