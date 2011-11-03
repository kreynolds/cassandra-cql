=begin
Copyright 2011 Inside Systems, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
=end

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