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
    class DoubleType < AbstractType
      def self.cast(value)
        # Do not return nil, this should be an error
        raise if value.empty?

        value.unpack('G')[0]
      rescue
        raise Error::CastException.new("Unable to convert bytes to double", value)
      end
    end
  end
end