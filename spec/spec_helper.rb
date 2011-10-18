require 'yaml'
require 'rspec'
$LOAD_PATH << "#{File.expand_path(File.dirname(__FILE__))}/../lib"
require 'cassandra-cql'

def yaml_fixture(file)
  if file.kind_of?(Symbol)
    file = "#{file}.yaml"
  elsif file !~ /\.yaml$/
    file = "#{file}.yaml"
  end
  YAML::load_file(File.dirname(__FILE__) + "/fixtures/#{file}")
end