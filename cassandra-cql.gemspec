# -*- encoding: utf-8 -*-
require File.expand_path("../lib/cassandra-cql/version", __FILE__)

Gem::Specification.new do |s|
  s.name        = "cassandra-cql"
  s.version     = CassandraCQL::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Kelley Reynolds"]
  s.email       = ["kelley@insidesystems.net"]
  s.homepage    = "http://rubygems.org/gems/cassandra-cql"
  s.summary     = "CQL Interface to Cassandra"
  s.description = "CQL Interface to Cassandra"

  s.required_rubygems_version = ">= 1.3.6"
  s.rubyforge_project         = "cassandra-cql"

  s.add_development_dependency "bundler", ">= 1.0.0"
  s.add_development_dependency "rcov", ">= 0.9.9"
  s.add_development_dependency "rspec", ">= 2.6.0"
  s.add_development_dependency "rake", ">= 0.9.2"
  s.add_dependency "simple_uuid", ">= 0.1.1"
  s.add_dependency "thrift_client", ">= 0.6.3"

  s.files        = `git ls-files`.split("\n")
  s.test_files   = `git ls-files -- {spec}/*`.split("\n")
  s.executables  = `git ls-files`.split("\n").map{|f| f =~ /^bin\/(.*)/ ? $1 : nil}.compact
  s.require_path = 'lib'
end
