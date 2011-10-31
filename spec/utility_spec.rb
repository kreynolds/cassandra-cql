# encoding: utf-8
require File.expand_path('spec_helper.rb', File.dirname(__FILE__))
include CassandraCQL

describe "compress" do
  it "should return some valid gzipped stuff" do
    stuff = "This is some stuff"
    bytes = Utility.compress(stuff)
    Utility.binary_data?(bytes).should be_true
    Utility.decompress(bytes).should eq(stuff)
  end
  
  it "should be binary data" do
    if RUBY_VERSION >= "1.9"
      Utility.binary_data?("binary\x00".force_encoding('ASCII-8BIT')).should be_true
    else
      Utility.binary_data?("binary\x00").should be_true
    end
  end

  it "should not be binary data" do
    Utility.binary_data?("test").should_not be_true
    Utility.binary_data?("snårk").should_not be_true
  end
end

