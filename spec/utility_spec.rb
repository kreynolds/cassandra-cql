require File.expand_path('spec_helper.rb', File.dirname(__FILE__))
include CassandraCQL

describe "compress" do
  it "should return some valid gzipped stuff" do
    stuff = "This is some stuff"
    bytes = Utility.compress(stuff)
    Utility.binary_data?(bytes).should be_true
    Utility.decompress(bytes).should eq(stuff)
  end
end

