require 'bundler'
Bundler::GemHelper.install_tasks

require 'rake'
require 'rspec/core'
require 'rspec/core/rake_task'

CassandraBinaries = {
  '0.8' => 'http://archive.apache.org/dist/cassandra/0.8.8/apache-cassandra-0.8.8-bin.tar.gz',
  '1.0' => 'http://archive.apache.org/dist/cassandra/1.0.5/apache-cassandra-1.0.5-bin.tar.gz',
}

CASSANDRA_VERSION = ENV['CASSANDRA_VERSION'] || '1.0'
CASSANDRA_HOME = ENV['CASSANDRA_HOME'] || File.dirname(__FILE__) + '/tmp'
CASSANDRA_PIDFILE = ENV['CASSANDRA_PIDFILE'] || "#{CASSANDRA_HOME}/cassandra.pid"

RSpec::Core::RakeTask.new(:spec) do |spec|
  spec.pattern = FileList['spec/**/*_spec.rb']
end

RSpec::Core::RakeTask.new(:rcov) do |spec|
  spec.pattern = FileList['spec/**/*_spec.rb']
  spec.rcov = true
  spec.rcov_opts = "--exclude 'spec/*'"
end

desc "Download Cassandra and run specs against it"
task :spec_with_server do
  Rake::Task["cassandra:clean"].invoke
  Rake::Task["cassandra:start"].invoke
  error = nil
  begin
    Rake::Task["spec"].invoke
  rescue
    error = $!
  end
  Rake::Task["cassandra:stop"].invoke
  raise $! if $!
end

task :default => :spec

def setup_cassandra_version(version = CASSANDRA_VERSION)
  FileUtils.mkdir_p CASSANDRA_HOME

  destination_directory = File.join(CASSANDRA_HOME, 'cassandra-' + CASSANDRA_VERSION)

  unless File.exists?(File.join(destination_directory, 'bin','cassandra'))
    download_source       = CassandraBinaries[CASSANDRA_VERSION]
    download_destination  = File.join(CASSANDRA_HOME, File.basename(download_source))
    untar_directory       = File.join(CASSANDRA_HOME,  File.basename(download_source,'-bin.tar.gz'))

    puts "downloading cassandra"
    sh "curl -L -o #{download_destination} #{download_source}"

    sh "tar xzf #{download_destination} -C #{CASSANDRA_HOME}"
    sh "mv #{untar_directory} #{destination_directory}"
  end
end

def setup_environment
  env = ""
  if !ENV["CASSANDRA_INCLUDE"]
    env << "CASSANDRA_INCLUDE=#{File.expand_path(Dir.pwd)}/spec/conf/#{CASSANDRA_VERSION}/cassandra.in.sh "
    env << "CASSANDRA_HOME=#{CASSANDRA_HOME}/cassandra-#{CASSANDRA_VERSION} "
    env << "CASSANDRA_CONF=#{File.expand_path(Dir.pwd)}/spec/conf/#{CASSANDRA_VERSION}"
  else
    env << "CASSANDRA_INCLUDE=#{ENV['CASSANDRA_INCLUDE']} "
    env << "CASSANDRA_HOME=#{ENV['CASSANDRA_HOME']} "
    env << "CASSANDRA_CONF=#{ENV['CASSANDRA_CONF']}"
  end

  env
end

def running?(pid_file = nil)
  pid_file ||= CASSANDRA_PIDFILE

  if File.exists?(pid_file)
    pid = File.new(pid_file).read.to_i
    begin
      Process.kill(0, pid)
      return true
    rescue
      File.delete(pid_file)
    end
  end

  false
end

namespace :cassandra do
  desc "Start Cassandra"
  task :start, [:daemonize] => :java do |t, args|
    args.with_defaults(:daemonize => true)

    setup_cassandra_version

    env = setup_environment

    Dir.chdir(File.join(CASSANDRA_HOME, "cassandra-#{CASSANDRA_VERSION}")) do
      sh("env #{env} bin/cassandra #{'-f' unless args.daemonize} -p #{CASSANDRA_PIDFILE}")
    end
    $stdout.puts "Sleeping for 8 seconds to wait for Cassandra to start ..."
    sleep(8)
  end

  desc "Stop Cassandra"
  task :stop => :java do
    setup_cassandra_version
    env = setup_environment
    sh("kill $(cat #{CASSANDRA_PIDFILE})")
  end

  desc "Delete all data files in #{CASSANDRA_HOME}"
  task :clean do
    sh("rm -rf #{File.join(CASSANDRA_HOME, "cassandra-#{CASSANDRA_VERSION}", 'data')}")
  end

end

desc "Start Cassandra"
task :cassandra => :java do
  begin
    Rake::Task["cassandra:start"].invoke(false)
  rescue RuntimeError => e
    raise e unless e.message =~ /Command failed with status \(130\)/ # handle keyboard interupt errors
  end
end

desc "Run the Cassandra CLI"
task :cli do
  Dir.chdir(File.join(CASSANDRA_HOME, "cassandra-#{CASSANDRA_VERSION}")) do
    sh("bin/cassandra-cli -host localhost -port 9160")
  end
end

desc "Check Java version"
task :java do
  unless `java -version 2>&1`.split("\n").first =~ /java version "1.6/ #"
    puts "You need to configure your environment for Java 1.6."
    puts "If you're on OS X, just export the following environment variables:"
    puts '  JAVA_HOME="/System/Library/Frameworks/JavaVM.framework/Versions/1.6/Home"'
    puts '  PATH="/System/Library/Frameworks/JavaVM.framework/Versions/1.6/Home/bin:$PATH"'
    exit(1)
  end
end

require 'yard'
YARD::Rake::YardocTask.new
