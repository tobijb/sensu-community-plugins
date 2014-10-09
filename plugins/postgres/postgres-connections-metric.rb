#!/usr/bin/env ruby
#
# Postgres Connection Metrics
# ===
#
# Dependencies
# -----------
# - Ruby gem `pg`
#
#
# Copyright 2012 Kwarter, Inc <platforms@kwarter.com>
# Author Gilles Devaux <gilles.devaux@gmail.com>
#
# Released under the same terms as Sensu (the MIT license); see LICENSE
# for details.
require 'rubygems' if RUBY_VERSION < '1.9.0'
require 'pg'
require 'socket'
require 'sensu-plugin/metric/cli'

class PostgresConnectionsMetric < Sensu::Plugin::Metric::CLI::Graphite

  option :user,
         :description => "Postgres User",
         :short       => '-u USER',
         :long        => '--user USER'

  option :password,
         :description => "Postgres Password",
         :short       => '-p PASS',
         :long        => '--password PASS'

  option :hostname,
         :description => "Hostname to login to",
         :short       => '-h HOST',
         :long        => '--hostname HOST',
         :default     => 'localhost'

  option :port,
         :description => "Database port",
         :short       => '-P PORT',
         :long        => '--port PORT',
         :default     => 5432

  option :db,
         :description => "Database name",
         :short       => '-d DB',
         :long        => '--db DB',
         :default     => 'all'

  option :scheme,
         :description => "Metric naming scheme, text to prepend to $queue_name.$metric",
         :long        => "--scheme SCHEME",
         :default     => "#{Socket.gethostname}.postgresql"

  TRUE_OR_FALSE = {
    't' => true,
    'f' => false
  }

  def run
    timestamp = Time.now.to_i

    # Required since we need a DB to connect to and one to filter results on...
    database_to_use = config[:db] == 'all' ? 'postgres' : config[:db]

    conn = PG::Connection.new(config[:hostname], config[:port], nil, nil, database_to_use, config[:user], config[:password])

    request = %Q(
      SELECT count(*), datname, waiting FROM pg_stat_activity 
      #{"WHERE datname = '#{database_to_use}'" if config[:db] != 'all'} 
      GROUP BY datname, waiting
    )

    total_connections = 0
    conn.exec(request) do |result|
      result.each do |row|
        total_connections += row['count'].to_i;
        output "#{config[:scheme]}.#{row['datname']}.connections.waiting.#{TRUE_OR_FALSE[row['waiting']]}", row['count'], timestamp
      end
      output "#{config[:scheme]}.connections.total", total_connections, timestamp
    end

    ok
  end
 
 end
 
 
 

 
 
 

 
 
 
 
