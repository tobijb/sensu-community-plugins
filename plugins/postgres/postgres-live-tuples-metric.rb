#!/usr/bin/env ruby
#
# Postgres Approximate Row Count Metrics
# ===
#
# Dependencies
# -----------
# - Ruby gem `pg`
#
# Template for this script grabbed from:
#   Copyright 2012 Kwarter, Inc <platforms@kwarter.com>
#   Author Gilles Devaux <gilles.devaux@gmail.com>
#
# Author: Josh Brown <josh.brown@tobi.com>
#
# Released under the same terms as Sensu (the MIT license); see LICENSE
# for details.

require 'rubygems' if RUBY_VERSION < '1.9.0'
require 'sensu-plugin/metric/cli'
require 'pg'
require 'socket'

class PostgresStatsDBMetrics < Sensu::Plugin::Metric::CLI::Graphite

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
         :default     => 'postgres'

  option :schema,
         :description => "Schema name",
         :long        => '--schema SCHEMA'

  option :scheme,
         :description => "Metric naming scheme, text to prepend to $queue_name.$metric",
         :long        => "--scheme SCHEME",
         :default     => "#{Socket.gethostname}.postgresql"

  def run
    timestamp = Time.now.to_i

    conn = PG::Connection.new(config[:hostname], config[:port], nil, nil, config[:db], config[:user], config[:password])

    where_clause = config[:schema] ? "WHERE schemaname = '#{config[:schema]}'" : ""
    request = "SELECT schemaname, relname, n_live_tup FROM pg_stat_user_tables #{where_clause}"

    conn.exec(request) do |result|
      result.each do |row|
        output "#{config[:scheme]}.#{config[:db]}.#{row['schemaname']}.#{row['relname']}.n_live_tup", row['n_live_tup'], timestamp
      end
    end

    ok

  end

end
