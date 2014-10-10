#!/usr/bin/env ruby
#
# Postgres Table Inserts, Updates, and Deletes
# ===
#
# Dependencies
# -----------
# - Ruby gem `pg`
#
#
#
# Released under the same terms as Sensu (the MIT license); see LICENSE
# for details.
require 'rubygems' if RUBY_VERSION < '1.9.0'
require 'pg'
require 'socket'
require 'sensu-plugin/metric/cli'

class PostgresMetricGraphite < Sensu::Plugin::Metric::CLI::Graphite

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

  option :scheme,
         :description => "Metric naming scheme, text to prepend to $queue_name.$metric",
         :long        => "--scheme SCHEME",
         :default     => "#{Socket.gethostname}.postgresql"

  def run
    timestamp = Time.now.to_i

    conn = PG::Connection.new(config[:hostname], config[:port], nil, nil, config[:db], config[:user], config[:password])

    request = %Q(
      SELECT
          schemaname || '.' || relname as "schema.table",
          n_tup_ins as number_of_tuples_inserted, 
          n_tup_upd as number_of_tuples_updated, 
          n_tup_del as number_of_tuples_deleted
      FROM pg_stat_user_tables;
    )

    output_columns = %w( number_of_tuples_inserted number_of_tuples_updated number_of_tuples_deleted)

    conn.exec(request) do |result|
      result.each do |row|

        output_columns.each do |column|
          output "#{config[:scheme]}.#{config[:db]}.#{row['schema.table']}.#{column}", row[column], timestamp
        end
      end
    end

    ok
  end
 
 end
 
