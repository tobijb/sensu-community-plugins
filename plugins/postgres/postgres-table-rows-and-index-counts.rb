#!/usr/bin/env ruby
#
# Postgres Table Size, Rows, and Index Counts
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
          nspname || '.' || pg_class.relname as "schema.table",
          pg_class.reltuples::bigint AS total_rows,
          count(indexname) AS number_of_indexes,
          CASE WHEN x.is_unique IS NULL THEN 0 ELSE x.is_unique END as has_unique_indexes,
          SUM(case WHEN number_of_columns = 1 THEN 1
                    ELSE 0
                  END) AS single_column,
          SUM(case WHEN number_of_columns IS NULL THEN 0
                   WHEN number_of_columns = 1 THEN 0
                   ELSE 1
                 END) AS multi_column
      FROM pg_namespace 
      LEFT OUTER JOIN pg_class ON pg_namespace.oid = pg_class.relnamespace
      LEFT OUTER JOIN
             (SELECT indrelid,
                 max(CAST(indisunique AS integer)) AS is_unique
             FROM pg_index
             GROUP BY indrelid) x
             ON pg_class.oid = x.indrelid
      LEFT OUTER JOIN
          ( SELECT c.relname AS ctablename, ipg.relname AS indexname, x.indnatts AS number_of_columns FROM pg_index x
                 JOIN pg_class c ON c.oid = x.indrelid
                 JOIN pg_class ipg ON ipg.oid = x.indexrelid  )
          AS foo
          ON pg_class.relname = foo.ctablename
      WHERE 
           pg_namespace.nspname NOT IN ('pg_catalog', 'information_schema')
      AND  pg_class.relkind = 'r'
      GROUP BY nspname, pg_class.relname, pg_class.reltuples, x.is_unique
      ORDER BY 2;
    )

    output_columns = %w( total_rows number_of_indexes has_unique_indexes single_column multi_column )

    conn.exec(request) do |result|
      result.each do |row|

        output_columns.each do |column|
          output "#{config[:scheme]}.#{row['schema.table']}.#{column}", row[column], timestamp
        end
      end
    end

    ok
  end
 
 end
 
